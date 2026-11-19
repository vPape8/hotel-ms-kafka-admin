import { Admin, AssignerProtocol, ConfigResourceTypes, ConfigSource, Kafka, logLevel } from 'kafkajs';
import { esTopicoInterno, kafkaAdminConfig } from '../config/kafka.config';
import { DestinoReset } from '../dto/consumerGroupDto';
import { AumentarParticionesDto, CrearTopicoDto } from '../dto/topicoDto';
import { CONFIGS_PERMITIDAS, ErrorValidacion } from '../dto/validacion';

export class RecursoNoEncontradoError extends Error {}
export class ConflictoKafkaError extends Error {}
export class RecursoProtegidoError extends Error {}
export class KafkaNoDisponibleError extends Error {}
export class ValidacionKafkaError extends Error {
	constructor(readonly detalles: ErrorValidacion[]) {
		super('Configuración no válida para este clúster');
	}
}

export interface ClusterInfo {
	clusterId: string;
	controller: number | null;
	brokers: { nodeId: number; host: string; port: number }[];
}

export interface TopicoResumen {
	name: string;
	partitions: number;
	replicationFactor: number;
	internal: boolean;
	protected: boolean;
}

export interface ParticionInfo {
	partition: number;
	leader: number;
	replicas: number[];
	isr: number[];
	/** Primer y siguiente offset disponibles: mensajes guardados = high - low. */
	lowOffset: string;
	highOffset: string;
}

export interface TopicoDetalle extends TopicoResumen {
	partitionsDetail: ParticionInfo[];
	/** Configuraciones relevantes con su valor efectivo; "isDefault" indica si viene del broker. */
	configs: { name: string; value: string; isDefault: boolean }[];
}

export interface GrupoResumen {
	groupId: string;
	state: string;
	members: number;
}

export interface ParticionGrupo {
	topic: string;
	partition: number;
	/** Marca del grupo (-1 si todavía no confirmó ninguna). */
	currentOffset: string;
	logEndOffset: string;
	lag: number;
	/** Miembro que tiene asignada la partición (clientId@host), o null si el grupo está detenido. */
	member: string | null;
}

export interface GrupoDetalle extends GrupoResumen {
	membersDetail: { memberId: string; clientId: string; host: string; assignment: { topic: string; partitions: number[] }[] }[];
	partitions: ParticionGrupo[];
	totalLag: number;
}

/** Operaciones de alto nivel que usan los controladores: no saben nada de kafkajs. */
export interface KafkaAdmin {
	cluster(): Promise<ClusterInfo>;
	listarTopicos(): Promise<TopicoResumen[]>;
	detalleTopico(nombre: string): Promise<TopicoDetalle>;
	crearTopico(dto: CrearTopicoDto): Promise<TopicoDetalle>;
	modificarConfigs(nombre: string, configs: Record<string, string>): Promise<TopicoDetalle>;
	aumentarParticiones(nombre: string, dto: AumentarParticionesDto): Promise<TopicoDetalle>;
	eliminarTopico(nombre: string): Promise<void>;
	listarGrupos(): Promise<GrupoResumen[]>;
	detalleGrupo(groupId: string): Promise<GrupoDetalle>;
	resetearOffsets(groupId: string, topic: string, destino: DestinoReset): Promise<GrupoDetalle>;
}

const ERRORES_DE_CONEXION = ['KafkaJSConnectionError', 'KafkaJSNumberOfRetriesExceeded', 'KafkaJSBrokerNotFound', 'KafkaJSRequestTimeoutError'];
const ERRORES_DE_VALIDACION = ['INVALID_REPLICATION_FACTOR', 'INVALID_PARTITIONS', 'INVALID_CONFIG', 'INVALID_REQUEST', 'POLICY_VIOLATION', 'INVALID_REPLICA_ASSIGNMENT'];

/** Traduce los errores de kafkajs a errores de la API. */
function traducirError(error: unknown): Error {
	if (error instanceof RecursoNoEncontradoError || error instanceof ConflictoKafkaError
		|| error instanceof RecursoProtegidoError || error instanceof ValidacionKafkaError || error instanceof KafkaNoDisponibleError) {
		return error;
	}
	const { name, type, message } = error as { name?: string; type?: string; message?: string };
	const mensaje = message ?? String(error);
	if (type === 'UNKNOWN_TOPIC_OR_PARTITION' || type === 'GROUP_ID_NOT_FOUND') return new RecursoNoEncontradoError(mensaje);
	if (type === 'TOPIC_ALREADY_EXISTS' || type === 'NON_EMPTY_GROUP') return new ConflictoKafkaError(mensaje);
	if (type && ERRORES_DE_VALIDACION.includes(type)) return new ValidacionKafkaError([{ campo: 'kafka', mensaje }]);
	if (name && ERRORES_DE_CONEXION.includes(name)) return new KafkaNoDisponibleError(mensaje);
	return new KafkaNoDisponibleError(mensaje);
}

function esProtegido(nombre: string): boolean {
	return esTopicoInterno(nombre) || kafkaAdminConfig.protegidos.includes(nombre);
}

export class KafkaAdminService implements KafkaAdmin {
	private readonly kafka: Kafka;
	private admin?: Promise<Admin>;

	/** crearAdmin permite usar un cliente falso en los tests; en producción se usa el de kafkajs. */
	constructor(brokers: string[], private readonly crearAdmin?: () => Admin) {
		this.kafka = new Kafka({ clientId: kafkaAdminConfig.clientId, brokers, logLevel: logLevel.WARN, retry: { retries: 3 } });
	}

	async cerrar(): Promise<void> {
		const admin = await this.admin?.catch(() => undefined);
		this.admin = undefined;
		await admin?.disconnect().catch(() => undefined);
	}

	// ---------- Clúster ----------

	async cluster(): Promise<ClusterInfo> {
		return this.conAdmin((admin) => admin.describeCluster());
	}

	// ---------- Tópicos ----------

	async listarTopicos(): Promise<TopicoResumen[]> {
		return this.conAdmin(async (admin) => {
			const nombres = await admin.listTopics();
			const { topics } = await admin.fetchTopicMetadata({ topics: nombres });
			return topics
				.map((topico) => ({
					name: topico.name,
					partitions: topico.partitions.length,
					replicationFactor: topico.partitions[0]?.replicas.length ?? 0,
					internal: esTopicoInterno(topico.name),
					protected: esProtegido(topico.name)
				}))
				.sort((a, b) => Number(a.internal) - Number(b.internal) || a.name.localeCompare(b.name));
		});
	}

	async detalleTopico(nombre: string): Promise<TopicoDetalle> {
		return this.conAdmin(async (admin) => {
			if (!(await admin.listTopics()).includes(nombre)) {
				throw new RecursoNoEncontradoError(`El tópico ${nombre} no existe`);
			}
			const [{ topics }, offsets, configs] = await Promise.all([
				admin.fetchTopicMetadata({ topics: [nombre] }),
				admin.fetchTopicOffsets(nombre),
				admin.describeConfigs({ includeSynonyms: false, resources: [{ type: ConfigResourceTypes.TOPIC, name: nombre }] })
			]);
			const particiones = topics[0]?.partitions ?? [];
			return {
				name: nombre,
				partitions: particiones.length,
				replicationFactor: particiones[0]?.replicas.length ?? 0,
				internal: esTopicoInterno(nombre),
				protected: esProtegido(nombre),
				partitionsDetail: particiones
					.map((particion) => {
						const offset = offsets.find((valor) => valor.partition === particion.partitionId);
						return {
							partition: particion.partitionId,
							leader: particion.leader,
							replicas: particion.replicas,
							isr: particion.isr,
							lowOffset: offset?.low ?? '0',
							highOffset: offset?.high ?? '0'
						};
					})
					.sort((a, b) => a.partition - b.partition),
				configs: (configs.resources[0]?.configEntries ?? [])
					.filter((entrada) => entrada.configName in CONFIGS_PERMITIDAS)
					.map((entrada) => ({ name: entrada.configName, value: entrada.configValue, isDefault: entrada.isDefault }))
			};
		});
	}

	async crearTopico(dto: CrearTopicoDto): Promise<TopicoDetalle> {
		await this.conAdmin(async (admin) => {
			await this.verificarReplicacion(admin, dto.replicationFactor);
			if ((await admin.listTopics()).includes(dto.name)) {
				throw new ConflictoKafkaError(`El tópico ${dto.name} ya existe`);
			}
			await admin.createTopics({
				waitForLeaders: true,
				topics: [{
					topic: dto.name,
					numPartitions: dto.partitions,
					replicationFactor: dto.replicationFactor,
					configEntries: Object.entries(dto.configs).map(([name, value]) => ({ name, value }))
				}]
			});
		});
		return this.detalleTopico(dto.name);
	}

	/**
	 * alterConfigs de Kafka REEMPLAZA todas las configuraciones propias del tópico por las enviadas:
	 * por eso primero se leen las actuales (las que no vienen del broker) y se mezclan con las nuevas.
	 */
	async modificarConfigs(nombre: string, nuevas: Record<string, string>): Promise<TopicoDetalle> {
		await this.conAdmin(async (admin) => {
			if (!(await admin.listTopics()).includes(nombre)) {
				throw new RecursoNoEncontradoError(`El tópico ${nombre} no existe`);
			}
			if (nuevas['min.insync.replicas']) {
				const { topics } = await admin.fetchTopicMetadata({ topics: [nombre] });
				const replicas = topics[0]?.partitions[0]?.replicas.length ?? 0;
				if (Number(nuevas['min.insync.replicas']) > replicas) {
					throw new ValidacionKafkaError([{ campo: 'configs.min.insync.replicas', mensaje: `No puede superar la replicación del tópico (${replicas})` }]);
				}
			}
			const actuales = await admin.describeConfigs({ includeSynonyms: false, resources: [{ type: ConfigResourceTypes.TOPIC, name: nombre }] });
			const propias = Object.fromEntries((actuales.resources[0]?.configEntries ?? [])
				.filter((entrada) => entrada.configSource === ConfigSource.TOPIC_CONFIG)
				.map((entrada) => [entrada.configName, entrada.configValue]));
			const mezcladas = { ...propias, ...nuevas };
			await admin.alterConfigs({
				validateOnly: false,
				resources: [{
					type: ConfigResourceTypes.TOPIC,
					name: nombre,
					configEntries: Object.entries(mezcladas).map(([name, value]) => ({ name, value }))
				}]
			});
		});
		return this.detalleTopico(nombre);
	}

	async aumentarParticiones(nombre: string, dto: AumentarParticionesDto): Promise<TopicoDetalle> {
		await this.conAdmin(async (admin) => {
			if (esTopicoInterno(nombre)) {
				throw new RecursoProtegidoError(`${nombre} es un tópico interno de Kafka`);
			}
			if (!(await admin.listTopics()).includes(nombre)) {
				throw new RecursoNoEncontradoError(`El tópico ${nombre} no existe`);
			}
			const { topics } = await admin.fetchTopicMetadata({ topics: [nombre] });
			const actuales = topics[0]?.partitions.length ?? 0;
			if (dto.count <= actuales) {
				throw new ValidacionKafkaError([{ campo: 'count', mensaje: `Kafka solo permite aumentar particiones: el tópico ya tiene ${actuales}` }]);
			}
			await admin.createPartitions({ topicPartitions: [{ topic: nombre, count: dto.count }] });
		});
		return this.detalleTopico(nombre);
	}

	async eliminarTopico(nombre: string): Promise<void> {
		if (esProtegido(nombre)) {
			throw new RecursoProtegidoError(`El tópico ${nombre} lo usan los microservicios del sistema y no se puede eliminar`);
		}
		await this.conAdmin(async (admin) => {
			if (!(await admin.listTopics()).includes(nombre)) {
				throw new RecursoNoEncontradoError(`El tópico ${nombre} no existe`);
			}
			await admin.deleteTopics({ topics: [nombre] });
		});
	}

	// ---------- Consumer groups ----------

	async listarGrupos(): Promise<GrupoResumen[]> {
		return this.conAdmin(async (admin) => {
			const { groups } = await admin.listGroups();
			const ids = groups.filter((grupo) => grupo.protocolType === 'consumer' || grupo.protocolType === '').map((grupo) => grupo.groupId);
			if (ids.length === 0) return [];
			const descripcion = await admin.describeGroups(ids);
			return descripcion.groups
				.map((grupo) => ({ groupId: grupo.groupId, state: grupo.state, members: grupo.members.length }))
				.sort((a, b) => a.groupId.localeCompare(b.groupId));
		});
	}

	/** Lo mismo que "kafka-consumer-groups --describe --members": miembros, particiones asignadas, offsets y lag. */
	async detalleGrupo(groupId: string): Promise<GrupoDetalle> {
		return this.conAdmin(async (admin) => {
			const { groups } = await admin.describeGroups([groupId]);
			const grupo = groups[0];
			if (!grupo || grupo.state === 'Dead') {
				throw new RecursoNoEncontradoError(`El consumer group ${groupId} no existe`);
			}

			const miembros = grupo.members.map((miembro) => {
				const asignacion = miembro.memberAssignment.length > 0
					? AssignerProtocol.MemberAssignment.decode(miembro.memberAssignment)?.assignment ?? {}
					: {};
				return {
					memberId: miembro.memberId,
					clientId: miembro.clientId,
					host: miembro.clientHost,
					assignment: Object.entries(asignacion).map(([topic, partitions]) => ({ topic, partitions: [...partitions].sort((a, b) => a - b) }))
				};
			});
			const duenoDe = (topic: string, partition: number) => {
				const miembro = miembros.find((valor) => valor.assignment.some((a) => a.topic === topic && a.partitions.includes(partition)));
				return miembro ? `${miembro.clientId}@${miembro.host}` : null;
			};

			const offsetsGrupo = await admin.fetchOffsets({ groupId });
			const particiones: ParticionGrupo[] = [];
			for (const { topic, partitions } of offsetsGrupo) {
				const finales = await admin.fetchTopicOffsets(topic);
				for (const { partition, offset } of partitions) {
					const final = finales.find((valor) => valor.partition === partition);
					const logEnd = Number(final?.high ?? 0);
					// Sin marca guardada, el grupo leería desde el principio de lo que queda (low).
					const desde = Number(offset) >= 0 ? Number(offset) : Number(final?.low ?? 0);
					particiones.push({
						topic,
						partition,
						currentOffset: offset,
						logEndOffset: final?.high ?? '0',
						lag: Math.max(0, logEnd - desde),
						member: duenoDe(topic, partition)
					});
				}
			}
			particiones.sort((a, b) => a.topic.localeCompare(b.topic) || a.partition - b.partition);

			return {
				groupId,
				state: grupo.state,
				members: miembros.length,
				membersDetail: miembros,
				partitions: particiones,
				totalLag: particiones.reduce((suma, particion) => suma + particion.lag, 0)
			};
		});
	}

	/** Mueve la marca del grupo al inicio o al final del tópico. Kafka exige que el grupo no tenga miembros activos. */
	async resetearOffsets(groupId: string, topic: string, destino: DestinoReset): Promise<GrupoDetalle> {
		await this.conAdmin(async (admin) => {
			const { groups } = await admin.describeGroups([groupId]);
			const grupo = groups[0];
			if (!grupo || grupo.state === 'Dead') {
				throw new RecursoNoEncontradoError(`El consumer group ${groupId} no existe`);
			}
			if (grupo.state !== 'Empty') {
				throw new ConflictoKafkaError(`El grupo ${groupId} tiene ${grupo.members.length} miembro(s) activo(s) (estado ${grupo.state}): deténganlos antes de mover sus offsets`);
			}
			if (!(await admin.listTopics()).includes(topic)) {
				throw new RecursoNoEncontradoError(`El tópico ${topic} no existe`);
			}
			await admin.resetOffsets({ groupId, topic, earliest: destino === 'earliest' });
		});
		return this.detalleGrupo(groupId);
	}

	// ---------- Infraestructura ----------

	private async verificarReplicacion(admin: Admin, replicacion: number): Promise<void> {
		const { brokers } = await admin.describeCluster();
		if (replicacion > brokers.length) {
			throw new ValidacionKafkaError([{ campo: 'replicationFactor', mensaje: `El clúster tiene ${brokers.length} broker(s): la replicación no puede ser mayor` }]);
		}
	}

	private obtenerAdmin(): Promise<Admin> {
		this.admin ??= (async () => {
			const admin = this.crearAdmin?.() ?? this.kafka.admin();
			await admin.connect();
			admin.on(admin.events.DISCONNECT, () => {
				this.admin = undefined;
			});
			return admin;
		})().catch((error: unknown) => {
			this.admin = undefined;
			throw new KafkaNoDisponibleError(error instanceof Error ? error.message : String(error));
		});
		return this.admin;
	}

	private async conAdmin<T>(operacion: (admin: Admin) => Promise<T>): Promise<T> {
		const admin = await this.obtenerAdmin();
		try {
			return await operacion(admin);
		} catch (error) {
			const traducido = traducirError(error);
			if (traducido instanceof KafkaNoDisponibleError) {
				// La conexión pudo quedar mala: la próxima petición se reconecta.
				this.admin = undefined;
				await admin.disconnect().catch(() => undefined);
			}
			throw traducido;
		}
	}
}
