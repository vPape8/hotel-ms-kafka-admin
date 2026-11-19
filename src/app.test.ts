import assert from 'node:assert/strict';
import { AddressInfo } from 'node:net';
import test from 'node:test';
import { crearApp } from './app';
import { CrearTopicoDto } from './dto/topicoDto';
import {
	ConflictoKafkaError,
	GrupoDetalle,
	KafkaAdmin,
	RecursoNoEncontradoError,
	RecursoProtegidoError,
	TopicoDetalle
} from './services/kafkaAdminService';

function detalle(nombre: string): TopicoDetalle {
	return { name: nombre, partitions: 3, replicationFactor: 3, internal: false, protected: false, partitionsDetail: [], configs: [] };
}

class KafkaAdminFalso implements KafkaAdmin {
	creados: CrearTopicoDto[] = [];
	configs: Record<string, string>[] = [];
	resets: string[] = [];

	async cluster() { return { clusterId: 'c1', controller: 1, brokers: [1, 2, 3].map((nodeId) => ({ nodeId, host: `kafka-${nodeId}`, port: 29092 })) }; }
	async listarTopicos() { return [{ name: 'reservas', partitions: 3, replicationFactor: 3, internal: false, protected: true }]; }
	async detalleTopico(nombre: string) {
		if (nombre === 'no-existe') throw new RecursoNoEncontradoError('no existe');
		return detalle(nombre);
	}
	async crearTopico(dto: CrearTopicoDto) {
		if (dto.name === 'reservas') throw new ConflictoKafkaError('ya existe');
		this.creados.push(dto);
		return detalle(dto.name);
	}
	async modificarConfigs(nombre: string, configs: Record<string, string>) { this.configs.push(configs); return detalle(nombre); }
	async aumentarParticiones(nombre: string) { return detalle(nombre); }
	async eliminarTopico(nombre: string) {
		if (nombre === 'reservas') throw new RecursoProtegidoError('protegido');
	}
	async listarGrupos() { return [{ groupId: 'reportes', state: 'Stable', members: 3 }]; }
	async detalleGrupo(groupId: string): Promise<GrupoDetalle> {
		return { groupId, state: 'Empty', members: 0, membersDetail: [], partitions: [], totalLag: 0 };
	}
	async resetearOffsets(groupId: string, topic: string, destino: string) {
		if (groupId === 'reportes') throw new ConflictoKafkaError('tiene miembros activos');
		this.resets.push(`${groupId}:${topic}:${destino}`);
		return this.detalleGrupo(groupId);
	}
}

test('API del administrador de Kafka', async (t) => {
	const servicio = new KafkaAdminFalso();
	const servidor = crearApp(servicio, (_solicitud, _respuesta, siguiente) => siguiente()).listen(0);
	await new Promise<void>((resolve) => servidor.once('listening', resolve));
	t.after(() => servidor.close());
	const base = `http://127.0.0.1:${(servidor.address() as AddressInfo).port}/api/v1`;
	const json = (metodo: string, cuerpo: unknown) => ({ method: metodo, headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(cuerpo) });

	await t.test('crea un tópico con sus configuraciones normalizadas a texto', async () => {
		const respuesta = await fetch(`${base}/topics`, json('POST', {
			name: 'pagos', partitions: 3, replicationFactor: 3,
			configs: { 'cleanup.policy': 'delete', 'retention.ms': 86400000, 'min.insync.replicas': '2' }
		}));
		assert.equal(respuesta.status, 201);
		assert.deepEqual(servicio.creados[0]?.configs, { 'cleanup.policy': 'delete', 'retention.ms': '86400000', 'min.insync.replicas': '2' });
	});

	await t.test('rechaza nombres, particiones y configuraciones inválidas', async () => {
		const casos: unknown[] = [
			{ name: '', partitions: 3, replicationFactor: 3 },
			{ name: '__consumer_offsets', partitions: 3, replicationFactor: 3 },
			{ name: 'con espacios', partitions: 3, replicationFactor: 3 },
			{ name: 'ok', partitions: 0, replicationFactor: 3 },
			{ name: 'ok', partitions: 3, replicationFactor: 3, configs: { 'cleanup.policy': 'borrar' } },
			{ name: 'ok', partitions: 3, replicationFactor: 3, configs: { 'retention.ms': '10' } },
			{ name: 'ok', partitions: 3, replicationFactor: 3, configs: { 'unclean.leader.election.enable': 'true' } },
			{ name: 'ok', partitions: 3, replicationFactor: 2, configs: { 'min.insync.replicas': '3' } },
			{ name: 'ok', partitions: 3, replicationFactor: 3, extra: 1 }
		];
		for (const cuerpo of casos) {
			const respuesta = await fetch(`${base}/topics`, json('POST', cuerpo));
			assert.equal(respuesta.status, 400, JSON.stringify(cuerpo));
			assert.equal(((await respuesta.json()) as { error: string }).error, 'VALIDACION');
		}
	});

	await t.test('modifica configuraciones y aumenta particiones con validación', async () => {
		assert.equal((await fetch(`${base}/topics/reservas/configs`, json('PATCH', { configs: { 'retention.ms': '1209600000' } }))).status, 200);
		assert.deepEqual(servicio.configs[0], { 'retention.ms': '1209600000' });
		assert.equal((await fetch(`${base}/topics/reservas/configs`, json('PATCH', { configs: {} }))).status, 400);
		assert.equal((await fetch(`${base}/topics/reservas/partitions`, json('POST', { count: 6 }))).status, 200);
		assert.equal((await fetch(`${base}/topics/reservas/partitions`, json('POST', { count: 1 }))).status, 400);
	});

	await t.test('traduce errores del servicio a códigos HTTP', async () => {
		assert.equal((await fetch(`${base}/topics`, json('POST', { name: 'reservas', partitions: 3, replicationFactor: 3 }))).status, 409);
		assert.equal((await fetch(`${base}/topics/no-existe`)).status, 404);
		assert.equal((await fetch(`${base}/topics/reservas`, { method: 'DELETE' })).status, 409);
		assert.equal((await fetch(`${base}/topics/pagos`, { method: 'DELETE' })).status, 204);
	});

	await t.test('consumer groups: listar, detalle y reset de offsets', async () => {
		assert.equal((await fetch(`${base}/consumer-groups`)).status, 200);
		assert.equal((await fetch(`${base}/consumer-groups/auditoria`)).status, 200);
		assert.equal((await fetch(`${base}/consumer-groups/reportes/offsets/reset`, json('POST', { topic: 'reservas', to: 'earliest' }))).status, 409);
		assert.equal((await fetch(`${base}/consumer-groups/auditoria/offsets/reset`, json('POST', { topic: 'reservas', to: 'principio' }))).status, 400);
		assert.equal((await fetch(`${base}/consumer-groups/auditoria/offsets/reset`, json('POST', { topic: 'reservas', to: 'earliest' }))).status, 200);
		assert.deepEqual(servicio.resets, ['auditoria:reservas:earliest']);
	});
});
