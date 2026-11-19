import assert from 'node:assert/strict';
import test from 'node:test';
import { Admin, AssignerProtocol, ConfigSource } from 'kafkajs';
import { ConflictoKafkaError, KafkaAdminService, RecursoProtegidoError, ValidacionKafkaError } from './kafkaAdminService';

/** Cliente admin falso con el mínimo que usa el servicio. */
function adminFalso(extra: Partial<Record<keyof Admin, unknown>> = {}) {
	const llamadas: { alterConfigs?: unknown; resetOffsets?: unknown } = {};
	const admin = {
		connect: async () => {},
		disconnect: async () => {},
		on: () => () => {},
		events: { DISCONNECT: 'admin.disconnect' },
		listTopics: async () => ['reservas', '__consumer_offsets'],
		describeCluster: async () => ({ clusterId: 'c1', controller: 1, brokers: [1, 2, 3].map((nodeId) => ({ nodeId, host: 'k', port: 1 })) }),
		fetchTopicMetadata: async () => ({ topics: [{ name: 'reservas', partitions: [0, 1, 2].map((partitionId) => ({ partitionId, leader: partitionId + 1, replicas: [1, 2, 3], isr: [1, 2, 3], partitionErrorCode: 0 })) }] }),
		fetchTopicOffsets: async () => [
			{ partition: 0, offset: '10', high: '10', low: '0' },
			{ partition: 1, offset: '7', high: '7', low: '0' },
			{ partition: 2, offset: '4', high: '4', low: '0' }
		],
		describeConfigs: async () => ({
			resources: [{ configEntries: [
				{ configName: 'cleanup.policy', configValue: 'delete', isDefault: false, configSource: ConfigSource.TOPIC_CONFIG },
				{ configName: 'retention.ms', configValue: '604800000', isDefault: false, configSource: ConfigSource.TOPIC_CONFIG },
				{ configName: 'min.insync.replicas', configValue: '2', isDefault: false, configSource: ConfigSource.TOPIC_CONFIG },
				{ configName: 'segment.ms', configValue: '604800000', isDefault: true, configSource: ConfigSource.DEFAULT_CONFIG }
			] }]
		}),
		alterConfigs: async (opciones: unknown) => { llamadas.alterConfigs = opciones; },
		describeGroups: async (ids: string[]) => ({
			groups: ids.map((groupId) => groupId === 'reportes'
				? {
					groupId, state: 'Stable', protocol: 'RoundRobin', protocolType: 'consumer',
					members: [0, 1, 2].map((particion) => ({
						memberId: `m${particion}`, clientId: 'hotel-ms-reportes', clientHost: `/10.0.0.${particion}`,
						memberMetadata: Buffer.alloc(0),
						memberAssignment: AssignerProtocol.MemberAssignment.encode({ version: 0, assignment: { reservas: [particion] }, userData: Buffer.alloc(0) })
					}))
				}
				: { groupId, state: groupId === 'auditoria' ? 'Empty' : 'Dead', protocol: '', protocolType: 'consumer', members: [] })
		}),
		fetchOffsets: async () => [{ topic: 'reservas', partitions: [{ partition: 0, offset: '10' }, { partition: 1, offset: '5' }, { partition: 2, offset: '-1' }] }],
		resetOffsets: async (opciones: unknown) => { llamadas.resetOffsets = opciones; },
		...extra
	};
	return { admin: admin as unknown as Admin, llamadas };
}

test('modificarConfigs mezcla con las configuraciones actuales, porque alterConfigs las reemplaza todas', async () => {
	const { admin, llamadas } = adminFalso();
	const servicio = new KafkaAdminService(['k:1'], () => admin);
	await servicio.modificarConfigs('reservas', { 'retention.ms': '1209600000' });
	const enviado = llamadas.alterConfigs as { resources: { configEntries: { name: string; value: string }[] }[] };
	assert.deepEqual(Object.fromEntries(enviado.resources[0]!.configEntries.map((c) => [c.name, c.value])), {
		'cleanup.policy': 'delete',
		'retention.ms': '1209600000',
		'min.insync.replicas': '2'
	});
	await assert.rejects(servicio.modificarConfigs('reservas', { 'min.insync.replicas': '4' }), ValidacionKafkaError);
});

test('detalleGrupo calcula el lag por partición y el miembro dueño, como kafka-consumer-groups --describe', async () => {
	const { admin } = adminFalso();
	const grupo = await new KafkaAdminService(['k:1'], () => admin).detalleGrupo('reportes');
	assert.equal(grupo.members, 3);
	assert.deepEqual(grupo.partitions.map((p) => [p.partition, p.lag, p.member]), [
		[0, 0, 'hotel-ms-reportes@/10.0.0.0'],
		[1, 2, 'hotel-ms-reportes@/10.0.0.1'],
		[2, 4, 'hotel-ms-reportes@/10.0.0.2']
	]);
	assert.equal(grupo.totalLag, 6);
});

test('reset de offsets solo con el grupo detenido; tópicos protegidos no se eliminan; replicación ≤ brokers', async () => {
	const { admin, llamadas } = adminFalso();
	const servicio = new KafkaAdminService(['k:1'], () => admin);
	await assert.rejects(servicio.resetearOffsets('reportes', 'reservas', 'earliest'), ConflictoKafkaError);
	await servicio.resetearOffsets('auditoria', 'reservas', 'earliest');
	assert.deepEqual(llamadas.resetOffsets, { groupId: 'auditoria', topic: 'reservas', earliest: true });
	await assert.rejects(servicio.eliminarTopico('reservas'), RecursoProtegidoError);
	await assert.rejects(servicio.eliminarTopico('__consumer_offsets'), RecursoProtegidoError);
	await assert.rejects(servicio.crearTopico({ name: 'nuevo', partitions: 3, replicationFactor: 4, configs: {} }), ValidacionKafkaError);
});
