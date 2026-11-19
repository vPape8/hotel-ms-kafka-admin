// Configuración centralizada del administrador de Kafka.

function lista(variable: string | undefined, porDefecto: string[] = []): string[] {
	const valores = (variable ?? '').split(',').map((valor) => valor.trim()).filter(Boolean);
	return valores.length > 0 ? valores : porDefecto;
}

export const kafkaAdminConfig = {
	/** bootstrap-servers del clúster (3 brokers en docker-compose). */
	brokers: lista(process.env.KAFKA_BROKERS),
	clientId: process.env.KAFKA_CLIENT_ID ?? 'hotel-ms-kafka-admin',

	/**
	 * Tópicos de los que dependen los microservicios: la API no deja eliminarlos (409 RECURSO_PROTEGIDO).
	 * Sus configuraciones sí se pueden modificar (por ejemplo, la retención).
	 */
	protegidos: lista(process.env.KAFKA_TOPICOS_PROTEGIDOS, ['reservas', 'reservas.DLT', 'catalogo', 'catalogo.DLT']),

	limites: {
		particionesMaximas: 50,
		replicacionMaxima: 10
	}
} as const;

/** Los tópicos que empiezan con "__" son internos de Kafka (offsets de los grupos, transacciones). */
export function esTopicoInterno(nombre: string): boolean {
	return nombre.startsWith('__');
}
