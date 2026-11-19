import 'dotenv/config';
import { crearApp } from './app';
import { kafkaAdminConfig } from './config/kafka.config';
import { KafkaAdminService } from './services/kafkaAdminService';

const puerto = Number(process.env.PORT ?? 8085);

if (kafkaAdminConfig.brokers.length === 0) {
	console.error('Falta KAFKA_BROKERS (por ejemplo kafka-1:29092,kafka-2:29092,kafka-3:29092)');
	process.exit(1);
}

const servicio = new KafkaAdminService([...kafkaAdminConfig.brokers]);

const servidor = crearApp(servicio).listen(puerto, () => {
	console.log(`hotel-ms-kafka-admin escuchando en el puerto ${puerto}`);
});

for (const senal of ['SIGINT', 'SIGTERM'] as const) {
	process.once(senal, () => {
		servidor.close();
		void servicio.cerrar().finally(() => process.exit(0));
	});
}
