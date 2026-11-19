import { RequestHandler, Router } from 'express';
import { KafkaAdminController } from '../controllers/kafkaAdminController';
import { KafkaAdmin } from '../services/kafkaAdminService';

export function crearKafkaRoutes(servicio: KafkaAdmin, autenticacion: RequestHandler): Router {
	const router = Router();
	const controlador = new KafkaAdminController(servicio);

	router.use(autenticacion);

	router.get('/cluster', controlador.cluster);

	router.get('/topics', controlador.listarTopicos);
	router.post('/topics', controlador.crearTopico);
	router.get('/topics/:name', controlador.detalleTopico);
	router.patch('/topics/:name/configs', controlador.modificarConfigs);
	router.post('/topics/:name/partitions', controlador.aumentarParticiones);
	router.delete('/topics/:name', controlador.eliminarTopico);

	router.get('/consumer-groups', controlador.listarGrupos);
	router.get('/consumer-groups/:id', controlador.detalleGrupo);
	router.post('/consumer-groups/:id/offsets/reset', controlador.resetearOffsets);

	return router;
}
