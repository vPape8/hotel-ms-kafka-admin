import express, { Express, NextFunction, Request, RequestHandler, Response } from 'express';
import { entraAuthMiddleware } from './middlewares/authMiddleware';
import { crearKafkaRoutes } from './routes/kafkaRoutes';
import { KafkaAdmin } from './services/kafkaAdminService';

export function crearApp(servicio: KafkaAdmin, autenticacion: RequestHandler = entraAuthMiddleware): Express {
	const app = express();
	app.use(express.json());

	app.get('/health', (_solicitud, respuesta) => {
		respuesta.json({ status: 'ok' });
	});

	app.use('/api/v1', crearKafkaRoutes(servicio, autenticacion));

	app.use((_solicitud, respuesta) => {
		respuesta.status(404).json({ error: 'RUTA_NO_ENCONTRADA' });
	});

	app.use((error: unknown, _solicitud: Request, respuesta: Response, _siguiente: NextFunction) => {
		if (error instanceof SyntaxError) {
			respuesta.status(400).json({ error: 'JSON_INVALIDO' });
			return;
		}
		respuesta.status(500).json({ error: 'ERROR_INTERNO' });
	});

	return app;
}
