import { Request, Response } from 'express';
import { validarResetOffsets } from '../dto/consumerGroupDto';
import { validarAumentarParticiones, validarCrearTopico, validarModificarConfigs } from '../dto/topicoDto';
import { ErrorValidacion, validarNombreGrupo } from '../dto/validacion';
import {
	ConflictoKafkaError,
	KafkaAdmin,
	KafkaNoDisponibleError,
	RecursoNoEncontradoError,
	RecursoProtegidoError,
	ValidacionKafkaError
} from '../services/kafkaAdminService';

function responderValidacion(respuesta: Response, detalles: ErrorValidacion[]): void {
	respuesta.status(400).json({ error: 'VALIDACION', detalles });
}

function responderError(respuesta: Response, error: unknown): void {
	const mensaje = error instanceof Error ? error.message : undefined;
	if (error instanceof ValidacionKafkaError) {
		responderValidacion(respuesta, error.detalles);
	} else if (error instanceof RecursoNoEncontradoError) {
		respuesta.status(404).json({ error: 'RECURSO_NO_ENCONTRADO', mensaje });
	} else if (error instanceof RecursoProtegidoError) {
		respuesta.status(409).json({ error: 'RECURSO_PROTEGIDO', mensaje });
	} else if (error instanceof ConflictoKafkaError) {
		respuesta.status(409).json({ error: 'CONFLICTO_KAFKA', mensaje });
	} else if (error instanceof KafkaNoDisponibleError) {
		respuesta.status(503).json({ error: 'KAFKA_NO_DISPONIBLE', mensaje });
	} else {
		respuesta.status(500).json({ error: 'ERROR_INTERNO' });
	}
}

/** El nombre del tópico viene en la URL: no se valida como para crear (ahí sí se exige el formato completo). */
function nombreEnRuta(solicitud: Request): string {
	return decodeURIComponent(String(solicitud.params.name));
}

/** Controladores delgados: validan la entrada con los DTO y delegan en KafkaAdmin. */
export class KafkaAdminController {
	constructor(private readonly servicio: KafkaAdmin) {}

	private ejecutar(operacion: (solicitud: Request) => Promise<unknown>, estado = 200) {
		return async (solicitud: Request, respuesta: Response): Promise<void> => {
			try {
				const data = await operacion(solicitud);
				if (estado === 204) {
					respuesta.status(204).send();
				} else {
					respuesta.status(estado).json({ data });
				}
			} catch (error) {
				responderError(respuesta, error);
			}
		};
	}

	cluster = this.ejecutar(() => this.servicio.cluster());

	listarTopicos = this.ejecutar(() => this.servicio.listarTopicos());

	detalleTopico = this.ejecutar((solicitud) => this.servicio.detalleTopico(nombreEnRuta(solicitud)));

	crearTopico = async (solicitud: Request, respuesta: Response): Promise<void> => {
		const validacion = validarCrearTopico(solicitud.body);
		if (!validacion.ok) {
			responderValidacion(respuesta, validacion.errores);
			return;
		}
		await this.ejecutar(() => this.servicio.crearTopico(validacion.valor), 201)(solicitud, respuesta);
	};

	modificarConfigs = async (solicitud: Request, respuesta: Response): Promise<void> => {
		const validacion = validarModificarConfigs(solicitud.body);
		if (!validacion.ok) {
			responderValidacion(respuesta, validacion.errores);
			return;
		}
		await this.ejecutar(() => this.servicio.modificarConfigs(nombreEnRuta(solicitud), validacion.valor.configs))(solicitud, respuesta);
	};

	aumentarParticiones = async (solicitud: Request, respuesta: Response): Promise<void> => {
		const validacion = validarAumentarParticiones(solicitud.body);
		if (!validacion.ok) {
			responderValidacion(respuesta, validacion.errores);
			return;
		}
		await this.ejecutar(() => this.servicio.aumentarParticiones(nombreEnRuta(solicitud), validacion.valor))(solicitud, respuesta);
	};

	eliminarTopico = this.ejecutar((solicitud) => this.servicio.eliminarTopico(nombreEnRuta(solicitud)), 204);

	listarGrupos = this.ejecutar(() => this.servicio.listarGrupos());

	detalleGrupo = async (solicitud: Request, respuesta: Response): Promise<void> => {
		const errores: ErrorValidacion[] = [];
		const groupId = validarNombreGrupo(solicitud.params.id, 'id', errores);
		if (!groupId) {
			responderValidacion(respuesta, errores);
			return;
		}
		await this.ejecutar(() => this.servicio.detalleGrupo(groupId))(solicitud, respuesta);
	};

	resetearOffsets = async (solicitud: Request, respuesta: Response): Promise<void> => {
		const errores: ErrorValidacion[] = [];
		const groupId = validarNombreGrupo(solicitud.params.id, 'id', errores);
		const validacion = validarResetOffsets(solicitud.body);
		if (!groupId || !validacion.ok) {
			responderValidacion(respuesta, [...errores, ...(validacion.ok ? [] : validacion.errores)]);
			return;
		}
		await this.ejecutar(() => this.servicio.resetearOffsets(groupId, validacion.valor.topic, validacion.valor.to))(solicitud, respuesta);
	};
}
