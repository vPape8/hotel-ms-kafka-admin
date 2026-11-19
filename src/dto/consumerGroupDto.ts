import { esObjeto, ResultadoValidacion, validarNombreTopico, Validador } from './validacion';

export const DESTINOS_RESET = ['earliest', 'latest'] as const;
export type DestinoReset = (typeof DESTINOS_RESET)[number];

/** POST /api/v1/consumer-groups/{id}/offsets/reset */
export interface ResetOffsetsDto {
	topic: string;
	/** earliest = vuelve a leer todo lo que queda en el tópico; latest = salta al final. */
	to: DestinoReset;
}

export function validarResetOffsets(cuerpo: unknown): ResultadoValidacion<ResetOffsetsDto> {
	if (!esObjeto(cuerpo)) {
		return { ok: false, errores: [{ campo: 'body', mensaje: 'Se esperaba un objeto JSON' }] };
	}
	const v = new Validador(cuerpo, ['topic', 'to']);
	return v.resultado({
		topic: validarNombreTopico(v.get('topic'), 'topic', v.errores) ?? '',
		to: v.opcion('to', DESTINOS_RESET) ?? 'earliest'
	});
}
