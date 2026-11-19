import { esObjeto, LIMITES, ResultadoValidacion, validarConfigs, validarNombreTopico, Validador } from './validacion';

/** POST /api/v1/topics */
export interface CrearTopicoDto {
	name: string;
	partitions: number;
	replicationFactor: number;
	configs: Record<string, string>;
}

/** PATCH /api/v1/topics/{name}/configs */
export interface ModificarConfigsDto {
	configs: Record<string, string>;
}

/** POST /api/v1/topics/{name}/partitions */
export interface AumentarParticionesDto {
	/** Total de particiones que tendrá el tópico (Kafka solo permite aumentarlas). */
	count: number;
}

function cuerpoObjeto<T>(cuerpo: unknown): ResultadoValidacion<T> | undefined {
	return esObjeto(cuerpo) ? undefined : { ok: false, errores: [{ campo: 'body', mensaje: 'Se esperaba un objeto JSON' }] };
}

export function validarCrearTopico(cuerpo: unknown): ResultadoValidacion<CrearTopicoDto> {
	const noEsObjeto = cuerpoObjeto<CrearTopicoDto>(cuerpo);
	if (noEsObjeto) return noEsObjeto;
	const v = new Validador(cuerpo as Record<string, unknown>, ['name', 'partitions', 'replicationFactor', 'configs']);
	const dto: CrearTopicoDto = {
		name: validarNombreTopico(v.get('name'), 'name', v.errores) ?? '',
		partitions: v.entero('partitions', 1, LIMITES.particionesMaximas) ?? 1,
		replicationFactor: v.entero('replicationFactor', 1, LIMITES.replicacionMaxima) ?? 1,
		configs: validarConfigs(v.get('configs'), 'configs', v.errores, false) ?? {}
	};
	const minIsr = Number(dto.configs['min.insync.replicas']);
	if (minIsr > dto.replicationFactor) {
		v.errores.push({ campo: 'configs.min.insync.replicas', mensaje: 'No puede ser mayor que replicationFactor' });
	}
	return v.resultado(dto);
}

export function validarModificarConfigs(cuerpo: unknown): ResultadoValidacion<ModificarConfigsDto> {
	const noEsObjeto = cuerpoObjeto<ModificarConfigsDto>(cuerpo);
	if (noEsObjeto) return noEsObjeto;
	const v = new Validador(cuerpo as Record<string, unknown>, ['configs']);
	return v.resultado({ configs: validarConfigs(v.get('configs'), 'configs', v.errores, true) ?? {} });
}

export function validarAumentarParticiones(cuerpo: unknown): ResultadoValidacion<AumentarParticionesDto> {
	const noEsObjeto = cuerpoObjeto<AumentarParticionesDto>(cuerpo);
	if (noEsObjeto) return noEsObjeto;
	const v = new Validador(cuerpo as Record<string, unknown>, ['count']);
	return v.resultado({ count: v.entero('count', 2, LIMITES.particionesMaximas) ?? 2 });
}
