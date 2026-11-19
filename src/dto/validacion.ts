import { kafkaAdminConfig } from '../config/kafka.config';

export interface ErrorValidacion {
	campo: string;
	mensaje: string;
}

export type ResultadoValidacion<T> = { ok: true; valor: T } | { ok: false; errores: ErrorValidacion[] };

// Regla de Kafka para nombres de tópicos: hasta 249 caracteres, solo letras, números, ".", "_" y "-".
const NOMBRE_TOPICO = /^[A-Za-z0-9._-]{1,249}$/;
// Grupos: se permite lo mismo que en los tópicos (los nuestros son "reportes", "auditoria", "reportes-dlt"...).
const NOMBRE_GRUPO = /^[A-Za-z0-9._-]{1,249}$/;

export function esObjeto(valor: unknown): valor is Record<string, unknown> {
	return typeof valor === 'object' && valor !== null && !Array.isArray(valor);
}

export function validarNombreTopico(valor: unknown, campo: string, errores: ErrorValidacion[]): string | undefined {
	if (typeof valor !== 'string' || !valor.trim()) {
		errores.push({ campo, mensaje: 'Es obligatorio y no puede estar vacío' });
		return undefined;
	}
	if (valor === '.' || valor === '..') {
		errores.push({ campo, mensaje: 'Kafka no acepta "." ni ".." como nombre' });
		return undefined;
	}
	if (valor.startsWith('__')) {
		errores.push({ campo, mensaje: 'El prefijo "__" está reservado para los tópicos internos de Kafka' });
		return undefined;
	}
	if (!NOMBRE_TOPICO.test(valor)) {
		errores.push({ campo, mensaje: 'Solo letras, números, ".", "_" y "-" (máx. 249, sin espacios)' });
		return undefined;
	}
	return valor;
}

export function validarNombreGrupo(valor: unknown, campo: string, errores: ErrorValidacion[]): string | undefined {
	if (typeof valor !== 'string' || !NOMBRE_GRUPO.test(valor)) {
		errores.push({ campo, mensaje: 'Solo letras, números, ".", "_" y "-" (máx. 249)' });
		return undefined;
	}
	return valor;
}

interface ReglaConfig {
	descripcion: string;
	validar: (valor: string) => boolean;
}

function enteroEntre(minimo: number, maximo: number, permitirMenosUno = false): (valor: string) => boolean {
	return (valor) => {
		if (!/^-?\d+$/.test(valor)) return false;
		const numero = Number(valor);
		return (permitirMenosUno && numero === -1) || (numero >= minimo && numero <= maximo);
	};
}

const UN_ANIO_MS = 365 * 24 * 60 * 60 * 1000;

/**
 * Configuraciones de tópico que se pueden poner o cambiar desde la API, con sus valores válidos.
 * Son las que ajustamos por flujo: retención, cleanup policy, réplicas mínimas y tamaño de segmento.
 */
export const CONFIGS_PERMITIDAS: Record<string, ReglaConfig> = {
	'cleanup.policy': {
		descripcion: 'delete, compact o compact,delete',
		validar: (valor) => ['delete', 'compact', 'compact,delete', 'delete,compact'].includes(valor)
	},
	'retention.ms': {
		descripcion: '-1 (sin límite) o entre 60000 (1 minuto) y 31536000000 (1 año)',
		validar: enteroEntre(60_000, UN_ANIO_MS, true)
	},
	'retention.bytes': {
		descripcion: '-1 (sin límite) o entre 1048576 (1 MB) y 1099511627776 (1 TB)',
		validar: enteroEntre(1_048_576, 1_099_511_627_776, true)
	},
	'min.insync.replicas': {
		descripcion: 'entero entre 1 y 10, sin superar la replicación del tópico',
		validar: enteroEntre(1, 10)
	},
	'segment.ms': {
		descripcion: 'entre 60000 (1 minuto) y 604800000 (7 días)',
		validar: enteroEntre(60_000, 604_800_000)
	},
	'max.message.bytes': {
		descripcion: 'entre 1024 (1 KB) y 10485760 (10 MB)',
		validar: enteroEntre(1024, 10_485_760)
	}
};

/** Valida un objeto { "retention.ms": "604800000", ... }. Acepta números o texto y devuelve todo como texto. */
export function validarConfigs(valor: unknown, campo: string, errores: ErrorValidacion[], obligatorio: boolean): Record<string, string> | undefined {
	if (valor === undefined && !obligatorio) {
		return undefined;
	}
	if (!esObjeto(valor) || (obligatorio && Object.keys(valor).length === 0)) {
		errores.push({ campo, mensaje: 'Debe ser un objeto con al menos una configuración, por ejemplo { "retention.ms": "604800000" }' });
		return undefined;
	}
	const configs: Record<string, string> = {};
	for (const [nombre, crudo] of Object.entries(valor)) {
		const regla = CONFIGS_PERMITIDAS[nombre];
		if (!regla) {
			errores.push({ campo: `${campo}.${nombre}`, mensaje: `Configuración no permitida. Usa: ${Object.keys(CONFIGS_PERMITIDAS).join(', ')}` });
			continue;
		}
		const texto = typeof crudo === 'number' ? String(crudo) : crudo;
		if (typeof texto !== 'string' || !regla.validar(texto.trim())) {
			errores.push({ campo: `${campo}.${nombre}`, mensaje: `Valor inválido: ${regla.descripcion}` });
			continue;
		}
		configs[nombre] = texto.trim();
	}
	return configs;
}

export class Validador {
	readonly errores: ErrorValidacion[] = [];

	constructor(private readonly cuerpo: Record<string, unknown>, permitidos: string[]) {
		for (const campo of Object.keys(cuerpo)) {
			if (!permitidos.includes(campo)) {
				this.errores.push({ campo, mensaje: `Campo no permitido. Usa: ${permitidos.join(', ')}` });
			}
		}
	}

	get(campo: string): unknown {
		return this.cuerpo[campo];
	}

	entero(campo: string, minimo: number, maximo: number, obligatorio = true): number | undefined {
		const valor = this.cuerpo[campo];
		if (valor === undefined && !obligatorio) {
			return undefined;
		}
		if (typeof valor !== 'number' || !Number.isInteger(valor) || valor < minimo || valor > maximo) {
			this.errores.push({ campo, mensaje: `Debe ser un entero entre ${minimo} y ${maximo}` });
			return undefined;
		}
		return valor;
	}

	opcion<T extends string>(campo: string, opciones: readonly T[]): T | undefined {
		const valor = this.cuerpo[campo];
		if (typeof valor !== 'string' || !opciones.includes(valor as T)) {
			this.errores.push({ campo, mensaje: `Debe ser uno de: ${opciones.join(', ')}` });
			return undefined;
		}
		return valor as T;
	}

	resultado<T>(valor: T): ResultadoValidacion<T> {
		return this.errores.length > 0 ? { ok: false, errores: this.errores } : { ok: true, valor };
	}
}

export const LIMITES = kafkaAdminConfig.limites;
