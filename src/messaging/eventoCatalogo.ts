import { ErrorNoRecuperable } from './errores';

// Contrato del tópico "catalogo": lo publica hotel-ms-catalogo (src/messaging/eventoCatalogo.ts).
// La auditoría solo lee lo que necesita: quién, qué, cuándo, desde dónde y qué cambió.

export type TipoEventoCatalogo = 'HABITACION_CREADA' | 'HABITACION_ACTUALIZADA' | 'DISPONIBILIDAD_CAMBIADA';

const TIPOS: readonly TipoEventoCatalogo[] = ['HABITACION_CREADA', 'HABITACION_ACTUALIZADA', 'DISPONIBILIDAD_CAMBIADA'];

export interface HabitacionEventoDto {
	eventoId: string;
	tipo: TipoEventoCatalogo;
	ocurridoEn: string;
	actor: { tipo: 'INTERNO' | 'SISTEMA'; id: string; nombre?: string; roles: string[] };
	origen: { canal: string; ip?: string; codigoReserva?: string };
	habitacion: { id: string; idHotel: string; nombre: string; precioPorNoche: number; unidades: number; activa: boolean; ocupaciones: unknown[] };
	cambios?: { campo: string; antes: unknown; despues: unknown }[];
}

function objeto(valor: unknown, campo: string): Record<string, unknown> {
	if (typeof valor !== 'object' || valor === null || Array.isArray(valor)) {
		throw new ErrorNoRecuperable(`"${campo}" debe ser un objeto`);
	}
	return valor as Record<string, unknown>;
}

function texto(origen: Record<string, unknown>, campo: string): string {
	const valor = origen[campo];
	if (typeof valor !== 'string' || !valor.trim()) {
		throw new ErrorNoRecuperable(`"${campo}" ausente o vacío`);
	}
	return valor;
}

function numero(origen: Record<string, unknown>, campo: string): number {
	const valor = origen[campo];
	if (typeof valor !== 'number' || !Number.isFinite(valor) || valor < 0) {
		throw new ErrorNoRecuperable(`"${campo}" debe ser un número no negativo`);
	}
	return valor;
}

/** Valida el JSON recibido contra el DTO. Si no cumple, el evento va directo a catalogo.DLT. */
export function validarEventoCatalogo(cuerpo: unknown): HabitacionEventoDto {
	const evento = objeto(cuerpo, 'evento');
	const actor = objeto(evento.actor, 'actor');
	const origen = objeto(evento.origen, 'origen');
	const habitacion = objeto(evento.habitacion, 'habitacion');
	const tipo = evento.tipo;
	if (!TIPOS.includes(tipo as TipoEventoCatalogo)) {
		throw new ErrorNoRecuperable(`"tipo" debe ser uno de: ${TIPOS.join(', ')}`);
	}
	const ocurridoEn = texto(evento, 'ocurridoEn');
	if (Number.isNaN(Date.parse(ocurridoEn))) {
		throw new ErrorNoRecuperable('"ocurridoEn" no es una fecha válida');
	}
	if (actor.tipo !== 'INTERNO' && actor.tipo !== 'SISTEMA') {
		throw new ErrorNoRecuperable('"actor.tipo" debe ser INTERNO o SISTEMA');
	}
	return {
		eventoId: texto(evento, 'eventoId'),
		tipo: tipo as TipoEventoCatalogo,
		ocurridoEn,
		actor: {
			tipo: actor.tipo,
			id: texto(actor, 'id'),
			...(typeof actor.nombre === 'string' ? { nombre: actor.nombre } : {}),
			roles: Array.isArray(actor.roles) ? actor.roles.filter((rol): rol is string => typeof rol === 'string') : []
		},
		origen: {
			canal: texto(origen, 'canal'),
			...(typeof origen.ip === 'string' ? { ip: origen.ip } : {}),
			...(typeof origen.codigoReserva === 'string' ? { codigoReserva: origen.codigoReserva } : {})
		},
		habitacion: {
			id: texto(habitacion, 'id'),
			idHotel: texto(habitacion, 'idHotel'),
			nombre: texto(habitacion, 'nombre'),
			precioPorNoche: numero(habitacion, 'precioPorNoche'),
			unidades: numero(habitacion, 'unidades'),
			activa: habitacion.activa === true,
			ocupaciones: Array.isArray(habitacion.ocupaciones) ? habitacion.ocupaciones : []
		},
		...(Array.isArray(evento.cambios)
			? { cambios: evento.cambios.filter((c): c is { campo: string; antes: unknown; despues: unknown } => typeof c === 'object' && c !== null && typeof (c as { campo?: unknown }).campo === 'string') }
			: {})
	};
}
