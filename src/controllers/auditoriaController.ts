import { Request, Response } from 'express';
import { log } from '../logs/logger';
import { AuditoriaRepository, Entidad, FiltrosAuditoria, TipoEvento } from '../repositories/auditoriaRepository';

const TIPOS: readonly TipoEvento[] = [
	'RESERVA_CREADA', 'RESERVA_CONFIRMADA', 'RESERVA_CANCELADA', 'RESERVA_COMPLETADA',
	'HABITACION_CREADA', 'HABITACION_ACTUALIZADA', 'DISPONIBILIDAD_CAMBIADA'
];
const ENTIDADES: readonly Entidad[] = ['RESERVA', 'HABITACION'];
const TAMANO_MAXIMO = 100;

interface ErrorFiltro {
	campo: string;
	mensaje: string;
}

function parametro(solicitud: Request, nombre: string): string | undefined {
	const valor = solicitud.query[nombre];
	return typeof valor === 'string' && valor.trim() ? valor.trim() : undefined;
}

function fecha(valor: string | undefined, campo: string, finDelDia: boolean, errores: ErrorFiltro[]): Date | undefined {
	if (!valor) return undefined;
	if (!/^\d{4}-\d{2}-\d{2}$/.test(valor) || Number.isNaN(Date.parse(valor))) {
		errores.push({ campo, mensaje: 'Debe ser una fecha con formato YYYY-MM-DD' });
		return undefined;
	}
	return new Date(`${valor}T${finDelDia ? '23:59:59.999' : '00:00:00.000'}Z`);
}

function entero(valor: string | undefined, campo: string, porDefecto: number, maximo: number, errores: ErrorFiltro[]): number {
	if (!valor) return porDefecto;
	const numero = Number(valor);
	if (!Number.isInteger(numero) || numero < 1 || numero > maximo) {
		errores.push({ campo, mensaje: `Debe ser un entero entre 1 y ${maximo}` });
		return porDefecto;
	}
	return numero;
}

export class AuditoriaController {
	constructor(private readonly repositorio: AuditoriaRepository) {}

	/** GET /api/v1/auditoria?usuario=&tipo=&entidad=&codigo=&desde=YYYY-MM-DD&hasta=YYYY-MM-DD&pagina=&tamano= */
	buscar = async (solicitud: Request, respuesta: Response): Promise<void> => {
		const errores: ErrorFiltro[] = [];
		const tipo = parametro(solicitud, 'tipo');
		if (tipo && !TIPOS.includes(tipo as TipoEvento)) {
			errores.push({ campo: 'tipo', mensaje: `Debe ser uno de: ${TIPOS.join(', ')}` });
		}
		const entidad = parametro(solicitud, 'entidad');
		if (entidad && !ENTIDADES.includes(entidad as Entidad)) {
			errores.push({ campo: 'entidad', mensaje: `Debe ser uno de: ${ENTIDADES.join(', ')}` });
		}
		const usuario = parametro(solicitud, 'usuario');
		const codigo = parametro(solicitud, 'codigo');
		const desde = fecha(parametro(solicitud, 'desde'), 'desde', false, errores);
		const hasta = fecha(parametro(solicitud, 'hasta'), 'hasta', true, errores);
		const filtros: FiltrosAuditoria = {
			...(usuario ? { usuario } : {}),
			...(tipo ? { tipo: tipo as TipoEvento } : {}),
			...(entidad ? { entidad: entidad as Entidad } : {}),
			...(codigo ? { codigo } : {}),
			...(desde ? { desde } : {}),
			...(hasta ? { hasta } : {}),
			pagina: entero(parametro(solicitud, 'pagina'), 'pagina', 1, 100_000, errores),
			tamano: entero(parametro(solicitud, 'tamano'), 'tamano', 20, TAMANO_MAXIMO, errores)
		};
		if (errores.length > 0) {
			respuesta.status(400).json({ error: 'VALIDACION_FILTROS', detalles: errores });
			return;
		}
		try {
			const { registros, total } = await this.repositorio.buscar(filtros);
			respuesta.json({ data: registros, total, pagina: filtros.pagina, tamano: filtros.tamano });
		} catch (error) {
			log('error', 'ERROR_AUDITORIA', { motivo: error instanceof Error ? error.message : String(error) });
			respuesta.status(500).json({ error: 'ERROR_INTERNO' });
		}
	};
}
