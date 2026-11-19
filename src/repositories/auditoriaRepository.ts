import { ResultSetHeader, RowDataPacket } from 'mysql2';
import { Pool } from 'mysql2/promise';
import { HabitacionEventoDto, TipoEventoCatalogo } from '../messaging/eventoCatalogo';
import { ReservaEventoDto, TipoEventoReserva } from '../messaging/eventoReserva';

export type Entidad = 'RESERVA' | 'HABITACION';
export type TipoEvento = TipoEventoReserva | TipoEventoCatalogo;

/** Una fila de auditoría: quién, qué, cuándo y desde dónde, más dónde estaba el evento en Kafka. */
export interface RegistroAuditoria {
	eventoId: string;
	tipo: TipoEvento;
	entidad: Entidad;
	ocurridoEn: string;
	actor: { tipo: 'INVITADO' | 'CLIENTE' | 'INTERNO' | 'SISTEMA'; id: string; nombre?: string; roles: string[] };
	origen: { canal: string; ip?: string };
	/** Sobre qué se hizo: la reserva (código) o la habitación. */
	objetivo: { id: string; referencia: string; idHabitacion: string; estado?: string; estadoAnterior?: string };
	/** Qué cambió, en texto (por ejemplo "precioPorNoche: 248000 → 260000"). */
	detalle?: string;
	kafka: { topico: string; particion: number; offset: string };
	registradoEn: string;
}

export interface FiltrosAuditoria {
	/** Id o nombre del usuario (o "invitado", o "hotel-ms-catalogo" para los cambios automáticos). */
	usuario?: string;
	tipo?: TipoEvento;
	entidad?: Entidad;
	/** Código de reserva o id de habitación. */
	codigo?: string;
	desde?: Date;
	hasta?: Date;
	pagina: number;
	tamano: number;
}

export type ResultadoRegistro = 'REGISTRADO' | 'DUPLICADO';

export interface AuditoriaRepository {
	/** Idempotente: el eventoId es la clave, un evento repetido no se registra dos veces. */
	registrar(registro: RegistroAuditoria): Promise<ResultadoRegistro>;
	buscar(filtros: FiltrosAuditoria): Promise<{ registros: RegistroAuditoria[]; total: number }>;
}

type UbicacionKafka = RegistroAuditoria['kafka'];

export function desdeEventoReserva(evento: ReservaEventoDto, kafka: UbicacionKafka): RegistroAuditoria {
	const { reserva } = evento;
	return {
		eventoId: evento.eventoId,
		tipo: evento.tipo,
		entidad: 'RESERVA',
		ocurridoEn: evento.ocurridoEn,
		actor: evento.actor,
		origen: evento.origen,
		objetivo: {
			id: reserva.id,
			referencia: reserva.codigo,
			idHabitacion: reserva.idHabitacion,
			estado: reserva.estado,
			...(reserva.estadoAnterior ? { estadoAnterior: reserva.estadoAnterior } : {})
		},
		detalle: `${reserva.noches} noche(s), ${reserva.cantidadHuespedes} huésped(es), total ${reserva.precioTotal}`,
		kafka,
		registradoEn: new Date().toISOString()
	};
}

export function desdeEventoCatalogo(evento: HabitacionEventoDto, kafka: UbicacionKafka): RegistroAuditoria {
	const { habitacion } = evento;
	const detalle = evento.cambios && evento.cambios.length > 0
		? evento.cambios.map((cambio) => `${cambio.campo}: ${String(cambio.antes)} → ${String(cambio.despues)}`).join('; ')
		: evento.tipo === 'DISPONIBILIDAD_CAMBIADA'
			? `Por la reserva ${evento.origen.codigoReserva ?? '?'}: ${habitacion.ocupaciones.length} de ${habitacion.unidades} unidad(es) ocupada(s)`
			: `Precio ${habitacion.precioPorNoche}, ${habitacion.unidades} unidad(es)`;
	return {
		eventoId: evento.eventoId,
		tipo: evento.tipo,
		entidad: 'HABITACION',
		ocurridoEn: evento.ocurridoEn,
		actor: evento.actor,
		origen: { canal: evento.origen.canal, ...(evento.origen.ip ? { ip: evento.origen.ip } : {}) },
		objetivo: { id: habitacion.id, referencia: habitacion.id, idHabitacion: habitacion.id, estado: habitacion.activa ? 'ACTIVA' : 'INACTIVA' },
		detalle,
		kafka,
		registradoEn: new Date().toISOString()
	};
}

function cumple(registro: RegistroAuditoria, filtros: FiltrosAuditoria): boolean {
	const usuario = filtros.usuario?.toLowerCase();
	const ocurrido = new Date(registro.ocurridoEn);
	return (!usuario || registro.actor.id.toLowerCase() === usuario || (registro.actor.nombre ?? '').toLowerCase().includes(usuario))
		&& (!filtros.tipo || registro.tipo === filtros.tipo)
		&& (!filtros.entidad || registro.entidad === filtros.entidad)
		&& (!filtros.codigo || registro.objetivo.referencia === filtros.codigo)
		&& (!filtros.desde || ocurrido >= filtros.desde)
		&& (!filtros.hasta || ocurrido <= filtros.hasta);
}

/** Para tests y desarrollo sin MySQL. */
export class InMemoryAuditoriaRepository implements AuditoriaRepository {
	readonly registros = new Map<string, RegistroAuditoria>();

	async registrar(registro: RegistroAuditoria): Promise<ResultadoRegistro> {
		if (this.registros.has(registro.eventoId)) return 'DUPLICADO';
		this.registros.set(registro.eventoId, registro);
		return 'REGISTRADO';
	}

	async buscar(filtros: FiltrosAuditoria): Promise<{ registros: RegistroAuditoria[]; total: number }> {
		const todos = [...this.registros.values()]
			.filter((registro) => cumple(registro, filtros))
			.sort((a, b) => b.ocurridoEn.localeCompare(a.ocurridoEn));
		const inicio = (filtros.pagina - 1) * filtros.tamano;
		return { registros: todos.slice(inicio, inicio + filtros.tamano), total: todos.length };
	}
}

const ESQUEMA = `
CREATE TABLE IF NOT EXISTS auditoria_eventos (
	evento_id CHAR(36) PRIMARY KEY,
	tipo VARCHAR(40) NOT NULL,
	entidad VARCHAR(20) NOT NULL,
	ocurrido_en DATETIME(3) NOT NULL,
	actor_tipo VARCHAR(20) NOT NULL,
	actor_id VARCHAR(255) NOT NULL,
	actor_nombre VARCHAR(255) NULL,
	actor_roles VARCHAR(255) NOT NULL,
	canal VARCHAR(30) NOT NULL,
	ip VARCHAR(64) NULL,
	objetivo_id VARCHAR(64) NOT NULL,
	referencia VARCHAR(64) NOT NULL,
	id_habitacion VARCHAR(64) NOT NULL,
	estado VARCHAR(20) NULL,
	estado_anterior VARCHAR(20) NULL,
	detalle VARCHAR(500) NULL,
	topico VARCHAR(100) NOT NULL,
	particion INT NOT NULL,
	offset_kafka VARCHAR(30) NOT NULL,
	registrado_en DATETIME(3) NOT NULL,
	INDEX idx_auditoria_ocurrido (ocurrido_en),
	INDEX idx_auditoria_actor (actor_id),
	INDEX idx_auditoria_tipo (tipo),
	INDEX idx_auditoria_referencia (referencia)
)`;

interface FilaAuditoria extends RowDataPacket {
	evento_id: string;
	tipo: TipoEvento;
	entidad: Entidad;
	ocurrido_en: Date;
	actor_tipo: RegistroAuditoria['actor']['tipo'];
	actor_id: string;
	actor_nombre: string | null;
	actor_roles: string;
	canal: string;
	ip: string | null;
	objetivo_id: string;
	referencia: string;
	id_habitacion: string;
	estado: string | null;
	estado_anterior: string | null;
	detalle: string | null;
	topico: string;
	particion: number;
	offset_kafka: string;
	registrado_en: Date;
}

function desdeFila(fila: FilaAuditoria): RegistroAuditoria {
	return {
		eventoId: fila.evento_id,
		tipo: fila.tipo,
		entidad: fila.entidad,
		ocurridoEn: new Date(fila.ocurrido_en).toISOString(),
		actor: {
			tipo: fila.actor_tipo,
			id: fila.actor_id,
			...(fila.actor_nombre ? { nombre: fila.actor_nombre } : {}),
			roles: fila.actor_roles ? fila.actor_roles.split(',') : []
		},
		origen: { canal: fila.canal, ...(fila.ip ? { ip: fila.ip } : {}) },
		objetivo: {
			id: fila.objetivo_id,
			referencia: fila.referencia,
			idHabitacion: fila.id_habitacion,
			...(fila.estado ? { estado: fila.estado } : {}),
			...(fila.estado_anterior ? { estadoAnterior: fila.estado_anterior } : {})
		},
		...(fila.detalle ? { detalle: fila.detalle } : {}),
		kafka: { topico: fila.topico, particion: fila.particion, offset: fila.offset_kafka },
		registradoEn: new Date(fila.registrado_en).toISOString()
	};
}

export class MySqlAuditoriaRepository implements AuditoriaRepository {
	private esquemaListo?: Promise<void>;

	constructor(private readonly pool: Pool) {}

	private asegurarEsquema(): Promise<void> {
		this.esquemaListo ??= this.pool.query(ESQUEMA).then(() => undefined).catch((error: unknown) => {
			this.esquemaListo = undefined;
			throw error;
		});
		return this.esquemaListo;
	}

	async registrar(registro: RegistroAuditoria): Promise<ResultadoRegistro> {
		await this.asegurarEsquema();
		const [resultado] = await this.pool.execute<ResultSetHeader>(
			`INSERT IGNORE INTO auditoria_eventos (evento_id, tipo, entidad, ocurrido_en, actor_tipo, actor_id, actor_nombre, actor_roles,
				canal, ip, objetivo_id, referencia, id_habitacion, estado, estado_anterior, detalle, topico, particion, offset_kafka, registrado_en)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			[
				registro.eventoId, registro.tipo, registro.entidad, new Date(registro.ocurridoEn), registro.actor.tipo, registro.actor.id,
				registro.actor.nombre ?? null, registro.actor.roles.join(','), registro.origen.canal, registro.origen.ip ?? null,
				registro.objetivo.id, registro.objetivo.referencia, registro.objetivo.idHabitacion, registro.objetivo.estado ?? null,
				registro.objetivo.estadoAnterior ?? null, registro.detalle?.slice(0, 500) ?? null, registro.kafka.topico,
				registro.kafka.particion, registro.kafka.offset, new Date(registro.registradoEn)
			]
		);
		return resultado.affectedRows > 0 ? 'REGISTRADO' : 'DUPLICADO';
	}

	async buscar(filtros: FiltrosAuditoria): Promise<{ registros: RegistroAuditoria[]; total: number }> {
		await this.asegurarEsquema();
		const condiciones: string[] = [];
		const valores: unknown[] = [];
		if (filtros.usuario) {
			condiciones.push('(LOWER(actor_id) = LOWER(?) OR LOWER(actor_nombre) LIKE LOWER(?))');
			valores.push(filtros.usuario, `%${filtros.usuario}%`);
		}
		if (filtros.tipo) { condiciones.push('tipo = ?'); valores.push(filtros.tipo); }
		if (filtros.entidad) { condiciones.push('entidad = ?'); valores.push(filtros.entidad); }
		if (filtros.codigo) { condiciones.push('referencia = ?'); valores.push(filtros.codigo); }
		if (filtros.desde) { condiciones.push('ocurrido_en >= ?'); valores.push(filtros.desde); }
		if (filtros.hasta) { condiciones.push('ocurrido_en <= ?'); valores.push(filtros.hasta); }
		const donde = condiciones.length > 0 ? `WHERE ${condiciones.join(' AND ')}` : '';

		const [[conteo]] = await this.pool.query<RowDataPacket[]>(`SELECT COUNT(*) AS total FROM auditoria_eventos ${donde}`, valores);
		const [filas] = await this.pool.query<FilaAuditoria[]>(
			`SELECT * FROM auditoria_eventos ${donde} ORDER BY ocurrido_en DESC LIMIT ? OFFSET ?`,
			[...valores, filtros.tamano, (filtros.pagina - 1) * filtros.tamano]
		);
		return { registros: filas.map(desdeFila), total: Number(conteo?.total ?? 0) };
	}
}
