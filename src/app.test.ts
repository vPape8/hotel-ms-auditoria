import assert from 'node:assert/strict';
import { AddressInfo } from 'node:net';
import test from 'node:test';
import { RequestHandler } from 'express';
import { crearApp } from './app';
import { AuditoriaListener } from './consumers/AuditoriaListener';
import { HabitacionEventoDto } from './messaging/eventoCatalogo';
import { ReservaEventoDto } from './messaging/eventoReserva';
import { listenersKafkaRegistrados } from './messaging/KafkaListener';
import { InMemoryAuditoriaRepository, RegistroAuditoria } from './repositories/auditoriaRepository';

function evento(eventoId: string, tipo: ReservaEventoDto['tipo'], actor: ReservaEventoDto['actor'], ocurridoEn: string, codigo = 'RES-1'): ReservaEventoDto {
	return {
		eventoId,
		tipo,
		version: 1,
		ocurridoEn,
		actor,
		origen: { canal: actor.tipo === 'INVITADO' ? 'WEB_HUESPEDES' : 'PANEL_ADMIN', ip: '200.1.2.3' },
		reserva: {
			id: `id-${codigo}`, codigo, idHabitacion: 'HAB-101', estado: tipo === 'RESERVA_CREADA' ? 'PENDIENTE' : 'CONFIRMADA',
			...(tipo === 'RESERVA_CREADA' ? {} : { estadoAnterior: 'PENDIENTE' as const }),
			fechaEntrada: '2026-12-01T15:00:00.000Z', fechaSalida: '2026-12-03T11:00:00.000Z', noches: 2, cantidadHuespedes: 2, precioTotal: 200
		}
	};
}

const invitado = { tipo: 'INVITADO' as const, id: 'invitado', roles: [] };
const operador = { tipo: 'INTERNO' as const, id: 'oid-operador', nombre: 'Operador Hotel', roles: ['EMPLOYEE'] };

test('auditoría: registra quién, qué, cuándo y desde dónde; filtra; ignora repetidos', async (t) => {
	const repositorio = new InMemoryAuditoriaRepository();
	new AuditoriaListener(repositorio);
	const listener = listenersKafkaRegistrados().find((registrado) => registrado.groupId === 'auditoria')!;
	const listenerCatalogo = listenersKafkaRegistrados().find((registrado) => registrado.groupId === 'auditoria-catalogo')!;
	assert.equal(listener.concurrency, 1);
	assert.deepEqual(listenerCatalogo.topics, ['catalogo']);

	const contexto = (offset: string) => ({ topico: 'reservas', particion: 2, offset, headers: {}, miembro: 0 });
	await listener.manejar(evento('e1', 'RESERVA_CREADA', invitado, '2026-11-01T10:00:00.000Z'), contexto('0'));
	await listener.manejar(evento('e2', 'RESERVA_CONFIRMADA', operador, '2026-11-02T09:00:00.000Z'), contexto('1'));
	await listener.manejar(evento('e3', 'RESERVA_CREADA', invitado, '2026-11-03T12:00:00.000Z', 'RES-2'), contexto('2'));
	await listener.manejar(evento('e1', 'RESERVA_CREADA', invitado, '2026-11-01T10:00:00.000Z'), contexto('0'));
	assert.equal(repositorio.registros.size, 3);

	// Cambio de precio hecho por un admin en el catálogo (tópico "catalogo", grupo "auditoria-catalogo").
	const cambioPrecio: HabitacionEventoDto = {
		eventoId: 'c1', tipo: 'HABITACION_ACTUALIZADA', ocurridoEn: '2026-11-04T08:00:00.000Z',
		actor: { tipo: 'INTERNO', id: 'oid-admin', nombre: 'Admin Hotel', roles: ['ADMIN'] },
		origen: { canal: 'PANEL_ADMIN', ip: '200.1.2.3' },
		habitacion: { id: 'vpo-001-room-1', idHotel: 'vpo-001', nombre: 'Habitación estándar', precioPorNoche: 260000, unidades: 2, activa: true, ocupaciones: [] },
		cambios: [{ campo: 'precioPorNoche', antes: 248000, despues: 260000 }]
	};
	await listenerCatalogo.manejar(listenerCatalogo.validar(JSON.parse(JSON.stringify(cambioPrecio))), { ...contexto('9'), topico: 'catalogo' });
	assert.equal(repositorio.registros.get('c1')?.detalle, 'precioPorNoche: 248000 → 260000');
	assert.equal(repositorio.registros.get('c1')?.entidad, 'HABITACION');
	assert.deepEqual(repositorio.registros.get('e2')?.kafka, { topico: 'reservas', particion: 2, offset: '1' });

	const autenticacion: RequestHandler = (solicitud, respuesta, siguiente) => {
		if (solicitud.header('authorization') !== 'Bearer admin') {
			respuesta.status(401).json({ error: 'TOKEN_REQUERIDO' });
			return;
		}
		siguiente();
	};
	const servidor = crearApp({ repositorio, autenticacion }).listen(0);
	await new Promise<void>((resolve) => servidor.once('listening', resolve));
	t.after(() => servidor.close());
	const base = `http://127.0.0.1:${(servidor.address() as AddressInfo).port}/api/v1/auditoria`;
	const admin = { headers: { Authorization: 'Bearer admin' } };
	const buscar = async (query: string) => (await (await fetch(`${base}${query}`, admin)).json()) as { data: RegistroAuditoria[]; total: number };

	assert.equal((await fetch(base)).status, 401);
	const todo = await buscar('');
	assert.equal(todo.total, 4);
	assert.equal(todo.data[0]?.eventoId, 'c1');
	assert.equal((await buscar('?entidad=RESERVA')).total, 3);
	assert.equal((await buscar('?entidad=HABITACION&usuario=admin')).data[0]?.objetivo.referencia, 'vpo-001-room-1');
	assert.equal((await fetch(`${base}?entidad=PAGO`, admin)).status, 400);
	assert.equal((await buscar('?usuario=operador')).data[0]?.actor.nombre, 'Operador Hotel');
	assert.equal((await buscar('?usuario=invitado')).total, 2);
	assert.equal((await buscar('?tipo=RESERVA_CONFIRMADA')).data[0]?.objetivo.estadoAnterior, 'PENDIENTE');
	assert.equal((await buscar('?codigo=RES-2')).total, 1);
	assert.equal((await buscar('?desde=2026-11-02&hasta=2026-11-02')).total, 1);
	assert.equal((await fetch(`${base}?tipo=OTRO`, admin)).status, 400);
	assert.equal((await fetch(`${base}?desde=2026/11/02`, admin)).status, 400);
});
