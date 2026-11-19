import { kafkaConfig } from '../config/kafka.config';
import { log } from '../logs/logger';
import { HabitacionEventoDto, validarEventoCatalogo } from '../messaging/eventoCatalogo';
import { ReservaEventoDto, validarEventoReserva } from '../messaging/eventoReserva';
import { ContextoRegistro, KafkaListener } from '../messaging/KafkaListener';
import { AuditoriaRepository, desdeEventoCatalogo, desdeEventoReserva, RegistroAuditoria } from '../repositories/auditoriaRepository';

/**
 * Trazabilidad: registra cada evento de los tópicos "reservas" y "catalogo" con quién, qué, cuándo y desde dónde.
 * Grupo "auditoria": lee los mismos eventos de reservas que "reportes", pero con su propia marca.
 * Grupo "auditoria-catalogo": lee los cambios del catálogo (precios, unidades, disponibilidad).
 */
export class AuditoriaListener {
	constructor(private readonly repositorio: AuditoriaRepository) {}

	@KafkaListener({
		topics: [kafkaConfig.topicos.reservas],
		groupId: kafkaConfig.grupos.auditoria,
		concurrency: kafkaConfig.concurrencia,
		validar: validarEventoReserva
	})
	async registrarReserva(evento: ReservaEventoDto, contexto: ContextoRegistro): Promise<void> {
		await this.guardar(desdeEventoReserva(evento, { topico: contexto.topico, particion: contexto.particion, offset: contexto.offset }));
	}

	@KafkaListener({
		topics: [kafkaConfig.topicos.catalogo],
		groupId: kafkaConfig.grupos.auditoriaCatalogo,
		concurrency: kafkaConfig.concurrencia,
		validar: validarEventoCatalogo
	})
	async registrarCatalogo(evento: HabitacionEventoDto, contexto: ContextoRegistro): Promise<void> {
		await this.guardar(desdeEventoCatalogo(evento, { topico: contexto.topico, particion: contexto.particion, offset: contexto.offset }));
	}

	private async guardar(registro: RegistroAuditoria): Promise<void> {
		const resultado = await this.repositorio.registrar(registro);
		const datos = { eventoId: registro.eventoId, tipo: registro.tipo, referencia: registro.objetivo.referencia, actor: registro.actor.id, canal: registro.origen.canal };
		if (resultado === 'DUPLICADO') {
			log('warn', '[IDEMP]', { ...datos, detalle: 'evento ya registrado, se ignora' });
			return;
		}
		log('info', '[AUDITA]', datos);
	}
}
