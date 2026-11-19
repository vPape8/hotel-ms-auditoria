import { kafkaConfig, topicoDlt } from '../config/kafka.config';
import { log } from '../logs/logger';
import { ContextoRegistro, KafkaListener } from '../messaging/KafkaListener';

/**
 * Lee el dead-letter topic y registra cada mensaje muerto con su motivo (como verMuertos / cobros-dlt en la guía 3.3.3).
 * Los DLT los comparten varios grupos: este listener solo registra los que mandaron los grupos de auditoría.
 */
export class DltListener {
	@KafkaListener({
		topics: [topicoDlt(kafkaConfig.topicos.reservas), topicoDlt(kafkaConfig.topicos.catalogo)],
		groupId: kafkaConfig.grupos.auditoriaDlt,
		enviarADlt: false
	})
	async verMuertos(cuerpo: unknown, contexto: ContextoRegistro): Promise<void> {
		const grupoOriginal = contexto.headers['kafka_original-consumer-group'];
		if (grupoOriginal !== kafkaConfig.grupos.auditoria && grupoOriginal !== kafkaConfig.grupos.auditoriaCatalogo) {
			return;
		}
		const evento = (typeof cuerpo === 'object' && cuerpo !== null ? cuerpo : {}) as { eventoId?: unknown; tipo?: unknown };
		log('error', '[DLT]', {
			key: contexto.key,
			particion: contexto.particion,
			offset: contexto.offset,
			eventoId: evento.eventoId,
			tipo: evento.tipo,
			topicOriginal: contexto.headers['kafka_original-topic'],
			offsetOriginal: contexto.headers['kafka_original-offset'],
			excepcion: contexto.headers['kafka_exception-fqcn'],
			motivo: contexto.headers['kafka_exception-message'],
			intentos: contexto.headers.kafka_intentos
		});
	}
}
