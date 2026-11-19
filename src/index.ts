import 'dotenv/config';
import { crearApp } from './app';
import { crearPoolBaseDatos } from './config/database';
import { kafkaConfig } from './config/kafka.config';
import { AuditoriaListener } from './consumers/AuditoriaListener';
import { DltListener } from './consumers/DltListener';
import { log } from './logs/logger';
import { ContenedorKafka } from './messaging/ContenedorKafka';
import { listenersKafkaRegistrados } from './messaging/KafkaListener';
import { AuditoriaRepository, InMemoryAuditoriaRepository, MySqlAuditoriaRepository } from './repositories/auditoriaRepository';

const puerto = Number(process.env.PORT ?? 8087);
const pool = crearPoolBaseDatos();
const repositorio: AuditoriaRepository = pool ? new MySqlAuditoriaRepository(pool) : new InMemoryAuditoriaRepository();
if (!pool) {
	log('warn', 'AUDITORIA_EN_MEMORIA', { detalle: 'MYSQL_HOST no definido' });
}

// Cada instancia registra sus métodos @KafkaListener; el contenedor los suscribe al conectar.
new AuditoriaListener(repositorio);
new DltListener();

const contenedor = kafkaConfig.brokers.length > 0
	? new ContenedorKafka([...kafkaConfig.brokers], listenersKafkaRegistrados)
	: undefined;
if (!contenedor) {
	log('warn', 'KAFKA_DESACTIVADO', { detalle: 'KAFKA_BROKERS no definido' });
}

const servidor = crearApp({ repositorio, kafkaConectado: () => contenedor?.estaConectado ?? false }).listen(puerto, () => {
	log('info', 'SERVICIO_INICIADO', { puerto });
	void contenedor?.iniciar();
});

for (const senal of ['SIGINT', 'SIGTERM'] as const) {
	process.once(senal, () => {
		servidor.close();
		void Promise.all([contenedor?.detener(), pool?.end()]).finally(() => process.exit(0));
	});
}
