import express, { Express, NextFunction, Request, RequestHandler, Response } from 'express';
import { AuditoriaController } from './controllers/auditoriaController';
import { entraAuthMiddleware } from './middlewares/authMiddleware';
import { AuditoriaRepository } from './repositories/auditoriaRepository';

export interface OpcionesApp {
	repositorio: AuditoriaRepository;
	autenticacion?: RequestHandler;
	/** Estado de los consumidores de Kafka para /health. */
	kafkaConectado?: () => boolean;
}

export function crearApp(opciones: OpcionesApp): Express {
	const controlador = new AuditoriaController(opciones.repositorio);
	const autenticacion = opciones.autenticacion ?? entraAuthMiddleware;
	const app = express();

	app.get('/health', (_solicitud, respuesta) => {
		const kafka = opciones.kafkaConectado?.() ?? false;
		respuesta.json({ status: 'ok', kafka: kafka ? 'conectado' : 'desconectado' });
	});

	// Auditoría: solo Admin (el middleware exige el app role ADMIN, salvo ENTRA_EXIGIR_ROL=false).
	app.get('/api/v1/auditoria', autenticacion, controlador.buscar);

	app.use((_solicitud, respuesta) => {
		respuesta.status(404).json({ error: 'RUTA_NO_ENCONTRADA' });
	});

	app.use((_error: unknown, _solicitud: Request, respuesta: Response, _siguiente: NextFunction) => {
		respuesta.status(500).json({ error: 'ERROR_INTERNO' });
	});

	return app;
}
