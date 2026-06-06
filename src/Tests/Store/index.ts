import type { SQLiteStore } from './UseSqliteStore'
import { SqliteStoreConfig } from '../Types/Store'
import { useSQLiteStoreState } from './UseSqliteStore'
import { stat, rm } from 'fs/promises'
import logger from '../Utils/logger'

let storeInstance: SQLiteStore | null = null

export const initStore = async (config: SqliteStoreConfig): Promise<SQLiteStore> => {
	if (config.resetOnStartUp) {
		const exists = await stat(config.folder).catch(() => false)

		if (exists) {
			logger.debug('Resetando store: removendo pasta', config.folder as any)
			await rm(config.folder, { recursive: true, force: true })
		}
	}

	if (!storeInstance) {
		storeInstance = await useSQLiteStoreState(config)
		if (config.messagesTtll) {
			setInterval(
				async () => {
					try {
						await storeInstance!.cleanupOldMessages()
					} catch (err) {
						logger.error('Erro ao limpar mensagens antigas:', err)
					}
				},
				30 * 60 * 1000
			)
		}
	}
	return storeInstance
}

export const getStore = (): SQLiteStore | undefined => {
	if (!storeInstance) {
		return undefined
	}
	return storeInstance
}
