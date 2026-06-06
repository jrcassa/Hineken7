import { open, Database } from 'sqlite'
import sqlite3 from 'sqlite3'
import { mkdir, stat, rm } from 'fs/promises'
import { join } from 'path'
import { Mutex } from 'async-mutex'
import { proto } from '../../WAProto'
import { BufferJSON } from './generics'
import { initAuthCreds } from './auth-utils'
import { AuthenticationState, AuthenticationCreds, SignalDataTypeMap, SignalDataSet } from '../Types'
import logger from './logger'

const keyLocks = new Map<string, Mutex>()

const getKeyLock = (key: string): Mutex => {
	let lock = keyLocks.get(key)
	if (!lock) {
		lock = new Mutex()
		keyLocks.set(key, lock)
	}
	return lock
}

export const importSQLiteAuthState = async (
	folder: string,
	sessionData: { creds: AuthenticationCreds; keys: SignalDataSet }
): Promise<void> => {
	const dbPath = join(folder, 'db.sqlite')

	const folderInfo = await stat(folder).catch(() => undefined)
	if (!folderInfo) await mkdir(folder, { recursive: true })

	const db = await open({ filename: dbPath, driver: sqlite3.Database })

	await db.exec(`
	  CREATE TABLE IF NOT EXISTS auth_data (
		key TEXT PRIMARY KEY,
		value TEXT
	  );
	`)

	const insertStmt = await db.prepare(`INSERT OR REPLACE INTO auth_data (key, value) VALUES (?, ?)`)

	const writeData = async (key: string, data: any) => {
		const value = JSON.stringify(data, BufferJSON.replacer)
		await insertStmt.run(key, value)
	}

	try {
		// Salvar credenciais
		await writeData('creds', sessionData.creds)

		// Salvar chaves
		await db.exec('BEGIN TRANSACTION')
		try {
			for (const category in sessionData.keys) {
				for (const id in sessionData.keys[category]) {
					const value = sessionData.keys[category][id]
					const key = `${category}-${id}`
					if (value) await writeData(key, value)
				}
			}
			await db.exec('COMMIT')
		} catch (e) {
			await db.exec('ROLLBACK')
			throw e
		}
	} finally {
		await insertStmt.finalize()
		await db.close()
	}
}

export const useSQLiteAuthState = async (
	folder: string
): Promise<{
	state: AuthenticationState
	saveCreds: () => Promise<void>
	close: () => Promise<void>
}> => {
	const dbPath = join(folder, 'db.sqlite')

	const folderInfo = await stat(folder).catch(() => undefined)
	if (!folderInfo) await mkdir(folder, { recursive: true })
	const db: Database = await open({
		filename: dbPath,
		driver: sqlite3.Database
	})

	await db.exec(`
    PRAGMA journal_mode = WAL;
    PRAGMA synchronous = NORMAL;
    PRAGMA temp_store = MEMORY;
    PRAGMA busy_timeout = 60000;
    PRAGMA wal_autocheckpoint = 10000;
  `)

	await db.exec(`
    CREATE TABLE IF NOT EXISTS auth_data (
      key TEXT PRIMARY KEY,
      value TEXT
    );
  `)

	const insertStmt = await db.prepare(`INSERT OR REPLACE INTO auth_data (key, value) VALUES (?, ?)`)
	const deleteStmt = await db.prepare(`DELETE FROM auth_data WHERE key = ?`)

	// ------------------------------
	// WRITE PROTEGIDO POR MUTEX
	// ------------------------------
	const safeWrite = async (key: string, value: any) => {
		const lock = getKeyLock(key)

		return lock.runExclusive(async () => {
			const json = JSON.stringify(value, BufferJSON.replacer)

			try {
				await insertStmt.run(key, json)
			} catch (err) {
				logger.error(`Erro em write sqlite (${key}):`, err)
				throw err
			}
		})
	}

	const safeRead = async (key: string) => {
		const lock = getKeyLock(key)

		return lock.runExclusive(async () => {
			const row = await db.get(`SELECT value FROM auth_data WHERE key = ?`, [key])

			if (!row) return null

			try {
				return JSON.parse(row.value, BufferJSON.reviver)
			} catch {
				return null
			}
		})
	}

	const safeRemove = async (key: string) => {
		const lock = getKeyLock(key)

		return lock.runExclusive(async () => {
			try {
				await deleteStmt.run(key)
			} catch (err) {
				logger.error(`Erro ao remover key sqlite (${key}):`, err)
			}
		})
	}

	const safeRemoveByPattern = async (pattern: string) => {
		const lock = getKeyLock(`pattern-${pattern}`)

		return lock.runExclusive(async () => {
			try {
				await db.run(`DELETE FROM auth_data WHERE key LIKE ?`, [pattern])
			} catch (err) {
				logger.error(`Erro ao remover pattern sqlite (${pattern}):`, err)
			}
		})
	}

	const creds: AuthenticationCreds = (await safeRead('creds')) || initAuthCreds()

	const close = async () => {
		try {
			await insertStmt.finalize()
			await deleteStmt.finalize()
			await db.close()
			logger.info(`SQLite store fechado: ${dbPath}`)
		} catch (err) {
			logger.error(`Erro ao fechar store sqlite:`, err)
		}
	}

	return {
		state: {
			creds,

			keys: {
				get: async (type, ids) => {
					const data: {
						[_: string]: SignalDataTypeMap[typeof type]
					} = {}

					await Promise.all(
						ids.map(async id => {
							const key = `${type}-${id}`
							let value = await safeRead(key)

							if (type === 'app-state-sync-key' && value) {
								value = proto.Message.AppStateSyncKeyData.fromObject(value)
							}

							data[id] = value
						})
					)

					return data
				},

				set: async patch => {
					const tasks: Promise<any>[] = []

					for (const category in patch) {
						for (const id in patch[category]) {
							const key = `${category}-${id}`
							const value = patch[category][id]

							tasks.push(value ? safeWrite(key, value) : safeRemove(key))
						}
					}

					await Promise.all(tasks)
				},
				remove: async (type, ids) => {
					const data: {
						[_: string]: SignalDataTypeMap[typeof type]
					} = {}

					await Promise.all(
						ids.map(async id => {
							const key = `${type}-${id}`
							const deletet = await safeRemove(key)
							if (type === 'session') {
								const beforeAt = String(id).split('@')[0]

								const senderKeyId = beforeAt.includes('.') ? beforeAt.replace('.', '::') : `${beforeAt}::0`

								await safeRemoveByPattern(senderKeyId)
							}
						})
					)
				}
			}
		},

		saveCreds: async () => {
			await safeWrite('creds', creds)
		},

		close
	}
}

export const deleteSQLiteAuthState = async (folder: string) => {
	try {
		await rm(folder, { recursive: true, force: true })
		logger.info(`Sessão removida: ${folder}`)
	} catch (err) {
		logger.error(`Erro ao remover sessão:`, err)
	}
}
