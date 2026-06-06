import { open } from 'sqlite'
import sqlite3 from 'sqlite3'
import { mkdir, stat } from 'fs/promises'
import { join } from 'path'
import logger from '../Utils/logger'
import { Contact, GroupMetadata, GroupParticipant, ProcessedContact } from '../Types'
import { BufferJSON } from '../Utils'
import { SqliteStoreConfig } from '../Types/Store'
import { WAMessage } from '../Types'
import { jidDecode } from '../WABinary'

export interface MessageRow {
	id: string
	remoteJid: string
	fromMe: boolean
	participant?: string | null
	jsonMessage: any
	pushName?: string | null
	messageTimestamp?: number | null
	sender_lid?: string | null
	sender_pn?: string | null
	server_id?: string | null
	peer_recipient_pn?: string | null
	participant_lid?: string | null
	participant_pn?: string | null
	isViewOnce?: boolean | null
	peer_recipient_lid?: string | null
}

export type GroupParticipantUpdate = {
	lid: string
	jid?: string
	id?: string
	isAdmin?: string
	superadmin?: boolean
	isSuperAdmin?: boolean
	admin?: boolean
}

export type SQLiteStore = Awaited<ReturnType<typeof useSQLiteStoreState>>

export const useSQLiteStoreState = async (SqliteStoreConfig: SqliteStoreConfig) => {
	const dbPath = join(SqliteStoreConfig.folder, 'store.sqlite')
	const folderInfo = await stat(SqliteStoreConfig.folder).catch(() => undefined)
	if (!folderInfo) await mkdir(SqliteStoreConfig.folder, { recursive: true })
	const walCheckpoint = SqliteStoreConfig.wal_autocheckpoint ?? 1000

	const db = await open({ filename: dbPath, driver: sqlite3.Database })

	// PRAGMA tuning
	await db.exec(`
    PRAGMA journal_mode = WAL;
    PRAGMA synchronous = NORMAL;
    PRAGMA temp_store = MEMORY;
    PRAGMA busy_timeout = 60000;
	PRAGMA wal_autocheckpoint = ${walCheckpoint};
  `)

	// Tables: contacts, groups, group_participants, messages
	await db.exec(`
    CREATE TABLE IF NOT EXISTS contacts (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      lid TEXT,
      jid TEXT,
      name TEXT,
      notify TEXT,
      verifiedName TEXT,
      imgUrl TEXT,
      status TEXT,
      updatedAt INTEGER
    );

    CREATE UNIQUE INDEX IF NOT EXISTS idx_contacts_lid ON contacts(lid);
    CREATE UNIQUE INDEX IF NOT EXISTS idx_contacts_jid ON contacts(jid);

    CREATE TABLE IF NOT EXISTS groups (
      id TEXT PRIMARY KEY,
      addressingMode TEXT,
      owner TEXT,
      ownerJid TEXT,
      owner_country_code TEXT,
      subject TEXT,
      subjectOwner TEXT,
      subjectOwnerJid TEXT,
      subjectTime INTEGER,
      creation INTEGER,
      desc TEXT,
      descOwner TEXT,
      descOwnerJid TEXT,
      descId TEXT,
      descTime INTEGER,
      linkedParent TEXT,
      restrict_flag INTEGER,
      announce_flag INTEGER,
      memberAddMode INTEGER,
      joinApprovalMode INTEGER,
      isCommunity INTEGER,
      isCommunityAnnounce INTEGER,
      size INTEGER,
      participants_json TEXT,
      ephemeralDuration INTEGER,
      inviteCode TEXT,
      author TEXT,
      updatedAt INTEGER,
	  profilePicture TEXT	  
    );

    CREATE TABLE IF NOT EXISTS group_participants (
      group_id TEXT,
      lid TEXT,
      jid TEXT,
      isAdmin INTEGER,
      isSuperAdmin INTEGER,
      admin TEXT,
      PRIMARY KEY(group_id, lid)
    );

    CREATE INDEX IF NOT EXISTS idx_gp_group ON group_participants(group_id);
    CREATE INDEX IF NOT EXISTS idx_gp_participant ON group_participants(lid);

    CREATE TABLE IF NOT EXISTS messages (
      id TEXT PRIMARY KEY,
      remoteJid TEXT,
      fromMe INTEGER,
      participant TEXT,
      participant_lid TEXT,
      jsonMessage TEXT,
      pushName TEXT,
      messageTimestamp INTEGER,
      sender_lid TEXT,
      server_id TEXT,
      sender_pn TEXT,
      peer_recipient_lid TEXT,
      peer_recipient_pn TEXT,     
      createdAt INTEGER
    );

    CREATE INDEX IF NOT EXISTS idx_messages_remoteJid_createdAt ON messages(remoteJid, createdAt DESC);
    CREATE INDEX IF NOT EXISTS idx_messages_server_id ON messages(server_id);

  CREATE TABLE IF NOT EXISTS devices (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  key TEXT NOT NULL,
  device TEXT NOT NULL UNIQUE,
  updatedAt INTEGER
);

CREATE INDEX IF NOT EXISTS idx_devices_key ON devices(key);
CREATE INDEX IF NOT EXISTS idx_devices_device ON devices(device);

CREATE TABLE IF NOT EXISTS devicesControllers (
id INTEGER PRIMARY KEY AUTOINCREMENT,
key TEXT NOT NULL UNIQUE,
updatedAt INTEGER
);   

  `)

	const getContactStmt = await db.prepare(`SELECT * FROM contacts WHERE jid = ? or lid =? LIMIT 1`)
	const deleteContactStmt = await db.prepare(`DELETE FROM contacts WHERE jid = ? or lid =?`)
	const listContactsStmt = await db.prepare(`SELECT * FROM contacts ORDER BY updatedAt DESC LIMIT ? OFFSET ?`)
	const listAllContactsStmt = await db.prepare(`SELECT * FROM contacts ORDER BY updatedAt DESC`)

	const upsertGroupStmt = await db.prepare(
		`INSERT OR REPLACE INTO groups (
      id, addressingMode, owner, ownerJid, owner_country_code, subject, subjectOwner, subjectOwnerJid,
      subjectTime, creation, desc, descOwner, descOwnerJid, descId, descTime, linkedParent, restrict_flag,
      announce_flag, memberAddMode, joinApprovalMode, isCommunity, isCommunityAnnounce, size, participants_json,
      ephemeralDuration, inviteCode, author, updatedAt
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
	)

	const getGroupStmt = await db.prepare(`SELECT * FROM groups WHERE id = ? LIMIT 1`)
	const deleteGroupStmt = await db.prepare(`DELETE FROM groups WHERE id = ?`)

	const upsertGroupParticipantStmt = await db.prepare(
		`INSERT OR REPLACE INTO group_participants (group_id, lid, jid, isAdmin, isSuperAdmin, admin)
     VALUES (?, ?, ?, ?, ?, ?)`
	)
	const deleteGroupParticipantStmt = await db.prepare(
		`DELETE FROM group_participants WHERE group_id = ? AND (lid = ? OR jid = ?)`
	)
	const listGroupParticipantsStmt = await db.prepare(`SELECT * FROM group_participants WHERE group_id = ?`)

	const upsertMessageStmt = await db.prepare(`
  INSERT OR REPLACE INTO messages (
    id, remoteJid, fromMe, participant, participant_lid, jsonMessage, pushName, 
    messageTimestamp, sender_lid, sender_pn, server_id, peer_recipient_lid, 
    peer_recipient_pn, createdAt
  ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
`)
	const getMessageStmt = await db.prepare(`SELECT * FROM messages WHERE id = ? LIMIT 1`)
	const deleteMessageStmt = await db.prepare(`DELETE FROM messages WHERE id = ?`)
	const listMessagesByJidStmt = await db.prepare(
		`SELECT * FROM messages WHERE remoteJid = ? ORDER BY createdAt DESC LIMIT ? OFFSET ?`
	)

	// helper serialization
	const serialize = (obj: any) => JSON.stringify(obj)
	const deserialize = (text: string | null) => (text ? JSON.parse(text) : null)

	const verifyDevices = await db.prepare(`
    SELECT id, key, updatedAt
    FROM devicesControllers
    WHERE key = ?
    ORDER BY updatedAt DESC
  `)

	const getContactByJid = await db.prepare('SELECT * FROM contacts WHERE jid = ?')
	const getgroupProfilePicture = await db.prepare('SELECT * FROM groups WHERE id = ?')

	const upsertDeviceStmt = await db.prepare(`
    INSERT INTO devices (key, device, updatedAt)
    VALUES (?, ?, ?)
    ON CONFLICT(device) DO UPDATE SET 
      key = excluded.key,
      updatedAt = excluded.updatedAt
  `)
	const refreshDeviceStmt = await db.prepare(`
  INSERT INTO devicesControllers (key, updatedAt)
  VALUES (?, ?)
  ON CONFLICT(key) DO UPDATE SET
    updatedAt = excluded.updatedAt;
`)

	const listDevicesByKeyStmt = await db.prepare(`
    SELECT id, key, device, updatedAt
    FROM devices
    WHERE key = ?
    ORDER BY updatedAt DESC
  `)

	const getDeviceStmt2 = await db.prepare(`
    SELECT id, key, device, updatedAt
    FROM devices
    WHERE device = ?
    LIMIT 1
  `)

	const deleteDeviceStmt = await db.prepare(`
    DELETE FROM devices
    WHERE device = ?
  `)

	const deleteDevicesByKeyStmt = await db.prepare(`
    DELETE FROM devices
    WHERE key = ?
  `)

	// Array com todos os prepared statements para finalização no close()
	const allStatements = [
		getContactStmt,
		deleteContactStmt,
		listContactsStmt,
		listAllContactsStmt,
		upsertGroupStmt,
		getGroupStmt,
		deleteGroupStmt,
		upsertGroupParticipantStmt,
		deleteGroupParticipantStmt,
		listGroupParticipantsStmt,
		upsertMessageStmt,
		getMessageStmt,
		deleteMessageStmt,
		listMessagesByJidStmt,
		verifyDevices,
		getContactByJid,
		getgroupProfilePicture,
		upsertDeviceStmt,
		refreshDeviceStmt,
		listDevicesByKeyStmt,
		getDeviceStmt2,
		deleteDeviceStmt,
		deleteDevicesByKeyStmt
	]

	function buildStoreApi() {
		return {
			db,

			async upsertContact(contact: Contact) {
				const { lid, jid, name, notify, verifiedName, imgUrl, status } = contact
				const now = Date.now()
				const byLid = lid ? await db.get('SELECT * FROM contacts WHERE lid = ?', [lid]) : null
				const byJid = jid ? await db.get('SELECT * FROM contacts WHERE jid = ?', [jid]) : null
				let rowToUpdate: any = null
				if (byLid && byJid) {
					if (byLid.id !== byJid.id) {
						await db.run('DELETE FROM contacts WHERE id = ?', [byLid.id])
					}
					rowToUpdate = byJid || byLid
				} else {
					rowToUpdate = byLid || byJid
				}

				if (rowToUpdate) {
					await db.run(
						`
            UPDATE contacts SET
                lid = COALESCE(?, lid),
                jid = COALESCE(?, jid),
                name = COALESCE(?, name),
                notify = COALESCE(?, notify),
                verifiedName = COALESCE(?, verifiedName),
                imgUrl = COALESCE(?, imgUrl),
                status = COALESCE(?, status),
                updatedAt = ?
            WHERE id = ?
        `,
						[lid, jid, name, notify, verifiedName, imgUrl, status, now, rowToUpdate.id]
					)
				} else {
					await db.run(
						`
            INSERT INTO contacts (lid, jid, name, notify, verifiedName, imgUrl, status, updatedAt)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        `,
						[
							lid || null,
							jid || null,
							name || null,
							notify || null,
							verifiedName || null,
							imgUrl || null,
							status || null,
							now
						]
					)
				}
			},

			async refreshDevices(jid: string) {
				const now = Date.now()
				await refreshDeviceStmt.run(jid, now)
			},

			async getContact(id: string): Promise<Contact | null> {
				const row = await getContactStmt.get(id)
				if (!row) return null
				return {
					id: row.jid,
					lid: row.lid,
					jid: row.jid,
					name: row.name,
					notify: row.notify,
					verifiedName: row.verifiedName,
					imgUrl: row.imgUrl,
					status: row.status
				}
			},

			async deleteContact(id: string) {
				await deleteContactStmt.run(id)
			},

			async listContacts(limit = 50, offset = 0) {
				const rows = await listContactsStmt.all(limit, offset)
				return rows.map((row: any) => ({
					id: row.jid,
					lid: row.lid,
					jid: row.jid,
					name: row.name,
					notify: row.notify,
					verifiedName: row.verifiedName,
					imgUrl: row.imgUrl,
					status: row.status
				}))
			},

			async getContacts() {
				const rows = await listAllContactsStmt.all()
				if (!rows) {
					return []
				}
				return rows.map((row: any) => ({
					id: row.jid,
					lid: row.lid,
					jid: row.jid,
					name: row.name,
					notify: row.notify,
					verifiedName: row.verifiedName,
					imgUrl: row.imgUrl,
					status: row.status
				}))
			},

			async cleanupOldMessages() {
				const ttlDays = SqliteStoreConfig.messagesTtll ?? 15 // padrão: 15 dias
				const cutoff = Date.now() - ttlDays * 24 * 60 * 60 * 1000
				await db.run('DELETE FROM messages WHERE createdAt < ?', [cutoff])
			},

			async upsertGroup(group: GroupMetadata) {
				const participants_json = null
				const now = Date.now()
				await upsertGroupStmt.run(
					group.id,
					group.addressingMode || null,
					group.owner || null,
					group.ownerJid || null,
					group.owner_country_code || null,
					group.subject || null,
					group.subjectOwner || null,
					group.subjectOwnerJid || null,
					group.subjectTime || null,
					group.creation || null,
					group.desc || null,
					group.descOwner || null,
					group.descOwnerJid || null,
					group.descId || null,
					group.descTime || null,
					group.linkedParent || null,
					group.restrict ? 1 : 0,
					group.announce ? 1 : 0,
					group.memberAddMode ? 1 : 0,
					group.joinApprovalMode ? 1 : 0,
					group.isCommunity ? 1 : 0,
					group.isCommunityAnnounce ? 1 : 0,
					group.size || null,
					participants_json,
					group.ephemeralDuration || null,
					group.inviteCode || null,
					group.author || null,
					now
				)
				// also upsert participants table for quicker lookups
				if (group.participants && group.participants.length) {
					await db.exec('BEGIN TRANSACTION')
					try {
						for (const p of group.participants) {
							await upsertGroupParticipantStmt.run(
								group.id,
								p.lid,
								p.jid,
								p.isAdmin ? 1 : 0,
								p.isSuperAdmin ? 1 : 0,
								p.admin || null
							)
						}
						await db.exec('COMMIT')
					} catch (e) {
						await db.exec('ROLLBACK')
						throw e
					}
				}
			},
			async onWhatsapp(jid: string): Promise<ProcessedContact> {
				const ttlMinutes = SqliteStoreConfig.onWhatsappTll ?? 720 //12 horas
				const cutoff = Date.now() - ttlMinutes * 60 * 1000
				const contact = await getContactByJid.all(jid)
				if (!contact || contact.length === 0) {
					return { jid, exists: false, lid: undefined }
				}
				const data = contact[0]
				if (data.updatedAt < cutoff) {
					return { jid, exists: false, lid: undefined }
				}
				return {
					jid: data.jid,
					exists: true,
					lid: data.lid
				}
			},
			async profilePictureUrl(jid: string): Promise<string | undefined> {
				const ttlMinutes = SqliteStoreConfig.profilePictureTll ?? 720 //12 horas
				const cutoff = Date.now() - ttlMinutes * 60 * 1000
				const contact = await getContactByJid.all(jid)
				if (!contact || contact.length === 0) {
					return undefined
				}
				const data = contact[0]
				if (data.updatedAt < cutoff) {
					return undefined
				}
				if (data.imgUrl) {
					return data.imgUrl
				}
				return undefined
			},
			async groupProfilePictureUrl(jid: string): Promise<string | undefined> {
				const ttlMinutes = SqliteStoreConfig.profilePictureTll ?? 720 //12 horas
				const cutoff = Date.now() - ttlMinutes * 60 * 1000
				const group = await getgroupProfilePicture.all(jid)
				if (!group || group.length === 0) {
					return undefined
				}
				const data = group[0]
				if (data.updatedAt < cutoff) {
					return undefined
				}
				if (data.profilePicture) {
					return data.profilePicture
				}
				return undefined
			},

			async updateGroupProfilePicture(jid: string, profilePicture: string) {
				const updatedAt = Date.now()
				await db.run(`UPDATE groups SET profilePicture = ?, updatedAt = ? WHERE id = ?`, profilePicture, updatedAt, jid)
			},

			async getAllGroupsWithParticipants(): Promise<GroupMetadata[]> {
				const groupsRows = await db.all(`
        SELECT *
        FROM groups
    `)

				const participantsRows = await db.all(`
        SELECT *
        FROM group_participants
    `)

				const participantsByGroup: Record<string, GroupParticipant[]> = {}

				for (const row of participantsRows) {
					if (!participantsByGroup[row.group_id]) {
						participantsByGroup[row.group_id] = []
					}
					participantsByGroup[row.group_id].push({
						id: row.lid,
						lid: row.lid,
						jid: row.jid,
						isAdmin: !!row.isAdmin,
						isSuperAdmin: !!row.isSuperAdmin,
						admin: row.admin || null
					})
				}

				const result: GroupMetadata[] = []

				for (const g of groupsRows) {
					const meta: GroupMetadata = {
						id: g.id,
						addressingMode: g.addressingMode,
						owner: g.owner || undefined,
						ownerJid: g.ownerJid || undefined,
						owner_country_code: g.owner_country_code,
						subject: g.subject,
						subjectOwner: g.subjectOwner || undefined,
						subjectOwnerJid: g.subjectOwnerJid || undefined,
						subjectTime: g.subjectTime || undefined,
						creation: g.creation || undefined,
						desc: g.desc || undefined,
						descOwner: g.descOwner || undefined,
						descOwnerJid: g.descOwnerJid || undefined,
						descId: g.descId || undefined,
						descTime: g.descTime || undefined,
						linkedParent: g.linkedParent || undefined,
						restrict: !!g.restrict_flag,
						announce: !!g.announce_flag,
						memberAddMode: !!g.memberAddMode,
						joinApprovalMode: !!g.joinApprovalMode,
						isCommunity: !!g.isCommunity,
						isCommunityAnnounce: !!g.isCommunityAnnounce,
						size: g.size || undefined,
						participants: participantsByGroup[g.id] || [],
						ephemeralDuration: g.ephemeralDuration || undefined,
						inviteCode: g.inviteCode || undefined,
						author: g.author || undefined
					}

					result.push(meta)
				}

				return result
			},

			async getGroup(id: string): Promise<GroupMetadata | null> {
				const row = await getGroupStmt.get(id)
				if (!row) return null
				const ttlMinutes = SqliteStoreConfig.groupMetaDataTll ?? 720 //12 horas
				const cutoff = Date.now() - ttlMinutes * 60 * 1000
				if (row.updatedAt < cutoff) {
					return null
				}
				const participantsRows = await db.all(`SELECT * FROM group_participants WHERE group_id = ?`, [id])

				const participants: GroupParticipant[] = participantsRows.map(p => ({
					id: p.lid,
					lid: p.lid,
					jid: p.jid,
					isAdmin: !!p.isAdmin,
					isSuperAdmin: !!p.isSuperAdmin,
					admin: p.admin || null
				}))
				return {
					id: row.id,
					addressingMode: row.addressingMode,
					owner: row.owner,
					ownerJid: row.ownerJid,
					owner_country_code: row.owner_country_code,
					subject: row.subject,
					subjectOwner: row.subjectOwner,
					subjectOwnerJid: row.subjectOwnerJid,
					subjectTime: row.subjectTime,
					creation: row.creation,
					desc: row.desc,
					descOwner: row.descOwner,
					descOwnerJid: row.descOwnerJid,
					descId: row.descId,
					descTime: row.descTime,
					linkedParent: row.linkedParent,
					restrict: !!row.restrict_flag,
					announce: !!row.announce_flag,
					memberAddMode: !!row.memberAddMode,
					joinApprovalMode: !!row.joinApprovalMode,
					isCommunity: !!row.isCommunity,
					isCommunityAnnounce: !!row.isCommunityAnnounce,
					size: row.size,
					participants,
					ephemeralDuration: row.ephemeralDuration,
					inviteCode: row.inviteCode,
					author: row.author
				}
			},

			async deleteGroup(id: string) {
				await deleteGroupStmt.run(id)
				await db.exec(`DELETE FROM group_participants WHERE group_id = '${id}'`)
			},

			async listGroupParticipants(groupId: string) {
				const rows = await listGroupParticipantsStmt.all(groupId)
				return rows.map((r: any) => ({
					id: r.participant_id,
					isAdmin: !!r.isAdmin,
					isSuperAdmin: !!r.isSuperAdmin,
					admin: r.admin
				}))
			},

			async upsertGroupParticipant(groupId: string, participant: GroupParticipantUpdate) {
				await upsertGroupParticipantStmt.run(
					groupId,
					participant.id,
					participant.isAdmin ? 1 : 0,
					participant.isSuperAdmin ? 1 : 0,
					participant.admin || null
				)
			},

			async removeGroupParticipant(groupId: string, participantId: string) {
				await deleteGroupParticipantStmt.run(groupId, participantId)
			},

			async flushGroups() {
				await db.exec('BEGIN TRANSACTION')
				try {
					await db.exec('DELETE FROM groups')
					await db.exec('DELETE FROM group_participants')
					await db.exec('COMMIT')
				} catch (e) {
					await db.exec('ROLLBACK')
					throw e
				}
			},
			async exec(sql: string, params: any[] = []) {
				return await db.run(sql, params)
			},

			async query(sql: string, params: any[] = []) {
				return await db.all(sql, params)
			},

			async queryOne(sql: string, params: any[] = []) {
				return await db.get(sql, params)
			},

			async insertMessage(message: WAMessage) {
				const createdAt = Date.now()
				await upsertMessageStmt.run(
					message.key.id,
					message.key.remoteJid,
					message.key.fromMe ? 1 : 0,
					message.key.participant || null,
					message.key.participant_lid || null,
					serialize(message),
					message.pushName || null,
					message.messageTimestamp || null,
					message.key.sender_lid || null,
					message.key.sender_pn || null,
					message.key.server_id || null,
					message.key.peer_recipient_lid || null,
					message.key.peer_recipient_pn || null,
					createdAt
				)
			},

			async getMessage(id: string | undefined | null): Promise<MessageRow | null> {
				const row = await getMessageStmt.get(id)
				if (!row) return null
				return {
					id: row.id,
					remoteJid: row.remoteJid,
					fromMe: !!row.fromMe,
					participant: row.participant,
					jsonMessage: deserialize(row.jsonMessage),
					pushName: row.pushName,
					messageTimestamp: row.messageTimestamp,
					sender_lid: row.sender_lid,
					sender_pn: row.sender_pn,
					server_id: row.server_id
				}
			},

			async deleteMessage(id: string) {
				await deleteMessageStmt.run(id)
			},

			async listMessagesByJid(remoteJid: string, limit = 50, offset = 0) {
				const rows = await listMessagesByJidStmt.all(remoteJid, limit, offset)
				return rows.map((row: any) => ({
					id: row.id,
					remoteJid: row.remoteJid,
					fromMe: !!row.fromMe,
					participant: row.participant,
					jsonMessage: deserialize(row.jsonMessage),
					pushName: row.pushName,
					messageTimestamp: row.messageTimestamp,
					sender_lid: row.sender_lid,
					sender_pn: row.sender_pn,
					server_id: row.server_id
				}))
			},
			async upsertDevice(key: string, device: string) {
				const updatedAt = Date.now()
				await upsertDeviceStmt.run(key, device, updatedAt)
				await db.run(`UPDATE devices SET updatedAt = ? WHERE key = ?`, updatedAt, key)
			},

			async getDevices(key: string) {
				const verify = await verifyDevices.all(key)
				if (!verify.length) {
					return []
				}
				const ttlMinutes = SqliteStoreConfig.devicesTll ?? 30 //30 minutos
				const cutoff = Date.now() - ttlMinutes * 60 * 1000
				const newestUpdate = verify[0].updatedAt
				if (newestUpdate < cutoff) {
					return []
				}
				const rows = await listDevicesByKeyStmt.all(key)
				if (!rows.length) return []
				return rows.map(row => {
					const { user, device, server } = jidDecode(row.device)!
					return {
						user,
						device,
						jid: row.key
					}
				})
			},

			async getDevice(device: string) {
				const row = await getDeviceStmt2.get(device)
				if (!row) return null
				return {
					id: row.id,
					key: row.key,
					device: row.device,
					updatedAt: row.updatedAt
				}
			},

			async removeDevice(device: string) {
				const row = await getDeviceStmt2.get(device)
				if (!row) return
				const key = row.key
				const now = Date.now()
				await deleteDeviceStmt.run(device)
				await db.run(`UPDATE devices SET updatedAt = ? WHERE key = ?`, now, key)
			},

			async removeDevicesByKey(key: string) {
				await deleteDevicesByKeyStmt.run(key)
			},

			async clearStore() {
				await db.exec('BEGIN TRANSACTION')
				try {
					await db.exec('DELETE FROM messages')
					await db.exec('DELETE FROM group_participants')
					await db.exec('DELETE FROM groups')
					await db.exec('DELETE FROM contacts')
					await db.exec('DELETE FROM devices')
					await db.exec('DELETE FROM devicesControllers')
					await db.exec('COMMIT')
				} catch (e) {
					await db.exec('ROLLBACK')
					throw e
				}
			},

			async close() {
				// Finaliza todos os prepared statements para liberar locks do SQLite
				logger.info(`Finalizando ${allStatements.length} prepared statements...`)
				try {
					for (const stmt of allStatements) {
						try {
							await stmt.finalize()
						} catch (e) {
							// Ignora erros de statements já finalizados
						}
					}
					logger.info('Statements finalizados')
				} finally {
					try {
						await db.close()
						logger.info('Store sqlite fechado com sucesso')
					} catch (e) {
						logger.warn(`Store sqlite erro ao fechar: ${(e as Error).message}`)
					}
				}
			}
		}
	}

	return buildStoreApi()
}
