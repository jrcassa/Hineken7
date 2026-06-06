export type SqliteStoreConfig = {
	folder: string
	messagesTtll?: number
	devicesTll?: number
	onWhatsappTll?: number
	resetOnStartUp?: boolean
	profilePictureTll?: number
	groupMetaDataTll?: number
	wal_autocheckpoint?: number
}
