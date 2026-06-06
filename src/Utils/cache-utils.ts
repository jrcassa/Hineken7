import NodeCache from '@cacheable/node-cache'

const caches = {
	lidCache: new NodeCache({ stdTTL: 3600, checkperiod: 600, useClones: false }),
	devicesCache: new NodeCache({ stdTTL: 1000, checkperiod: 300, useClones: false }),
	sendMessageCache: new NodeCache({ stdTTL: 600, checkperiod: 120, useClones: false, maxKeys: 1000 }),
	groupMetaCache: new NodeCache({ stdTTL: 3600, checkperiod: 120, useClones: false, maxKeys: 1000 }),
	profilePicture: new NodeCache({ stdTTL: 7200, checkperiod: 240, useClones: false, maxKeys: 1000 }),
	onWhatsapp: new NodeCache({ stdTTL: 7200, checkperiod: 240, useClones: false, maxKeys: 1000 }),
	msgRetryCache: new NodeCache({ stdTTL: 3600, checkperiod: 120, useClones: false, maxKeys: 100 }),
	callOfferCache: new NodeCache({ stdTTL: 300, checkperiod: 60, useClones: false, maxKeys: 100 }),
	placeholderResendCache: new NodeCache({ stdTTL: 3600, checkperiod: 120, useClones: false, maxKeys: 100 })
}

export default caches
