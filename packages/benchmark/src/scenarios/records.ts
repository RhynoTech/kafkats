import { digits, hex, mulberry32, pick, uuid, type Rng } from './rng.js'

// Synthetic records shaped like the production payloads the scenarios model. Values are
// schemaless Debezium envelopes (value schemas disabled), keys keep their Connect schema.

export interface GeneratedRecord {
	key: Buffer
	value: Buffer
}

const FIRST = [
	'James',
	'Mary',
	'Robert',
	'Patricia',
	'John',
	'Jennifer',
	'Michael',
	'Linda',
	'David',
	'Elizabeth',
].concat(['William', 'Barbara', 'Richard', 'Susan', 'Joseph', 'Jessica', 'Thomas', 'Sarah', 'Chris', 'Karen'])
const LAST = [
	'Smith',
	'Johnson',
	'Williams',
	'Brown',
	'Jones',
	'Garcia',
	'Miller',
	'Davis',
	'Rodriguez',
	'Martinez',
].concat(['Hernandez', 'Lopez', 'Gonzalez', 'Wilson', 'Anderson', 'Thomas', 'Taylor', 'Moore', 'Jackson', 'Martin'])
const CITY = ['Detroit', 'Toronto', 'Philadelphia', 'Indianapolis', 'Columbus', 'Chicago', 'Boston', 'Denver']
const STREET = ['Main St', 'Oak Ave', 'Maple Dr', 'Woodward Ave', 'Cedar Ln', 'Park Blvd', 'Elm St']
const DSNS = ['pistons', 'pacers', 'flyers', 'bluejackets', 'arctics', 'eagles', 'raptors', 'leafs']
const VOCAB = [
	'Season Ticket Holder',
	'Individual',
	'Primary',
	'A',
	'Active',
	'Web',
	'Box Office',
	'Group Sales',
].concat(['Full Season', 'English'])
const TRAIT_VOCAB = [...VOCAB, 'true', 'false', 'Gold', 'Silver', 'Lower Bowl']
const EVENTS = ['Screen Viewed', 'Ticket Scanned', 'Purchase Completed', 'Message Opened', 'App Opened', 'Game Viewed']

/** Workspaces shared by the fans and user-events generators; the first three are hot. */
export const WORKSPACES: readonly string[] = (() => {
	const rng = mulberry32(9001)
	return Array.from({ length: 12 }, () => uuid(rng))
})()
const WORKSPACE_WEIGHTS = [0.35, 0.2, 0.15, ...Array.from({ length: 9 }, () => 0.3 / 9)]

export function pickWorkspace(rng: Rng): number {
	let r = rng()
	for (let i = 0; i < WORKSPACE_WEIGHTS.length; i++) {
		r -= WORKSPACE_WEIGHTS[i]!
		if (r < 0) return i
	}
	return WORKSPACE_WEIGHTS.length - 1
}

const TAG_IDS: readonly string[] = (() => {
	const rng = mulberry32(9002)
	return Array.from({ length: 200 }, () => uuid(rng))
})()

const isoTs = (rng: Rng) => new Date(Date.UTC(2026, 9, 1) + Math.floor(rng() * 2e9)).toISOString()

function envelope(
	rng: Rng,
	o: { before: unknown; after: unknown; op: string; srcName: string; db: string; table: string; tsMs: number }
) {
	const snapshot = o.op === 'r' ? 'true' : 'false'
	return {
		before: o.before,
		after: o.after,
		source: {
			version: '3.2.0.Final',
			connector: 'postgresql',
			name: o.srcName,
			ts_ms: o.tsMs,
			snapshot,
			db: o.db,
			sequence: o.op === 'r' ? `[null,"${digits(rng, 11)}"]` : `["${digits(rng, 11)}","${digits(rng, 11)}"]`,
			ts_us: o.tsMs * 1000,
			ts_ns: o.tsMs * 1000000,
			schema: 'public',
			table: o.table,
			txId: Number(digits(rng, 8)),
			lsn: Number(digits(rng, 11)),
			xmin: null,
		},
		transaction: null,
		op: o.op,
		ts_ms: o.tsMs + 3,
		ts_us: (o.tsMs + 3) * 1000,
		ts_ns: (o.tsMs + 3) * 1000000,
	}
}

// ---- ticketmaster.public.ticketmaster_customers snapshot read (106 columns, half filled)

interface Column {
	name: string
	type: string
	optional: boolean
}

const TM_COLUMNS: readonly Column[] = [
	'acct_id BIGINT NN,dsn TEXT NN,cust_name_id BIGINT,relationship_type TEXT,primary_code TEXT,rec_status TEXT',
	'acct_type_desc TEXT,salutation TEXT,company_name TEXT,name_first TEXT,name_mi TEXT,name_last TEXT,name_title TEXT',
	'street_addr_1 TEXT,street_addr_2 TEXT,city TEXT,state TEXT,zip TEXT,country TEXT,phone_day TEXT,phone_eve TEXT',
	'phone_fax TEXT,tag TEXT,acct_rep_num INT,acct_rep_name TEXT,priority TEXT,points INT,points_ytd INT,points_itd INT',
	'email_addr TEXT,add_date DATE,add_user TEXT,upd_date TIMESTAMPTZ NN,upd_user TEXT,birth_date DATE,since_date DATE',
	'old_acct_id TEXT,household_id INT,fan_loyalty_id INT,ext_system1_id TEXT,ext_system2_id TEXT,other_info_1 TEXT',
	'other_info_2 TEXT,other_info_3 TEXT,other_info_4 TEXT,other_info_5 TEXT,other_info_6 TEXT,other_info_7 TEXT',
	'other_info_8 TEXT,other_info_9 TEXT,other_info_10 TEXT,source TEXT,source_name TEXT,source_desc TEXT,gender TEXT',
	'solicit_phone_day TEXT,solicit_phone_eve TEXT,solicit_phone_fax TEXT,solicit_email TEXT,solicit_addr TEXT',
	'marital_status TEXT,sic_code TEXT,sic_name TEXT,industry TEXT,phone_cell TEXT,solicit_phone_cell TEXT',
	'email_addr2 TEXT,source_list_name TEXT,maiden_name TEXT,name_type TEXT,other_info_11 TEXT,other_info_12 TEXT',
	'other_info_13 TEXT,other_info_14 TEXT,other_info_15 TEXT,other_info_16 TEXT,other_info_17 TEXT,other_info_18 TEXT',
	'other_info_19 TEXT,other_info_20 TEXT,account_manager_access TEXT,admin_invoice TEXT,admin_event_info TEXT',
	'admin_rofr TEXT,admin_donation TEXT,admin_other TEXT,account_nick_name TEXT,main_acct_id_ind TEXT',
	'language_name TEXT,privacy_restrict TEXT,privacy_restrict_upd_user TEXT,privacy_opt_out TEXT',
	'privacy_opt_out_upd_user TEXT,privacy_restrict_upd_datetime TIMESTAMPTZ,privacy_opt_out_upd_datetime TIMESTAMPTZ',
	'privacy_opt_out_source TEXT,ma_first_name TEXT,ma_last_name TEXT,ma_phone TEXT,ma_email TEXT',
	'ma_link_date TIMESTAMPTZ,ma_link_value TEXT,ma_phone_number TEXT,updated_at TIMESTAMPTZ NN',
]
	.join(',')
	.split(',')
	.map(c => {
		const [name, type, nn] = c.split(' ')
		return { name: name!, type: type!, optional: nn !== 'NN' }
	})

const TM_KEY_SCHEMA = {
	type: 'struct',
	fields: [
		{ type: 'string', optional: false, field: 'dsn' },
		{ type: 'int64', optional: false, field: 'acct_id' },
	],
	optional: false,
	name: 'ticketmaster.public.ticketmaster_customers.Key',
}
const TM_FILL = 0.5

const tmTs = (rng: Rng) =>
	new Date(Date.UTC(2026, 9, 20, 6, 0, 0) + Math.floor(rng() * 1e9)).toISOString().replace('.000Z', 'Z')

function tmValue(rng: Rng, c: Column, fn: string, ln: string): unknown {
	switch (c.name) {
		case 'acct_id':
			return Number(digits(rng, 8))
		case 'dsn':
			return pick(rng, DSNS)
		case 'name_first':
		case 'ma_first_name':
			return fn
		case 'name_last':
		case 'ma_last_name':
			return ln
		case 'email_addr':
		case 'ma_email':
			return `${fn}.${ln}${digits(rng, 3)}@example.com`.toLowerCase()
		case 'city':
			return pick(rng, CITY)
		case 'state':
			return pick(rng, ['MI', 'ON', 'PA', 'IN', 'OH', 'IL'])
		case 'zip':
			return digits(rng, 5)
		case 'country':
			return pick(rng, ['USA', 'CAN'])
		case 'street_addr_1':
			return `${digits(rng, 4)} ${pick(rng, STREET)}`
		case 'upd_date':
		case 'updated_at':
			return tmTs(rng)
	}
	if (!c.optional) return pick(rng, DSNS)
	if (rng() > TM_FILL) return null
	switch (c.type) {
		case 'BIGINT':
			return Number(digits(rng, 9))
		case 'INT':
			return Number(digits(rng, Math.ceil(rng() * 5)))
		case 'DATE':
			return 10000 + Math.floor(rng() * 10000)
		case 'TIMESTAMPTZ':
			return tmTs(rng)
	}
	if (c.name.startsWith('phone') || c.name.endsWith('phone') || c.name.endsWith('phone_number'))
		return `(${digits(rng, 3)}) ${digits(rng, 3)}-${digits(rng, 4)}`
	if (c.name.startsWith('solicit') || c.name.startsWith('admin') || c.name.startsWith('privacy'))
		return pick(rng, ['Y', 'N'])
	const i = Math.floor(rng() * (VOCAB.length + 1))
	return i < VOCAB.length ? VOCAB[i] : digits(rng, 6)
}

/** About 3.26 KB decoded (key 260 B, value 2.9 KB); about 3.4x smaller in a snappy batch. */
export function tmCustomer(rng: Rng, seq: number): GeneratedRecord {
	const fn = pick(rng, FIRST)
	const ln = pick(rng, LAST)
	const after: Record<string, unknown> = {}
	for (const c of TM_COLUMNS) after[c.name] = tmValue(rng, c, fn, ln)
	const payload = envelope(rng, {
		before: null,
		after,
		op: 'r',
		srcName: 'ticketmaster',
		db: 'ticketmaster-production',
		table: 'ticketmaster_customers',
		tsMs: 1792476000000 + seq,
	})
	const key = { schema: TM_KEY_SCHEMA, payload: { dsn: after.dsn, acct_id: after.acct_id } }
	return { key: Buffer.from(JSON.stringify(key)), value: Buffer.from(JSON.stringify(payload)) }
}

/** The userID TM processors key their output by; read from the key without parsing it. */
export function tmUserId(key: Buffer | null): string {
	if (!key) return 'unknown'
	const s = key.toString('latin1')
	const at = s.lastIndexOf('"acct_id":')
	return at < 0 ? s : s.slice(at + 10, s.indexOf('}', at))
}

// ---- bobcat-cdc-fans.public.fan_users update (REPLICA IDENTITY FULL, 35 custom traits)

const FAN_TEXT_COLUMNS = ['user_id', 'email', 'name', 'phone', 'owner', 'city', 'dma', 'region', 'country']
	.concat(['country_code', 'continent_code', 'time_zone', 'device_os', 'device_model', 'device_language'])
	.concat(['notification_authorization', 'push_token', 'app_version', 'sdk_version', 'preferred_channel'])
	.concat(['most_active_channel', 'last_ticket_scanned_gate', 'last_ticket_scanned_section'])
	.concat(['last_ticket_scanned_row', 'last_ticket_scanned_seat'])
const FAN_TSTZ_COLUMNS = ['first_seen', 'last_seen', 'last_replied_at', 'last_ticket_scanned_at'].concat([
	'last_received_test_message',
	'birthday',
	'member_since',
])
const FAN_CUSTOM_TRAITS = 35
const FAN_UUID_FIELD = (field: string) => ({
	type: 'string',
	optional: false,
	name: 'io.debezium.data.Uuid',
	version: 1,
	field,
})
const FAN_KEY_SCHEMA = {
	type: 'struct',
	fields: [FAN_UUID_FIELD('id'), FAN_UUID_FIELD('workspace_id')],
	optional: false,
	name: 'bobcat-cdc-fans.public.fan_users.Key',
}
const camel = (s: string) => s.replace(/_([a-z])/g, (_, c: string) => c.toUpperCase())

function fanRow(rng: Rng, workspace: string) {
	const fn = pick(rng, FIRST)
	const ln = pick(rng, LAST)
	const t: Record<string, unknown> = {
		userID: digits(rng, 8),
		email: `${fn}.${ln}${digits(rng, 3)}@example.com`.toLowerCase(),
		name: `${fn} ${ln}`,
		phone: `+1${digits(rng, 10)}`,
		city: pick(rng, CITY),
		region: pick(rng, ['Michigan', 'Ontario', 'Pennsylvania', 'Indiana', 'Ohio', 'Illinois']),
		country: pick(rng, ['United States', 'Canada']),
		countryCode: pick(rng, ['US', 'CA']),
		continentCode: 'NA',
		timeZone: pick(rng, ['America/Detroit', 'America/Toronto', 'America/New_York', 'America/Chicago']),
		firstSeen: isoTs(rng),
		lastSeen: isoTs(rng),
		tags: Array.from({ length: 2 + Math.floor(rng() * 8) }, () => pick(rng, TAG_IDS)),
		subscriptionOptIns: ['sms', 'email'].slice(0, 1 + Math.floor(rng() * 2)),
	}
	if (rng() < 0.5)
		Object.assign(t, {
			deviceOS: pick(rng, ['iOS', 'Android']),
			deviceModel: pick(rng, ['iPhone15,2', 'iPhone14,5', 'SM-S918U', 'Pixel 8']),
			deviceLanguage: 'en',
			notificationAuthorization: pick(rng, ['authorized', 'denied']),
			pushToken: hex(rng, 64),
			appVersion: `4.${digits(rng, 1)}.${digits(rng, 1)}`,
			sdkVersion: `5.${digits(rng, 1)}.${digits(rng, 1)}`,
		})
	if (rng() < 0.3)
		Object.assign(t, {
			lastTicketScannedAt: isoTs(rng),
			lastTicketScannedGate: `Gate ${digits(rng, 1)}`,
			lastTicketScannedSection: digits(rng, 3),
			lastTicketScannedRow: digits(rng, 2),
			lastTicketScannedSeat: digits(rng, 2),
		})
	if (rng() < 0.3) t.birthday = isoTs(rng)
	if (rng() < 0.2) t.memberSince = isoTs(rng)
	for (let i = 0; i < FAN_CUSTOM_TRAITS; i++) {
		const column = TM_COLUMNS[(i % (TM_COLUMNS.length - 4)) + 2]!
		const r = rng()
		t[column.name] =
			r < 0.55
				? pick(rng, TRAIT_VOCAB)
				: r < 0.8
					? Number(digits(rng, 1 + Math.floor(rng() * 6)))
					: r < 0.9
						? isoTs(rng)
						: `${pick(rng, FIRST)} ${pick(rng, LAST)}`
	}
	const updatedAt = Date.UTC(2026, 9, 14, 20, 24) + Math.floor(rng() * 240000)
	const row: Record<string, unknown> = {
		id: uuid(rng),
		workspace_id: workspace,
		traits: JSON.stringify(t),
		updated_at: updatedAt,
		segment_ids: null,
		revision_at: updatedAt,
	}
	for (const c of FAN_TEXT_COLUMNS) row[c] = t[camel(c)] ?? null
	row.user_id = t.userID
	row.device_os = t.deviceOS ?? null
	for (const c of FAN_TSTZ_COLUMNS) row[c] = t[camel(c)] ?? null
	row.is_vip = rng() < 0.05 ? true : null
	row.tags = t.tags
	row.subscription_opt_ins = t.subscriptionOptIns
	row.subscription_opt_outs = null
	return { row, traits: t }
}

/** About 7.1 KB decoded; about 4x smaller in 73-record snappy batches (Connect's batch.size 512 KiB). */
export function fanUser(rng: Rng, seq: number): GeneratedRecord {
	const workspace = WORKSPACES[pickWorkspace(rng)]!
	const { row: after, traits } = fanRow(rng, workspace)
	const beforeTraits = { ...traits, tags: (traits.tags as string[]).slice(1) }
	const before = {
		...after,
		traits: JSON.stringify(beforeTraits),
		tags: beforeTraits.tags,
		updated_at: (after.updated_at as number) - 86400000,
		revision_at: (after.revision_at as number) - 86400000,
	}
	const payload = envelope(rng, {
		before,
		after,
		op: 'u',
		srcName: 'bobcat-cdc-fans',
		db: 'bobcat_fans',
		table: 'fan_users',
		tsMs: 1792009440000 + seq,
	})
	const key = { schema: FAN_KEY_SCHEMA, payload: { id: after.id, workspace_id: workspace } }
	return { key: Buffer.from(JSON.stringify(key)), value: Buffer.from(JSON.stringify(payload)) }
}

// ---- bobcat-user-events enriched event (not Debezium)

export function userEvent(rng: Rng): GeneratedRecord {
	const roverID = uuid(rng)
	const now = new Date().toISOString()
	const event = {
		type: 'track',
		roverID,
		workspaceID: WORKSPACES[pickWorkspace(rng)],
		name: pick(rng, EVENTS),
		timestamp: now,
		receivedAt: now,
		messageID: uuid(rng),
		attributes: { screen: `screen_${hex(rng, 4)}`, value: Math.floor(rng() * 1000), gameID: hex(rng, 10) },
		context: {
			device: {
				id: uuid(rng),
				os: pick(rng, ['iOS', 'Android']),
				osVersion: '17.4',
				appVersion: `5.${Math.floor(rng() * 30)}.0`,
				locale: 'en_US',
				timeZone: 'America/Detroit',
			},
			traits: { firstName: pick(rng, FIRST), lastName: pick(rng, LAST) },
		},
		resolution: { identities: [{ type: 'deviceID', value: uuid(rng) }], resolvedAt: now },
	}
	return { key: Buffer.from(roverID), value: Buffer.from(JSON.stringify(event)) }
}

const WORKSPACE_MARKERS = ['"workspace_id":"', '"workspaceID":"']

/** Index into WORKSPACES of the workspace a fans or user-events record belongs to, read without parsing. */
export function workspaceOf(buf: Buffer | null): number {
	if (!buf) return 0
	for (const marker of WORKSPACE_MARKERS) {
		const at = buf.indexOf(marker)
		if (at >= 0) {
			const ws = buf.toString('latin1', at + marker.length, at + marker.length + 36)
			const index = WORKSPACES.indexOf(ws)
			if (index >= 0) return index
		}
	}
	return 0
}

// ---- ~500 B JSON events for the fairness scenario

const ALNUM = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789'
const PAD_SOURCE = (() => {
	const rng = mulberry32(9003)
	let s = ''
	for (let i = 0; i < 65536; i++) s += ALNUM[Math.floor(rng() * ALNUM.length)]
	return s
})()
const PAD_LENGTH = 330

export function json500(rng: Rng, seq: number, partition: number): GeneratedRecord {
	const off = Math.floor(rng() * (PAD_SOURCE.length - PAD_LENGTH))
	const id = uuid(rng)
	const value =
		`{"id":"${id}","partition":${partition},"seq":${seq},"type":"${pick(rng, EVENTS)}",` +
		`"ts":${1792476000000 + seq},"user":${Math.floor(rng() * 1e6)},"workspace":"${WORKSPACES[pickWorkspace(rng)]}",` +
		`"pad":"${PAD_SOURCE.slice(off, off + PAD_LENGTH)}"}`
	return { key: Buffer.from(id.slice(0, 13)), value: Buffer.from(value) }
}

export function generatorFor(kind: string): (rng: Rng, seq: number, partition: number) => GeneratedRecord {
	switch (kind) {
		case 'json500':
			return json500
		case 'tm':
			return tmCustomer
		case 'fans':
			return fanUser
	}
	throw new Error(`unknown record kind ${kind}`)
}
