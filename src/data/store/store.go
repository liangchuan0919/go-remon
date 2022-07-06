package store

const syncCacheScript = `
	local sync_key = KEYS[1]
	local base_key = ARGV[1]
	local version = ARGV[2]
	if redis.call('hget',sync_key,base_key) == version then
		redis.call('hdel',sync_key,base_key)
		return 1
	end
	return 0
`

const syncCacheScriptByScore = `
	local sync_key = KEYS[1]
	local base_key = ARGV[1]
	local score = ARGV[2]
	if redis.call('zscore',sync_key,base_key) == score then
		redis.call('zrem',sync_key,base_key)
		return 1
	end
	return 0
`

type StoreSync interface {
	Name() string
	IsNeedSync() bool
	Sync() error
}
