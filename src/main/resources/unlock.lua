---@diagnostic disable: undefined-global
-- unlock.lua
-- 比较线程标识和锁中的标识是否一致
if redis.call('get', KEYS[1]) == ARGV[1] then
    -- 一致，释放锁
    return redis.call('del', KEYS[1])
end
-- 不一致，不删锁，返回0
return 0

