local json = require 'json'
local yaml = require 'yaml'
local log = require 'log'

local consul = require 'consul'

local SESSION_TTL = '10s'
local SESSION_BEHAVIOR = 'delete'
local MASTER_KEY_POLL_TIMEOUT = '5s'

local M = {}

function M._get_single_key(self, key, params)
    local index, data = self.consul_client.kv.get(key, params)
    if data == nil then
        return index, nil
    end
    if data[1] == nil or data[1].Value == nil then
        error('error locating value')
    end
    local value = data[1].Value
    value = yaml.decode(value)
    return index, value
end

function M._get_session_name(self)
    return string.format('session-%s', self.instance_name)
end

function M._ensure_sid(self)
    if self._sid then return self._sid end

    local sid
    local name = self:_get_session_name()

    -- first try to find existing session by name
    local sessions = self.consul_client.session.list()
    for _, session_obj in ipairs(sessions) do
        if session_obj.Name == name then
            sid = session_obj.ID
            log.info('found existing session with id %s', sid)
            break
        end
    end

    if sid == nil then
        -- if not exists create a new one
        sid = self.consul_client.session.create({
            name = name,
            ttl = SESSION_TTL,
            behavior = SESSION_BEHAVIOR,
            lock_delay = '0.1ms'
        })
    end
    self._sid = sid
    return self._sid
end

function M.get_config(self)
    local index, val = self:_get_single_key(self.prefix .. '/config')
    self._index_config = index
    return val
end

function M.wait_for_changes(self)
    local new_index, _ = self.consul_client.kv.get(self.prefix .. '/config', {
        index = self._index_config,
        wait = '5s',
    })
    local have_changes = new_index ~= self._index_config
    self._index_config = new_index
    return have_changes
end

function M.elect_master(self)
    local sid = self:_ensure_sid()

    local key = self.prefix .. '/_master'
    local value = {
        master = self.instance_name
    }
    log.info('Trying to set master to %s. Session: %s', self.instance_name, sid)
    local acquired = self.consul_client.kv.put(key, json.encode(value), {
        acquire = sid
    })

    if acquired then
        local master_index, _ = self:_get_single_key(key)
        self._index_master = master_index

        log.info('Set master to %s. Session: %s', self.instance_name, sid)
        return value.master
    end

    log.info('Getting master from consul. Session: %s', sid)

    local master_index, value = self:_get_single_key(key)
    if value == nil then
        log.info('Master not found. Session: %s', sid)
        return nil
    end

    self._index_master = master_index

    log.info('Master found: %s. Session: %s', value.master, sid)
    return value.master
end

function M.wait_for_master_change(self)
    local new_index, value = self:_get_single_key(self.prefix .. '/_master', {
        index = self._index_master,
        wait = MASTER_KEY_POLL_TIMEOUT,
    })
    if value == nil and self._index_master ~= nil then
        -- key disappeared
        self._index_master = nil
        return true
    end

    log.info('current master index: %s. new_index: %s', self._index_master, new_index)

    if new_index ~= self._index_master then
        self._index_master = new_index
        return true
    end

    return false
end

function M.send_heartbeat(self)
    if self._sid == nil then
        return
    end

    self.consul_client.session.renew(self._sid)
end

function M.clear(self)
    self._sid = box.NULL
    self._index_config = box.NULL
    self._index_master = box.NULL
end

return function(harper)
    local config = harper.config.consul
    local self = {
        config = config,
        consul_client = consul.new(config),
        prefix = config.prefix,
        instance_name = harper.instance_name,

        _sid = box.NULL,
        _index_config = box.NULL,
        _index_master = box.NULL,
    }

    assert(self.prefix ~= nil, 'prefix is required')
    assert(self.instance_name ~= nil, 'instance_name is required')

    return setmetatable(self, {
        __index = M,
        __tostring = function() 
            return string.format('ConsulBackend prefix=%s sid=%s', self.prefix, self._sid)
        end
    })
end
