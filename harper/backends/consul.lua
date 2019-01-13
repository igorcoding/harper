local json = require 'json'
local yaml = require 'yaml'

local consul = require 'harper.consul'

return function(harper)
    local backend_config = harper.config.consul
    local consul_client = consul.new(backend_config)
    local prefix = backend_config.prefix
    assert(prefix ~= nil, 'prefix is required')

    local function get_single_key(key)
        local index, data = consul_client.kv.get(key)
        if data == nil or data[1] == nil or data[1].Value == nil then
            error('not found common config')
        end
        local value = data[1].Value
        value = yaml.decode(value)
        return index, value
    end

    return {
        client = consul_client,

        get_config = function(context)
            context = context or {}
            local index, val = get_single_key(prefix .. '/config')
            context.index = index
            return context, val
        end,

        wait_for_changes = function(context)
            context = context or {}
            local new_index, _ = consul_client.kv.get(prefix .. '/config', {
                index = context.index,
                wait = '10s',
            })
            local have_changes = new_index ~= context.index
            context.index = new_index
            return context, have_changes
        end,
    }
end