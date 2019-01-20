local json = require 'json'


return function(consul)
    return {
        create = function(params)
            params = params or {}
            local query = {
                dc = params.dc,
            }

            local data = {
                LockDelay = params.lock_delay,
                Node = params.node,
                Name = params.name,
                Checks = params.checks,
                Behavior = params.behavior,
                TTL = params.ttl,
            }

            if params.Checks ~= nil then
                assert(type(params.checks) == 'table' and #checks > 0, 'checks must be an array')
            end

            local path = '/v1/session/create'
            local res = consul:http_request('PUT', path, query, json.encode(data))
            if res.status ~= 200 then
                error(string.format('Error from consul: status=%d body=%s', res.status, res.body))
            end
            return res.body.ID
        end,

        destroy = function(uuid, params)
            params = params or {}
            params = {
                dc = params.dc
            }

            assert(uuid ~= nil, 'uuid is required')

            local path = '/v1/session/destroy/' .. uuid
            local res = consul:http_request('PUT', path, params)
            if res.status ~= 200 then
                error(string.format('Error from consul: status=%d body=%s', res.status, res.body))
            end

            if #res.body == 0 then
                return box.NULL
            end
            return res.body[1]
        end,

        info = function(uuid, params)
            params = params or {}
            params = {
                dc = params.dc
            }

            assert(uuid ~= nil, 'uuid is required')

            local path = '/v1/session/info/' .. uuid
            local res = consul:http_request('GET', path, params)
            if res.status ~= 200 then
                error(string.format('Error from consul: status=%d body=%s', res.status, res.body))
            end
            
            if #res.body == 0 then
                return box.NULL
            end
            return res.body[1]
        end,

        list = function(params)
            params = params or {}
            params = {
                dc = params.dc
            }

            local path = '/v1/session/list'
            local res = consul:http_request('GET', path, params)
            if res.status ~= 200 then
                error(string.format('Error from consul: status=%d body=%s', res.status, res.body))
            end
            return res.body
        end,

        node = function(node, params)
            params = params or {}
            params = {
                dc = params.dc
            }

            assert(node ~= nil, 'node is required')

            local path = '/v1/session/node/' .. node
            local res = consul:http_request('GET', path, params)
            if res.status ~= 200 then
                error(string.format('Error from consul: status=%d body=%s', res.status, res.body))
            end
            return res.body
        end,

        renew = function(uuid, params)
            params = params or {}
            params = {
                dc = params.dc
            }

            assert(uuid ~= nil, 'uuid is required')

            local path = '/v1/session/renew/' .. uuid
            local res = consul:http_request('PUT', path, params)
            if res.status ~= 200 then
                error(string.format('Error from consul: status=%d body=%s', res.status, res.body))
            end

            if #res.body == 0 then
                return box.NULL
            end
            return res.body[1]
        end,
    }
end