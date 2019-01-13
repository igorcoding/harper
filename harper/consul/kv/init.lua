local M = {}


return function(consul)
    return {
        get = function(key, params)
            assert(key ~= nil, 'key is required')
            params = params or {}
            params = {
                index = params.index,
                wait = params.wait,

                dc = params.dc,
                recurse = params.recurse,
                raw = params.raw,
                keys = params.keys,
                separator = params.separator,
            }

            local path = '/v1/kv/' .. key
            local res = consul:http_request('GET', path, params, nil, {
                parse_json = not params.raw,
                parse_b64 = 'Value'
            })
            if res.status ~= 200 then
                error(string.format('Error from consul: status=%d body=%s', res.status, res.body))
            end

            local index = res.headers['x-consul-index']
            return index, res.body
        end
    }
end