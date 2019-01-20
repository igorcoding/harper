local httplib = require 'http.client'
local neturl = require 'net.url'
local json = require 'json'
local digest = require 'digest'

local mod = {
    kv = require 'consul.kv',
    session = require 'consul.session',
}

local consul_methods = {
    http_request = function(self, method, path, query, body, opts)
        query = query or {}
        opts = opts or {}
        if opts.parse_json == nil then
            opts.parse_json = true
        end

        local proto = self.config.secure and 'https' or 'http'
        local u = neturl.parse(proto .. '://' .. self.config.host):resolve(path)
        u:setQuery(query)
        -- print(tostring(u))
        local result = self._http:request(method, tostring(u), body, {
            timeout = opts.timeout,
            headers = opts.headers,
        })

        if result.status >= 200 and result.status < 400 and opts.parse_json and result.body then
            local ok, body = pcall(json.decode, result.body)
            if ok then
                result.body = body
                if opts.parse_b64 and type(result.body) == 'table' then
                    if result.body == 0 then
                        -- probably a map
                        local val = result.body[opts.parse_b64]
                        if val ~= nil then
                            result.body[opts.parse_b64] = digest.base64_decode(val)
                        end
                    else
                        for _, el in ipairs(result.body) do
                            local val = el[opts.parse_b64]
                            if val ~= nil then
                                el[opts.parse_b64] = digest.base64_decode(val)
                            end
                        end
                    end
                end
            end
        end

        return result
    end,
}


local function validate_config(config)
    local validated = {}
    if config.host ~= nil and type(config.host) ~= 'string' then
        error('host invalid')
    end

    validated.host = config.host

    return validated
end


local function new(config)
    config = config or {}

    local self = {}
    for k, v in pairs(mod) do
        if type(v) == 'function' then
            v = v(self)
        end

        self[k] = v
    end

    config = validate_config(config)

    self.config = {
        host = config.host or '127.0.0.1:8500',
        secure = config.secure or false
    }

    self._http = httplib.new({
        max_connections = 5
    })

    return setmetatable(self, {
        __index = consul_methods
    })
end

return {
    new = new
}