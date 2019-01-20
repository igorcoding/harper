local soext = (jit.os == "OSX" and "dylib" or "so")
local function script_path() local fio = require "fio"; local b = debug.getinfo(2, "S").source:sub(2); local b_dir = fio.dirname(b); local lb = fio.readlink(b); while lb ~= nil do if not string.startswith(lb, '/') then lb = fio.abspath(fio.pathjoin(b_dir, lb)) end; b = lb; lb = fio.readlink(b) end return b:match("(.*/)") end
local function addpaths(dst, ...) local cwd = script_path() or fio.cwd() .. "/"; local pp = {}; for s in package[dst]:gmatch("([^;]+)") do pp[s] = 1 end; local add = ""; for _, p in ipairs({...}) do local ap = cwd .. p; if string.startswith(p, '/') then ap = p end; if not pp[ap] then add = add .. ap .. ";" end; end package[dst] = add .. package[dst] return end
addpaths('path', '?.lua', '?/init.lua', '../?.lua', '../?/init.lua')

require 'strict'.on()
require 'package.reload'
local yaml = require 'yaml'
local fio = require 'fio'
local configlib = require 'config'

local instance_name = fio.basename(arg[0], '.lua')

xpcall(function()
    local harper
    local config_args = {
        file = './conf.lua',
        load = function(_, cfg)
            if cfg.harper == nil then
                return cfg
            end

            if harper == nil then
                harper = require 'harper'.new(instance_name, cfg.harper)
            end
            return harper:get_config(cfg)
        end
    }
    configlib(config_args)

    if harper ~= nil then
        harper:start(function()
            configlib(config_args)
        end)

        package.reload:register(harper.destroy, harper)
        rawset(_G, 'harper', harper)
    end

    box.once('access:v1', function()
        box.schema.user.create('replicator', { password = 'replicator_pass' })
        box.schema.user.grant('replicator', 'replication')
    end)

    require 'console'.start()
    os.exit()
end, function(err)
    print(err .. '\n' .. debug.traceback())
end)