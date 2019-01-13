local fiber = require 'fiber'
local log = require 'log'

local function deep_merge(dst,src)
    if not src or not dst then error("Call to deepmerge with bad args",2) end
    for k,v in pairs(src) do
        if type(v) == 'table' then
            if not dst[k] then dst[k] = {} end
            deep_merge(dst[k],src[k])
        else
            dst[k] = src[k]
        end
    end
end


local function get_config(self, local_config)
    local backend_config
    self.backend_context, backend_config = self.backend.get_config(self.backend_context)

    local common_box_config = backend_config.box
    local cluster_config = backend_config.cluster

    local config = {}
    deep_merge(config, { box = common_box_config })

    local instance_config = cluster_config.instances[self.instance_name]
    assert(instance_config ~= nil, string.format('Instance %s not found', self.instance_name))

    if instance_config.disabled then
        error('Current instance is disabled')
    end

    -- merge replication config

    for k, cfg in pairs(cluster_config.instances) do
        if not cfg.disabled then
            cfg.replication = cfg.replication or {}
            deep_merge(cfg.replication, cluster_config.replication or {})

            assert(cfg.listen, string.format('no listen in %s instance', k))
            assert(cfg.replication, string.format('no replication in %s instance', k))

            if cfg.remote_addr == nil then
                cfg.remote_addr = cfg.listen
            end
        end
    end

    local make_replication_addr = function(cfg)
        if cfg.replication.username and cfg.replication.password then
            return cfg.remote_addr
        end
        return string.format('%s:%s@%s', cfg.replication.username, cfg.replication.password, cfg.remote_addr)
    end

    local get_master = function(cluster_config)
        local master = cluster_config.master
        assert(master, 'master is required in cluster')
        assert(cluster_config.instances[master], string.format('master %s not found', master))
        assert(not cluster_config.instances[master].disabled, string.format('master %s is disabled', master))

        return master
    end

    -- building replication config
    local replication_policies = {
        mesh = function(cluster_config)
            local master = get_master(cluster_config)
            local is_master = (master == self.instance_name)

            local replication = {}
            for k, cfg in pairs(cluster_config.instances) do
                if not cfg.disabled then
                    table.insert(replication, make_replication_addr(cfg))
                end
            end
            return {
                replication = replication,
                read_only = not is_master
            }, is_master
        end,
        master_slave = function(cluster_config)
            local master = get_master(cluster_config)
            local is_master = (master == self.instance_name)
            if is_master then
                return {
                    replication = '',
                    read_only = false
                }
            end

            return {
                replication = {
                    make_replication_addr(cluster_config.instances[master])
                },
                read_only = not is_master
            }, is_master
        end,
        none = function(cluster_config)
            if #cluster_config.instances > 1 then
                error('Too many instances for replication_policy=none (must be 1)')
            end
            return {
                replication = '',
                read_only = false
            }, true
        end
    }
    local replication_policy = replication_policies[cluster_config.replication_policy or 'none']
    if replication_policy == nil then
        local supported_policies = {}
        for k, _ in pairs(replication_policies) do
            table.insert(supported_policies, k)
        end
        table.sort(supported_policies)

        error(string.format(
            'Replication policy "%s" is unsupported. Currently support: %s',
            cluster_config.replication_policy, table.concat(supported_policies, ', ')
        ))
        return nil
    end
    local instance_replication, is_master = replication_policy(cluster_config)

    instance_config.remote_addr = nil
    instance_config.disabled = nil

    deep_merge(config, {box = instance_config})
    deep_merge(config, {box = instance_replication})
    deep_merge(config, local_config or {})
    print(require'yaml'.encode(config))

    self.is_master = is_master
    return config
end


local function start_watcher(self, on_new_config)
    assert(on_new_config, 'on_new_config must be a callable')

    if self.watcher_fid ~= nil then
        error('watcher is already running')
    end

    self._watcher_fid = fiber.create(function()
        fiber.self():name('harper.watcher')
        log.info('Started harper watcher')
        local gen = package.reload.count
        while gen == package.reload.count do
            local prev_context = table.copy(self.backend_context)
            local ok, err = xpcall(function()
                local have_changes
                self.backend_context, have_changes = self.backend.wait_for_changes(self.backend_context)
                if have_changes then
                    log.info('harper: got new config')
                    on_new_config()
                end
            end, function(err)
                log.error(err .. '\n' .. debug.traceback())
                self.backend_context = prev_context
                fiber.sleep(1)
            end)
        end
        log.info('Finished harper watcher')
        self._watcher_fid = nil
    end):id()
end


local function stop_watcher(self)
    log.info('Stopping harper watcher')

    if self._watcher_fid ~= nil then
        local f = fiber.find(self._watcher_fid)
        if f and f:status() ~= 'dead' then
            f:cancel()
        end
        self.watcher_fid = nil
        self.backend_context = {}
    end
end

local function destroy(self)
    self:stop_watcher()
end


local harper_methods = {
    get_config = get_config,
    start_watcher = start_watcher,
    stop_watcher = stop_watcher,
    destroy = destroy,
}

local function new(instance_name, harper_config)
    if package.reload == nil then
        error('harper must be used with package.reload')
    end

    local self = {
        config = harper_config,
        instance_name = instance_name,
        is_master = box.NULL,
        backend_context = {},
    }

    assert(type(self.instance_name) == 'string', 'instance_name is required')
    if harper_config.backend == 'consul' then
        self.backend = require 'harper.backends.consul'(self)
    else
        if harper_config.backend == nil then
            error('Harper backend is required')
        end
        error(string.format('backend %s is not supported', harper_config.backend))
    end

    return setmetatable(self, {
        __index = harper_methods
    })
end

return {
    new = new
}