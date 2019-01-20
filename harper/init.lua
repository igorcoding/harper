local fiber = require 'fiber'
local log = require 'log'

local util = require 'harper.util'


local function get_master(self, backend_config, my_cluster_name)
    local my_cluster = backend_config.clusters[my_cluster_name]
    local master_mode
    if my_cluster == nil then
        master_mode = 'none'
    else
        master_mode = my_cluster.master_selection_policy or 'manual'
    end

    local master_mode_policies = {
        manual = function()
            local master = my_cluster.master
            assert(master, string.format('master is required in cluster %s', my_cluster_name))
            assert(backend_config.instances[master], string.format('master instance %s not found', master))
            -- assert(not cluster_config.instances[master].disabled, string.format('master %s is disabled', master))
            return master
        end,

        auto = function() 
            assert(false, 'not implemented yet')
            -- local master
            -- repeat
            --     master = self.backend:elect_master()
            --     if master == nil then
            --         log.warn("Couldn't elect master")
            --         fiber.sleep(1.0)
            --     end
            -- until master ~= nil

            -- assert(cluster_config.instances[master], string.format('master %s not found', master))
            -- return master
        end,

        none = function()
            return nil
        end
    }

    local policy = master_mode_policies[master_mode]
    if policy == nil then
        local supported_policies = {}
        for k, _ in pairs(master_mode_policies) do
            table.insert(supported_policies, k)
        end
        table.sort(supported_policies)

        error(string.format(
            'master_selection_policy "%s" is unsupported. Currently support: %s',
            cluster_config.master_mode, table.concat(supported_policies, ', ')
        ))
        return nil
    end

    return policy()
end


local function get_replication(self, backend_config, my_cluster_name, master, is_master, buddies)
    local my_cluster = backend_config.clusters[my_cluster_name]
    local replication_policy
    if my_cluster == nil then
        replication_policy = 'none'
    else
        replication_policy = my_cluster.replication_policy or 'mesh'
    end

    local make_replication_addr = function(cfg)
        if cfg.replication.username and cfg.replication.password then
            return string.format('%s:%s@%s', cfg.replication.username, cfg.replication.password, cfg.remote_addr)
        end
        return cfg.remote_addr
    end

    local replication_policies = {
        mesh = function()
            local replication = {}
            for _, cfg in pairs(buddies) do
                if not cfg.disabled then
                    table.insert(replication, make_replication_addr(cfg))
                end
            end
    
            return {
                box = {
                    replication = replication,
                    read_only = not is_master
                }
            }
        end,
        master_slave = function()
            assert(buddies[master], string.format('master %s not found in buddies (probably not part of a cluster)', master))

            if is_master then
                return {
                    box = {
                        replication = '',
                        read_only = false
                    }
                }
            end

            return {
                box = {
                    replication = {
                        make_replication_addr(buddies[master])
                    },
                    read_only = true
                }
            }
        end,
        none = function()
            return {
                box = {
                    replication = '',
                    read_only = false
                }
            }
        end
    }
    local replication_policy = replication_policies[replication_policy]
    if replication_policy == nil then
        local supported_policies = {}
        for k, _ in pairs(replication_policies) do
            table.insert(supported_policies, k)
        end
        table.sort(supported_policies)

        error(string.format(
            'Replication policy "%s" is unsupported. Currently support: %s',
            replication_policy, table.concat(supported_policies, ', ')
        ))
        return nil
    end

    return replication_policy()
end


local function get_config(self, local_config)
    local backend_config
    repeat
        backend_config = self.backend:get_config()
        if backend_config == nil then
            log.warn("No backend config found. Retrying")
            fiber.sleep(1.0)
        end
    until backend_config ~= nil

    local clusters = backend_config.clusters or {}
    local common = backend_config.common or {}
    local instances = backend_config.instances or {}

    backend_config.clusters = clusters
    backend_config.common = common
    backend_config.instances= instances

    local my_instance_cfg = instances[self.instance_name]
    assert(my_instance_cfg ~= nil, string.format('Instance %s not found', self.instance_name))
    local my_cluster_name = my_instance_cfg.cluster
    if my_cluster_name ~= nil then
        assert(clusters[my_cluster_name] ~= nil, string.format('Cluster %s not found', my_cluster_name))
    end

    local buddies = {}
    for instance_name, instance_cfg in pairs(instances) do
        if instance_cfg.remote_addr == nil then
            instance_cfg.remote_addr = (instance_cfg.box or {}).listen
        end

        if my_cluster_name ~= nil and instance_cfg.cluster == my_cluster_name then
            buddies[instance_name] = instance_cfg
        end

        -- merge replication with cluster settings
        if not instance_cfg.replication then
            instance_cfg.replication = (clusters[instance_cfg.cluster] or {}).replication
        end

        -- merge replication with common settings
        if not instance_cfg.replication then
            instance_cfg.replication = common.replication
        end
    end

    local master = get_master(self, backend_config, my_cluster_name)
    local is_master = (master == self.instance_name)
    local repl_part = get_replication(self, backend_config, my_cluster_name, master, is_master, buddies)

    local config = {}
    util.deep_merge(config, common)
    util.deep_merge(config, my_instance_cfg)
    util.deep_merge(config, repl_part)
    util.deep_merge(config, local_config or {})

    log.info('Current config: ' .. require'yaml'.encode(config))

    self.master = master

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
            -- local prev_context = table.copy(self.backend_context)
            local ok, err = xpcall(function()
                local have_changes
                have_changes = self.backend:wait_for_changes()
                if have_changes then
                    log.info('harper: got new config')
                    on_new_config()
                end
            end, function(err)
                log.error(err .. '\n' .. debug.traceback())
                -- self.backend_context = prev_context
                fiber.sleep(1)
            end)
        end
        log.info('Finished harper watcher')
        self._watcher_fid = nil
    end):id()
end

 
local function start_master_watcher(self, on_new_config)
    assert(on_new_config, 'on_new_config must be a callable')

    if self._master_watcher_fid ~= nil then
        error('master watcher is already running')
    end

    self._master_watcher_fid = fiber.create(function()
        fiber.self():name('harper.master_watcher')
        log.info('Started harper master watcher')
        local gen = package.reload.count
        while gen == package.reload.count do
            -- local prev_context = table.copy(self.backend_context)
            local ok, err = xpcall(function()
                local have_changes = self.backend:wait_for_master_change()
                if have_changes then
                    log.info('harper: got new master')
                    on_new_config()
                end
            end, function(err)
                log.error(err .. '\n' .. debug.traceback())
                -- self.backend_context = prev_context
                fiber.sleep(1)
            end)
        end
        log.info('Finished harper master watcher')
        self._master_watcher_fid = nil
    end):id()

    self._heartbeat_fid = fiber.create(function()
        fiber.self():name('harper.heartbeat')
        log.info('Started harper master heartbeat fiber')

        local gen = package.reload.count
        while gen == package.reload.count do
            self.backend:send_heartbeat()
            fiber.sleep(1)
        end
    end):id()
end


local function stop_watchers(self)
    log.info('Stopping harper master watcher')

    local fids = {
        self._watcher_fid,
        self._master_watcher_fid,
        self._heartbeat_fid
    }

    for _, fid in ipairs(fids) do
        if fid ~= nil then
            local f = fiber.find(fid)
            if f and f:status() ~= 'dead' then
                f:cancel()
            end
        end
    end

    self._watcher_fid = nil
    self._master_watcher_fid = nil
    self._heartbeat_fid = nil
end

local function start(self, on_new_config)
    self:start_watcher(on_new_config)
    -- self:start_master_watcher(on_new_config)
end

local function destroy(self)
    self:stop_watchers()
    self.backend:clear()
end


local harper_methods = {
    get_config = get_config,
    start_watcher = start_watcher,
    start_master_watcher = start_master_watcher,
    stop_watchers = stop_watchers,
    start = start,
    destroy = destroy,
    is_master = function(self)
        return self.master == self.instance_name
    end
}

local function new(instance_name, harper_config)
    if package.reload == nil then
        error('harper must be used with package.reload')
    end

    local self = {
        config = harper_config,
        instance_name = instance_name,
        master = box.NULL,
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