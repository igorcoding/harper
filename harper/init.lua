local fiber = require 'fiber'
local log = require 'log'
local nbox = require 'net.box'
local json = require 'json'

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
            assert(not backend_config.instances[master].disabled, string.format('master %s is disabled', master))
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

    return policy(), master_mode == 'auto'
end


local make_replication_addr = function(cfg)
    if cfg.replication.username and cfg.replication.password then
        return string.format('%s:%s@%s', cfg.replication.username, cfg.replication.password, cfg.remote_addr)
    end
    return cfg.remote_addr
end

local make_execute_addr = function(cfg)
    if cfg.access.username and cfg.access.password then
        return string.format('%s:%s@%s', cfg.access.username, cfg.access.password, cfg.remote_addr)
    end
    return cfg.remote_addr
end


local function get_replication(self, backend_config, my_cluster_name, master, is_master, buddies)
    local my_cluster = backend_config.clusters[my_cluster_name]
    local replication_policy
    if my_cluster == nil then
        replication_policy = 'none'
    else
        replication_policy = my_cluster.replication_policy or 'mesh'
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


local function get_node_info(self, master, master_addr)
    local info
    repeat 
        local ok, res = pcall(function()
            local conn = nbox.connect(master_addr)
            local info = conn:eval([[
                return {
                    id = box.info.id,
                    uuid = box.info.uuid
                }
            ]])
            conn:close()
            return info
        end)

        if ok then
            info = res
            log.info('Got node info: %s', json.encode(info))
            break
        end

        log.error('Error connecting to current master: %s', res)
        fiber.sleep(1.0)
    until info ~= nil

    self._nodes[master] = info
end


local function promote(self)
    -- TODO: if no connection - remove from cluster
    -- TODO: if no active replication - remove from cluster
    
    local prev_master = self.master
    assert(prev_master ~= nil, 'cluster should have a master to promote someone else')
    local prev_master_addr = make_execute_addr(self._backend_config.instances[prev_master])
    log.info('Connecting to prev master %s: %s', prev_master, prev_master_addr)

    local i = 1
    local max_retries = 10
    local ok, res
    while i <= max_retries and not ok do
        ok, res = pcall(function()
            local conn = nbox.connect(prev_master_addr)
            local res = conn:eval([[
                box.cfg{read_only = true}
                return {
                    id = box.info.id,
                    lsn = box.info.lsn
                }
            ]])
            conn:close()
            return res
        end)

        if ok then break end

        log.error('Error connecting to master [%d/%d]: %s', i, max_retries, res)
        fiber.sleep(0.2)
        i = i + 1
    end

    if not res then
        -- exception while getting node info
        if self._nodes[prev_master] ~= nil then
            log.error('Couldn\'t connect to master \'%s\' to wait for lsn. Removing node with id=%d uuid=%s from the cluster', 
                      prev_master, self._nodes[prev_master].id, self._nodes[prev_master].uuid)
            -- TODO: remove prev_master from the cluster
        else
            log.error('Couldn\'t connect to master \'%s\' to wait for lsn. Can\'t remove prev_master from the cluster as its id is unknown.', 
                      prev_master)
        end
        instances[prev_master].disabled = true
    else
        local prev_master_info = res
        log.info('Waiting for lsn %d (currently have %d) from node %d (%s)', 
                    prev_master_info.lsn, box.info.replication[prev_master_info.id].lsn, 
                    prev_master_info.id, prev_master_addr)
        
        local prev_lsn
        while true do
            local finished, cur_lsn = util.wait_lsn(prev_master_info.id, prev_master_info.lsn, 10)
            if finished then
                log.info('Lsn %d is reached', prev_master_info.lsn)
                break
            end

            if prev_lsn ~= nil and cur_lsn <= prev_lsn then
                -- probably replication is not running or just hangs
                
                if box.info.replication[server_id].upstream.status ~= 'follow' then
                    -- replication is broken
                    -- TODO: remove prev_master from the cluster
                    break
                end
            end

            prev_lsn = cur_lsn
        end
    end
end


local function get_config(self, local_config)
    local backend_config
    repeat
        local ok, res = pcall(function()
            backend_config = self.backend:get_config()
            if backend_config == nil then
                log.warn("No backend config found. Retrying")
                fiber.sleep(1)
            end
        end)

        if not ok then
            log.error('Error getting harper backend config: %s', res)
            fiber.sleep(1)
        end
    until backend_config ~= nil

    local clusters = backend_config.clusters or {}
    local common = backend_config.common or {}
    local instances = backend_config.instances or {}

    backend_config.clusters = clusters
    backend_config.common = common
    backend_config.instances = instances

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
            instance_cfg.replication = common.replication or {}
        end

        -- merge access with cluster settings
        if not instance_cfg.access then
            instance_cfg.access = (clusters[instance_cfg.cluster] or {}).access
        end

        -- merge access with common settings
        if not instance_cfg.access then
            instance_cfg.access = common.access or {}
        end
    end

    local master, is_master_auto = get_master(self, backend_config, my_cluster_name)
    local is_master = (master == self.instance_name)
    local prev_master = self.master

    print(prev_master, '->', master)

    -- find out id and uuid of a current master's node
    if self._nodes[master] == nil then
        if self._nodes_pollers[master] ~= nil then
            local f = fiber.find(self._nodes_pollers[master])
            if f ~= nil and f:status() ~= 'dead' then
                f:cancel()
            end
            self._nodes_pollers[master] = nil
        end
        local master_addr = make_execute_addr(instances[master])
        local f = fiber.new(get_node_info, self, master, master_addr)
        f:name('master_poller:' .. master_addr)
        fiber.yield()
        self._nodes_pollers[master] = f:id()
    end

    -- check if master has been changed
    if prev_master ~= nil and master ~= prev_master then
        if prev_master == self.instance_name then
            -- if I was a master, then just quietly do nothing
            log.info('Not master anymore. Master is %s', master)
        elseif is_master then
            -- if I became a master
            -- need to contact prev master and wait for its lsn

            promote(self)
        end
    end

    local repl_part = get_replication(self, backend_config, my_cluster_name, master, is_master, buddies)

    local config = {}
    util.deep_merge(config, common)
    util.deep_merge(config, my_instance_cfg)
    util.deep_merge(config, repl_part)
    util.deep_merge(config, local_config or {})

    log.info('Current config: ' .. require'yaml'.encode(config))

    self.master = master
    self.is_master_auto = is_master_auto
    self._prev_backend_config = self._backend_config
    self._backend_config = backend_config

    return config
end

 
local function start_config_watcher(self, on_new_config)
    assert(on_new_config, 'on_new_config must be a callable')

    if self._config_watcher_fid ~= nil then
        error('watcher is already running')
    end

    self._config_watcher_fid = fiber.create(function()
        fiber.self():name('harper.watcher')
        log.info('Started harper watcher')
        
        local gen = package.reload.count
        while gen == package.reload.count do
            self.backend:wait_for_config_change(on_new_config, function(err)
                log.error(err .. '\n' .. debug.traceback())
                fiber.sleep(1)
            end)
        end

        log.info('Finished harper watcher')
        self._config_watcher_fid = nil
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
            if self.is_master_auto then
                self.backend:wait_for_master_change(on_new_config, function(err)
                    log.error(err .. '\n' .. debug.traceback())
                    fiber.sleep(1)
                end)
            else
                fiber.sleep(1)
            end
        end
        log.info('Finished harper master watcher')
        self._master_watcher_fid = nil
    end):id()

    self._heartbeat_fid = fiber.create(function()
        fiber.self():name('harper.heartbeat')
        log.info('Started harper master heartbeat fiber')

        local gen = package.reload.count
        while gen == package.reload.count do
            if self.is_master_auto then
                self.backend:send_heartbeat()
            end
            fiber.sleep(1)
        end
    end):id()
end


local function stop_watchers(self)
    log.info('Stopping harper master watcher')

    local fids = {
        self._config_watcher_fid,
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

    self._config_watcher_fid = nil
    self._master_watcher_fid = nil
    self._heartbeat_fid = nil
end

local function start(self, on_new_config)
    self:start_config_watcher(on_new_config)
    self:start_master_watcher(on_new_config)
end

local function destroy(self)
    self:stop_watchers()
    self.backend:clear()
    self._nodes = {}
end


local harper_methods = {
    get_config = get_config,
    start = start,
    destroy = destroy,
    is_master = function(self)
        return self.master == self.instance_name
    end,
    start_config_watcher = start_config_watcher,
    start_master_watcher = start_master_watcher,
    stop_watchers = stop_watchers,
}

local function new(instance_name, harper_config)
    if package.reload == nil then
        error('harper must be used with package.reload')
    end

    local self = {
        config = harper_config,
        instance_name = instance_name,
        master = box.NULL,
        is_master_auto = false,
        _backend_config = box.NULL,
        _prev_backend_config = box.NULL,
        _nodes = {},
        _nodes_pollers = {},
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