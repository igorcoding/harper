local M = {}


function M.deep_merge(dst,src)
    if not src or not dst then error("Call to deepmerge with bad args",2) end
    for k,v in pairs(src) do
        if type(v) == 'table' then
            if not dst[k] then dst[k] = {} end
            M.deep_merge(dst[k],src[k])
        else
            dst[k] = src[k]
        end
    end
    return dst
end

function M.wait_lsn(server_id, lsn, timeout, pause)
    pause = pause or 0.01
    if box.info.replication[server_id].lsn >= lsn then return true end
    local start = fiber.time()
    repeat
        fiber.sleep(pause)
    until box.info.replication[server_id].lsn >= lsn or ( timeout and fiber.time() > start + timeout )
    return box.info.replication[server_id].lsn >= lsn
end


return M