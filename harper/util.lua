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

return M