package = 'harper'
version = 'scm-1'
source  = {
    url    = 'git://github.com/igorcoding/harper.git',
    branch = 'master',
}
description = {
    summary  = "Harper",
    homepage = 'https://github.com/igorcoding/harper',
    license  = 'MIT',
}
dependencies = {
    'lua >= 5.1',
    'net-url >= 0.9-1',
}
build = {
    type = 'builtin',
    modules = {
        ['harper'] = 'harper/init.lua',
        ['harper.backends.consul'] = 'harper/backends/consul.lua',
        
        ['harper.consul'] = 'harper/consul/init.lua',
        ['harper.consul.kv'] = 'harper/consul/kv/init.lua',
    }
}

-- vim: syntax=lua
