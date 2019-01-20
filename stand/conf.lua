box = {
    background = false,
    log = '| tee'
}

harper = {
    backend = 'consul',
    consul = {
        prefix = 'myapp',
        host = '127.0.0.1:8500'
    }
}