package = "lua-resty-tarantool"
version = "scm-1"
source = {
   url = "git+ssh://git@github.com/perusio/lua-resty-tarantool"
}
description = {
   summary = "Tarantool integration for Openresty",
   detailed = "Library for working with tarantool from nginx with the embedded Lua module for Openresty.",
   homepage = "github.com/perusio/lua-resty-tarantool",
   license = "MIT"
}
build = {
   type = "builtin",
   modules = {
      ["resty.tarantool"] = "lib/resty/tarantool.lua"
   }
}
