# Openresty library for querying the tarantool NoSQL database

## Introduction

This is a library to connect to the [tarantool](http://tarantool.org)
NoSQL database. This database has very interesting features that make
it be sort of a bridge between a traditional SQL based database and
document oriented storages like [CouchDB](http://couchdb.org).

It's a fork of another
[project](https://github.com/ziontab/lua-nginx-tarantool) that I was
unhappy with. It's abundantly documented and is update regarding the
tarantool API. Notably wtih support for the
[upsert](https://github.com/tarantool/tarantool/issues/905) command. 

Another thing to bear in mind is that the library tries to be
consistent between the way the `update` and `upsert` commands are
issued in the console using Lua and the way the API works. Notably the
field numbers. In the console a field number takes into account the
existence of a primary index as the first field. Hence any field that
come afterward will have an position that accounts for
it. Specifically when specifying the operators to use for the `update`
or `upsert` operations.

## Installation

### OpenResty

If you're using [OpenResty](http://openresty.org) the library should
be installed under: `/usr/local/openresty/lualib/resty`.

### Debian package

I package the library for debian
[here](https://debian.perusio.net). Just follow the instructions there
and install it.

### adhoc installation

Put the library in a place in your filesystem that you deem
appropriate. Don't forget to adjust the Lua package path, either by
setting `package.path` in Lua code or using the
[`lua_package_path`](https://www.nginx.com/resources/wiki/modules/lua/#lua-package-path)
directive.

## Requirements

Since tarantool uses [MessagePack](http://msgpack.org) for
serialization the
[lua-MessagePack](https://github.com/fperrad/lua-MessagePack) package
is required. 

It relies on the [BitOp](http://bitop.luajit.org/api.html) from
LuaJIT. Therefore you need a nginx Lua module that is linked against **LuaJIT** and
not Lua 5.1. 

## Usage

### Creating a connection

```lua
local tnt = require 'resty.tarantool'

local tar, err = tnt:new({
    host = '127.0.0.1',
    port = 3301,
    user = 'luser',
    password = 'some_password',
    socket_timeout = 2000,
    call_semantics = 'new' -- can be 'new' or 'old'*
})
```

The above creates a connection object that connects to a tarantool
server instance running on the loopback in port 3301, for user `luser`
with password `some password`. See the
[Tarantool manual in authentication](http://tarantool.org/doc/book/box/authentication.html)
for details on how to setup users and assigning privileges to them.

The socket timeout (receive and send) is 2 seconds (2000 ms).

\* The option `call_semantics` controls whether the code for the new call
method (0x0a) or the old one (0x06) is used.
The old method wraps every result in a table as described [here][binary-protocol].

[binary-protocol]: https://www.tarantool.io/en/doc/2.2/dev_guide/internals/box_protocol/ 'Tarantool Binary Protocol'

### set_timeout

    settimeout(<connection object>, <timeout in ms>)

Sets both the send and receive timeouts in miliseconds for a given
socket.

```lua
tnt:set_timeout(5000) -- 5s timeout for send/receive operations
```

The function returns true if the setting succeeds, `nil` if not. Note
that for the timeout to take effect this function needs to be invoked
**before** the connection is established, i.e., before invoking the
`connect` function. Alternatively the timeout can be specified when
creating the connection object (cosocket).

### connect

    connect(<connection object>)

Connects the socket created above to the port and address specified
when creating the connection object.

```lua
tar:connect()
```
The function returns true if the connection succeeds, `nil` if not.

### set_keepalive

    set_keepalive(<connection object>)

Makes the connection created get pushed to a connection pool so that
the connection is kept alive across multiple requests.

```lua
tar:set_keepalive()
```

The function returns true if the socket is successfully pushed to
connection pool (set keepalive). `nil` if not.

### disconnect

    disconnect(<connection object>)

Closes a connection to a given tarantool server running on a given
address and port.

```lua
tar:disconnect()
```

The function returns true if the connection is successfully closed. `nil` if not.


### ping

The ping command is useful for monitoring the tarantool server to see
if it's available. If it's available for queries it returns the string
`PONG`.

```lua
tar:ping()
-- returns PONG
```

### select

The select operation queries a given database (space) for retrieving
records.

    select(<connection object>, <space name>, <index>, <key>, <options>)

where `<options>` is an optional argument that can consists of a table
that can have the following keys:

 * `offset`: number of records to skip when doing the query.
 * `limit`: the maximum number of records to return.
 * `iterator`: a number specifiyng the iterator to use. Specified by
 the table:

```lua
local iterator_keys = {
  EQ = 0, -- equality
  REQ = 1, -- reverse equality
  ALL = 2, -- all tuples in an index
  LT = 3, -- less than
  LE = 4, -- less than or equal
  GE = 5, -- greater than or equal
  GT = 6, -- greater than
  BITSET_ALL_SET = 7, -- bits in the bitmask all set
  BITSET_ANY_SET = 8, -- any of the bist in the bitmask are set
  BITSET_ALL_NOT_SET = 9, -- none on the bits on the bitmask are set
}
```
More details about iterators on the [tarantool manual](http://tarantool.org/doc/book/box/box_index.html).

#### select examples

##### Query the `_space` space (DB) to get the space id of the `_index` space.

```lua
local res, err = tar:select('_space', 'name', '_index')

if err then return ngx.say(err) end

-- response:
[2881,"_index","memtx",0,"",
  [{"name":"id","type":"num"},
   {"name":"iid","type":"num"},
   {"name":"name","type":"str"},
   {"name":"type","type":"str"},
   {"name":"opts","type":"array"},
   {"name":"parts","type":"array"}]]]
```
The above request is equivalent to the console request:

```lua
box.space._space.index.name:select{ '_index' }
```

#### Query the space 'activities' for the activities with a `price` less than 300 

```lua
-- N.B. price is an index of the activities space.
local res, err = tar:select('activities', 'price', 300, { iterator = 'LT' })
```
The above request is equivalent to the console request:

```lua
box.space.activities.index.price:select({ 300 }, { iterator = 'LT' }) 
```
### insert

    insert(<connection object>, <space name>, <tuple>)

where `<tuple>` is the tuple to insert into `<space>` while setting
the primary index, which is unique, to the value specified in the
tuple.

The function returns the **inserted** record if the operation succeeds.

#### insert examples

```lua
local res, err = tar:insert('activities', { 16, 120, { activity = 'surf', price = 121 } })

-- response: 
[[16,120,{"activity":"surf","price":121}]]
```
The above request is equivalent to the console request:

```lua
box.space.activities:insert({16, 120, { activity = 'surf', price = 121 }})
```
16 is the value of the primary index here. This means that for an
integer type index this will be the record with primary index 16.

### replace

    replace(<connection object>, <space name>, <tuple>)

The replace command is similar in the invocation and signature to the
insert command. But now we're looking for **replacing** a record that
exists already instead of inserting a new one. We need again the value
of a primary unique index. But now the value must exist for the
operation to succeed. If the operations succeeds the record with the
**replaced** values is returned.

#### replace examples

```lua
local res, err = tar:replace('activities', { 16, 120, { activity = 'surf', price = 120 } })
-- response:
[[16,120,{"activity":"surf","price":120}]]
```
Here we replace the former 121 price by 120. The value of the primary
index, 16 matches the record we inserted above.

The above request is equivalent to the console request:

```lua
box.space.activities:update({ 16, 120, { activity = 'surf', price = 120 }})
```
### update

    update(<connection object>, <space name>, <index>, <key>, <operator list>) 

where `<operator list>` is the list of operators as specified n
[tarantool manual](http://tarantool.org/doc/book/box/box_space.html?highlight=operator#lua-function.space_object.update).
The pair (<index>, <key>) uniquely identifies a record, i.e., the
`<key>` is a value of the primary (unique) `<index>`.

`<operator list>` is a table of the form:

```lua
{ <operator>, <field position>, <value> }
```
the operators are:

 * `+` for adding to a numeric field. 
 * `-` for subtracting to a numeric field.
 * `&` for bitwise AND operation between two unsigned integers.
 * `|` for bitwise OR operation between two unsigned integers.
 * `^` for bitwise XOR operation between two unsigned integers.
 * `:` for string splicing.
 * `!` for field insertion.
 * `#` for field deletion.
 * `=` for assigning a given value to a field.

it returns the **updated** record if the operation is successful.

#### update examples

```lua
local res, err = tar:update('activities', 'primary', 16, { { '=', 2, 341 }, { '=', 3,  { activity = 'kitesurfing', price = 341 }}} )
-- response:
[16,341,{"activity":"kitesurfing","price":341}]]
```
The record with `primary` index 16 that we inserted above was updated.

The above request is equivalent to the console request:

```lua
box.space.activities.index.primary({ 16 }, { { '=', 2, 341 }, { '=', 3,  { activity = 'kitesurfing', price = 341 }}})
```
### upsert

    upsert(<connection object>, <space name>, <key>, <operator list>, <new tuple>)

apart from the `<new tuple>` argument the function signature is
similar to update. In fact upsert is two commands in one. update if
the record specified by the pair (<index>, <key>) exists and insert if
not. The key is a value from a primary index, i.e., is unique. The
`<new tuple>` is the tuple to be inserted if the `<key>` value doesn't
exist in the `<index>`. It returns an empty table `{}` if the
operation is successful. If the operation is unsuccessful it returns `nil`.

#### upsert examples

An **insert**.

```lua
local res, err = tar:upsert('activities', 17, { { '=', 2, 450 }, { '=', 3,  { activity = 'submarine tour 8', price = 450 }}}, { 17, 450, { activity = 'waterski', price = 365 }})
-- response:
{}
```
We **inserted** a new record with key 17 for the primary index from
the tuple:

```lua
{ 18, 450, { activity = 'waterski', price = 365 }}
```
The above request is equivalent to the console request:

```lua
box.space.activities:upsert({ 17 }, { { '=', 2, 450 }, { '=', 3,  { activity = 'submarine tour 8', price = 450 }}}, { 17, 450, { activity = 'waterski', price = 365 }})
```
An **update**.

```lua
local res, err = tar:upsert('activities', 17, { { '=', 2, 450 }, { '=', 3,  { activity = 'submarine tour 8', price = 450 }}}, { 18, 285, { activity = 'kitesurfing', price = 285 }})
-- response:
{}
```
Now we perform an update of the record identified by the key 17 in de
`primary` index (unique).

### delete

    delete(<connection object>, <space>, <key>)

deletes the record uniquely specified by `<key>` from `<space>`. Note
that `<key>` must belong to a primary (unique) index. It returns the
**deleted** record if the operation is successful.

#### delete examples

```lua
local response, err = tar:delete('activities', 17)
-- response:
[17,450,{"activity":"waterski","price":365}]]
```
We deleted the record uniquely identified by the key 17 in the primary
index from the activites space.

The above request is equivalent to the console request:

```lua
box.space.activities:delete({ 17 })

```

### call

    call(<connection object>, <proc>, <args>)

Invokes a
[stored procedure](http://tarantool.org/doc/book/app_c_lua_tutorial.html)
(Lua function) in the tarantool server we're connected to. It returns
the **results** of the invocation.

#### call examples

Since the tarantool console is a Lua REPL any function can be invoked
as long as it is available in the environment.

```lua
local res, err = tar:call('table.concat', {{ 'hello', ' ', 'world' }})
-- response:
[["hello world"]]
```
We called the `table.concat` function from the table library to
concatenate the table:

```lua
{'hello', ' ', 'world' }
```
The above request is equivalent to the console request:

```lua
table.concat({ 'hello', ' ', 'world' })
```

For many examples of tarantool stored procedures see the repository;
https://github.com/mailru/tarlua

### eval

    eval(<connection object>, <expression>, <return object>)

Invokes the tarantool embedded Lua interpreter to evaluate the given
`<expression>` and returns the result in the `<return object>`, which
is usually just an empty table `{ }`.

### eval examples

```lua
local res, err = tar:eval('return 23 * 20', { })
-- response:
[460]
```
we invoked the interpreter to evaluate the Lua expression:

```lua
return 23 * 20
```

which is also the equivalent tarantool console request.

### hide\_version\_header

    hide_version_header(<connection object>)

By default each response sends a custom HTTP header
`X-Tarantool-Version` with the version of the tarantool server.

    X-Tarantool-Version: 1.6.6-191-g82d1bc3

Invoking `hide_version_header` removes the header.

```lua
tar:hide_version_header()
```

It returns no values.

## TODO

 * Test setup.
