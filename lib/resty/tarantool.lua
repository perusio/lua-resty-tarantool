--- Module for working with tarantool in OpenResty or nginx with
--  embedded Lua
-- @module tarantool.lua
-- @author António P. P. Almeida <appa@perusio.net>
-- @license MIT
-- @alias M

-- Bit operations. Try to use the LuaJIT bit operations package.
local ok_bit, bit = pcall(require, 'bit')
-- If in Lua 5.2 use the bit32 package.
if not ok_bit and _VERSION == 'Lua 5.2' then
  bit = require 'bit32'
elseif not ok_bit then
  return nil, 'Bitwise operator support missing.'
end

-- MessagePack handling.
local mp = require 'MessagePack'

--- Some local definitions.
-- String functions.
local sub = string.sub
local gsub = string.gsub
local sbyte = string.byte
local schar = string.char
local slen = string.len
local format = string.format
local match = string.match
-- Table functions.
local concat = table.concat
-- nginx Lua functions.
local ngx = ngx
local tcp = ngx.socket.tcp
local sha1b = ngx.sha1_bin
-- Debugging related functions.
local log = ngx.log
local ERR = ngx.ERR
-- Generic Lua functions.
local type = type
local ipairs = ipairs
local pairs = pairs
local error = error
local tonumber = tonumber
local setmetatable = setmetatable

-- Depending on the LuaJIT version a table.new function may be
-- available. That speeds up operations since it bypasses most of the
-- memory management associated with table handling: it preallocates
-- the space. This function has the same signature as the C Lua API
-- lua_createtable. narr = number of array elements,
-- nrec = number of non array elements (records).
local ok, new_tab = pcall(require, 'table.new')
if not ok then
  new_tab = function (narr, nrec) return {} end
end

-- Avoid polluting the global environment.
-- If we are in Lua 5.1 this function exists.
if _G.setfenv then
  setfenv(1, {})
else -- Lua 5.2.
  _ENV = nil
end

-- IProto protocol basic settings.
local iproto = {
  -- Greeting message components size.
  greeting_size = 128,
  greeting_salt_offset = 64,
  greeting_salt_size = 44,
  -- Size of the length for header and body.
  head_body_len_size = 5,
  -- Maximum number of requests in a single connection.
  request_per_connection = 100000,
  max_limit = 0xffffffff,
}

-- IProto packet keys.
local packet_keys = {
  type = 0x00,
  sync = 0x01,
  space_id = 0x10,
  index_id = 0x11,
  limit = 0x12,
  offset = 0x13,
  iterator = 0x14,
  key = 0x20,
  tuple = 0x21,
  function_name = 0x22,
  username = 0x23,
  expression = 0x27,
  def_tuple = 0x28,
  data = 0x30,
  error = 0x31
}

-- IProto command keys.
local command_keys = {
  select = 0x01,
  insert = 0x02,
  replace = 0x03,
  update = 0x04,
  delete = 0x05,
  call = {
    -- Old call, wraps each return value in a table
    old = 0x06,
    -- Newer call that doesn't wrap values
    new = 0x0a
  },
  auth = 0x07,
  eval = 0x08,
  upsert = 0x09,
  -- Admin commands.
  ping = 0x40,
}

-- IProto response keys.
local response_keys = {
  ok = 0x00,
  -- Minimum value.
  error = 0x8000,
}

-- IDs for the various schemas that define tarantool metadata.
local _space_id = {
  schema = 272, -- _space
  space = 280, -- _schema
  index = 288, -- _index
  func = 296, -- _func
  user = 304, -- _user
  priv = 312, -- _priv
  cluster = 320, -- _cluster
}

-- IDs for indexes of _space and _index.
local indexes_id = {
  -- _space.
  space = {
    primary = 0,
    name = 2
  },
  -- _index.
  index = {
    primary = 0,
    name = 2
  }
}

-- Iterator keys, i.e., codes that represent it in terms of the IProto
-- protocol.
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
  BITSET_ALL_NOT_SET = 9, -- none of the bits on the bitmask are set
}

-- Default options.
local defaults = {
  host = '127.0.0.1',
  port = 3301,
  password = '',
  socket_timeout = 5000, -- ms
  call_semantics = 'old',
}

-- Lua pattern used to extract the version number from the server
-- response header.
local version_number_patt = '[%d.-]+[%l%x]*'
-- For versions less than 0.3.3 this setting makes sense.
-- Otherwise is deprecated.
local mp_version, reps = gsub(mp._VERSION, '%.', '')
if reps > 0 and tonumber(mp_version) < 33 then
  mp.set_integer('unsigned')
end

--- Module table.
local M = { _VERSION = '0.3.1', _NAME = 'tarantool', _DESCRIPTION = 'resty Lua library for tarantool' }
local mt = { __index = M }

--- Create a connection object.
--
-- @param self table connection object.
-- @param params table connection parameters.
--
-- @return table
--   The connection object.
function M.new(self, params)
  -- Create an object using the defaults.
  -- tarc = tarantool connection.
  local tarc = {
    -- If true it sends a custom header with the tarantool version.
    show_version_header = true,
  }
  for key, value in pairs(defaults) do
	  tarc[key] = value
  end
  -- Loop over the given parameters and assign the values accordingly.
  if params and type(params) == 'table' then
    for key, value in pairs(params) do
      if params[key] ~= nil then
        tarc[key] = params[key]
      end
    end
  end

  -- Create a TCP cosocket.
  local tcpsock = tcp()

  -- Check if the socket was successfully created.
  if not tcpsock then
    return nil, err
  end
  -- Set the socket timeout.
  if tarc.socket_timeout then
    tcpsock:settimeout(tarc.tcpsocket_timeout)
  end

  tarc.sock = tcpsock
  tarc._spaces  = {}
  tarc._indexes = {}
  return setmetatable(tarc, mt)
end

--- Closes a socket.
--
-- @param self table connection object.
--
-- @return boolean
--   true if the closing is successful.
function M.disconnect(self)
  if not self.sock then
    return nil, 'No socket created.'
  end
  return self.sock:close()
end

--- Pushes a socket into a connection pool to keep it alive.
--
-- @param self table connection object.
--
-- @return boolean
--   true if successful.
function M.set_keepalive(self)
  if not self.sock then
    return nil, 'No socket created.'
  end
  local ok, err = self.sock:setkeepalive()
  -- If we didn't manage to set it in keepalive, close it.
  if not ok then
    self:disconnect()
    return nil, err
  end
  return ok
end

--- Set the timeout for the operations on a socket.
--
-- @param self table connection object.
-- @param timeout integer in ms.
--
-- @return boolean
--   true if successful, false if not.
function M.set_timeout(self, timeout)
  local sock = self.sock
  if not sock then
    return nil, 'Not initialized.'
  end
  -- Set the timeout for all socket operations.
  return sock:settimeout(timeout)
end

--- XORs two given strings
--
-- @param str1 string string 1 to XOR.
-- @param str2 string string 2 to XOR.
--
-- @return string
--   XORed strings.
local function xorstr(str1, str2)
  local len = slen(str1)
  local result = new_tab(len, 0)
  -- Test if the strings are the same length.
  if len ~= slen(str2) then
    return nil
  end
  -- Loop over all the string.
  for i = 1, len do
    result[i] = schar(bit.bxor(sbyte(str1, i), sbyte(str2, i)))
  end
  -- Return the concatenated (XORed) string.
  return concat(result)
end

--- Serialize the request message using MsgPack.
--
-- @param header table request header.
-- @param body table request body.
--
-- @return string
--   Serialized request messge using MsgPack.
local function encode_request(header, body)
  local msg = new_tab(3, 0)
  msg[2] = mp.pack(header)
  msg[3] = mp.pack(body)
  msg[1] = mp.pack(slen(msg[2]) + slen(msg[3]))
  return concat(msg)
end

--- Serialize the request message using MsgPack.
--  for messages with no body (ping).
--
-- @param header table request header.
--
-- @return string
--   Serialized request messge using MsgPack.
local function encode_request_no_body(header)
  local msg = mp.pack(header)
  return mp.pack(slen(msg)) .. msg
end

--- Issue a request to tarantool.
--
-- @local
--
-- @param self table connection object.
-- @param header table request header.
-- @param body table request body (payload).
--
-- @return table or nil, string.
--  A table with the response or otherwise if there's an error.
local function request(self, header, body)
  local sock = self.sock
  local htype = type(header)
  -- The header must be a table. Bail out if not.
  if htype ~= 'table' then
    return nil, format('Invalid request header: type %s.', htype)
  end
  -- Get an ID for the response so that a request can be matched with
  -- a response. This is needed for asynch I/O on the server side. A
  -- coroutine (cosocket) may yield but we need to keep a count of how
  -- many streams are open. So that when it resumes we can match the
  -- proper response with the corresponding request.
  self.sync_id = ((self.sync_id or 0) + 1) % iproto.request_per_connection
  if not header[packet_keys.sync] then
    header[packet_keys.sync] = self.sync_id
  else
    self.sync_id = header[packet_keys.sync]
  end
  -- Serialize the request message.
  local request
  -- For all messages except PING the message has always a body.
  if body then
    request = encode_request(header, body)
  else
    -- No body: PING message.
    request = encode_request_no_body(header)
  end
  -- Send the request.
  local bytes, err = sock:send(request)
  -- Check if the request failed.
  if bytes == nil then
    sock:close()
    return nil, format('Failed to send request: %s.', err)
  end
  -- Receive the response, only the size.
  local size, err = sock:receive(iproto.head_body_len_size)
  if not size then
    sock:close()
    return nil, format('Failed to get response size: %s.', err)
  end
  -- Get the size (deserialize it).
  size = mp.unpack(size)
  -- If if fails then bail out.
  if not size then
    --  sock:close()
    return nil, 'Client response has invalid size.'
  end
  -- Extract the message (header and body).
  local header_and_body, err, partial = sock:receive(size)
  if not header_and_body then
    sock:close()
    return nil,  format('Failed to get response header and body: %s.', err)
  end
  -- Deserialize the response. Returns an iterator.
  local iterator = mp.unpacker(header_and_body)
  -- The first element is the header.
  local v, response_header = iterator()
  -- It should be a table.
  htype = type(response_header)
  if  htype ~= 'table' then
    return nil, format('Invalid header: %s (table expected)', htype)
  end
  -- Check if the response is the one corresponding to the current
  -- stream ID.
  if response_header[packet_keys.sync] ~= self.sync_id then
    return nil,
    format('Mismatch of response and request ids. req: %d res: %d.',
           self.sync_id,
           response_header[packet_keys.sync])
  end
  -- Handle the response body.
  local response_body
  v, response_body = iterator()
  -- If not a table then is empty.
  if type(response_body) ~= 'table' then
    response_body = {}
  end
  -- Return the response as a table with the data and the metadata.
  return { code = response_header[packet_keys.type],
           data = response_body[packet_keys.data],
           error = response_body[packet_keys.error],
         }
end

--- Perform the authentication with the tarantool server.
--
-- @param self table object representing the current connection with
--   all the parameters.
--
-- @local
--
-- @return boolean or nil, string
--   If the authentication succeeds return true, if not nil and the
--   error message.
local function authenticate(self)
  -- If there's no username no authentication is performed.
  if not self.user then
    return true
  end
  -- The authentication occurs in three steps.
  local first = sha1b(self.password)
  local last = sha1b(self._salt .. sha1b(first))
  -- Issue the authentication request.
  local response, err =
    request(self,
            { [packet_keys.type] = command_keys.auth },
            { [packet_keys.username] = self.user,
              [packet_keys.tuple] = { 'chap-sha1', xorstr(first, last) } })
    -- If there's an error signal the error and return nil.
    if err then
      return nil, err
      -- Check the server response code.
    elseif response and response.code ~= response_keys.ok then
      return nil, response and response.error or 'Internal error'
    else
      -- If the response code is correct return true.
      return true
    end
end

--- Performs the handshake of the IProto protocol.
--
-- @local
--
-- @param self table connection object.
--
-- @return boolean or nil, string
--   True if the handshake works. Signal an error if doesn't.
--
local function handshake(self)
  -- First we check if the connection is already in place. If so, if
  -- it is ok.
  local count, err = self.sock:getreusedtimes()
  -- Implementing the handshake of the IProto protocol.
  -- @see http://tarantool.org/doc/dev_guide/box-protocol.html.
  --
  local greeting, greeting_err
  -- If we're creating a new connection instead of reusing an old one
  -- comming from the connection pool, i.e., keepalive.
  if count == 0 then
    -- 1. Getting a greeting packet from the server.
    greeting, greeting_err = self.sock:receive(iproto.greeting_size)
    if not greeting or greeting_err then
      self.sock:close()
      return nil, greeting_err
    end
    -- Get the server version.
    self._version = match(sub(greeting, 1, iproto.greeting_salt_offset),
                          version_number_patt)
    -- Set a HTTP header with the server version.
    if self.show_version_header then
      ngx.header['X-Tarantool-Version'] = self._version
    end
    -- Get the salt embedded in greeting message.
    self._salt = sub(greeting, iproto.greeting_salt_offset + 1)
    -- Decode the base64 encoded salt. Note that according to the
    -- protocol specification currently only the first 20 bytes are
    -- used for the salt.
    -- @see http://tarantool.org/doc/dev_guide/box-protocol.html#authentication.
    self._salt = sub(ngx.decode_base64(self._salt), 1, 20)
    -- Proceed to authenticate now.
    return authenticate(self)
  end
  -- Return true if we're reusing a socket (keepalive).
  return true
end

--- Connect to the server using the created cosocket.
--
-- @param self table connection object.
-- @param host string connecting to.
-- @param port integer on this port.
--
-- @return boolean or nil
--   true if the connection suceeds, nil if not.
function M.connect(self, host, port)
  -- Check if we have a socket available.
  if not self.sock then
    return nil, 'No socket created.'
  end
  -- Connect to the server,
  local ok, err = self.sock:connect(host or self.host, port or self.port)
  if not ok then
    return ok, err
  end
  -- Perform the handshake.
  return handshake(self)
end

--- Performs a PING type request to the server.
--
-- @param self table connection object.
--
-- @return string
--   'PONG' if request is successful.
function M.ping(self)
  -- Issue the request. PING has no message body.
  local response, err = request(self, { [packet_keys.type] = command_keys.ping })
  if err then
    return nil, err
  elseif response and response.code ~= response_keys.ok then
    return nil, response and response.error or 'Internal error.'
  else
    return 'PONG'
  end
end

--- Sets the flag that signals to not send the
--  Tarantool version as a custom header.
--
-- @param self table connection object.
--
-- @return nothing
--   Side effects only.
function M.hide_version_header(self)
  self.show_version_header = false
end

--- Finds a space numeric id to be used in the IProto packets.
--
-- @local
--
-- @param self table connection object.
-- @param space string space id (name).
--
-- @return integer
--   Space numeric id.
local function get_space_id(self, space)
  -- Since the IProto protocol uses an integer space_id to associate a
  -- a request with a space we need to find first the id of the given
  -- space. This is done by querying the server and obtaining the id.
  -- The space we need to query where all the spaces are registered is
  -- _space. This space has the following metadata.
  --
  --  space_id = 280
  --  indexes (id, name, spec)
  --   0: primary - 1 part, NUM
  --   1: owner - 1 part, STR
  --   2: name - 1 part, STR
  -- We're interested in the index 2, which allow us to obtain the
  -- space_id, given the name. So the query is in the tarantool
  -- console:
  -- box.space._space.index.name:select{<space>}.
  -- This returns a table. The first element is the space_id.
  -- Otherwise if the space is already a number than we can skip
  -- this and return the value immediately.
  if type(space) == 'number' then
    return space
  end
  -- If it's a string check if we already have it.
  if type(space) == 'string' and type(self._spaces) == 'table' and type(self._spaces[space]) == 'number' then
    return self._spaces[space]
  end
  -- Query the _space space to retrieve space_id.
  local data, err = self:select(_space_id.space, indexes_id.space.name, space)
  -- If there was an error return it.
  if err then return nil, err end
  -- Check the reults. It's something of the form:
  -- [ space_id, owner_id, name, engine, options ].
  if type(data) == 'table' and type(data[1]) == 'table' and type(data[1][1]) == 'number' then
    -- We found the space_id set the attribute in the connection object
    -- and return it.
    self._spaces[space] = tonumber(data[1][1])
    return self._spaces[space]
  end
  -- Last resort: we didn't found anything.
  return nil, format('Cannot find space: %s.', space)
end

--- Finds a space numeric id to be used in the IProto packets.
--
-- @local
--
-- @param self table connection object.
-- @param space string space id (name).
-- @param index index index id (name).
--
-- @return integer
--   Index numeric id.
local function get_index_id(self, space, index)
  -- Since the IProto protocol uses an integer indexid to associate a
  -- a request with a index we need to find first the id of the given
  -- index. This is done by querying the server and obtaining the id.
  -- The index we need to query where all the indexs are registered is
  -- _index. This index has the following metadata.
  --
  --  space_id = 288
  --  indexes (id, name, spec)
  --   0: primary - 2 parts, (NUM, NUM)
  --   2: name - 2 parts, (NUM, STR)
  -- We're interested in the index 2, which allow us to obtain the
  -- index_id, given the space_id index name. So the query is in the
  -- tarantool console:
  -- box.index._index.index.name:select{<space_id>, <index name>}.
  -- This returns a table. The second element is the index id.
  -- Otherwise if the index is already a number than we can skip
  -- this and return the value immediately.
  if type(index) == 'number' then
    return index
  end
  -- If it's a string check if we already have it.
  if type(index) == 'string' and type(self._indexes) == 'table' and type(self._indexes[index]) == 'number' then
    return self._indexes[index]
  end
  -- First get the space_id.
  local spaceid, err = get_space_id(self, space)
  -- If there was an error return it.
  if not spaceid then return nil, err end
  -- Query _index to retrieve the index_id.
  local data, err = self:select(_space_id.index, indexes_id.index.name, { spaceid, index })
  -- If there was an error return it.
  if err then return nil, err end
  -- Check the reults. It's something of the form:
  -- [ spaceid, indexid, name, type, {"unique": <boolean>} [opts]].
  if type(data) == 'table' and type(data[1]) == 'table' and type(data[1][2]) == 'number' then
    -- We found the index_id set the attribute in the connection object
    -- and return it.
    self._indexes[index] = data[1][2]
    return self._indexes[index]
  end
  -- Last resort: we didn't found anything.
  return nil, err or format('Cannot find index: %s.', index)
end

--- Transforms the given argument into a table if it's not already
--  one. IProto key field, an array.
--
-- @local
--
-- @param value mixed the value to be used as query
--              key.
-- @return table
--  Given value in a table.
local function prepare_key(value)
  if type(value) == 'table' then
    return value
  elseif value == nil then
    return { }
  else
    return { value }
  end
end

--- Performs a select operation on given space with the given index.
--
-- @param self table connection object
-- @param space string space name.
-- @param index string index name.
-- @param key mixed query key.
-- @param opts table update operations list.
--
-- @return table
--   Select query result if successful.
function M.select(self, space, index, key, opts)
  -- If not options are given set it to {}.
  if opts == nil then opts = {} end
  -- First get the space numeric id, i.e., what's the value of the
  -- space_id field in the space.
  local spaceid, err = get_space_id(self, space)
  if not spaceid then
    return nil, err
  end
  -- Second get the index numeric id. Which position is the index
  -- we're using to select the record.
  local indexid, err = get_index_id(self, spaceid, index)
  if not indexid then
    return nil, err
  end
  -- Create the request body (packet payload).
  local body = new_tab(0, 6)
  body[packet_keys.space_id] = spaceid
  body[packet_keys.index_id] = indexid
  body[packet_keys.key] = prepare_key(key)
  -- Add the limit.
  if opts.limit ~= nil then
    body[packet_keys.limit] = tonumber(opts.limit)
  else
    body[packet_keys.limit] = iproto.max_limit
  end
  -- Add the offset.
  if opts.offset and type(opts.offset) == 'number' then
    body[packet_keys.offset] = opts.offset
  else
    body[packet_keys.offset] = 0
  end
  -- Add the iterator if specified.
  if opts.iterator and iterator_keys[opts.iterator] ~= nil then
    body[packet_keys.iterator] = iterator_keys[opts.iterator]
  else
    -- If no valid iterator is specified then set it to 'EQ'.
    body[packet_keys.iterator] = iterator_keys.EQ
  end
  -- Make the select request.
  local response, err = request(self,
                                { [packet_keys.type] = command_keys.select },
                                body)
  -- Handle the error if it occurs.
  if err then
    return nil, err
  elseif response and response.code ~= response_keys.ok then
    return nil, response and response.error or 'Internal error.'
  else
    -- Return the response data.
    return response.data
  end
end

--- Issue a insert or replace command to the tarantool DB.
--
-- @local
--
-- @param self table connection object.
-- @param space string space name.
-- @param tuple table values to be inserted/replaced
--              (indexes, field_values).
-- @param action string insert or replace.
--
-- @return table
--   Request data response.
local function insert_replace(self, space, tuple, action)
  -- First get the space numeric id, i.e., what's the value of the
  -- space_id field in the space.
  local spaceid, err = get_space_id(self, space)
  if not spaceid then
    return nil, err
  end
  -- Issue the request. It can be an insert or replace operations.
  local response, err = request(self,
                                { [packet_keys.type] = command_keys[action] },
                                { [packet_keys.space_id] = spaceid,
                                  [packet_keys.tuple] = tuple })
  -- Handle the error if it occurs.
  if err then
    return nil, err
  elseif response and response.code ~= response_keys.ok then
    return nil, response and response.error or 'Internal error.'
  else
    -- Return the response data.
    return response.data
  end
end

--- Inserts a given set of values specified by a tuple.
--
-- @param self table connection object.
-- @param space string space name.
-- @param tuple table (indexes, field_values).
--
-- @return table
--   Inserted record.
function M.insert(self, space, tuple)
  return insert_replace(self, space, tuple, 'insert')
end

--- Replaces a given set of values specified by a tuple.
--
-- @param self table connection object.
-- @param space string space name.
-- @param tuple table (indexes, field_values). The indexes have to
--              match an existing record (tuple).
--
-- @return table
--   Record with the replaced values.
function M.replace(self, space, tuple)
  return insert_replace(self, space, tuple, 'replace')
end

--- Deletes a given tuple (record) from a space (DB). Note that the key
--  specified must belong to a primary index or any other unique index.
-- @param self table connection object.
-- @param space string space name.
-- @param key mixed query key.
--
-- @return table.
--   The deleted record.
function M.delete(self, space, key)
  -- First get the space numeric id, i.e., what's the value of the
  -- space_id field in the space.
  local spaceid, err = get_space_id(self, space)
  if not spaceid then
    return nil, err
  end
  -- Issue the request.
  local response, err = request(self,
                                { [packet_keys.type] = command_keys.delete },
                                { [packet_keys.space_id] = spaceid,
                                  [packet_keys.key] = prepare_key(key) })
  -- Handle the error if it occurs.
  if err then
    return nil, err
  elseif response and response.code ~= response_keys.ok then
    return nil, response and response.error or 'Internal error.'
  else
    -- Return the response data.
    return response.data
  end
end

--- Massages the operator list for the update operation
--  so that the field numbers are the same as in the console.
--
-- @local
--
-- @param oplist table operator list.
--
-- @return table
--   Massaged operator list.
local function prepare_op(oplist)
  local new_oplist, len = oplist, #oplist
  -- Since in the operator list the field positions differ from
  -- the ones given on the console. Here we assume that the the
  -- primary index doesn't count. So what would in the console is
  -- field 2, here is field 1, and so on.
  -- Loop over all operators.
  for i = 1, len do
    -- In each operator list component the field number is always
    -- the second element. Subtract one to field number to get the
    -- field number we need to send in the request: 2 -> 1, 3 -> 2,
    -- and so on.
    new_oplist[i][2] = oplist[i][2] - 1
  end
  -- Returned the massaged operator list.
  return new_oplist
end

--- Updates a given tuple (record) in a space (DB). Note that the key
-- specified must belong to a primary index or any other unique index.
--
-- @param self table connection object.
-- @param space string space name.
-- @param index number or string index identifier.
-- @param key number or string query key.
-- @param oplist table update operator list.
--
-- @return table.
--   Updated or upserted record.
function M.update(self, space, index, key, oplist)
  -- First get the space numeric id, i.e., what's the value of the
  -- space_id field in the space.
  local spaceid, err = get_space_id(self, space)
  if not spaceid then
    return nil, err
  end
  -- Second get the index numeric id. Which position is the index
  -- we're using to select the record.
  local indexid, err = get_index_id(self, spaceid, index)
  if not indexid then
    return nil, err
  end
  -- Issue the request.
  local response, err = request(self,
                                { [packet_keys.type] = command_keys.update }, {
                                  [packet_keys.space_id] = spaceid,
                                  [packet_keys.index_id] = indexid,
                                  [packet_keys.key] = prepare_key(key),
                                  [packet_keys.tuple] = prepare_op(oplist) })
  -- Handle the error if it occurs.
  if err then
    return nil, err
  elseif response and response.code ~= response_keys.ok then
    return nil, response and response.error or 'Internal error.'
  else
    -- Return the response data.
    return response.data
  end
end

--- Upserts a given tuple (record) in a space (DB). Note that the key
--  specified must belong to a primary index or any other unique
--  index. Upsert means update if it exists, insert if not.
--
-- @param self table connection object.
-- @param space string space name.
-- @param key mixed query key.
-- @param oplist table upsert/update operator list. These values are
--        used for the update.
-- @param new_tuple table tuple to be used as the record value when
--        inserting.
--
-- @return table.
--   Currently returns an empty table if successful.
function M.upsert(self, space, key, oplist, new_tuple)
  -- First get the space numeric id, i.e., what's the value of the
  -- space_id field in the space.
  local spaceid, err = get_space_id(self, space)
  if not spaceid then
    return nil, err
  end
  -- Issue the request.
  local response, err = request(self,
                                { [packet_keys.type] = command_keys.upsert },
                                { [packet_keys.space_id] = spaceid,
                                  [packet_keys.key] = prepare_key(key),
                                  [packet_keys.tuple] = new_tuple,
                                  [packet_keys.def_tuple] = prepare_op(oplist) })
  -- Handle the error if it occurs.
  if err then
    return nil, err
  elseif response and response.code ~= response_keys.ok then
    return nil, response and response.error or 'Internal error.'
  else
    -- Return the response data.
    return response.data
  end
end

--- Executes a stored procedure (Lua function) in a tarantool server.
-- Uses the new (0x0a) command, which does not wrap each return value in a table.
--
-- @param self table connection object.
-- @param proc string function name.
-- @param args table function arguments.
--
-- @return table
--   Result of the stored procedure invocation.
function M.call(self, proc, args)
  -- Issue the request.
  local response, err = request(self,
                                { [packet_keys.type] = command_keys.call[self.call_semantics] or error('Incorrect value for "call_semantics" option') },
                                { [packet_keys.function_name] = proc,
                                  [packet_keys.tuple] = args } )
  -- Handle the error if it occurs.
  if err then
    return nil, err
  elseif response and response.code ~= response_keys.ok then
    return nil, response and response.error or 'Internal error.'
  else
    -- Return the response data.
    return response.data
  end
end

--- Performs an eval operation in the tarantool server. I.e., it
--  evaluates the given Lua code and returns the result in a tuple.
--
-- @param self table connection object
-- @param exp string containing the Lua expression to be evaluated.
-- @param result table the tuple where the result will be returned.
--
-- @return table
--   If the Lua code evaluation was successful.
function M.eval(self, exp, result)
  -- Issue the request.
  local response, err = request(self,
                                { [packet_keys.type] = command_keys.eval },
                                { [packet_keys.expression] = exp,
                                  [packet_keys.tuple] = result } )
  -- Handle the error if it occurs.
  if err then
    return nil, err
  elseif response and response.code ~= response_keys.ok then
    return nil, response and response.error or 'Internal error.'
  else
    -- Return the response data.
    return response.data
  end
end

-- Return the module table.
return M
