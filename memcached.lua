--[[

  Copyright (C) 2016 Masatoshi Teruya

  Permission is hereby granted, free of charge, to any person obtaining a copy
  of this software and associated documentation files (the "Software"), to deal
  in the Software without restriction, including without limitation the rights
  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
  copies of the Software, and to permit persons to whom the Software is
  furnished to do so, subject to the following conditions:
 
  The above copyright notice and this permission notice shall be included in
  all copies or substantial portions of the Software.
 
  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.  IN NO EVENT SHALL THE
  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
  THE SOFTWARE.
 
  memcached.lua
  lua-cache-resty-memcached
  
  Created by Masatoshi Teruya on 16/02/12.
  
--]]

-- modules
local Cache = require('cache');
local memcached = require('resty.memcached');
local encode = require('cjson.safe').encode;
local decode = require('cjson.safe').decode;
local typeof = require('util').typeof;
local unpack = unpack or table.unpack;
-- constants
local NULL = ngx.null;
local DEFAULT_HOST = '127.0.0.1';
local DEFAULT_PORT = 11211;
local DEFAULT_OPTS = {
    -- connect timeout
    timeout = 1000,
    -- unlimited
    idle = 0,
    -- pool size
    pool = 1
};
-- errors
local EENCODE = 'encoding error: %q';
local EDECODE = 'decoding error: %q';
local EEXEC = 'execution error: %q';

-- connection class
local MemcConn = require('halo').class.MemcConn;


function MemcConn:init( host, port, opts )
    local own = protected(self);
    local opt;
    
    host = host or DEFAULT_HOST;
    port = port or DEFAULT_PORT;
    if not typeof.string( host ) then
        return nil, 'host must be string';
    elseif not typeof.uint( port ) then
        return nil, 'port must be uint';
    elseif not opts then
        opts = {};
    elseif not typeof.table( opts ) then
        return nil, 'opts must be table';
    end
    
    own.host = host;
    own.port = port;
    
    for k, v in pairs( DEFAULT_OPTS ) do
        opt = opts[k];
        if opt == nil then
            own[k] = v;
        elseif not typeof.uint( opt ) then
            return nil, ('%s must be uint'):format( k );
        else
            own[k] = opt;
        end
    end
    
    return self;
end


function MemcConn:open()
    local own = protected(self);
    local db, err = memcached:new();
    local ok, res, kerr;
    
    -- internal error
    if not db then
        return nil, err;
    end
    
    db:set_timeout( own.timeout );
    ok, err = db:connect( own.host, own.port );
    if not ok then
        return nil, err;
    end
    
    return db;
end


function MemcConn:close( db )
    local own = protected(self);
    local ok, err = db:set_keepalive( own.idle, own.pool );
    
    return err;
end



MemcConn = MemcConn.exports;


-- cache class
local CacheMemcached = require('halo').class.CacheMemcached;


function CacheMemcached:init( host, port, opts, ttl )
    local own = protected(self);
    local err;
    
    own.conn, err = MemcConn.new( host, port, opts );
    if err then
        return nil, err;
    end
    
    return Cache.new( self, ttl );
end


function CacheMemcached:set( key, val, ttl )
    local conn = protected(self).conn;
    local db, ok, err;
    
    val, err = encode( val );
    if err then
        return false, EENCODE:format( err );
    end
    
    -- got internal error
    db, err = conn:open();
    if err then
        return false, err;
    end
    
    ok, err = db:set( key, val, ttl );
    conn:close( db );
    
    return err == nil, err;
end


function CacheMemcached:get( key, ttl )
    local conn = protected(self).conn;
    local db, err = conn:open();
    local res, ok;
    
    -- got internal error
    if err then
        return nil, err;
    end

    -- update ttl
    if ttl then
        ok, err = db:touch( key, ttl );
        if not ok then
            return nil, err ~= 'NOT_FOUND' and err or nil;
        end
    end
    
    res, _, err = db:get( key );
    conn:close( db );
    
    if res then
        res, err = decode( res );
        if err then
            return nil, EDECODE:format( err );
        end
    end
    
    return res, err;
end


function CacheMemcached:delete( key )
    local conn = protected(self).conn;
    local db, err = conn:open();
    local ok;
    
    if err then
        return false, err;
    end
    
    ok, err = db:delete( key );
    conn:close( db );
    
    if not ok then
        return false, err ~= 'NOT_FOUND' and err or nil;
    end
    
    return true;
end


return CacheMemcached.exports;
