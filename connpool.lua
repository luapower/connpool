
--Connection pools.
--Written by Cosmin Apreutesei. Public domain.

--Connection pools allow reusing and sharing a limited number of connections
--between multiple threads in order to 1) avoid creating too many connections
--and 2) avoid the lag of connecting and authenticating every time
--a connection is needed.

--The pool mechanics is simple (it's just a free list) until the connection
--limit is reached and then it gets more complicated because we need to put
--the threads on a waiting list and resume them in fifo order and we also
--need to remove them from wherever they are on the waiting list on timeout.
--This is made easy because we have: 1) a ring buffer that allows removal at
--arbitrary positions and 2) sock's interruptible timers.

local glue = require'glue'
local sock = require'sock'
local queue = require'queue'

local add = table.insert
local pop = table.remove

local M = {}

function M.new(opt)

	local all_limit = opt and opt.max_connections or 100
	local all_waitlist_limit = opt and opt.max_waiting_threads or 1000
	assert(all_limit >= 1)
	assert(all_waitlist_limit >= 0)

	local pools = {}
	local servers = {}

	local function pool(host, port)
		local key = host..':'..port
		local pool = servers[key]
		if pool then
			return pool
		end
		pool = {}
		servers[key] = pool

		local n = 0
		local free = {}
		local limit = all_limit
		local waitlist_limit = all_waitlist_limit

		function pool:setlimits(opt)
			limit = opt.max_connections or limit
			waitlist_limit = opt.max_waiting_threads or waitlist_limit
			assert(limit >= 1)
			assert(waitlist_limit >= 0)
		end

		local q
		local function wait(expires)
			if waitlist_limit < 1 or not expires or expires <= sock.clock() then
				return nil, 'timeout'
			end
			q = q or queue.new(waitlist_limit, 'queue_index')
			if q:full() then
				return nil, 'timeout'
			end
			local sleeper = sock.sleep_job()
			q:push(sleeper)
			if sleeper:sleep_until(expires) then
				return true
			else
				return nil, 'timeout'
			end
		end

		local function check_waitlist()
			local sleeper = q and q:pop()
			if not sleeper then return end
			sleeper:wakeup(true)
		end

		function pool:get(expires)
			local c = pop(free)
			if c then
				return c
			end
			if n >= limit then
				local ok, err = wait(expires)
				if not ok then return nil, err end
				local c = pop(free)
				if c then
					return c
				end
				if n >= limit then
					return nil, 'busy'
				end
			end
			return nil, 'empty'
		end

		function pool:put(c, s)
			assert(n < limit)
			pool[c] = true
			n = n + 1
			glue.before(s, 'close', function()
				pool[c] = nil
				n = n - 1
				check_waitlist()
			end)
			function c:release()
				add(free, c)
				check_waitlist()
			end
			return c
		end

		return pool
	end

	function pools:setlimits(host, port, opt)
		assert(limit >= 1)
		pool(host, port):setlimits(opt)
	end

	function pools:get(host, port, expires)
		return pool(host, port):get(expires)
	end

	function pools:put(host, port, c, s)
		return pool(host, port):put(c, s)
	end

	return pools
end


if not ... then

	local pool = M.new{max_connections = 2, max_waiting_threads = 1}
	local h, p = 'test', 1234

	sock.run(function()

		local c1 = pool:put(h, p, {}, {})
		local c2 = pool:put(h, p, {}, {})
		print(pool:get(h, p, sock.clock() + 1))

		--local c, err = pool:get(h, p, 5)
		--assert(not c and err == 'empty')
		--local s = {close = function() print'close' end}
		--local c1 = {s = s}
		--c = pool:put(h, p, c1, s)
		--c:release()
		--local c, err = pool:get(h, p, 5)
		--assert(c == c1)
		--s:close()

	end)

end

return M
