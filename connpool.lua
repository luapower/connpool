
--Connection pools.
--Written by Cosmin Apreutesei. Public domain.

--Connection pools allow reusing and sharing a limited number of connections
--between multiple threads in order to 1) avoid creating too many connections
--and 2) avoid the lag of connecting and authenticating every time
--a connection is needed.

--The pool mechanics is simple (it's just a free list) until the connection
--limit is reached and then it gets more complicated: in order to honor the
--timeout on connect() and also preserve fifo order of waiting threads,
--the threads are put on a fifo waiting list, but they're also put in a heap
--with the most-impatient thread at the top, and a "waker" thread is set up
--to time-out the expired threads in the heap.

local glue = require'glue'
local sock = require'sock'
local queue = require'queue'
local heap = require'heap'

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

		local q, h, exp_thread, exp_thread_suspended

		local function cmp_expires(a, b) return a[2] < b[2] end

		local function wait(expires)
			if waitlist_limit < 1 then --waiting list disabled
				return nil, 'busy'
			end
			if expires <= sock.clock() then --doesn't want to wait
				return nil, 'busy'
			end
			if not q then --first time we don't have enough connections.
				q = queue.new(waitlist_limit, 4)
				h = heap.valueheap{cmp = cmp_expires, index_key = 3}
				exp_thread = sock.newthread(function()
					while true do
						local t = h:peek()
						if t then
							local expires = t[2]
							local now = sock.clock()
							if expires < now then --expired
								h:pop()
								q:remove(t)
								sock.resume(t[1], nil, 'busy')
								break
							else
								sock.sleep_until(expires)
							end
						else
							exp_thread_suspended = true
							sock.suspend()
							exp_thread_suspended = false
						end
					end
				end)
				exp_thread_suspended = true
			end
			check_expired()
			if q:full() then --waiting list full
				return nil, 'busy'
			end
			--finally we can queue up and wait.
			local t = {sock.currentthread(), expires}
			q:push(t)
			h:push(t)
			if exp_thread_suspended then
				sock.resume(exp_thread)
			end
			return sock.suspend()
			--^^ either exp_thread or check_waitlist() will wake us up.
		end

		local function check_waitlist()
			local t = q and q:pop()
			if not t then return end
			h:remove(t)
			sock.resume(t[1], true)
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
