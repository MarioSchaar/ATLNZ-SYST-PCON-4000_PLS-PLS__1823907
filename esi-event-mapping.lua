-- esi-event-mapping
-- todo rename to esi-mbro-event-mapping
local MSI = require('mbro-msi')
local MBRO = require("esi-mbro")
local J = require("rapidjson")
local O = require("esi-objects")
local TOOL = require("esi-tool")
local V = require("esi-variables")
local luaxp = require("luaxp")

local lib = {EVENT_EXEC_SLOT = 5}

function lib.INFO(_)
    return {
        version = {major = 0, minor = 8, revision = 0},
        contacts = {{name = "Florian Seidl", email = "Florian.Seidl@cts-gmbh.de"}},
        library = {modulename = 'esi-event-mapping'},
        dependencies = {{modulename = 'mbro-msi', version = {major = 0, minor = 0, revision = 0}}},
        -- todo dependencies
    }
end

--- `PUSHEVENT`
--- todo add doc
function lib:PUSHEVENT(arg)
    local ok, msg = pcall(function()
        arg = arg or {}
        local rawevent = arg["event"]
                             or TOOL:GETREFERENCE(arg, {"eventpath"}, nil, function(p)
                return syslib.getvalue(p)
            end)
        local event = self:STANDARDIZEEVENT{realtime = rawevent}

        for id, provider in ipairs(arg["providers"]) do
            local funcluaxp = function(v) return luaxp.evaluate(provider["condition"], {v = v}) end
            if TOOL:GETREFERENCE({event}, {1}, false, funcluaxp) then
                local exception = {}
                for _, map in ipairs(provider["mapping"]) do
                    self:_setbyreference(exception,
                        TOOL:GETREFERENCE(event, map["lookup"], map["default"], map["func"]), map["target"], nil)
                end
                if arg["debug"] == true and arg["debugobj"] then
                    V:SET{object = arg["debugobj"], path = "event", v = event}
                    V:SET{object = arg["debugobj"], path = "eventType", v = type(event)}
                    V:SET{object = arg["debugobj"], path = "debugexpression", v = provider["condition"]}
                    V:SET{
                        object = arg["debugobj"],
                        path = "debugIf",
                        v = TOOL:GETREFERENCE({event}, {1}, false, funcluaxp),
                    }
                    V:SET{object = arg["debugobj"], path = "exception", v = exception}
                end
                -- * push exception
                local exists, exProviderObj = O:EXISTS{path = provider["providerPath"]}
                if not exists then
                    error(("Provider object does not exist (check providerPath from provider #%d)"):format(id), 2)
                end
                local exProviderId = exProviderObj:numid()
                local ex_queue_id = syslib.msgqueue(exProviderId, MSI.EXCEPTION_SLOT)
                syslib.msgpush(ex_queue_id, J.encode(exception))
            end
        end
    end)
    if ok ~= true then
        syslib.log(1, ("esi-event-mapping:PUSHEVENT %s"):format(msg), "")
        error(("esi-event-mapping:PUSHEVENT %s"):format(msg), 2)
    end
    return {ok = ok, msg = msg}
end

--- `PUSHEVENTFILTER`
function lib.PUSHEVENTFILTER(_, arg)
    local ms = syslib.now()
    arg = arg or {}
    local event = TOOL:GETREFERENCE(arg, {"eventpath"}, nil, function(p) return syslib.getvalue(p) end)
    if TOOL:STRINGTYPE(event) then event = J.decode(event) end
    local exists, exProviderObj = O:EXISTS{path = arg.providerpath}
    if not exists then error("path not provided", 2) end
    -- local exProviderId = exProviderObj:numid()
    -- local ex_queue_id = syslib.msgqueue(exProviderId, MSI.EXCEPTION_SLOT)
    local toPASXTime = function(t)
        return string.gsub(string.gsub(string.gsub(syslib.gettime(t), 'T', ' '), 'Z', ''), '%.', ',')
    end
    local exception = {
        causedAt = TOOL:GETREFERENCE(event, {"System.Timestamp"}, nil, toPASXTime), -- "OpcEvent.Header.Timestamp"
        exceptionType = TOOL:GETREFERENCE(event, {"OpcEvent.Header.Category.Text"}),
        user = TOOL:GETREFERENCE(event, {"OpcEvent.Attribs", "PROCESSVALUE06"}),
        systemDescription = TOOL:GETREFERENCE(event, {"OpcEvent.Attribs", "TEXT03"}),
        -- userDescription = TOOL:GETREFERENCE(event, {""}),
        -- manufacturingOrderID
        -- sfoID
        batchNumber = TOOL:GETREFERENCE(event, {"OpcEvent.Attribs", "TEXT04"}),
        productionUnit = TOOL:GETREFERENCE(event, {"OpcEvent.Attribs", "TEXT02"}),
        -- equipmentID
        -- operationID
        -- stepID
        exceptionComment = {
            changeDate = TOOL:GETREFERENCE(event, {"System.Timestamp"}, nil, toPASXTime), -- "OpcEvent.Header.Timestamp"
            commentText = TOOL:GETREFERENCE(event, {"OpcEvent.Attribs", "TEXT05"}),
            user = TOOL:GETREFERENCE(event, {"OpcEvent.Attribs", "PROCESSVALUE06"}),
        },
    }
    for _, loookup in ipairs(TOOL:GETREFERENCE(arg, {"lookups"}, {})) do
        if TOOL:GETREFERENCE(event, loookup.attribute, false, function(s)
            if string.find(s, loookup.pattern) then return true end
            return false
        end) then
            V:SET{path = "foundevent", v = event}
            break
        end
    end
    V:SET{path = "event", v = event}
    V:SET{path = "eventType", v = type(event)}
    V:SET{path = "exception", v = exception}
    V:SET{path = "_PC/runtime", v = syslib.now() - ms}
    -- * push exception
    -- syslib.msgpush(ex_queue_id, J.encode(exception))
    return true
end

function lib:SENDEVENT_TO_SLOT(arg)
    local ms = syslib.now()
    local rawevent = TOOL:GETREFERENCE(arg, {"eventpath"}, nil, function(p) return syslib.getvalue(p) end)
    local exists, exProviderObj = O:EXISTS{path = arg.targetpath}
    if exists then
        local event = self:STANDARDIZEEVENT{realtime = rawevent}
        -- todo add filter
        local exProviderId = exProviderObj:numid()
        local ex_queue_id = syslib.msgqueue(exProviderId, self.EVENT_EXEC_SLOT)
        local ok, errmsg = syslib.msgpush(ex_queue_id, J.encode(event))
        -- V:SET{path="_PC/runtime",v=syslib.now() -ms}
        return ("[%2dms] Send event OK=%s %s"):format(syslib.now() - ms, ok, errmsg or "")
    end
end

function lib:PROCESS_EXEC_EVENT()
    local ms = syslib.now()
    local res = {}

    local ex_queue_id = syslib.msgqueue(syslib.getself(), lib.EVENT_EXEC_SLOT)
    local events = {}
    local last_msgid
    for msgid, msg in pairs(ex_queue_id) do
        last_msgid = msgid
        table.insert(events, J.decode(msg))
    end

    if last_msgid then
        syslib.msgpop(ex_queue_id, last_msgid)
        MBRO:PROCESS_EXEC_SLOT{events = events}
        MBRO:_ensure_msgs_popped(ex_queue_id, {last_msgid})
    else
        -- * for history event only
        MBRO:PROCESS_EXEC_SLOT()
    end

    V:SET{path = "runtime", v = syslib.now() - ms}
    return res
end

function lib:STANDARDIZEEVENT(arg)
    -- * split helper
    local function split(str, separator)
        local tbl = {}
        for w in str:gmatch("([^" .. separator .. "]+)") do table.insert(tbl, w) end
        return tbl
    end
    local event = {}
    local revent = TOOL:GETREFERENCE(arg, {"realtime"}, nil)
    if TOOL:TABLETYPE(revent) then
        for k, v in pairs(revent) do
            local keyparts = split(k, ".")
            self:_setbyreference(event, v, keyparts)
        end

        local attribs = {}
        for k, v in pairs(TOOL:GETREFERENCE(event, {"OpcEvent", "Attribs"}, {})) do
            local keyparts = split(k, ".")
            self:_setbyreference(attribs, v, keyparts)
        end

        self:_setbyreference(event, attribs, {"OpcEvent", "Attribs"})
    end
    local mapping = {
        ["c_"] = {
            ["$transform"] = "Common", --
            ["m"] = {
                ["$transform"] = "Message", --
            },
            ["t"] = {
                ["$transform"] = "Timestamp", --
            },
        },
        ["o_"] = {
            ["$transform"] = "OpcEvent", --
            ["h_"] = {
                ["$transform"] = "Header", --
                ["n"] = {
                    ["$transform"] = "NewState", --
                },
            },
        },
        -- todo add rest of the Attributes
    }
    local histevents = TOOL:GETREFERENCE(arg, {"history"}, nil)
    if TOOL:TABLETYPE(histevents) then
        local eventlist = {}
        -- TODO: Is this really true? When has this behavior changed?
        for k, v in pairs(histevents) do -- * workaround, because keys could be stings!!!
            table.insert(eventlist, {k = k, v = v})
        end

        do
            table.sort(eventlist, function(a, b) return a.k < b.k end)
            local _eventlist = {}
            for _, v in ipairs(eventlist) do table.insert(_eventlist, v.v) end
            eventlist = _eventlist
        end

        for _, hevent in ipairs(eventlist) do
            local newevent = {}
            local rawevent = TOOL:GETREFERENCE(hevent, {"e_"}, {})
            -- * get OPC A&E standard Attributes
            self:_standardizeeventhistory(rawevent, mapping, {}, newevent)
            -- * get Custom Attributes
            local attlist = TOOL:GETREFERENCE(rawevent, {"o_", "a_", "a_"})
            if type(attlist) == "table" then
                for _, att in pairs(attlist) do
                    if type(att) == "table" then
                        if type(att.l) == "string" and type(att["d_"]) == "table" then
                            local keyparts = split("OpcEvent.Attribs." .. att.l, ".")
                            self:_setbyreference(newevent, att["d_"]["v"] or "", keyparts)
                        end
                    end
                end
            end
            table.insert(event, newevent)
        end
    end
    return event
end

function lib:_standardizeeventhistory(rawevent, mapping, neweventkeyparts, newevent)
    for lookup, map in pairs(mapping) do

        local transform = TOOL:GETREFERENCE(map, {"$transform"}, nil, function(t)
            if TOOL:STRINGTYPE(t) then
                local n = TOOL:DEEPCOPY(neweventkeyparts)
                table.insert(n, t)
                return n
            end
        end)
        if TOOL:TABLETYPE(transform) and lookup ~= "$transform" then
            local value = TOOL:GETREFERENCE(rawevent, {lookup})
            if TOOL:TABLETYPE(value) then
                self:_standardizeeventhistory(value, map, transform, newevent) -- * unitl value in not a table
            else
                self:_setbyreference(newevent, value, transform)
            end
        end
    end
end

--- `GETEVENTHISTORY` return the events from `arg.historySource` between the `arg.start` and `arg.end`.
--- The events are standardized by the function `STANDARDIZEEVENT`.
--- @param arg table
--- @return table events, table QueryStatus
function lib:GETEVENTHISTORY(arg)
    if not TOOL:TABLETYPE(arg.historySource) or TOOL:NILTYPE(next(arg.historySource)) then return {}, nil end

    syslib.defaults({event_history_table = true})
    local function _pasxToUnix(t)
        local utc = string.gsub(string.gsub(t, ' ', 'T'), '%,', '.') .. "Z"
        return syslib.gettime(utc)
    end
    local ms = syslib.now()
    local starttime = TOOL:GETREFERENCE(arg, {"start"}, syslib.now() - 24 * 60 * 60000, _pasxToUnix) -- todo default?
    local endtime = TOOL:GETREFERENCE(arg, {"end"}, syslib.now(), _pasxToUnix) -- todo default?
    local rawevents = {}
    local ok, msg = pcall(function()
        rawevents = syslib.geteventhistory(arg.historySource, starttime, endtime, {
            filter = TOOL:GETREFERENCE(arg, {"filter"}, nil, function(filter)
                return ('{"e_.c_.m":{"$regex":%q,"$options":""}}'):format(filter)
            end), -- '{"e_.c_.m":{"$regex":"RP104","$options":""}}'
            sort = ('{"e_.c_.t" : %d}'):format( -- Default (as in "anything that is not DESC) is ASC
            arg.ordering == "DESC" and -1 or 1),
            data_store = arg.data_store,
            limit = arg.limit,
            skip = arg.skip,
        })
    end)
    -- todo add log error if ok == false
    local mshistquery = syslib.now() - ms
    local events = self:STANDARDIZEEVENT{history = rawevents}
    local status = {
        ["RuntimeQuery"] = mshistquery,
        ["Runtime"] = syslib.now() - ms,
        ["starttimeUTC"] = syslib.gettime(starttime),
        ["endtimeUTC"] = syslib.gettime(endtime),
        ["starttime"] = starttime,
        ["endtime"] = endtime,
        ["geteventhistoryOK"] = ok,
        ["geteventhistoryMsg"] = msg,
        ["EventCount"] = #events,
    }
    return events, status
end

function lib.FIND_FIRST_EVENT(_, events, condition)
    V:SET{path = "_PC/number_of_events", v = #events}

    if TOOL:TABLETYPE(events) then
        V:SET{path = "_PC/number_of_events", v = #events}

        local function findraw(argv)
            -- LuaXP calls such functions with one argument, which is a list of all given actual arguments
            local s, pattern, init = table.unpack(argv)

            -- Use parens to only use first return value. LuaXP does the same with find, so let's stay consistent here.
            return (string.find(s, pattern, init, true) or 0)
        end

        local expression = luaxp.compile(condition)
        for k = 1, #events, 1 do
            local funcluaxp = function(v)
                local context = {v = v, findraw = findraw}
                return luaxp.run(expression, context)
            end

            if TOOL:GETREFERENCE(events, {k}, false, funcluaxp) then return events[k] end
        end
    end
    return {}
end

function lib:FIND_FIRST_EVENT_FROM_HISTORY(arg)
    local result

    while not (result and next(result)) do
        if arg.pagination_size > 0 then
            arg.limit = arg.pagination_size
            arg.skip = (arg.skip or -arg.pagination_size) + arg.pagination_size
        end

        local events = self:GETEVENTHISTORY(arg)
        result = self:FIND_FIRST_EVENT(events, arg.condition)

        if arg.pagination_size <= 0 or #events ~= arg.pagination_size then
            break
        end
    end

    return result or {}
end

--- _setbyreference helper to set values
--- <Example>
--- local _tonumber=function(a) return tonumber(a) end
--- self:_setbyreference(result, 10, {"ranges","min"},_tonumber)
--- </Example>
--- @param data table | "the destination table where the 'value' gets stored"
--- @param value any | "can be a string, number, boolean, table"
--- @param ref table | "table structure in hierarchical ordered keys"
--- @param func function | "the given 'value' gets taken as function parameter"
function lib._setbyreference(_, data, value, ref, func)
    local ok, msg = pcall(function()
        local dataref = data
        for k, target in ipairs(ref) do
            if #ref == k then
                if TOOL:FUNCTIONTYPE(func) then pcall(function() value = func(value) end) end
                dataref[target] = value
            else
                dataref[target] = dataref[target] or {}
                if TOOL:TABLETYPE(dataref[target]) then
                    dataref = dataref[target]
                else
                    return false
                end
            end
        end
        return false
    end)
    if ok then
        return msg
    else
        return {msg = msg}
    end
end

return lib
