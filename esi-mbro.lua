-- esi-mbro
local inQ = require("inmation.api-quality")
local J = require("rapidjson")
local CAT = require("esi-catalog")
local TOOL = require("esi-tool")
local O = require("esi-objects")
local V = require("esi-variables")
local STR = require("esi-string")
local MSI = require('mbro-msi')
local luaxp = require("luaxp")

local lib = {
    funclibs = {},
    MES_TO_EXEC_SLOT = 4,
    RESPONSE_SLOT = 5,
    EXECUTOR_MODE = {
        ONESHOT = 1,
        TRIGGER = 2,
        CONTINUOUS = 4,
        CONTINUOUS_EVENTS = 8,
    },
    DEFAULT_DATA_STORE_NAME = "ResponseMessagesBuffer",
}

function lib.INFO(_)
    return {
        version = {
            major = 0,
            minor = 6,
            revision = 0,
        },
        contacts = {
            {
                name = "Florian Seidl",
                email = "Florian.Seidl@cts-gmbh.de",
            },
            {
                name = "Florian Gamböck",
                email = "Florian.Gamboeck@cts-gmbh.de",
            },
        },
        library = {
            modulename = 'esi-mbro',
        },
        dependencies = {
            {
                modulename = "inmation.api-quality",
                version = {
                    major = 0,
                    minor = 0,
                    revision = 0,
                },
            },
            {
                modulename = "dkjson",
                version = {
                    major = 0,
                    minor = 0,
                    revision = 0,
                },
            },
            {
                modulename = "esi-tool",
                version = {
                    major = 1,
                    minor = 0,
                    revision = 5,
                },
            },
            {
                modulename = "esi-objects",
                version = {
                    major = 1,
                    minor = 72,
                    revision = 0,
                },
            },
            {
                modulename = "esi-variables",
                version = {
                    major = 1,
                    minor = 0,
                    revision = 2,
                },
            },
            {
                modulename = "esi-string",
                version = {
                    major = 1,
                    minor = 72,
                    revision = 0,
                },
            },
            {
                modulename = "mbro-msi",
                version = {
                    major = 1,
                    minor = 66,
                    revision = 1,
                },
            },
            {
                modulename = "luaxp",
                version = {
                    major = 0,
                    minor = 0,
                    revision = 0,
                },
            },
        },
    }
end

--- _scopedcall
---
--- Call a function with a given comment for the Audit trail. The `funcname`
--- will be included in the comment and should be the calling function. The
--- `func` argument is the function object that will be called.
---
--- @param funcname string Name of the calling function
--- @param comment string Comment to include
--- @param func function Function to be called
--- @vararg any Arguments for called function
local function _scopedcall(funcname, comment, func, ...)
    local prefix
    do
        local libname = lib:INFO().library.modulename
        local version = lib:INFO().version

        prefix = ("%s:%s-%s.%s.%s"):format(libname, funcname, version.major, version.minor, version.revision)
    end

    return syslib.scopedcall(
            { comment = ("%s: %s"):format(prefix, comment) },
            func,
            ...
    )
end

--- INIT
---
--- Initialize required objects to make this library work as intended.
function lib:INIT()
    local core_path = syslib.getcorepath(syslib.getselfpath())
    local core_object = syslib.getobject(core_path)

    -- GTSB
    do
        local data_store_config = self:_get_data_store_config_from_name(lib.DEFAULT_DATA_STORE_NAME)

        local parent_path, object_name
        if data_store_config then
            local object = syslib.getobject(data_store_config.datastores[1])
            parent_path = object:parent():path()
            object_name = object.ObjectName
        else
            parent_path = core_path .. "/Data Stores"
            object_name = lib.DEFAULT_DATA_STORE_NAME
        end

        local object = O:UPSERTOBJECT{
            path = parent_path,
            class = "MODEL_CLASS_GENERICTIMESERIESBUFFER",
            properties = {
                [".ObjectName"] = object_name,
                [".ObjectDescription"] = "Buffer to forward response messages to the MessageProcessor",
                [".DisabledSafMode"] = syslib.model.codes.DisabledSaFMode.KEEP,
                [".AdvancedLuaScript"] = [=[
-- GTSB script body

-- force reload of esi-mbro to pick up latest changes
package.loaded["esi-mbro"] = nil

return require("esi-mbro"):HANDLE_RESPONSE_MESSAGES(...)
            ]=],
            },
        }

        if not data_store_config then
            local data_store_sets = core_object.DataStoreConfiguration.DataStoreSets

            local next_id
            for _, row in ipairs(data_store_sets or {}) do
                if not next_id or next_id < row.rowid then next_id = row.rowid end
            end
            next_id = (next_id or 31) + 1

            table.insert(data_store_sets, {
                name = lib.DEFAULT_DATA_STORE_NAME,
                description = "Data Store to buffer and forward response messages to the MessageProcessor",
                datastores = {object:numid() + 1},
                rowid = next_id,
            })

            core_object.DataStoreConfiguration.DataStoreSets = data_store_sets
            core_object:commit()
        end
    end

    -- Garbage Collector
    do
        local path = core_path .. "/Core Logic"
        local name = "Garbage Collector"

        -- Properties that will be ensured on every call
        local properties = {
            [".ObjectName"] = name,
            [".ObjectDescription"] = "Cleans up stale objects",
            [".GenerationType"] = syslib.model.codes.SelectorGenItem.LUASCRIPT,
            [".DedicatedThreadExecution"] = true,
            [".GenLuaScript.AdvancedLuaScript"] = [=[
-- Garbage Collector script body

-- force reload of esi-mbro to pick up latest changes
package.loaded["esi-mbro"] = nil

return require("esi-mbro"):GC()
            ]=],
        }

        -- Properties that will only be set when newly created
        local properties_new = {
            -- Might have been manually updated, do not overwrite!
            [".GenerationPeriod"] = 60000,
        }

        if not syslib.getobject(("%s/%s"):format(path, name)) then TOOL:MERGETABLE(properties, properties_new) end

        O:UPSERTOBJECT{path = path, class = "MODEL_CLASS_GENITEM", properties = properties}
    end
end

--- GC
---
--- Clean up stale objects. Mainly this is used to remove stale items under Connectors, since syslib.deleteobject does
--- only work from Core components, not within Connectors.
function lib:GC()
    local function _is_object_under_my_core(object)
        local path = object:path()
        local core_path = syslib.getcorepath()
        return path:sub(1, core_path:len()) == core_path
    end

    -- Delete marked objects. Necessary if they are beneath connector,
    -- where deletion is not possible from within the connector.
    do
        local to_delete = CAT:FIND("CustomPropertyName.esi-mbro:gc", "delete")
        for _, o in ipairs(to_delete) do
            -- CAT always returns all objects from the whole system. We can and should only operate on "our" elements,
            -- so restrict it to the current LocalCore.
            if _is_object_under_my_core(o) then pcall(syslib.deleteobject, o) end
        end
    end

    -- Purge executor queue. Necessary if executor is beneath connector,
    -- where deletion is not possible during execution.
    do
        local slots = CAT:FIND("CustomPropertyName.esi-mbro:gc", "custom_queue_slot")
        for _, o in ipairs(slots) do
            -- CAT always returns all objects from the whole system. We can and should only operate on "our" elements,
            -- so restrict it to the current LocalCore.
            if _is_object_under_my_core(o) then self:queue_get(o:parent():parent(), o.ObjectName):purge() end
        end
    end
end

--- _get_timer
---
--- Create new timer object. This can be used for easier time measurements.
---
--- The optional arguments table can contain the following members:
--- -   `write_immediately`, boolean: Write the new time measurement as soon
---     as `t_end()` is called.
--- -   `delete_after_write`, boolean: Used together with `write_immediately`.
---     Delete time measurement right after immediately writing it. This can
---     help if there are a lot of time measurements that are not needed once
---     they were written.
---
--- Example:
---
--- ```
--- local MBRO = require("esi-mbro")
---
--- local timer = MBRO._get_timer()
--- timer.overall:t_start()
---
--- timer.first_sleep:t_start()
--- syslib.sleep(5)
--- timer.first_sleep:t_end()
---
--- timer.second_sleep:t_start()
--- syslib.sleep(5)
--- timer.second_sleep:t_end()
---
--- timer.third_sleep:t_run(function()
---     syslib.sleep(5)
--- end)
---
--- timer.overall:t_end()
---
--- -- Write times to variables beneath _PC/Times
--- timer:write_times()
--- ```
---
--- Example with immediate write:
---
--- ```
--- local MBRO = require("esi-mbro")
---
--- local timer = self:_get_timer {
---     write_immediately = true,
---     delete_after_write = true,
--- }
--- timer.overall:t_start()
---
--- timer.sleep:t_start()
--- syslib.sleep(5)
--- timer.sleep:t_end()
---
--- -- The time for "sleep" has automatically been written
---
--- timer.overall:t_end()
---
--- -- No need to explicitly call `write_times()`. In fact, it would not write
--- -- anything, since `delete_after_write` automatically cleaned the
--- -- measurements after writing.
--- ```
---
--- @param arg table Arguments table
--- @return table Timer object
function lib._get_timer(_, arg)
    local write_immediately = TOOL:GETREFERENCE(arg, {"write_immediately"})
    local delete_after_write = TOOL:GETREFERENCE(arg, {"delete_after_write"})
    local callback_start = TOOL:GETREFERENCE(arg, {"callback_start"})
    local callback_end = TOOL:GETREFERENCE(arg, {"callback_end"})

    local timer = {
        write_times = function(self)
            for k, v in pairs(self) do
                if type(v) == "table" and v.value ~= nil then
                    V:SET{
                        path = "_PC/Times/" .. k,
                        v = v.value,
                    }
                end
            end
        end,
    }

    local meta = {
        __index = function(t, i)
            local new_item = {
                t_start = function(self)
                    self.value = syslib.now()

                    if TOOL:FUNCTIONTYPE(callback_start) then
                        callback_start(i, self.value)
                    end
                end,

                t_end = function(self)
                    self.value = syslib.now() - self.value

                    if write_immediately then
                        V:SET{
                            path = "_PC/Times/" .. i,
                            v = self.value,
                        }
                        if delete_after_write then t[i] = nil end
                    end

                    if TOOL:FUNCTIONTYPE(callback_end) then
                        callback_end(i, self.value)
                    end
                end,

                t_run = function(self, func, ...)
                    self:t_start()
                    local results = table.pack(func(...))
                    self:t_end()
                    return table.unpack(results)
                end
            }
            t[i] = new_item
            return new_item
        end,
    }

    setmetatable(timer, meta)

    return timer
end

local function _add_tracker_item(arg, step)
    arg.Tracker = arg.Tracker or {}

    local tbl = arg.Tracker
    local timestamp = syslib.now()

    local track_item = {
        step = step,
        timestamp = timestamp,
        -- if tbl is empty, use current timestamp, so the diff is 0
        diff_to_previous = timestamp - (TOOL:GETREFERENCE(tbl, {#tbl, "timestamp"}, timestamp))
    }

    table.insert(tbl, track_item)
end

--- GETFUNC
---
--- Load function definition (funcdef) from specified library.
---
--- The `arg` table is expected to contain two members:
--- -   `name` being the name of the desired function definition.
--- -   `library` being the library where to find the funcdef.
---
--- The `default` table is used when no library was given in the argument
--- table. It consists of two members:
--- -   `libdef` being the imported library where to find the desired funcdef.
--- -   `libname` being the imported name of the library.
---
--- @param arg table Arguments table
--- @param default table Defaults table
--- @return table Function definition
function lib.GETFUNC(_, arg, default)
    local name = TOOL:GETREFERENCE(arg, {"name"}, "")
    local definition
    local _, msg = pcall(function()
        local libdef = TOOL:GETREFERENCE(default, {"libdef"})
        local libname = TOOL:GETREFERENCE(default, {"libname"})
        TOOL:GETREFERENCE(arg, {"library"}, nil, function(l)
            libname = tostring(l)
            libdef = require(libname)
        end)

        local f = libdef[name]
        if not TOOL:FUNCTIONTYPE(f) then error(name .. " function not found in " .. libname .. "!", 2) end

        definition = f(libname)
    end)

    if not TOOL:TABLETYPE(definition) then
        error(("Unable to get the definition %s! Error: %s"):format(name, msg), 2)
    end

    return definition
end

--- GETFUNC_PARAMETER_LOOKUP
---
--- Load parameter lookup table for funcdef.
---
--- This function is similar as `GETFUNC`, but uses the second return value of
--- the funcdef to build the lookup table for more complex parameter
--- processing.
---
--- The `arg` table is expected to contain two members:
--- -   `name` being the name of the desired function definition.
--- -   `library` being the library where to find the funcdef.
---
--- The `default` table is used when no library was given in the argument
--- table. It consists of two members:
--- -   `libdef` being the imported library where to find the desired funcdef.
--- -   `libname` being the imported name of the library.
---
--- @param arg table Arguments table
--- @param default table Defaults table
--- @return table Lookup table for function definition
function lib:GETFUNC_PARAMETER_LOOKUP(arg, default)
    local name = TOOL:GETREFERENCE(arg, {"name"}, "")
    local lookup
    local _, msg = pcall(function()
        local libdef = TOOL:GETREFERENCE(default, {"libdef"})
        local libname = TOOL:GETREFERENCE(default, {"libname"})
        TOOL:GETREFERENCE(arg, {"library"}, nil, function(l)
            libname = tostring(l)
            libdef = require(libname)
        end)

        local f = libdef[name]
        if not TOOL:FUNCTIONTYPE(f) then error(name .. " function not found in " .. libname .. "!", 2) end

        local _, func = f(libname)
        local parameter = TOOL:GETREFERENCE(arg, {"content", "parameter"}, {},
            function(p) return self:PASXPARAM2TABLE(p) end)

        lookup = func(parameter)
    end)

    if not TOOL:TABLETYPE(lookup) then
        error(("Unable to get the lookup-parameter %s! Error: %s"):format(name, msg), 2)
    end

    return lookup
end

--- FUNCTIONEXECUTION
---
--- Calls the function defined as a funcdef in the `arg` table.
---
--- The `arg` table is expected to contain the following members:
--- -   `id`: Message id, this comes from the second parameter of the input
---     function.
--- -   `queue_object`: The target object which will be periodically checked
---     by the ouput function.
---
---     This can either be an absolute path to an object or a simple string
---     like a short name to identify the message. In the latter case, the
---     target object will be found under the MessageProcessor, in a folder
---     with the given name.
---
---     In each case, if the object does not yet exist, it will be
---     automatically created, including intermediate components.
--- -   `data_store_name`: The data store that will be used to process the
---     result and create the response message. Most likely this will be a
---     GTSB. If unset, the default value of "ResponseMessagesBuffer" will be
---     used.
---
---     Technically, this works by creating a new object for the result (if
---     not already exists) and set the found data store as the object
---     archive. The given data store is expected to create the response
---     message.
--- -   `pasx`: The message content, this comes from the first parameter of
---     the input function. It is possible to preprocess the data before
---     creating the function definition, as long as the structure of the
---     message contents are not altered.
--- -   `funcdefinition`: The function definition to execute.
--- -   `parameter`: The parameters to use together with the funcdef. Most
---     likely this will be `TOOL:GETREFERENCE(content, {"parameter"}, {},
---     function(p) return MBRO:PASXPARAM2TABLE(p) end)`. With this, one gets
---     access of all Pas-X parameters in a proper Lua table.
--- -   `lookup-parameter`: Optional table of parameters that need to be
---     calculated from the message content.
--- -   `default`: Optional table of static default parameters.
--- -   `chunk_function`: Instead of `funcdefinition`, a reference to a local
---     Lua function can be given, which gets automatically converted into a
---     binary Lua chunk that gets sent to the executor.
--- -   `chunk_argument`: Arguments for the chunk function.
--- -   `credentials`: Optional table of credentials if such are needed. Most
---     likely this will be of the form `MBRO:GETCREDENTIALS{profile =
---     "cred_Demo_Connector"}`.
--- -   `path`: The inmation path where the function should be executed. This
---     can be a path on the core system or a remote path under a connector. If
---     unset, a default path "MessageExecutor" under the first found Core
---     object will be used.
--- -   `timeout`: For asynchronous messages, this is the timeout in milliseconds
---     after which the function will no longer be executed. If this is set to 0,
---     the message will never time out, it will stay in the message queue until
---     it produces a response.
--- -   `timeout_error`: This table configures the error message that will be
---     created when the above defined timeout has been reached. It consists of
---     the following two optional members:
---     -   `code`: Arbitrary error code. Defaults to empty string if unset.
---     -   `text`: Arbitrary error text. Defaults to "Reached timeout" if
---         unset.
---
--- Example for Input-Script:
---
--- ```lua
--- local MBRO = require("esi-mbro")
--- local TOOL = require("esi-tool")
---
--- local funcdef = {
---     {
---         func = {
---             lib = "MYLIB",
---             libname = "mylib",
---             func = "echo",
---             arg = {
---                 text = {
---                     ["$lookup"] = {"lookup-parameter", "Text"},
---                     ["$default"] = {
---                         ["$lookup"] = {"default", "Text"},
---                     },
---                 },
---             },
---         },
---     },
--- }
---
--- local function lookup(parameter)
---     local text = TOOL:GETREFERENCE(parameter, {"Text", "value"}, nil,
---         function(s) return ("Given text: %s"):format(s) end)
---     return {
---         Text = text,
---     }
--- end
---
--- return function(content, id)
---     local parameter = TOOL:GETREFERENCE(content, {"parameter"}, {}, function(p) return MBRO:PASXPARAM2TABLE(p) end)
---
---     MBRO:FUNCTIONEXECUTION{
---         id = id,
---         queue_object = "DemoMessage",
---         data_store_name = "DemoGTSB",
---         pasx = content,
---         funcdefinition = funcdef,
---         parameter = parameter,
---         ["lookup-parameter"] = lookup(parameter),
---         default = {
---             Text = "Default text",
---         },
---         credentials = MBRO:GETCREDENTIALS{
---             profile = "cred_Demo_Connector",
---         },
---         path = "/System/Core/Demo_Connector/MessageExecutor",
---         timeout = 10,
---     }
--- end
--- ```
---
--- @param arg table Arguments table
function lib:FUNCTIONEXECUTION(arg)
    arg.pasx = arg.pasx or {}
    _add_tracker_item(arg.pasx, "Received message")

    V:SET{
        path = "debug/received_message",
        v = {[arg.pasx.messageId] = arg.pasx},
    }

    self:INIT()

    arg.func = "_functionexecution"
    return self:_callfunc(arg)
end

-- TODO: Maybe move to a more general library, like esi-tool?
--- _create_hierarchy
---
--- This function creates a given folder, together with all missing components
--- in between.
---
--- @param path string Path of the folder to be created
--- @param object_type number Object type of the hierarchy to be created, given as a constant
local function _create_hierarchy(path, object_type)
    object_type = object_type or syslib.model.classes.GenFolder

    local to_create = {}

    local current_path = path
    while true do
        local parentpath, name = syslib.splitpath(current_path)

        if not (parentpath and name) then
            break
        end

        local exists, _ = O:EXISTS {
            parentpath = parentpath,
            objectname = name,
        }

        if exists then
            break
        else
            table.insert(to_create, 1, {
                class = object_type,
                operation = syslib.model.codes.MassOp.UPSERT,
                path = current_path,
                ["ObjectName"] = name,
            })
        end

        current_path = parentpath
    end

    if to_create[1] ~= nil then
        syslib.mass(to_create)
    end
end

-- TODO: Maybe move to a more general library, like esi-tool?
--- _create_hierarchy_from_base
---
--- This function creates a given folder, together with all missing components
--- in between, starting from a given base directory.
---
--- In contrast to _create_hierarchy, this method expects the new path to be
--- given as a table of single object names. This is easier if some components
--- might contain characters that need to be escaped, which is done
--- automatically in this method.
---
--- @param base string Base path of the new objects
--- @param parts table Path of the folder to be created, given as path segments
--- @param object_type string Object type of the hierarchy to be created, given as string
local function _create_hierarchy_from_base(base, parts, object_type)
    object_type = object_type or "MODEL_CLASS_GENFOLDER"

    local current_path = base
    for _, name in ipairs(parts)do
        local exists, obj = O:EXISTS {
            parentpath = current_path,
            objectname = name,
        }

        if not exists then
            obj = syslib.createobject(current_path, object_type)
            obj.ObjectName = name
            obj:commit()
        end

        current_path = obj:path()
    end
end

--- _get_response_queue_path
---
--- Get the absolute path to the queue object.
---
--- The `queue_object` can either be given:
--- -   as an absolute path (starting with "/"), in which case it gets
---     returned unaltered.
--- -   as the name of the desired queue object, in which case the resulting
---     path will be `<queue_object>/queue` under the current object.
---
--- @param queue_object string Path or name of desired response queue object
--- @return string Absolute path to queue object
local function _get_response_queue_path(queue_object)
    if queue_object:sub(1, 1) ~= "/" then
        -- If queue_object is not an absolute path, assume it to be the
        -- MessageId and use the default path.
        queue_object = ("%s/%s/queue"):format(syslib.getselfpath(), queue_object)
    end

    return queue_object
end

--- _callfunc
---
--- Create necessary objects and call given funcdef.
---
--- @param arg table Arguments table
function lib:_callfunc(arg)
    -- Use sane default if path is unset
    arg.path = arg.path or ("%s/MessageExecutor"):format(syslib.getcorepath())

    if not arg.executor_init_done then
        -- Recursively create folder structure if not exists
        _create_hierarchy(arg.path, syslib.model.classes.GenFolder)

        if type(arg.queue_object) == "string" then
            arg.queue_object = _get_response_queue_path(arg.queue_object)
        end

        if arg.chunk_function then
            if type(arg.chunk_function) == "function" then
                arg.chunk_function = syslib.enbase64(string.dump(arg.chunk_function))
            else
                -- needs to be handled in Executor, to show the error in the right place
                arg.chunk_function_invalid = true
            end
        end
    end

    local libName = self:INFO().library.modulename
    local call = ("return require(%q):%s(%q)"):format(libName, arg.func, J.encode(arg))
    local resexecute, resmsg = syslib.execute(arg.path, call, 0)
    return {resexecute, resmsg}
end

-- TODO: Not limited to mbro, maybe move to more general library?
--- GETCREDENTIALS
---
--- Get specified credentials.
---
--- The arguments table contains:
--- -   `profile` being the name of the object which holds the desired credentials.
---
--- @param arg table Arguments table
--- @return table Found credentials
function lib.GETCREDENTIALS(_, arg)
    local ex, profile = O:EXISTS{path = ("/%s"):format(arg.profile)}
    if not ex then error("Credential Profile not found!", 2) end
    local credentials = {}
    credentials.username = O:GETCUSTOM{object = profile, key = "username"}
    credentials.usernamelong = O:GETCUSTOM{object = profile, key = "usernamelong"}
    credentials.password = profile.InmationProfileCredentials.Password
    credentials.domain = O:GETCUSTOM{object = profile, key = "domain"}
    return credentials
end

-- TODO: Not limited to mbro, maybe move to more general library?
--- GETDCOMCREDENTIALS
---
--- Get DCOM credentials from current Connector.
---
--- The arguments table contains:
--- -   `path` being the path of an object under a Connector. Can be nil to use
---     the current executing object path.
---
--- @param arg table Arguments table
--- @return table Found credentials
function lib.GETDCOMCREDENTIALS(_, arg)
    arg = arg or {}

    local connector_path = syslib.getconnectorpath(arg.path)
    if not connector_path then error("Must be called from within a connector!", 2) end

    local credentials = {}
    credentials.username = syslib.getvalue(connector_path .. ".DCOMCredentials.UserName")
    credentials.password = syslib.getvalue(connector_path .. ".DCOMCredentials.Password")
    credentials.domain = syslib.getvalue(connector_path .. ".DCOMCredentials.Domain")

    return credentials
end

-- TODO: Not limited to mbro, maybe move to more general library?
--- GETCREDENTIALSFROMSTORE
---
--- Get credentials from a Stored Credential object.
---
--- The arguments table contains:
--- -   `path` being the path of the desired Stored Credential object.
---
--- @param arg table Arguments table
--- @return table Found credentials
function lib.GETCREDENTIALSFROMSTORE(_, arg)
    arg = arg or {}
    local credentials

    if not arg.path then
        -- Compatibility to previous logic: If the argument is nil, just
        -- return it as such. Some calls do not need credentials and before we
        -- did not raise an error beacuse of this.
    elseif arg.path.username and arg.path.password and arg.path.domain then
        -- Compatibility to previous logic: If the argument is in fact a table
        -- with credentials, use these.
        credentials = arg.path
    else
        -- Otherwise, fetch them from the store object.
        local cred_object = syslib.getobject(arg.path)
        if not cred_object then error("Stored Credentials object does not exist!", 2) end

        credentials = {}
        credentials.username = cred_object.DomainCredentials.UserName
        credentials.password = cred_object.DomainCredentials.Password
        credentials.domain = cred_object.DomainCredentials.Domain
    end

    return credentials
end

--- GETCENTRALMAPPINGTABLE
---
--- Return an easy access mapping table for easier usage in custom scripts.
---
--- The arguments table contains:
--- -   `path` being the path to the desired mapping table. This parameter is
---     optional. If not given, the CentralMapping table will be determined by
---     starting from the LocalCore, looking for "Message Broker (MSI)/Message
---     Processor.MSIMsgDHistorianConfigList".
---
--- Example:
---
--- ```lua
--- local MBRO = require("esi-mbro")
---
--- -- This will return the mapping table of MessageProcessor of the current LocalCore
--- local mapping = MBRO:GETCENTRALMAPPINGTABLE()
--- syslib.getvalue(mapping.DummyEquipment1.DummyTag2)
---
--- -- This will return the specified mapping table
--- local path = "/System/Core/SITE-Relay/SITE/Message Broker (MSI)/FlexMapping.MSIMsgDProperties.MSIMsgDFlexOptions.MSIMsgDFlexConfigList"
--- local mapping = MBRO:GETCENTRALMAPPINGTABLE{path=path}
--- syslib.getvalue(mapping.CustomEquipment.CustomTag)
--- ```
---
--- @param arg table Arguments table
--- @return table Mapping table
function lib.GETCENTRALMAPPINGTABLE(_, arg)
    arg = arg or {}

    if not arg.path then
        pcall(function()
            -- The explicit call to selfpath is necessary because it seems sometimes
            -- the default behavior does not work together with syslib.execute.
            local corepath = syslib.getcorepath(syslib.getselfpath())
            local obj = syslib.getobject(corepath):child("Message Broker (MSI)"):child("Message Processor")
            arg.path = ("%s.MSIMsgDHistorianConfigList"):format(obj:path())
        end)
    end

    local mapping
    local ok = pcall(function()
        mapping = syslib.getvalue(arg.path)
    end)

    if not ok then
        error("Mapping table not found")
    end

    local t = {}
    for _, entry in ipairs(mapping) do
        t[entry.EquipmentId] = t[entry.EquipmentId] or {}
        t[entry.EquipmentId][entry.Tag] = entry.Path
    end

    return t
end

--- PASXPARAM2TABLE
---
--- Convert Pas-X parameters to proper Lua tables for easier access.
---
--- @param data table Parameter table from Pas-X
--- @param key string Name of the key to extract, defaults to "name"
--- @return table Lua table of Pas-X parameters
function lib.PASXPARAM2TABLE(_, data, key)
    key = key or "name"
    local parameter = {}
    for _, param in ipairs(data) do parameter[param[key]] = param end
    return parameter
end

--- _addPasxParameter
---
--- Add parameter in a Pas-X conform format to the given parameter table. The table is modified in-place.
---
--- @param parameter table Existing parameter table
--- @param name string Name of the new parameter
--- @param value any Value of the new parameter
--- @param dataType string Data type of new parameter
--- @param isQualifier boolean | string Flag if new parameter is qualifier, defaults to false
--- @param acquisitionTime string Timestamp for the new parameter in Pas-X format, defaults to current time
function lib:_addPasxParameter(parameter, name, value, dataType, isQualifier, acquisitionTime)
    if dataType == "DateTime" then value = self:_toPASXTime(value) end
    table.insert(parameter, {
        ["name"] = name,
        ["value"] = value,
        ["dataType"] = dataType,
        ["isQualifier"] = STR:STRING(isQualifier or false),
        ["acquisitionTime"] = acquisitionTime or self:_toPASXTime(syslib.now()),
    })
end

--- _toPASXTime
---
--- Convert timestamp into PAS-X compatible time string.
---
--- @param t string | number Either a ISO 8601 time string or a UNIX timestamp in milliseconds
--- @return string PAS-X compatible time string
function lib._toPASXTime(_, t)
    do
        local _, _t = pcall(tonumber, t)
        if _t then
            -- if t is a numeric value, assume it to be a UNIX timestamp and
            -- convert it back to a time string
            _t = syslib.gettime(_t)
            t = _t
        end
    end

    -- convert to string again, to handle cases where t might be nil
    t = tostring(t)
    return string.gsub(string.gsub(string.gsub(t, 'T', ' '), 'Z', ''), '%.', ',')
end

--- _fromPASXtoISOTime
---
--- Convert PAS-X time string into standard ISO 8601 format.
---
--- @param t string PAS-X time string
--- @return string ISO 8601 format time string
function lib._fromPASXtoISOTime(_, t)
    return string.gsub(string.gsub(t, ' ', 'T'), ',', '.') .. 'Z'
end

-- TODO: Will no longer necessary when _executefuncCall implements `callself`
--- _sleep
---
--- Sleep for a specified number of milliseconds. This function is a delegate
--- to `syslib.sleep`, useful in funcdefs.
---
--- @param duration number Milliseconds to sleep for
function lib._sleep(_, duration) syslib.sleep(duration) end

local function _error(errormsg, arg)
    _add_tracker_item(arg.pasx, "error")
    arg["errormsg"] = errormsg
    -- TODO: Send error back to PAS-X
    V:SET{path = "debug/_executefunc", v = arg}
    error(errormsg, 2)
end

--- _executefunc
---
--- Execute the given function definition. For detailed description, see
--- documentation to `FUNCTIONEXECUTION`.
---
--- @param arg table Arguments table
--- @return boolean
function lib:_executefunc(arg)
    local _timer_callback_start = function(k, _)
        local execution_count = arg.pasx.execution_count or 0

        if execution_count > 5 then
            -- Avoid excessive tracking!
            return
        end

        _add_tracker_item(arg.pasx, "start " .. k)
    end

    local _timer_callback_end = function(k, v)
        local execution_count = arg.pasx.execution_count or 0

        if execution_count > 5 then
            -- Avoid excessive tracking!
            return
        end

        _add_tracker_item(arg.pasx, "end " .. k)

        execution_count = tostring(arg.pasx.execution_count or 0)
        local _k = tostring(k)

        arg.pasx.Times = arg.pasx.Times or {}
        arg.pasx.Times.FUNCTIONEXECUTION = arg.pasx.Times.FUNCTIONEXECUTION or {}
        arg.pasx.Times.FUNCTIONEXECUTION[execution_count] =
            arg.pasx.Times.FUNCTIONEXECUTION[execution_count] or {}
        arg.pasx.Times.FUNCTIONEXECUTION[execution_count][_k] = v
    end

    if arg.id and not arg.executor_init_done then
        -- Use new message executor with local message queues

        arg.pasx = arg.pasx or {}

        local timer = self:_get_timer {
            callback_start = _timer_callback_start,
            callback_end = _timer_callback_end,
        }

        timer.init:t_start()

        arg.pasx.id = arg.id
        arg.pasx.queue_slot = arg.queue_slot or lib.RESPONSE_SLOT

        if type(arg.queue_object) ~= "string" then
            _error(
                    ("Queue object needs to be a string, but found '%s' instead"):format(type(arg.queue_object)),
                    arg
            )
        end
        arg.pasx.queue_object = arg.queue_object

        arg.data_store_name = arg.data_store_name or lib.DEFAULT_DATA_STORE_NAME

        arg.pasx.data_store_id = lib:_get_data_store_id_from_name(arg.data_store_name)
        if arg.pasx.data_store_id == nil then
            _error(("Data Store '%s' not found"):format(arg.data_store_name), arg)
        end

        if TOOL:NUMBERTYPE(arg.timeout) and arg.timeout > 0 then
            arg.deadline = syslib.now() + arg.timeout
        end

        if arg.chunk_function_invalid then
            _error("chunk_function must be a function", arg)
        end

        local this = syslib.getselfpath()

        local message_id = TOOL:GETREFERENCE(arg, {"pasx", "messageId"})

        if message_id == nil then
            _error("Message ID cannot be deduced", arg)
        end

        -- executor_mode defaults to ONESHOT. If it is a table, then the first
        -- element must be the mode to use, the second element must be a table
        -- again and can be used to further configure the mode. If only
        -- default values are needed, the mode can be given raw and it will be
        -- automatically converted to a table.
        arg.executor_mode = arg.executor_mode or {lib.EXECUTOR_MODE.ONESHOT}
        if type(arg.executor_mode) ~= "table" then
            arg.executor_mode = {arg.executor_mode}
        end

        if arg.executor_mode[1] == lib.EXECUTOR_MODE.ONESHOT then
            -- The ONESHOT executor runs exactly once, hence the executor item
            -- can be a GenFolder, in which the funcdef is executed.

            local item_name = TOOL:GETREFERENCE(
                    arg,
                    {"executor_mode", 2, "name"},
                    message_id
            )

            local exists, _ = O:EXISTS{
                parentpath = this,
                objectname = item_name
            }

            local references = {
                {
                    path = ("%s/%s/manual trigger"):format(this, item_name),
                    name = 'manual trigger',
                    type = 'OBJECT_LINK'
                }
            }

            if not exists then
                syslib.mass({
                    {
                        class = syslib.model.classes.GenFolder,
                        operation = syslib.model.codes.MassOp.INSERT,
                        path = ("%s/%s"):format(this, message_id),
                        ["ObjectName"] = message_id,
                    },
                    {
                        class = syslib.model.classes.Variable,
                        operation = syslib.model.codes.MassOp.INSERT,
                        path = ("%s/%s/manual trigger"):format(this, item_name),
                        ["ObjectName"] = "manual trigger",
                    },
                    {
                        class = syslib.model.classes.ActionItem,
                        operation = syslib.model.codes.MassOp.INSERT,
                        path = ("%s/%s/Executor"):format(this, item_name),
                        ["ObjectName"] = "Executor",
                        ["AdvancedLuaScript"] = [==[
                            -- force reload of esi-mbro to pick up latest changes
                            package.loaded["esi-mbro"] = nil
                            local MBRO = require("esi-mbro")
                            return function()
                                return MBRO:PROCESS_EXEC_SLOT()
                            end
                        ]==],
                        ["DedicatedThreadExecution"] = true,
                        ["ActivationMode"] = 1,
                        references = references,
                    }
                })
            end

            arg.path = ("%s/%s/Executor"):format(arg.path, item_name)
            arg.pasx.executor_path = arg.path
            arg.executor_init_done = true

            timer.init:t_end()
            lib:_msgpush_MES_TO_EXEC_SLOT(
                    J.encode(arg),
                    true
            )

            syslib.setvalue(("%s/%s/manual trigger"):format(this, item_name), syslib.now())

            return
        elseif arg.executor_mode[1] & lib.EXECUTOR_MODE.TRIGGER ~= 0 then
            -- The TRIGGER executor creates an ActionItem that references an
            -- active trigger item, usually some sort of an event stream.
            -- Pending funcdefs will be sent to a message queue in the
            -- ActionItem. On every new event, all pending funcdefs will be
            -- executed one after another. The TRIGGER executor can create an
            -- ActionItem *and* being called immediately. Hence, we can use
            -- the bitwise AND operator (&) here to check for both flags. If
            -- the flag is set, the bitwise AND will not return 0.

            local item_name = TOOL:GETREFERENCE(
                    arg,
                    {"executor_mode", 2, "name"},
                    message_id
            )

            local trigger_items = TOOL:GETREFERENCE(
                    arg,
                    {"executor_mode", 2, "trigger_items"}
            )

            local standardize_events = TOOL:GETREFERENCE(
                    arg,
                    {"executor_mode", 2, "standardize_events"},
                    true
            )

            if trigger_items == nil then
                _error("No trigger items given", arg)
            end

            if type(trigger_items) ~= "table" then
                trigger_items = {trigger_items}
            end

            local exists, _ = O:EXISTS{
                parentpath = this,
                objectname = item_name
            }

            local references = {
                {
                    path = ("%s/%s/manual trigger"):format(this, item_name),
                    name = 'manual trigger',
                    type = 'OBJECT_LINK'
                }
            }

            for i, trigger_item in ipairs(trigger_items) do
                table.insert(
                        references,
                        {
                            path = trigger_item,
                            name = ('event_%s'):format(i),
                            type = 'OBJECT_LINK'
                        }
                )
            end

            if not exists then
                syslib.mass({
                    {
                        class = syslib.model.classes.GenFolder,
                        operation = syslib.model.codes.MassOp.INSERT,
                        path = ("%s/%s"):format(this, item_name),
                        ["ObjectName"] = item_name,
                    },
                    {
                        class = syslib.model.classes.Variable,
                        operation = syslib.model.codes.MassOp.INSERT,
                        path = ("%s/%s/manual trigger"):format(this, item_name),
                        ["ObjectName"] = "manual trigger",
                    },
                    {
                        class = syslib.model.classes.ActionItem,
                        operation = syslib.model.codes.MassOp.INSERT,
                        path = ("%s/%s/Executor"):format(this, item_name),
                        ["ObjectName"] = "Executor",
                        ["AdvancedLuaScript"] = [==[
                            -- force reload of esi-mbro to pick up latest changes
                            package.loaded["esi-mbro"] = nil
                            local MBRO = require("esi-mbro")
                            local V = require("esi-variables")
                            return function()
                                -- find latest event

                                local ref_values = {}
                                for _, ref in ipairs(syslib.getself().refs) do
                                    if ref.name:sub(1, 5) == "event" then
                                        local v, _, t = syslib.getvalue(ref.path)
                                        ref_values[ref.path] = {v = v, t = t}
                                    end
                                end

                                local previous_values = V:GET("previous_values")

                                local rawevent

                                if previous_values then
                                    for k, v in pairs(ref_values) do
                                        if v.v["System.CoreEventID"] ~= previous_values[k] then
                                            previous_values[k] = v.v["System.CoreEventID"]
                                            rawevent = v.v
                                            break
                                        end
                                    end
                                else
                                    previous_values = {}
                                    for k, v in pairs(ref_values) do
                                        previous_values[k] = v.v["System.CoreEventID"]
                                    end
                                    local latest = 0
                                    for k, v in pairs(ref_values) do
                                        if (v.t or 0) > latest then
                                            rawevent = v.v
                                            latest = v.t
                                        end
                                    end
                                end

                                V:SET{
                                    path = "previous_values",
                                    v = previous_values,
                                    hist = false,
                                }

                                if rawevent then
                                    ]==] .. (standardize_events and [==[
                                    local EM = require("esi-event-mapping")
                                    local event = EM:STANDARDIZEEVENT{realtime = rawevent}
                                    ]==] or [==[
                                    local event = rawevent
                                    ]==]) .. [==[
                                    local res = MBRO:PROCESS_EXEC_SLOT{
                                        event = event,
                                    }
                                    return res
                                end
                            end
                        ]==],
                        ["ActivationMode"] = 1,
                        references = references,
                    }
                })
            end

            arg.path = ("%s/%s/Executor"):format(arg.path, item_name)
            arg.pasx.executor_path = arg.path
            arg.executor_init_done = true
            timer.init:t_end()
            lib:_msgpush_MES_TO_EXEC_SLOT(
                    J.encode(arg),
                    -- If ONESHOT is requested, we must send the message
                    -- synchronously, so the executor can find it immediately
                    -- when it runs next.
                    arg.executor_mode[1] & lib.EXECUTOR_MODE.ONESHOT ~= 0
            )

            if arg.executor_mode[1] & lib.EXECUTOR_MODE.ONESHOT ~= 0 then
                -- If ONESHOT flag is set alongside TRIGGER, force an
                -- immediate run by setting the manual trigger item.
                syslib.setvalue(("%s/%s/manual trigger"):format(this, item_name), syslib.now())
            end

            return
        elseif arg.executor_mode[1] == lib.EXECUTOR_MODE.CONTINUOUS then
            -- A CONTINUOUS executor creates a GenItem that executes all
            -- pending funcdefs periodically. This is useful if we need event
            -- like handling, but we do not have an actual event stream to
            -- subscribe.

            local item_name = TOOL:GETREFERENCE(
                    arg,
                    {"executor_mode", 2, "name"},
                    message_id
            )

            local interval = TOOL:GETREFERENCE(
                    arg,
                    {"executor_mode", 2, "interval"},
                    10000,
                    tonumber
            )
            -- Retaining backwards compatibility, convert milliseconds to seconds
            interval = interval // 1000

            local exists, _ = O:EXISTS{
                parentpath = this,
                objectname = item_name
            }

            if not exists then
                syslib.mass({
                    {
                        class = syslib.model.classes.GenFolder,
                        operation = syslib.model.codes.MassOp.INSERT,
                        path = ("%s/%s"):format(this, item_name),
                        ["ObjectName"] = item_name,
                    },
                    {
                        class = syslib.model.classes.ActionItem,
                        operation = syslib.model.codes.MassOp.INSERT,
                        path = ("%s/%s/Executor"):format(this, item_name),
                        ["ObjectName"] = "Executor",
                        ["AdvancedLuaScript"] = [==[
                            -- force reload of esi-mbro to pick up latest changes
                            package.loaded["esi-mbro"] = nil
                            return require('esi-mbro'):PROCESS_EXEC_SLOT()
                        ]==],
                        ["DedicatedThreadExecution"] = true,
                        ["ActivationMode"] = 1,
                        references = {
                            {
                                path = ("%s/%s/scheduler"):format(this, item_name),
                                name = "scheduler",
                                type = "OBJECT_LINK"
                            },
                        },
                    },
                    {
                        class = syslib.model.classes.SchedulerItem,
                        operation = syslib.model.codes.MassOp.UPSERT,
                        path = ("%s/%s/scheduler"):format(this, item_name),
                        ["ObjectName"] = "scheduler",
                        ["EdgeDuration"] = interval,
                        ["Schedule.RecurBySecond.RecurEnd"] = syslib.gettime("2099-12-31T23:00:00.000Z"),
                        ["Schedule.RecurBySecond.RecurStart"] = syslib.gettime("2020-01-01T00:00:00.000Z"),
                        ["Schedule.RecurBySecond.RecurSecDistance"] = interval * 2,
                    },
                })
            end

            arg.path = ("%s/%s/Executor"):format(arg.path, item_name)
            arg.pasx.executor_path = arg.path
            arg.executor_init_done = true
            timer.init:t_end()
            lib:_msgpush_MES_TO_EXEC_SLOT(J.encode(arg))
            return
        elseif arg.executor_mode[1] == lib.EXECUTOR_MODE.CONTINUOUS_EVENTS then
            -- The CONTINUOUS_EVENTS executor is similar to the TRIGGER
            -- executor, but it creates a GenItem that gets events from
            -- ActionItems instead of being triggered directly. This is useful
            -- if the funcdef could take a long time to complete, which would
            -- otherwise block inmation completely.

            local item_name = TOOL:GETREFERENCE(
                    arg,
                    {"executor_mode", 2, "name"},
                    message_id
            )

            local interval = TOOL:GETREFERENCE(
                    arg,
                    {"executor_mode", 2, "interval"},
                    10000,
                    tonumber
            )
            -- Retaining backwards compatibility, convert milliseconds to seconds
            interval = interval // 1000

            local event_sources = TOOL:GETREFERENCE(
                    arg,
                    {"executor_mode", 2, "event_sources"}
            )

            if event_sources == nil then
                _error("No event sources given", arg)
            end

            if type(event_sources) ~= "table" then
                event_sources = {event_sources}
            end

            local exists, _ = O:EXISTS{
                parentpath = this,
                objectname = item_name
            }

            if not exists then
                local mass = {
                    {
                        class = syslib.model.classes.GenFolder,
                        operation = syslib.model.codes.MassOp.INSERT,
                        path = ("%s/%s"):format(this, item_name),
                        ["ObjectName"] = item_name,
                    },
                    {
                        class = syslib.model.classes.ActionItem,
                        operation = syslib.model.codes.MassOp.INSERT,
                        path = ("%s/%s/Executor"):format(this, item_name),
                        ["ObjectName"] = "Executor",
                        ["AdvancedLuaScript"] = [==[
                            -- force reload of libraries to pick up latest changes
                            package.loaded["esi-event-mapping"] = nil
                            package.loaded["esi-mbro"] = nil
                            return require('esi-event-mapping'):PROCESS_EXEC_EVENT()
                        ]==],
                        ["DedicatedThreadExecution"] = true,
                        ["ActivationMode"] = 1,
                        references = {
                            {
                                path = ("%s/%s/scheduler"):format(this, item_name),
                                name = "scheduler",
                                type = "OBJECT_LINK"
                            },
                        },
                    },
                    {
                        class = syslib.model.classes.SchedulerItem,
                        operation = syslib.model.codes.MassOp.UPSERT,
                        path = ("%s/%s/scheduler"):format(this, item_name),
                        ["ObjectName"] = "scheduler",
                        ["EdgeDuration"] = interval,
                        ["Schedule.RecurBySecond.RecurEnd"] = syslib.gettime("2099-12-31T23:00:00.000Z"),
                        ["Schedule.RecurBySecond.RecurStart"] = syslib.gettime("2020-01-01T00:00:00.000Z"),
                        ["Schedule.RecurBySecond.RecurSecDistance"] = interval * 2,
                    },
                }

                for i, event_source in ipairs(event_sources) do
                    table.insert(mass, {
                        class = syslib.model.classes.ActionItem,
                        operation = syslib.model.codes.MassOp.UPSERT,
                        path = ("%s/%s/pushevent"):format(this, item_name),
                        ["ObjectName"] = ("pushevent_%s"):format(i),
                        ["AdvancedLuaScript"] = [=[
                            local EM = require("esi-event-mapping")

                            return function()
                                return EM:SENDEVENT_TO_SLOT{
                                    eventpath = "event",
                                    targetpath = "]=] .. ("%s/%s/Executor"):format(this, item_name) .. [=[",
                                }
                            end
                        ]=],
                        ["ActivationMode"] = 1,
                        references = {
                            {
                                path = event_source,
                                name = 'event',
                                type = 'OBJECT_LINK'
                            }
                        }
                    })
                end

                syslib.mass(mass)
            end

            arg.path = ("%s/%s/Executor"):format(arg.path, item_name)
            arg.pasx.executor_path = arg.path
            arg.executor_init_done = true
            timer.init:t_end()
            lib:_msgpush_MES_TO_EXEC_SLOT(J.encode(arg))
            return
        else
            _error(("Executor mode '%s' unknown"):format(arg.executor_mode))
        end
    end

    arg.pasx.execution_count = (arg.pasx.execution_count or 0) + 1

    if TOOL:NUMBERTYPE(arg.deadline) and syslib.now() >= arg.deadline then
        arg.errormsg = "Reached timeout"
        arg.aborted = true
        arg.pasx.errors = {
            {
                code = TOOL:GETREFERENCE(arg, {"timeout_error", "code"}, ""),
                text = TOOL:GETREFERENCE(arg, {"timeout_error", "text"}, arg.errormsg),
            },
        }
        self:_msgpush_to_buffer(arg.pasx)
        _add_tracker_item(arg.pasx, "timeout")
        V:SET{path = "debug/_executefunc", v = arg}
        return true, {}
    end

    local timer = self:_get_timer {
        write_immediately = true,
        delete_after_write = true,
        callback_start = _timer_callback_start,
        callback_end = _timer_callback_end,
    }

    timer.execute:t_start()

    arg.result = arg.result or {}
    local skip = {}
    -- If $FFW directive found, skip to the designated funcdef index
    local ffw = 1
    for kfuncdef, funcdef in ipairs(TOOL:GETREFERENCE(arg, {"funcdefinition"}, {})) do
        local step = {kfuncdef = kfuncdef}
        local continue = true
        local cbreak = false
        if not TOOL:GETREFERENCE(funcdef, {"$Skip"}, false) and kfuncdef >= ffw and TOOL:TABLETYPE(funcdef) then
            local suc, msg = pcall(function()
                -- * call pre functions
                step["call"] = "pre"
                local prefunc_result = {}
                for kprefunc, prefunc in ipairs(TOOL:GETREFERENCE(funcdef, {"prefunc"}, {})) do
                    step["callNo"] = kprefunc
                    timer[("funcdef_%s_pre_%s"):format(kfuncdef, kprefunc)]:t_start()
                    local res = table.pack(self:_executefuncCall(arg, prefunc))
                    timer[("funcdef_%s_pre_%s"):format(kfuncdef, kprefunc)]:t_end()
                    prefunc_result[kprefunc] = res
                end
                arg.prefunc_result = prefunc_result
                -- * call main function
                step["call"] = "main"
                step["callNo"] = 1
                timer[("funcdef_%s_main"):format(kfuncdef)]:t_start()
                if TOOL:GETREFERENCE(funcdef.func, {"$NOP"}, false) == true then
                    arg.result[kfuncdef] = {"$NOP"}
                else
                    arg.prefunc_result = prefunc_result
                    local res = table.pack(self:_executefuncCall(arg, funcdef.func))
                    arg.prefunc_result = nil
                    arg.result[kfuncdef] = res
                end
                timer[("funcdef_%s_main"):format(kfuncdef)]:t_end()
                -- * call post functions
                step["call"] = "post"
                step["callNo"] = 0
                for kpostfunc, postfunc in ipairs(TOOL:GETREFERENCE(funcdef, {"postfunc"}, {})) do
                    step["callNo"] = kpostfunc
                    timer[("funcdef_%s_post_%s"):format(kfuncdef, kpostfunc)]:t_start()
                    self:_executefuncCall(arg, postfunc)
                    timer[("funcdef_%s_post_%s"):format(kfuncdef, kpostfunc)]:t_end()
                end
                arg.prefunc_result = nil
                -- * check condition
                step["call"] = "condition"
                step["callNo"] = 1
                if TOOL:TABLETYPE(funcdef["condition"]) then
                    -- * break
                    if TOOL:TABLETYPE(funcdef["condition"]["$break"]) then
                        local breakcondition, err = self:_argconvert(arg, funcdef["condition"]["$break"])
                        V:SET{path = "debug/_executefunc_err", v = err}
                        funcdef["breakcondition"] = breakcondition
                        if breakcondition == true then
                            cbreak = breakcondition
                            arg["$break"] = true
                            V:SET{path = "debug/_executefunc", v = arg}
                            return true
                        end
                    end
                    -- * continue
                    if TOOL:TABLETYPE(funcdef["condition"]["$continue"]) then
                        local continuecondition, err = self:_argconvert(arg, funcdef["condition"]["$continue"])
                        V:SET{path = "debug/_executefunc_err", v = err}
                        funcdef["continuecondition"] = continuecondition
                        continue = continuecondition
                        if continuecondition then
                            local onsuc_skip = TOOL:GETREFERENCE(funcdef, {"condition", "$OnSuccess", "$Skip"}, {})
                            for _, s in ipairs(onsuc_skip) do table.insert(skip, s) end
                        elseif TOOL:TABLETYPE(TOOL:GETREFERENCE(funcdef, {"condition", "$OnFail"})) then
                            local _ffw = TOOL:GETREFERENCE(funcdef, {"condition", "$OnFail", "$FFW"})
                            if TOOL:NUMBERTYPE(_ffw) then
                                ffw = _ffw
                                funcdef["fast_forward_to"] = ffw
                                continue = true
                            end
                        else
                            V:SET{path = "debug/_executefunc", v = arg}
                            return false
                        end
                    end
                end
            end)
            if cbreak == true then return true, skip end
            if continue ~= true then return false, skip end
            if not suc then
                _error(
                        ("_executefunc [%s/%s/%s] %s"):format(step["kfuncdef"], step["call"], step["callNo"], msg),
                        arg
                )
            end
        else
            arg.result[kfuncdef] = arg.result[kfuncdef] or {"$Skip"}
        end
    end

    if arg.chunk_function then
        local ok, errormsg = pcall(
                function()
                    -- Base64 is necessary here, because binary strings of
                    -- string.dump can easily get messed up in JSON. The
                    -- counterpart in the callee is:
                    --     syslib.enbase64(string.dump(chunk))
                    local chunk = syslib.debase64(arg.chunk_function)
                    local args = lib:_argconvert(arg, arg.chunk_argument)
                    arg.chunk_result = load(chunk)(args)
                end
        )

        if not ok then
            _error(
                    ("_executefunc [chunk_function] %s"):format(errormsg),
                    arg
            )
        end
    end

    timer.execute:t_end()

    V:SET{path = "debug/_executefunc", v = arg}
    return true, skip
end

--- _executefuncCall
---
--- Execute single step of a function definition.
---
--- The `arg` table is the original table, which contains the complete
--- function definition.
---
--- The `func` table contains exactly one step of the function definition, so
--- either one of the pre or post functions or the main function itself. It is
--- expected to contain the following members:
--- -   `lib`: The internal variable name to give the imported library. It
---     should keep consistent throughout the funcdef to avoid shadowing of
---     already imported libraries and to avoid cluttering the namespace.
--- -   `libname`: The name of the library, which is used for the `require`
---     statement.
--- -   `func`: Name of the function inside the given library to call.
--- -   `arg`: Arguments to pass to `func`.
--- -   `callself`: Boolean flag, whether to call `func` with `self` as first
---     parameter.
--- -   `argunpack`: Boolean flag whether to unpack the given arguments or
---     pass them as a single arguments table.
---
--- @param arg table Original function definition table
--- @param func table Single step of function definition
function lib:_executefuncCall(arg, func)
    if not TOOL:TABLETYPE(func) then error("func argument is not a table!", 2) end
    local l = self
    if func.lib == "syslib" then
        l = syslib
    elseif TOOL:STRINGTYPE(func.lib) and TOOL:STRINGTYPE(func.libname) then
        self.funclibs[func.lib] = self.funclibs[func.lib] or require(func.libname)
        l = self.funclibs[func.lib]
    end
    local f = l[func.func]
    if not TOOL:FUNCTIONTYPE(f) then error(("function %s "):format(func.func), 2) end
    -- DEEPCOPY necessary to not accidentally leak sensitive data
    local a = self:_argconvert(arg, TOOL:DEEPCOPY(func.arg))
    local farg
    if func.argunpack == true then
        farg = {table.unpack(a)}
    else
        farg = {a}
    end
    if func.callself then
        table.insert(farg, 1, l)
    end
    return f(table.unpack(farg))
end

--- _argconvert
---
--- Convert argument lookup syntax for function definition to proper argument.
---
--- Such an argument can either be a simple value, in which case it just gets
--- returned unaltered. Or it can be a more complex argument table, consisting
--- of the following members:
--- -   `$lookup`: This table will be passed as second parameter to
---     `lib.GETREFERENCE`.
--- -   `$default`: This value will be passed as third parameter to
---     `lib.GETREFERENCE`.
--- -   `$luaxp`: LuaXP function that will be passed as fourth parameter to
---     `lib.GETREFERENCE`.
---
--- Each member of the lookup table can be nested by another lookup table. It
--- will be resolved recursively, starting from the innermost. The `base`
--- table will remain the same throughout the recursive resolve, so the lookup
--- always "starts" at the `base`.
---
--- @param base table Original function definition table
--- @param arg any Argument to be converted
function lib:_argconvert(base, arg)
    if TOOL:TABLETYPE(arg) then
        if TOOL:TABLETYPE(arg["$lookup"]) then
            -- * check nested lookup
            for k, t in pairs(arg["$lookup"]) do arg["$lookup"][k] = self:_argconvert(base, t) end
            -- * check luaxp lookup
            if TOOL:TABLETYPE(arg["$luaxp"]) then arg["$luaxp"] = self:_argconvert(base, arg["$luaxp"]) end
            -- * check luaxp string
            local funcluaxp = TOOL:STRINGTYPE(arg["$luaxp"]) and function(v)
                return luaxp.evaluate(arg["$luaxp"], {v = v}) or arg["$default"] -- todo rename v to x
            end or nil
            arg = self:GETREFERENCE(base, arg["$lookup"], arg["$default"], funcluaxp) -- ! not using TOOL
            arg = self:_argconvert(base, arg)
        else
            for k, t in pairs(arg) do arg[k] = self:_argconvert(base, t) end
        end
    end
    return arg
end

--- _functionexecution
---
--- Convenience wrapper for `_executefunc`, that decodes the serialized
--- arguments table first.
---
--- @param arg string Arguments table, serialized as JSON
function lib:_functionexecution(arg)
    arg = J.decode(arg)
    arg = arg or {}
    return self:_executefunc(arg)
end

-- TODO: Still in use?
--- _msgpush_SF_TO_MES_SLOT
---
--- Send message to dedicated message queue slot SF_TO_MES_SLOT.
---
--- @param data any Data to be sent
--- @param path string Path of object to send to
function lib._msgpush_SF_TO_MES_SLOT(_, data, path)
    local exProviderObj
    if path ~= nil then
        exProviderObj = inmation.getobject(path)
    else
        exProviderObj = syslib.getself()
    end

    V:SET{object = exProviderObj, path = "debug/_msgpush_SF_TO_MES_SLOT", v = data}

    local exProviderId = exProviderObj:numid()
    local ex_queue_id = syslib.msgqueue(exProviderId, MSI.SF_TO_MES_SLOT)
    syslib.msgpush(ex_queue_id, J.encode(data))
end

--- _msgpush_to_buffer
---
--- Send result to buffer object, create it if not exists. The newly created
--- buffer object will have the given data store set as archive option.
---
--- The `msg` table is expected to contain the following members:
--- -   `executor_path`: Path to the executor object, under which the buffer
---     object resides. The buffer will be created under this object if it
---     does not exist yet.
--- -   `data_store_id`: The id of the data store that should be set as the
---     archive of the buffer.
---
--- The resulting message will be written to the buffer object. The data store
--- set as archive is responsible for creating a proper response message from
--- the result.
---
--- @param msg table Message object
function lib._msgpush_to_buffer(_, msg)
    do
        local exists, _ = O:EXISTS{
            parentpath = msg.executor_path,
            objectname = "buffer"
        }

        if not exists then
            O:UPSERTOBJECT{
                path = msg.executor_path,
                class = "MODEL_CLASS_VARIABLE",
                properties = {
                    [".ObjectName"] = "buffer",
                    [".ArchiveOptions.StorageStrategy"] =
                            syslib.model.flags.ItemValueStorageStrategy.STORE_RAW_HISTORY,
                    [".ArchiveOptions.ArchiveSelector"] = msg.data_store_id,
                    [".ArchiveOptions.PersistencyMode"] = syslib.model.codes.PersistencyMode.PERSIST_PERIODICALLY,
                }
            }

        end
    end

    V:SET{
        object = syslib.getobject(msg.executor_path),
        path = "buffer",
        v = J.encode(msg),
    }
end

--- _get_data_store_config_from_name
---
--- Search the nearest core object for a data store with the given name and
--- return its config table.
---
--- @param data_store_name string Name of the desired data store
--- @return table Config table of desired data store
function lib._get_data_store_config_from_name(_, data_store_name)
    -- The explicit call to selfpath is necessary because it seems sometimes
    -- the default behavior does not work together with syslib.execute.
    local corepath = syslib.getcorepath(syslib.getselfpath())
    for _, store in ipairs(syslib.get(corepath .. ".DataStoreConfiguration.DataStoreSets")) do
        if store.name == data_store_name then
            return store
        end
    end
end

--- _get_data_store_id_from_name
---
--- Search the nearest core object for a data store with the given name and
--- return its id.
---
--- @param data_store_name string Name of the desired data store
--- @return number ID of desired data store
function lib:_get_data_store_id_from_name(data_store_name)
    local store = self:_get_data_store_config_from_name(data_store_name)
    if store then
        return store.rowid
    end
end

-- TODO: Still in use?
--- _msgpush_MES_TO_EXEC_SLOT
---
--- Send message to dedicated message queue slot MES_TO_EXEC_SLOT.
--- @param arg any Data to be sent
--- @param sync boolean Flag whether to send synchronously
function lib:_msgpush_MES_TO_EXEC_SLOT(arg, sync)
    arg = J.decode(arg)
    V:SET{path = "debug/_msgpush_SF_TO_MES_SLOT", v = arg}
    local exists, exProviderObj = O:EXISTS{path = arg.path}
    if not exists then error("arg.path not rpovided for _msgpush_MES_TO_EXEC_SLOT function", 2) end
    local exProviderId = exProviderObj:numid()
    local ex_queue_id = syslib.msgqueue(exProviderId, self.MES_TO_EXEC_SLOT)
    if sync then
        lib:_sync_msgpush(ex_queue_id, J.encode(arg))
    else
        syslib.msgpush(ex_queue_id, J.encode(arg))
    end
end

--- HANDLE_RESPONSE_MESSAGES
---
--- Receive buffered response messages and send them to their destination
--- queue.
---
--- This function is intended to be called from a GTSB like data store.
---
--- Example of "Lua Script Body" of a GTSB:
---
--- ```lua
--- return require("esi-mbro"):HANDLE_RESPONSE_MESSAGES(...)
--- ```
---
--- The `...` are no mistake or placeholder, it is Lua syntax for a varargs
--- parameter. The GTSB will automatically call this function with a special
--- iterator, hence the varargs.
---
--- @vararg any Automatic iterator parameters from GTSB
function lib:HANDLE_RESPONSE_MESSAGES(...)
    local iter = ...

    local last_saf_id
    for saf_id, _, msg, _, _, _ in iter() do
        last_saf_id = saf_id

        -- pcall is necessary because with an recurring error we get stuck in
        -- an infinite loop
        pcall(function()
            msg = J.decode(msg)

            if type(msg) == "table" and msg.queue_object then
                _add_tracker_item(msg, "Enqueue response")

                -- ensure queue object exists
                do
                    local object, path = syslib.splitpath(msg.queue_object)
                    _create_hierarchy(object, syslib.model.classes.VariableGroup)
                    V:SET {
                        object = syslib.getobject(object),
                        path = path,
                    }
                end

                local q = syslib.msgqueue(
                        msg.queue_object,
                        TOOL:GETREFERENCE(msg, { "queue_slot" }, lib.RESPONSE_SLOT)
                )
                syslib.msgpush(q, J.encode(msg))
            end
        end)
    end

    if last_saf_id then
        iter:deleteupto(last_saf_id)
    end
end

--- GET_MESSAGE_FROM_QUEUE
---
--- Receive a response message from a message queue. This function is intended
--- to be called from the "Output Script" of a MessageConfiguration. It will
--- then periodically check for new response messages.
---
--- The `queue_object` can either be given:
--- -   as an absolute path (starting with "/").
--- -   as the name of the desired queue object.
---
--- @param queue_object string Queue object name or path
--- @param queue_slot number Queue slot to use, optional
--- @return table Response message if no errors occurred
--- @return table Error object if necessary
--- @return number Automatic message id as given by "Input Script"
function lib:GET_MESSAGE_FROM_QUEUE(queue_object, queue_slot)
    queue_object = _get_response_queue_path(queue_object)

    queue_slot = queue_slot or lib.RESPONSE_SLOT

    local msg
    do
        local exists, _ = O:EXISTS{path = queue_object}
        if exists then
            local q = syslib.msgqueue(queue_object, queue_slot)
            for _msgid, _msg in pairs(q) do
                _, msg = pcall(J.decode, _msg)
                syslib.msgpop(q, _msgid)
                break
            end
        end
    end

    if msg == nil or msg.id == nil then return end

    _add_tracker_item(msg, "Send response")

    local message_duration
    do
        local tracker = msg.Tracker
        message_duration = tracker[#tracker].timestamp - tracker[1].timestamp
    end

    msg.Times = msg.Times or {}
    msg.Times.message_duration = message_duration
    V:SET{
        path = "debug/message_duration",
        v = message_duration,
    }

    V:SET{
        path = "debug/sent_message",
        v = {[msg.messageId] = msg},
    }

    if msg.errors then
        return nil, msg, msg.id
    else
        return msg, nil, msg.id
    end
end

-- TODO: Consider moving to dedicated library
--- _kv_table_equals
---
--- Compare two Lua tables elementwise.
---
--- @param t1 table Table 1
--- @param t2 table Table 2
--- @return boolean True if and only if all elements match
local function _kv_table_equals(t1, t2)
    if #t1 ~= #t2 then
        return false
    end

    for k, v in pairs(t1) do
        if v ~= t2[k] then
            return false
        end
    end

    return true
end

-- TODO: Consider moving to dedicated library
--- _custom_table_get_all_tables_and_pos
---
--- Get all custom table data from a specified path as well as the position of
--- a given table name. The reason that the whole data set is needed is that
--- when altering some data, the complete data set must be written back to the
--- property. It is not possible to set the data of just a single custom
--- table.
---
--- @param path string Path to the object with the desired Custom Tables
--- @param name string Name of the desired Custom Table
--- @return table Data of all Custom Tables
--- @return number Positional index of the desired table data
local function _custom_table_get_all_tables_and_pos(path, name)
    local names = syslib.getvalue(("%s.CustomOptions.CustomTables.CustomTableName"):format(path))
    local pos
    for i, _name in ipairs(names) do
        if _name == name then
            pos = i
            break
        end
    end

    if not pos then
        pos = #names + 1
        names[pos] = name
        syslib.setvalue(("%s.CustomOptions.CustomTables.CustomTableName"):format(path), names)
    end

    local data = syslib.getvalue(("%s.CustomOptions.CustomTables.TableData"):format(path))
    if not data[pos] then
        for i=1,pos do
            data[i] = data[i] or {}
        end

        syslib.setvalue(("%s.CustomOptions.CustomTables.TableData"):format(path), data)
    end

    return data, pos
end

-- TODO: Consider moving to dedicated library
--- _custom_table_get_data
---
--- Get the data set of a Custom Table with a given name at the given path.
---
--- @param path string Path to the object with the desired Custom Tables
--- @param name string Name of the desired Custom Table
--- @return table Data of the desired Custom Table
local function _custom_table_get_data(path, name)
    local data, pos = _custom_table_get_all_tables_and_pos(path, name)
    return data[pos]
end

-- TODO: Consider moving to dedicated library
--- _custom_table_replace_data
---
--- Replace data of a single Custom Table.
---
--- @param path string Path to the object with the desired Custom Tables
--- @param name string Name of the desired Custom Table
--- @param data_new table New data of the desired Custom Table
local function _custom_table_replace_data(path, name, data_new)
    local data, pos = _custom_table_get_all_tables_and_pos(path, name)
    data[pos] = data_new
    syslib.setvalue(("%s.CustomOptions.CustomTables.TableData"):format(path), data)
end

-- TODO: Consider moving to dedicated library
--- _custom_table_append_one
---
--- Append a single row to a Custom Table.
---
--- @param path string Path to the object with the desired Custom Tables
--- @param name string Name of the desired Custom Table
--- @param row table New row for the desired Custom Table
local function _custom_table_append_one(path, name, row)
    local data, pos = _custom_table_get_all_tables_and_pos(path, name)
    table.insert(data[pos], row)
    syslib.setvalue(("%s.CustomOptions.CustomTables.TableData"):format(path), data)
end

-- TODO: Consider moving to dedicated library
--- _custom_table_append_all
---
--- Append data to a Custom Table.
---
--- @param path string Path to the object with the desired Custom Tables
--- @param name string Name of the desired Custom Table
--- @param new_data table New data for the desired Custom Table
local function _custom_table_append_all(path, name, new_data)
    local data, pos = _custom_table_get_all_tables_and_pos(path, name)
    for _, row in ipairs(new_data) do
        table.insert(data[pos], row)
    end
    syslib.setvalue(("%s.CustomOptions.CustomTables.TableData"):format(path), data)
end

-- TODO: Consider moving to dedicated library
--- _custom_table_delete_one
---
--- Delete a single row from a Custom Table.
---
--- @param path string Path to the object with the desired Custom Tables
--- @param name string Name of the desired Custom Table
--- @param row table Row to delete from the desired Custom Table
local function _custom_table_delete_one(path, name, row)
    local data, pos = _custom_table_get_all_tables_and_pos(path, name)
    for i, _row in ipairs(data[pos]) do
        if _kv_table_equals(_row, row) then
            table.remove(data[pos], i)
            break
        end
    end
    syslib.setvalue(("%s.CustomOptions.CustomTables.TableData"):format(path), data)
end

-- TODO: Consider moving to dedicated library
--- _custom_table_delete_all
---
--- Delete all matching rows from a Custom Table.
---
--- @param path string Path to the object with the desired Custom Tables
--- @param name string Name of the desired Custom Table
--- @param rows table Rows to delete from the desired Custom Table
local function _custom_table_delete_all(path, name, rows)
    local data, pos = _custom_table_get_all_tables_and_pos(path, name)
    for _, row in ipairs(rows) do
        for i, _row in ipairs(data[pos]) do
            if _kv_table_equals(_row, row) then
                table.remove(data[pos], i)
                break
            end
        end
    end
    syslib.setvalue(("%s.CustomOptions.CustomTables.TableData"):format(path), data)
end

function lib:_embed_table(arg)
    local base=arg.base
    local embedded=arg.embedded
    local target_field=arg.target_field

    if not TOOL:TABLETYPE(target_field) then
        target_field = {target_field}
    end

    local t = base
    local final_t
    local final_part
    for _, part in ipairs(target_field) do
        final_part = part
        final_t = t
        t[part] = t[part] or {}
        t = t[part]
    end

    final_t[final_part] = embedded

    return base
end

function lib:_tagquery(parameter, collection, maxtags, pasxparameter)
    -- * split helper
    local function split(str, separator)
        local tbl = {}
        for w in str:gmatch("([^" .. separator .. "]+)") do table.insert(tbl, w) end
        return tbl
    end
    -- * check return values
    for i = 1, maxtags, 1 do
        local tagIn = ("Tag%02d"):format(i)
        local tagOut = ("Return%02d"):format(i)
        local query = TOOL:GETREFERENCE(parameter, {tagIn, "value"}, nil, function(s) return split(s, ".") end)
        local value = TOOL:GETREFERENCE(collection, query, "")
        self:_addPasxParameter(pasxparameter, tagOut, value, "String", "false")
    end
end

function lib:_build_returns_from_list(list, max_returns, pasx_parameter)
    for i = 1, max_returns, 1 do
        local tagOut = ("Return%02d"):format(i)
        local value = TOOL:GETREFERENCE(list, {i}, "")
        self:_addPasxParameter(pasx_parameter, tagOut, value, "String", "false")
    end
end

--- PROCESS_EXEC_SLOT
---
--- Execute function definitions from the message queue. New messages will be
--- collected from the message queue and their containing funcdefs will be put
--- into a Custom Table of an object beneath the executor so they can easily
--- be watched. Those funcdefs will then be periodically executed, until they
--- return successful.
---
--- Whatever the `arg` parameter contains will be available inside the
--- funcdefs via the member `PROCESS_EXEC_SLOT`.
---
--- @param arg any Additional argument that will be accessible in the funcdefs
--- @return string JSON encoded table of processed messages
function lib:PROCESS_EXEC_SLOT(arg)
    local timer = self:_get_timer {
        write_immediately = true,
        delete_after_write = true,
    }

    timer["PROCESS_EXEC_SLOT/overall"]:t_start()

    local message_table_path = ("%s/%s"):format(syslib.getselfpath(), "Messages")
    local message_table_name = "Messages"

    -- Insert pending messages into Custom Table
    timer["PROCESS_EXEC_SLOT/insert_new_messages"]:t_start()

    local queue_to_wait
    local msgid_to_wait
    do
        local _, object_name = syslib.splitpath(message_table_path)
        _scopedcall(
                "PROCESS_EXEC_SLOT",
                "Create new empty Messages object",
                syslib.mass,
                {
                    {
                        class = syslib.model.classes.VariableGroup,
                        operation = syslib.model.codes.MassOp.UPSERT,
                        path = message_table_path,
                        ObjectName = object_name,
                    },
                }
        )

        local new_messages = {}
        local pending_items, queue = self:_msgget_MES_TO_EXEC_SLOT()
        local last_msgid
        for _, item in ipairs(pending_items) do
            last_msgid = item.msgid
            table.insert(new_messages, { MessageID = item.msg.id, Message = J.encode(item.msg) })
        end

        if last_msgid then
            _scopedcall(
                    "PROCESS_EXEC_SLOT",
                    "Append new funcdefs",
                    _custom_table_append_all,
                    message_table_path, message_table_name, new_messages
            )
            syslib.msgpop(queue, last_msgid)
            queue_to_wait = queue
            msgid_to_wait = last_msgid
        end
    end

    timer["PROCESS_EXEC_SLOT/insert_new_messages"]:t_end()

    -- Load and execute funcdefs from Custom Table

    local finished_funcdefs = {}
    local aborted_funcdefs = {}
    local active_funcdefs = {}

    -- Actual commit will only happen if table does not exist yet
    local workitems = _scopedcall(
            "PROCESS_EXEC_SLOT",
            "Create new empty Custom Table",
            _custom_table_get_data,
            message_table_path, message_table_name
    )
    for k, work in ipairs(workitems) do
        timer["PROCESS_EXEC_SLOT/process_message"]:t_start()

        local msg = J.decode(work.Message)
        V:SET{path = "msg", v = ("[%s/%s] start with id %s"):format(k, #workitems, msg.id)}
        local popmsg, skip
        -- todo abort on error?
        local workdef = TOOL:DEEPCOPY(msg)
        local ok, res = pcall(function()
            if TOOL:GETREFERENCE(arg, {"events"}) then
                for _, event in ipairs(arg.events) do
                    workdef["PROCESS_EXEC_SLOT"] = {event = event}
                    popmsg, skip = self:_executefunc(workdef)
                    if popmsg then
                        break
                    end
                end
            else
                workdef["PROCESS_EXEC_SLOT"] = arg
                popmsg, skip = self:_executefunc(workdef)
            end
        end)

        if msg.executor_mode[1] == lib.EXECUTOR_MODE.ONESHOT then
            popmsg = true
        end

        if popmsg then
            if workdef.aborted then
                table.insert(aborted_funcdefs, work)
            else
                table.insert(finished_funcdefs, work)
            end
        else
            if TOOL:TABLETYPE(skip) and skip[1] then
                -- Mark funcdefs to skip with a special flag that will be
                -- handled in _executefunc
                for _, kskip in ipairs(skip) do
                    local funcdef = TOOL:GETREFERENCE(msg, {"funcdefinition", kskip}, nil)
                    if TOOL:TABLETYPE(funcdef) then funcdef["$Skip"] = true end
                end
            end

            msg.pasx = workdef.pasx
            msg.result = workdef.result

            table.insert(active_funcdefs, { MessageID = msg.id, Message = J.encode(msg) })
        end

        V:SET{path = "popmsg", v = popmsg}
        local errormsg = ok and "" or "error:" .. res
        V:SET{
            path = "msg",
            v = ("[%s/%s] finised with id %s. Pop message = %s, %s"):format(k, #workitems, msg.id, popmsg, errormsg),
        }

        timer["PROCESS_EXEC_SLOT/process_message"]:t_end()

        -- Wait one millisecond as to not instantly overwrite the previous status message
        syslib.sleep(1)
    end

    -- Update Custom Table by deleting old funcdefs and replacing updated ones

    timer["PROCESS_EXEC_SLOT/update_custom_table"]:t_start()

    _scopedcall(
            "PROCESS_EXEC_SLOT",
            "Remove finished funcdefs",
            _custom_table_delete_all,
            message_table_path, message_table_name, finished_funcdefs
    )

    _scopedcall(
            "PROCESS_EXEC_SLOT",
            "Remove aborted funcdefs",
            _custom_table_delete_all,
            message_table_path, message_table_name, aborted_funcdefs
    )

    _scopedcall(
            "PROCESS_EXEC_SLOT",
            "Update active messages",
            _custom_table_replace_data,
            message_table_path, message_table_name, active_funcdefs
    )

    timer["PROCESS_EXEC_SLOT/update_custom_table"]:t_end()

    -- Wait for message deletion from the beginning
    timer["PROCESS_EXEC_SLOT/wait_for_new_message_deletion"]:t_start()
    lib:_ensure_msgs_popped(queue_to_wait, {msgid_to_wait})
    timer["PROCESS_EXEC_SLOT/wait_for_new_message_deletion"]:t_end()

    timer["PROCESS_EXEC_SLOT/overall"]:t_end()
    return J.encode(workitems)
end

-- TODO: Consider moving to dedicated library (esi-tool?)
--- _ensure_msgs_pushed
---
--- Block until each given message has been successfully pushed onto the given
--- queue.
---
--- Since pushing and popping to and from a queue are asynchronous operations,
--- it will take some time after `syslib.msgpush` and `syslib.msgpop` for the
--- messages to be actually inserted or deleted. This can range from a few
--- milliseconds to multiple seconds, depending on the current system load. To
--- ensure synchronous like operations, this function actively waits for the
--- messages to appear in the queue.
---
--- @param q table Queue object as returned by `syslib.msgqueue`
--- @param msgs table List of messages that shall be waited for to be inserted
function lib:_ensure_msgs_pushed(q, msgs)
    if not (TOOL:TABLETYPE(msgs) and msgs[1]) then return end

    local msgs_set = {}
    for _, msg in ipairs(msgs) do msgs_set[msg] = true end

    local to_check = 0
    for _, _ in pairs(msgs_set) do to_check = to_check + 1 end

    while true do
        for _, msg in q{wait_for = 5000} do
            if msgs_set[msg] then
                msgs_set[msg] = nil
                to_check = to_check - 1
                if to_check == 0 then return end
            end
        end
        syslib.sleep(100)
    end
end

-- TODO: Consider moving to dedicated library (esi-tool?)
--- _ensure_msgs_popped
---
--- Block until each given message id has been successfully deleted from the
--- given queue.
---
--- Since pushing and popping to and from a queue are asynchronous operations,
--- it will take some time after `syslib.msgpush` and `syslib.msgpop` for the
--- messages to be actually inserted or deleted. This can range from a few
--- milliseconds to multiple seconds, depending on the current system load. To
--- ensure synchronous like operations, this function actively waits for the
--- message ids to disappear from the queue.
---
--- @param q table Queue object as returned by `syslib.msgqueue`
--- @param ids table List of message ids that shall be waited for to be removed
function lib:_ensure_msgs_popped(q, ids)
    if not (TOOL:TABLETYPE(ids) and ids[1]) then return end

    local ids_set = {}
    for _, id in ipairs(ids) do ids_set[id] = true end

    while true do
        local still_there = false
        for id, _ in q() do
            if ids_set[id] then
                still_there = true
                break
            end
        end

        if still_there then
            syslib.sleep(100)
        else
            return
        end
    end
end

-- TODO: Consider moving to dedicated library (esi-tool?)
--- _sync_msgpush
---
--- Block until given message has been successfully pushed onto the given
--- queue.
---
--- Since pushing and popping to and from a queue are asynchronous operations,
--- it will take some time after `syslib.msgpush` and `syslib.msgpop` for the
--- messages to be actually inserted or deleted. This can range from a few
--- milliseconds to multiple seconds, depending on the current system load. To
--- ensure synchronous like operations, this function actively waits for the
--- message to appear in the queue.
---
--- @param q table Queue object as returned by `syslib.msgqueue`
--- @param msg table Message that shall be pushed synchronously
function lib:_sync_msgpush(q, msg)
    syslib.msgpush(q, msg)
    lib:_ensure_msgs_pushed(q, {msg})
end

-- TODO: Consider moving to dedicated library (esi-tool?)
--- _sync_msgpop
---
--- Block until given message id has been successfully deleted from the given
--- queue.
---
--- Since pushing and popping to and from a queue are asynchronous operations,
--- it will take some time after `syslib.msgpush` and `syslib.msgpop` for the
--- messages to be actually inserted or deleted. This can range from a few
--- milliseconds to multiple seconds, depending on the current system load. To
--- ensure synchronous like operations, this function actively waits for the
--- message id to disappear from the queue.
---
--- @param q table Queue object as returned by `syslib.msgqueue`
--- @param id table Message id that shall be waited for to be removed
function lib:_sync_msgpop(q, id)
    syslib.msgpop(q, id)
    lib:_ensure_msgs_popped(q, {id})
end

--- _msgget_MES_TO_EXEC_SLOT
---
--- Retrieve messages from dedicated MES_TO_EXEC_SLOT. The queue of the
--- current object (`syslib.getself`) will be used.
---
--- The messages will *not* be deleted, it is the responsibility of the caller
--- to do so.
---
--- @return table List of messages
--- @return table Queue object of the current object
function lib:_msgget_MES_TO_EXEC_SLOT()
    local data = {}
    local exProviderObj = syslib.getself()
    local exProviderId = exProviderObj:numid()
    local ex_queue_id = syslib.msgqueue(exProviderId, self.MES_TO_EXEC_SLOT)
    for msgid, msg in pairs(ex_queue_id) do
        msg = J.decode(msg) or msg
        table.insert(data, {msgid = msgid, msg = msg})
    end
    return data, ex_queue_id
end

-- TODO: still in use?
--- GETMSG
---
--- Retrieve response message from dedicated SF_TO_MES_SLOT of the target
--- queue.
---
--- The `arg` table is expected to contain the following members:
--- -   `path`: The path of the target object. If this path is beneath a
---     connector, the call will be executed there.
--- -   `debug`: Optional path to a debug object, under which debug variables
---     will be created.
---
--- @param arg table Arguments table
function lib.GETMSG(_, arg)
    syslib.sleep(arg.delay or 0)
    local ms = syslib.now()
    local call = [[
    local O = require("esi-objects")
    local J = require("dkjson")
    local MSI = require('mbro-msi')
    local TOOL = require("esi-tool")
    local _, exProviderObj = O:EXISTS{path = "]] .. arg.path .. [["}
    local exProviderId = exProviderObj:numid()
    local ex_queue_id = syslib.msgqueue(exProviderId, MSI.SF_TO_MES_SLOT)
    -- ! get 1
    local msgid, msg = syslib.msgnext(ex_queue_id)
    if TOOL:NILTYPE(msgid) then return nil end
    syslib.msgpop(ex_queue_id, msgid)
    return {msgid=msgid, msg=msg}
    -- ! get all
    -- local data = {}
    -- for msgid, msg in pairs(ex_queue_id) do
    --     table.insert(data, {msgid, msg})
    --     -- syslib.msgpop(msgid)
    -- end
    -- return data]]
    -- do return call end
    local resexecute, resmsg = syslib.execute(arg.path, call)
    -- return {resexecute, resmsg}
    local msg = TOOL:GETREFERENCE(resexecute, {"msg"}, nil, function(m) return J.decode(m) end)
    if TOOL:STRINGTYPE(arg.debug) then
        local exists, obj = O:EXISTS{path = arg.debug}
        if exists then
            V:SET{object = obj, path = "debug/execute", v = {resexecute, resmsg}}
            V:SET{object = obj, path = "debug/msg", v = msg}
            V:SET{object = obj, path = "debug/rt", v = syslib.now() - ms}
        end
    end
    return msg
end

--- GET_FIRST_MATCHING_FROM_RAW_HISTORY
---
--- Return the first value of a historical object that matches a condition.
--- The desired object will be accessed via Historian Mapping.
---
--- The `arg` table is expected to contain the following members:
--- -   `mapping`: The path to the Historian Mapping object, which contain the
---     desired tag.
--- -   `tag`: The tag that was given to the desired historian object.
--- -   `start_time`: Start of the time range to be searched.
--- -   `end_time`: End of the time range to be searched.
--- -   `include_boundaries`: Boolean, whether to also include the first value
---     even if it was set before `start_time`. Default: false.
--- -   `condition`: The condition that should be tried on the historical
---     values. The first value to match this condition will be returned.
---
--- @param arg table Arguments table
function lib:GET_FIRST_MATCHING_FROM_RAW_HISTORY(arg)
    local path = self:GETREFERENCE(syslib.get(arg.mapping), {("$matchFirst Tag == '%s'"):format(arg.tag), "Path"})
    local start_time = arg.start_time
    local end_time = arg.end_time
    local include_boundaries = arg.include_boundaries or false
    local condition = luaxp.compile(arg.condition)

    local function _check_condition(t, v, q) return luaxp.run(condition, {t = t, v = v, q = q}) end

    local rs, _ = syslib.getrawhistory(path, include_boundaries, start_time, end_time, 0)

    for t, v, q in rs() do
        if _check_condition(t, v, q) then
            _, q = inQ:simpleQuality(q)
            t = self:_toPASXTime(t)
            return {t = t, v = v, q = q}
        end
    end

    return nil
end

function lib:queue_get(queue, slot)
    local q = {queue = syslib.getobject(queue), slot = tostring(slot)}

    local PREFIX_DELETED = "__deleted__"

    local function with_backoff(func)
        local max_sleep_duration = 100
        local timeout = 30 * 1000

        local n = 1
        local start = syslib.now()
        while not func() do
            if syslib.now() - start > timeout then error("Timeout") end

            syslib.sleep(n)
            n = math.min(n * 2, max_sleep_duration)
        end
    end

    function q:_get_message_path_relative(message_id) return ("_queue/%s/%s"):format(self.slot, message_id) end

    function q:_get_message_path_absolute(message_id)
        return ("%s/%s"):format(self.queue:path(), self:_get_message_path_relative(message_id))
    end

    function q:_list_impl(list_deleted)
        local list = {}

        local object = self.queue
        local components = {"_queue", self.slot}
        for _, component in ipairs(components) do
            object = object:child(component)
            if not object then return list end
        end

        for _, child in ipairs(object:children()) do
            local is_deleted = child.ObjectName:sub(1, PREFIX_DELETED:len()) == PREFIX_DELETED
            if (list_deleted and is_deleted) or (not list_deleted and not is_deleted) then
                table.insert(list, child.ObjectName)
            end
        end

        table.sort(list)

        return list
    end

    function q:list() return self:_list_impl(false) end

    function q:list_deleted() return self:_list_impl(true) end

    function q:iter(consuming)
        local message_ids = self:list()
        local index = 0
        local count = #message_ids

        return function()
            local content
            local message_id
            while not content and index <= count do
                index = index + 1
                message_id = message_ids[index]
                content = self:get(message_id)

                if consuming then q:remove(message_id) end
            end

            return message_id, content
        end
    end

    function q:add(message)
        local message_id = ("%s_%s"):format(syslib.now(), syslib.uuid())
        self:set(message_id, message)
        return message_id
    end

    function q:add_all(messages)
        local message_ids = {}
        for _, message in ipairs(messages) do
            local message_id = self:add(message)
            table.insert(message_ids, message_id)
        end

        return message_ids
    end

    function q:remove(message_id)
        local path = self:_get_message_path_absolute(message_id)

        -- Due to synchronization issues, the deletion / renaming must be done repeatedly to make sure it gets through
        -- at some point.
        local ok = pcall(with_backoff, function()
            local object = syslib.getobject(path)
            if object then
                if not pcall(syslib.deleteobject, path) then
                    -- Deletion failed. Maybe we are under a connector, which is not allowed to delete objects?
                    -- Rename and empty it to make it no longer appear in listings.
                    pcall(function()
                        syslib.setvalue(object, nil)
                        object.ObjectName = PREFIX_DELETED .. object.ObjectName
                        object.ArchiveOptions.StorageStrategy = 0
                        object.ArchiveOptions.ArchiveSelector = inmation.model.codes.ArchiveTarget.ARC_TEST
                        object:commit()
                    end)
                end
            end

            return syslib.getobject(path) == nil
        end)

        if not ok then error("[esi-mbro:queue] Timeout for deleting message") end
    end

    function q:remove_all(message_ids) for _, message_id in ipairs(message_ids) do self:remove(message_id) end end

    function q:clear() for _, message_id in ipairs(self:list()) do self:remove(message_id) end end

    function q:purge() for _, message_id in ipairs(self:list_deleted()) do self:remove(message_id) end end

    function q:get(message_id)
        local path = self:_get_message_path_relative(message_id)
        return V:GET{object = self.queue, path = path}
    end

    function q:set(message_id, message)
        if type(message) == "table" then message = J.encode(message) end

        -- Prepare the hierarchy and the message object itself
        do
            local message_path = self:_get_message_path_absolute(message_id)

            local parent, name = syslib.splitpath(message_path)
            _create_hierarchy(parent, syslib.model.classes.VariableGroup)

            local exists, _ = O:EXISTS{parentpath = parent, objectname = name}

            if not exists then
                O:UPSERTOBJECT{
                    path = parent,
                    class = "MODEL_CLASS_VARIABLE",
                    properties = {
                        [".ObjectName"] = name,
                        [".ArchiveOptions.StorageStrategy"] = syslib.model.flags.ItemValueStorageStrategy
                            .STORE_RAW_HISTORY,
                        [".ArchiveOptions.ArchiveSelector"] = inmation.model.codes.ArchiveTarget.ARC_PRODUCTION,
                        [".ArchiveOptions.PersistencyMode"] = syslib.model.codes.PersistencyMode.PERSIST_IMMEDIATELY,
                    },
                }
            end
        end

        -- Wait until the object is materialized and in a stable status
        do
            local ok = pcall(with_backoff, function()
                -- While the object is still not in final state, strange race conditions can happen, like re-appearing
                -- messages after being deleted. So, explicitly wait for modified to be cleared.
                local object = syslib.getobject(self:_get_message_path_absolute(message_id))
                return object ~= nil and object:state() == syslib.model.flags.ModObjectState.OBJ_FINAL_CONSTRUCTED
            end)

            if not ok then error("[esi-mbro:queue] Timeout for creating message") end
        end

        -- Set message content and make sure it is really stored in the message. Due to synchronization issues, this
        -- must be done repeatedly to make sure it gets through at some point.
        do
            local ok = pcall(with_backoff, function()
                local object = syslib.getobject(self:_get_message_path_absolute(message_id))
                local read_message = syslib.getvalue(object)
                local is_same = read_message == message
                if not is_same then
                    -- Set content again, in case it got lost somewhere
                    syslib.setvalue(object, message)
                end
                return is_same
            end)

            if not ok then error("[esi-mbro:queue] Timeout for filling message with content") end
        end
    end

    return q
end

-- `GETREFERENCE` area

local function _isSequence(t)
    if t == nil then return false end
    if type(t) ~= "table" then return false end

    if #t > 0 then
        return true
    else
        for _, _ in pairs(t) do return false end
        return true
    end
end

-- TODO: Merge this into TOOL:GETREFERENCE
--- GETREFERENCE
---
--- `GETREFERENCE` is a save way to get an element from a table, without
--- getting nil exception. It is also possible to add a default return value,
--- if the element is not found or `nil`. To get a higher flexibility, it is
--- possible to provide a function that is called on the found element. This
--- function is not called to a nil value or the default value. If the
--- function throws an error, the default value is returned.
---
--- @param tbl table
--- @param tableParts table
--- @param default nil | any
--- @param func nil | function
function lib.GETREFERENCE(_, tbl, tableParts, default, func)
    local target = tbl
    if type(tbl) == "table" and type(tableParts) == "table" then
        local skip_next = false
        for tablePartsIndex, v in ipairs(tableParts) do
            if skip_next then
                skip_next = false
                goto continue
            end

            -- * check if table type
            if not TOOL:TABLETYPE(target) then return default end
            -- * $last query
            if v == "$last" then
                target = target[v] or target[#target]
                -- * key word check
            elseif TOOL:STRINGTYPE(v) then
                -- * matchFirst
                if v:sub(1, 6) == "$match" then
                    v = v:sub(7, -1)
                    local match_type
                    if v:sub(1, 3) == "All" then
                        match_type = "all"
                        v = v:sub(4, -1)
                    elseif v:sub(1, 5) == "First" then
                        match_type = "first"
                        v = v:sub(6, -1)
                    else
                        return default
                    end

                    local matchfound = false

                    local mf_func = function(target_arg)
                        return luaxp.evaluate(v, target_arg)
                    end

                    local result_table = {}
                    for _, targetpart in ipairs(target) do
                        if not TOOL:TABLETYPE(targetpart) then return default end

                        local suc, res = pcall(mf_func, targetpart)
                        if suc == true and res == true then
                            matchfound = true
                            if match_type == "first" then
                                target = targetpart
                                break
                            elseif match_type == "all" then
                                table.insert(result_table, targetpart)
                            end
                        end
                    end

                    if matchfound == false then return default end

                    if match_type == "all" then
                        target = result_table
                    end

                    -- * evaluate
                elseif v:sub(1, 9) == "$evaluate" then
                    local mf_func = function(target_arg)
                        return luaxp.evaluate(v:sub(10, -1), target_arg)
                    end
                    local suc, res = pcall(function() return mf_func(target) end)
                    if suc == true and res == true then
                        target = target
                    else
                        target = nil
                    end
                    -- * ensureSequence
                elseif v == "$ensureSequence" then
                    if _isSequence(target) then
                        target = target
                    else
                        target = {target}
                    end
                elseif v == "$each" then
                    if _isSequence(target) then
                        local result_table = {}
                        for _, part in ipairs(target) do
                            table.insert(result_table, lib:GETREFERENCE(part, tableParts[tablePartsIndex + 1]))
                        end
                        target = result_table
                        skip_next = true
                    else
                        target = nil
                    end
                elseif v == "$flatten" then
                    if _isSequence(target) then
                        local result_table = {}
                        for _, part in ipairs(target) do
                            if _isSequence(part) then
                                for _, part_of_part in ipairs(part) do
                                    table.insert(result_table, part_of_part)
                                end
                            else
                                table.insert(result_table, part)
                            end
                        end
                        target = result_table
                    else
                        target = nil
                    end
                    -- * default lookup
                else
                    target = target[v]
                end
                -- * default lookup
            else
                target = target[v]
            end
            if target == nil then return default end

            ::continue::
        end
        if type(func) == "function" then
            local suc, res = pcall(function() return func(target) end)
            return TOOL:IIF(suc, res, default)
        end
        return target
    end
    return default
end

return lib