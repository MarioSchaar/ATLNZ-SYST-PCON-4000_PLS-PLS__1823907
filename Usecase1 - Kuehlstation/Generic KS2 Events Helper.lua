local base = "/System/Core/ATLNZ-Relay/ATLNZ/ATLNZ-V305-Con01/1823907_PLS4000_ATS00753/PROD/GenTemplate Companions"

syslib.mass({
	{
		class = syslib.model.classes.ActionItem,
		operation = syslib.model.codes.MassOp.UPSERT,
		path =  base .. "/Generic KS2 Events Helper",
		["ObjectName"] = "Generic KS2 Events Helper",
		["AdvancedLuaScript"] = [=[local TOOL = require("esi-tool")
local O = require("esi-objects")
local V = require("esi-variables")

local path = O:GETCUSTOM { object = syslib.getself(), key = "path" }
local wait = O:GETCUSTOM { object = syslib.getself(), key = "wait" }
local activeResult = O:GETCUSTOM { object = syslib.getself(), key = "activeResult" }
local inactiveResult = O:GETCUSTOM { object = syslib.getself(), key = "inactiveResult" }

local object = syslib.getself()
local refs = object.refs
local result = V:GET("Save/Last") or ""

for _, ref in ipairs(refs) do
    local ok, value = pcall(syslib.getvalue, ref.path)
    if ok then
        if ref.name == "_trigger_" and value ~= "Bereit" then
            syslib.sleep(tonumber(wait))
            ok, value = pcall(syslib.getvalue, path)
            if ok then
                result = TOOL:IIF(value, activeResult, inactiveResult)
            end
        end
    end
end

V:SET("Save/Last", result)

return result]=],
		["DedicatedThreadExecution"] = true,
		["ActivationMode"] = 1,
		["CustomOptions.CustomProperties.CustomPropertyValue"] = {
			"/System/Core/ATLNZ-Relay/ATLNZ/ATLNZ-V305-Con01/1823907_PLS4000_ATS00753/PROD/LIQS/4128_AL10__1825145/090_Kuehlstation_2__1823585/cool_active",
			"2000",
			"Kühlen und Rühren",
			"Rühren",
		},
		["CustomOptions.CustomProperties.CustomPropertyName"] = {
			"path",
			"wait",
			"activeResult",
			"inactiveResult",
		},
		references = {
			{
                path = "/System/Core/ATLNZ-Relay/ATLNZ/ATLNZ-V305-Con01/1823907_PLS4000_ATS00753/PROD/LIQS/4128_AL10__1825145/090_Kuehlstation_2__1823585/step",
                name = '_trigger_',
                type = 'OBJECT_LINK'
            }
        }
	}
})