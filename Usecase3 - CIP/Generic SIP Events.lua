local base = "/System/Core/ATLNZ-Relay/ATLNZ/ATLNZ-V305-Con01/1823907_PLS4000_ATS00753/PROD/GenTemplate Companions"

syslib.mass({
	{
		class = syslib.model.classes.ActionItem,
		operation = syslib.model.codes.MassOp.UPSERT,
		path =  base .. "/Generic SIP Events",
		["ObjectName"] = "Generic SIP Events",
		["AdvancedLuaScript"] = [=[package.loaded["tak-dl-getevent-gentemplate"] = nil
local EVENTS = require("tak-dl-getevent-gentemplate")
local O = require("esi-objects")


local CONF = {
    customTableName = "StaticValues",
    Tags = { ReadyToTransfer = "Print_Report" },
    ErrorCodes = { Success = 200, GeneralError = 500, DataProcessingError = 501 },
    WaitTime = 100,
    Event =
    {
        key = "event",
        messageData = {
            { data = { key = "EventType" } },
            { data = { key = "Equipment" } },
            { centralMapping = { key = "SIP_cycle_Id" } },
            { centralMapping = { key = "SIP_container_Id" } }
        },
        customData = {
            { key = "SIP_cycle_Id",          centralMapping = { key = "SIP_cycle_Id" } },
            { key = "SIP_container_Id",      centralMapping = { key = "SIP_container_Id" } },
            { key = "SIP_recipe_ver",        centralMapping = { key = "SIP_recipe_ver" } },
            { key = "SIP_recipe_name",       centralMapping = { key = "SIP_recipe_name" } },
            { key = "SIP_cycle_result",      centralMapping = { key = "SIP_cycle_result" } },
            { key = "SIP_t_start",           centralMapping = { key = "SIP_t_start" } },
            { key = "SIP_t_end",             centralMapping = { key = "SIP_t_end" } },
            { key = "SIP_t_Ti02_lolo",       centralMapping = { key = "SIP_t_Ti02_lolo" } },
            { key = "SIP_tClean",            centralMapping = { key = "SIP_tClean" } },
            { key = "SIP_T_min_coldSpot_PV", centralMapping = { key = "SIP_T_min_coldSpot_PV" } },
            { key = "SIP_T_min_coldSpot_SP", centralMapping = { key = "SIP_T_min_coldSpot_SP" } },
            { key = "SIP_tPrint",            centralMapping = { key = "SIP_tPrint" } },
            { key = "SIP_print_by",          centralMapping = { key = "SIP_print_by" } },
            { key = "SIP_on_off",            centralMapping = { key = "SIP_on_off" } },
            { key = "SIP_T_4201TI02_PV",     centralMapping = { key = "SIP_T_4201TI02_PV" } },
            { key = "SIP_step",              centralMapping = { key = "SIP_step" } },
        },
        condition = true
    },
}

local equipment = O:GETCUSTOM { object = syslib.getself(), key = "equipment" }
local mapping_table = O:GETCUSTOM { object = syslib.getself(), key = "mapping_table" }

return EVENTS:RUN(CONF, mapping_table, equipment)]=],
		["DedicatedThreadExecution"] = true,
		["ActivationMode"] = 1,
		["CustomOptions.CustomTables.TableData"] = {
			[=[
			{
			"data": {
				"Key": [
				"Equipment",
				"EventType"
				],
				"Value": [
				"PLS4000",
				"SIP"
				]
			}
			}
			
]=],
		},
		["CustomOptions.CustomTables.CustomTableName"] = {
			"StaticValues",
		},
		["CustomOptions.CustomProperties.CustomPropertyValue"] = {
			"/System/Core/ATLNZ-Relay/ATLNZ/ATLNZ-V305-Con01/1823907_PLS4000_ATS00753/PROD/GenTemplate Companions/Generic SIP-Mapping.TableData",
			"PROD",
		},
		["CustomOptions.CustomProperties.CustomPropertyName"] = {
			"mapping_table",
			"equipment",
		},
		references = {
			{
                path = "/System/Core/ATLNZ-Relay/ATLNZ/ATLNZ-V305-Con01/1823907_PLS4000_ATS00753/PROD/LIQS/4201__CIPSIP_1/CIPSIP_1__1818971/trigg_report",
                name = '_trigger_io_',
                type = 'OBJECT_LINK'
            }
        }
	}
})