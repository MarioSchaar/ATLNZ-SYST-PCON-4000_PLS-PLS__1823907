local base = "/System/Core/ATLNZ-Relay/ATLNZ/ATLNZ-V305-Con01/1823907_PLS4000_ATS00753/PROD/GenTemplate Companions"

syslib.mass({
	{
		class = syslib.model.classes.ActionItem,
		operation = syslib.model.codes.MassOp.UPSERT,
		path =  base .. "/Generic 4201 SIP Events",
		["ObjectName"] = "Generic 4201 SIP Events",
		["AdvancedLuaScript"] = [=[package.loaded["tak-dl-getevent-gentemplate"] = nil
local EVENTS = require("tak-dl-getevent-gentemplate")
local O = require("esi-objects")


local CONF = {
    customTableName = "StaticValues",
    Tags = { ReadyToTransfer = "SIP_trigg_report" },
    ErrorCodes = { Success = 200, GeneralError = 500, DataProcessingError = 501 },
    WaitTime = 2000,
    Event =
    {
        key = "event",
        messageData = {
            { data = { key = "Equipment" } },
            { data = { key = "EquipmentReadableName" } },
            { func = function (self, data) return 
                "CycleId " .. self:_getDataByType({ centralMapping = { key = "SIP_cycle_Id" } })
            end },
            { func = function (self, data) return 
                "ContainerId " .. self:_getDataByType({ centralMapping = { key = "SIP_container_Id" } })
            end },
            { func = function (self, data) return 
                "SIP"
            end }
        },
        customData = {
            { key = "SIP_cycle_Id",          centralMapping = { key = "SIP_cycle_Id" } },
            { key = "SIP_container_Id",      centralMapping = { key = "SIP_container_Id" } },
            { key = "SIP_recipe_ver",        centralMapping = { key = "SIP_recipe_ver" } },
            { key = "SIP_recipe_name",       centralMapping = { key = "SIP_recipe_name" } },

            { key = "SIP_cycle_result",      centralMapping = { key = "SIP_cycle_result" } },
            { key = "SIP_t_start",           centralMapping = { key = "SIP_t_start" } },
            { key = "SIP_t_end",             centralMapping = { key = "SIP_t_end" } },

            { key = "SIP_tClean",            centralMapping = { key = "SIP_tClean" } },

            { key = "SIP_T_min_coldSpot_PV", centralMapping = { key = "SIP_T_min_coldSpot_PV" } },
                { key = "SIP_T_min_coldSpot_SP", centralMapping = { key = "SIP_T_min_coldSpot_SP" } },
                
            { key = "SIP_tPrint",            centralMapping = { key = "SIP_tPrint" } },
            { key = "print_by",          centralMapping = { key = "print_by" } },
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
                    "EquipmentReadableName"
				],
				"Value": [
                    "1818971",
                    "PLS4000 4201"
				]
			}
			}
			
]=],
		},
		["CustomOptions.CustomTables.CustomTableName"] = {
			"StaticValues",
		},
		["CustomOptions.CustomProperties.CustomPropertyValue"] = {
			"/System/Core/ATLNZ-Relay/ATLNZ/ATLNZ-V305-Con01/CentralMappingConnector.TableData",
			"1818971",
		},
		["CustomOptions.CustomProperties.CustomPropertyName"] = {
			"mapping_table",
			"equipment",
		},
		references = {
			{
                path = "/System/Core/ATLNZ-Relay/ATLNZ/ATLNZ-V305-Con01/1823907_PLS4000_ATS00753/PROD/LIQS/4201__CIPSIP_1/CIPSIP_1__1818971/SIP_trigg_report",
                name = '_trigger_io_',
                type = 'OBJECT_LINK'
            }
        }
	}
})