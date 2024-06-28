local base = "/System/Core/ATLNZ-Relay/ATLNZ/ATLNZ-V305-Con01/1823907_PLS4000_ATS00753/PROD/GenTemplate Companions"

syslib.mass({
	{
		class = syslib.model.classes.ActionItem,
		operation = syslib.model.codes.MassOp.UPSERT,
		path =  base .. "/Generic CIP Events",
		["ObjectName"] = "Generic CIP Events",
		["AdvancedLuaScript"] = [=[package.loaded["tak-dl-getevent-gentemplate"] = nil
local EVENTS = require("tak-dl-getevent-gentemplate")
local O = require("esi-objects")


local CONF = {
    customTableName = "StaticValues",
    Tags = { ReadyToTransfer = "Print_Report" },
    ErrorCodes = { Success = 200, GeneralError = 500, DataProcessingError = 501 },
    WaitTime = 2000,
    Event =
    {
        key = "event",
        messageData = {
            { data = { key = "Equipment" } },
            { data = { key = "EquipmentReadableName" } },
            { func = function (self, data) return 
                "CycleId " .. self:_getDataByType({ centralMapping = { key = "CIP_cycle_Id" } })
            end },
            { func = function (self, data) return 
                "ContainerId " .. self:_getDataByType({ centralMapping = { key = "CIP_container_Id" } })
            end },
            { centralMapping = { key = "cycle_type" } }
        },
        customData = {
            { key = "CIP_G_max_PV",          centralMapping = { key = "CIP_G_max_PV" } },
            { key = "CIP_T_min_PV",          centralMapping = { key = "CIP_T_min_PV" } },
            { key = "CIP_container_Id",      centralMapping = { key = "CIP_container_Id" } },
            { key = "CIP_cycle_Id",          centralMapping = { key = "CIP_cycle_Id" } },
            { key = "CIP_cycle_result",      centralMapping = { key = "CIP_cycle_result" } },
            { key = "CIP_dFlush_PV",         centralMapping = { key = "CIP_dFlush_PV" } },
            { key = "CIP_dFlush_SP",         centralMapping = { key = "CIP_dFlush_SP" } },
            { key = "CIP_print_by",          centralMapping = { key = "CIP_print_by" } },
            { key = "CIP_recipe_name",       centralMapping = { key = "CIP_recipe_name" } },
            { key = "CIP_recipe_ver",        centralMapping = { key = "CIP_recipe_ver" } },
            { key = "CIP_tClean_stir_on_PV", centralMapping = { key = "CIP_tClean_stir_on_PV" } },
            { key = "CIP_tClean_stir_on_SP", centralMapping = { key = "CIP_tClean_stir_on_SP" } },
            { key = "CIP_tPrint",            centralMapping = { key = "CIP_tPrint" } },
            { key = "CIP_t_end",             centralMapping = { key = "CIP_t_end" } },
            { key = "CIP_t_start",           centralMapping = { key = "CIP_t_start" } },
            { key = "cycle_type",             centralMapping = { key = "cycle_type" } }
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
                    "PLS4000 CIPSIP"
				]
			}
			}
			
]=],
		},
		["CustomOptions.CustomTables.CustomTableName"] = {
			"StaticValues",
		},
		["CustomOptions.CustomProperties.CustomPropertyValue"] = {
			"/System/Core/ATLNZ-Relay/ATLNZ/ATLNZ-V305-Con01/1823907_PLS4000_ATS00753/PROD/GenTemplate Companions/Generic CIP-Mapping.TableData",
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