local base = "/System/Core/ATLNZ-Relay/ATLNZ/ATLNZ-V305-Con01/1823907_PLS4000_ATS00753/PROD/GenTemplate Companions"

syslib.mass({
	{
		class = syslib.model.classes.ActionItem,
		operation = syslib.model.codes.MassOp.UPSERT,
		path =  base .. "/Generic 4201 CCIP Events",
		["ObjectName"] = "Generic 4201 CCIP Events",
		["AdvancedLuaScript"] = [=[package.loaded["tak-dl-getevent-gentemplate"] = nil
local EVENTS = require("tak-dl-getevent-gentemplate")
local O = require("esi-objects")


local CONF = {
    customTableName = "StaticValues",
    Tags = { ReadyToTransfer = "CCIP_trigg_report" },
    ErrorCodes = { Success = 200, GeneralError = 500, DataProcessingError = 501 },
    WaitTime = 2000,
    Event =
    {
        key = "event",
        messageData = {
            { data = { key = "Equipment" } },
            { data = { key = "EquipmentReadableName" } },
            { func = function (self, data) return 
                "CycleId " .. self:_getDataByType({ centralMapping = { key = "CCIP_cycle_Id" } })
            end },
            { func = function (self, data) return 
                "ContainerId " .. self:_getDataByType({ centralMapping = { key = "CCIP_container_Id" } })
            end },
            { centralMapping = { key = "cycle_type" } }
        },
        customData = {
            { key = "CCIP_cycle_Id",          centralMapping = { key = "CCIP_cycle_Id" } },
            { key = "CCIP_container_Id",      centralMapping = { key = "CCIP_container_Id" } },
            { key = "CCIP_recipe_ver",        centralMapping = { key = "CCIP_recipe_ver" } },
            { key = "CCIP_recipe_name",       centralMapping = { key = "CCIP_recipe_name" } },
            { key = "cycle_type",             centralMapping = { key = "cycle_type" } },

            { key = "CCIP_cycle_result",      centralMapping = { key = "CCIP_cycle_result" } },
            { key = "CCIP_t_start",           centralMapping = { key = "CCIP_t_start" } },
            { key = "CCIP_t_end",             centralMapping = { key = "CCIP_t_end" } },

            { key = "CCIP_T_naoh1_PV",        centralMapping = { key = "CCIP_T_naoh1_PV" } },
                { key = "CCIP_T_naoh1_SP",        centralMapping = { key = "CCIP_T_naoh1_SP" } },
            { key = "CCIP_G_naoh1_PV",        centralMapping = { key = "CCIP_G_naoh1_PV" } },
                { key = "CCIP_G_naoh1_SP",        centralMapping = { key = "CCIP_G_naoh1_SP" } },
            { key = "CCIP_T_naoh2_PV",        centralMapping = { key = "CCIP_T_naoh2_PV" } },
                { key = "CCIP_T_naoh2_SP",        centralMapping = { key = "CCIP_T_naoh2_SP" } },
            { key = "CCIP_G_naoh2_PV",        centralMapping = { key = "CCIP_G_naoh2_PV" } },
                { key = "CCIP_G_naoh2_SP",        centralMapping = { key = "CCIP_G_naoh2_SP" } },
            { key = "CCIP_T_hno3_PV",         centralMapping = { key = "CCIP_T_hno3_PV" } },
                { key = "CCIP_T_hno3_SP",         centralMapping = { key = "CCIP_T_hno3_SP" } },
            { key = "CCIP_G_hno3_PV",         centralMapping = { key = "CCIP_G_hno3_PV" } },
                { key = "CCIP_G_hno3_SP",         centralMapping = { key = "CCIP_G_hno3_SP" } },
        
                { key = "CCIP_dFlush_lost_PV",    centralMapping = { key = "CCIP_dFlush_lost_PV" } },
                    { key = "CCIP_dFlush_lost_SP",    centralMapping = { key = "CCIP_dFlush_lost_SP" } },
                { key = "T_min_PV",          centralMapping = { key = "T_min_PV" } },
                { key = "CCIP_tClean_stir_on_PV", centralMapping = { key = "CCIP_tClean_stir_on_PV" } },
                    { key = "CCIP_tClean_stir_on_SP", centralMapping = { key = "CCIP_tClean_stir_on_SP" } },
                { key = "G_max_PV",          centralMapping = { key = "G_max_PV" } },

            { key = "CCIP_tPrint",            centralMapping = { key = "CCIP_tPrint" } },
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
                path = "/System/Core/ATLNZ-Relay/ATLNZ/ATLNZ-V305-Con01/1823907_PLS4000_ATS00753/PROD/LIQS/4201__CIPSIP_1/CIPSIP_1__1818971/CCIP_print_report",
                name = '_trigger_io_',
                type = 'OBJECT_LINK'
            }
        }
	}
})