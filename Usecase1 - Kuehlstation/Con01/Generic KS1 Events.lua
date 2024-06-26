local base = "/System/Core/ATLNZ-Relay/ATLNZ/ATLNZ-V305-Con01/1823907_PLS4000_ATS00753/PROD/GenTemplate Companions"

syslib.mass({
	{
		class = syslib.model.classes.ActionItem,
		operation = syslib.model.codes.MassOp.UPSERT,
		path =  base .. "/Generic KS1 Events",
		["ObjectName"] = "Generic KS1 Events",
		["AdvancedLuaScript"] = [=[package.loaded["tak-dl-getevent-gentemplate"] = nil
local EVENTS = require("tak-dl-getevent-gentemplate")
local O = require("esi-objects")


local CONF = {
    customTableName = "StaticValues",
    Tags = { ReadyToTransfer = "trigg_report" },
    ErrorCodes = { Success = 200, GeneralError = 500, DataProcessingError = 501 },
    WaitTime = 2000,
    Event =
    {
        key = "event",
        messageData = {
            { data = { key = "Equipment" } },
            { data = { key = "EquipmentReadableName" } },
            { func = function (self, data) return 
                "CycleId " .. self:_getDataByType({ centralMapping = { key = "cycle_Id" } })
            end },
            { func = function (self, data) return 
                "ContainerId " .. self:_getDataByType({ centralMapping = { key = "container_Id" } })
            end },
            { centralMapping = { key = "cycle_type" } }
        },
        customData = {
            { key = "station_name",             centralMapping = { key = "station_name" } },

            { key = "cycle_Id",                 centralMapping = { key = "cycle_Id" } },
            { key = "container_Id",             centralMapping = { key = "container_Id" } },
            { key = "recipe_name",              centralMapping = { key = "recipe_name" } },
            { key = "recipe_ver",               centralMapping = { key = "recipe_ver" } },
            { key = "batch_Id",                 centralMapping = { key = "batch_Id" } },
            { key = "matr_Id",                  centralMapping = { key = "matr_Id" } },
                { key = "cycle_type",               centralMapping = { key = "cycle_type" } },

            { key = "cycle_result",             centralMapping = { key = "cycle_result" } },
            { key = "t_start",                  centralMapping = { key = "t_start" } },
            { key = "t_end",                    centralMapping = { key = "t_end" } },
            
            { key = "vRotStir_SP",              centralMapping = { key = "vRotStir_SP" } },
            { key = "dStir",                    centralMapping = { key = "dStir" } },
                { key = "Tcool_SP",                 centralMapping = { key = "Tcool_SP" } },
                { key = "Tcool_tolerance_lolo",     centralMapping = { key = "Tcool_tolerance_lolo" } },
                { key = "Tcool_tolerance_hihi",     centralMapping = { key = "Tcool_tolerance_hihi" } },
            { key = "Tequil_SP",                centralMapping = { key = "Tequil_SP" } },
            { key = "Tequil_tolerance_lolo",    centralMapping = { key = "Tequil_tolerance_lolo" } },
            { key = "Tequil_tolerance_hihi",    centralMapping = { key = "Tequil_tolerance_hihi" } },
                { key = "dStore_day",               centralMapping = { key = "dStore_day" } },
                { key = "dStore_hour",              centralMapping = { key = "dStore_hour" } },
                { key = "dStore_min",               centralMapping = { key = "dStore_min" } },
                { key = "dStore_sec",               centralMapping = { key = "dStore_sec" } },
                
            { key = "tPrint",                   centralMapping = { key = "tPrint" } },
            { key = "print_by",                centralMapping = { key = "print_by" } },
 
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
                    "1823584",
                    "PLS4000 KS1"
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
			"1823584",
		},
		["CustomOptions.CustomProperties.CustomPropertyName"] = {
			"mapping_table",
			"equipment",
		},
		references = {
			{
                path = "/System/Core/ATLNZ-Relay/ATLNZ/ATLNZ-V305-Con01/1823907_PLS4000_ATS00753/PROD/LIQS/4128_AL10__1825145/090_Kuehlstation_1__1823584/trigg_report",
                name = '_trigger_io_',
                type = 'OBJECT_LINK'
            }
        }
	}
})