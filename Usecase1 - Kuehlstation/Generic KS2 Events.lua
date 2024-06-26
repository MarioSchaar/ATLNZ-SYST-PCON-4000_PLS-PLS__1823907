local base = "/System/Core/ATLNZ-Relay/ATLNZ/ATLNZ-V305-Con01/1823907_PLS4000_ATS00753/PROD/GenTemplate Companions"

syslib.mass({
	{
		class = syslib.model.classes.ActionItem,
		operation = syslib.model.codes.MassOp.UPSERT,
		path =  base .. "/Generic KS2 Events",
		["ObjectName"] = "Generic KS2 Events",
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
            { centralMapping = { key = "cycle_Id" } },
            { centralMapping = { key = "container_Id" } },
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
            { key = "print_by",                 centralMapping = { key = "print_by" } },
            { key = "Tflow_4128-02TI242",       centralMapping = { key = "Tflow_4128-02TI242" } },
            { key = "vRot_4111-p2SI282",        centralMapping = { key = "vRot_4111-p2SI282" } },
            { key = "T_container_4111-p2TI241", centralMapping = { key = "T_container_4111-p2TI241" } },
            { key = "sens_leak_4128-02LSH221",  centralMapping = { key = "sens_leak_4128-02LSH221" } },
            { key = "step",                     centralMapping = { key = "step" } },
            { key = "cool_active",              centralMapping = { key = "cool_active" } },
            { key = "stir_active",              centralMapping = { key = "stir_active" } },
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
				"EventType"
				],
				"Value": [
				"PLS4000",
				"KS2"
				]
			}
			}
			
]=],
		},
		["CustomOptions.CustomTables.CustomTableName"] = {
			"StaticValues",
		},
		["CustomOptions.CustomProperties.CustomPropertyValue"] = {
			"/System/Core/ATLNZ-Relay/ATLNZ/ATLNZ-V305-Con01/1823907_PLS4000_ATS00753/PROD/GenTemplate Companions/Generic KS2-Mapping.TableData",
			"PROD",
		},
		["CustomOptions.CustomProperties.CustomPropertyName"] = {
			"mapping_table",
			"equipment",
		},
		references = {
			{
                path = "/System/Core/ATLNZ-Relay/ATLNZ/ATLNZ-V305-Con01/1823907_PLS4000_ATS00753/PROD/LIQS/4128_AL10__1825145/090_Kuehlstation_2__1823585/trigg_report",
                name = '_trigger_io_',
                type = 'OBJECT_LINK'
            }
        }
	}
})