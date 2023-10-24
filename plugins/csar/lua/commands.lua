-----------------------------------------------------
-- All callback commands have to go here.
-- You have to make sure, that these commands are
-- named uniquely in the DCSServerBot context.
-----------------------------------------------------
local base 	    = _G
local dcsbot    = base.dcsbot
local utils 	= base.require("DCSServerBotUtils")

-- function dcsbot.csarStatData(json)
--     log.write('DCSServerBot', log.DEBUG, 'CSAR: csarStatData()')
--     local msg = {}
--     msg.command = 'csarStatData'
--     msg.message = json.message
--     utils.sendBotTable(msg, json.channel)
-- end

-- function dcsbot.savePersistentData(json)
--     log.write('DCSServerBot', log.DEBUG, 'CSAR: savePersistentData()')
--     local msg = {}
--     msg.command = 'savePersistentData'
--     msg.message = json.message
--     utils.sendBotTable(msg, json.channel)
-- end

-- function dcsbot.getPersistentData(json)
--     log.write('DCSServerBot', log.DEBUG, 'CSAR: getPersistentData()')
--     local msg = {}
--     msg.command = 'getPersistentData'
--     msg.message = json.message
--     utils.sendBotTable(msg, json.channel)
-- end

function dcsbot.csarUpdatePersistentData(json)
    log.write('DCSServerBot', log.DEBUG, 'CSAR: csarUpdatePersistentData() (commands.lua)')
    -- log.write('DCSServerBot', log.DEBUG, 'CSAR: data:' .. utils.basicSerialize(json.data))
    local script = 'dcsbot._csarUpdatePersistentData(' .. utils.basicSerialize(json.data) .. ')'  --, ' .. json.points .. '
    net.dostring_in('mission', 'a_do_script(' .. utils.basicSerialize(script) .. ')')
end
