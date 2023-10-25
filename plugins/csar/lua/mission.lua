local base		= _G
dcsbot 			= base.dcsbot

--[[
    Initialize the CSAR Events handlers.
]]--

-- function csar.onChatMessage(message, from)
--   log.write('DCSServerBot', log.DEBUG, 'CSAR: onChatMessage()')
-- 	local msg = {}
-- 	msg.command = 'CSAR'
-- 	msg.message = message
-- 	msg.from_id = net.get_player_info(from, 'id')
-- 	msg.from_name = net.get_player_info(from, 'name')
-- 	utils.sendBotTable(msg, config.CHAT_CHANNEL)
-- end

function dcsbot.csarStatData(data)
	-- log.write('DCSServerBot', log.INFO, 'CSAR: csarStatData() (mission.lua)')
	local msg = {}
	msg.command = 'csarStatData'
  local json = net.lua2json(data)
	msg.data = json
  dcsbot.sendBotTable(msg)
end

function dcsbot.csarSavePersistentData(data)
	-- log.write('DCSServerBot', log.DEBUG, 'CSAR: savePersistentData() (mission.lua)')
	local msg = {}
	msg.command = 'csarSavePersistentData'
  local json = net.lua2json(data)
	msg.data = json
	dcsbot.sendBotTable(msg)
end

function dcsbot.csarGetPersistentData(data)
	log.write('DCSServerBot', log.DEBUG, 'CSAR: getPersistentData() (mission.lua)')
	local msg = {}
	msg.command = 'csarGetPersistentData'
  local json = net.lua2json(data)
	msg.data = json
	dcsbot.sendBotTable(msg)
end

function dcsbot._csarUpdatePersistentData(json)
	log.write('DCSServerBot', log.DEBUG, 'CSAR: _csarUpdatePersistentData() (mission.lua)')
	local lua = net.json2lua(json)
	csar.spawnCsar(lua)
end

function dcsbot.rescuedPilot(playername, typename, pilotname)
	log.write('DCSServerBot', log.DEBUG, 'CSAR: rescuedPilot (mission.lua)')
	msg.command = 'rescuedPilot'
	msg.playername = playername
	msg.typename = typename
	msg.pilotname = pilotname 
	dcsbot.sendBotTable(msg)
end

env.info("DCSServerBot - CSAR: mission.lua loaded.")
