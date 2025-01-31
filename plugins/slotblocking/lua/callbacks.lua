local base 	    = _G
local dcsbot    = base.dcsbot
local utils 	= base.require("DCSServerBotUtils")
local config	= base.require("DCSServerBotConfig")

local slotblock = slotblock or {}


local function has_value(tab, value)
    if not tab then
        return false
    end
    for idx1, value1 in ipairs(tab) do
        if type(value) == "table" then
            for idx2, value2 in ipairs(value) do
                if value1 == value2 then
                    return true
                end
            end
        elseif value1 == value then
            return true
        end
    end
    return false
end

local function is_vip(ucid)
    if not dcsbot.params then
        return false
    end
    local cfg = dcsbot.params['slotblocking']['VIP']
    if not cfg then
        return false
    end
    if cfg['ucid'] and has_value(cfg['ucid'], ucid) then
        return true
    end
    if cfg['discord'] and dcsbot.userInfo[ucid].roles ~= nil and has_value(dcsbot.userInfo[ucid].roles, cfg['discord']) then
        return true
    end
    return false
end

function slotblock.onPlayerTryConnect(addr, name, ucid, playerID)
    if playerID == 1 then
        return
    end
    log.write('DCSServerBot', log.DEBUG, 'Slotblocking: onPlayerTryConnect()')
    if not dcsbot.params or dcsbot.params['slotblocking'] == nil then
        return
    end
    local cfg = dcsbot.params['slotblocking']['VIP']
    if not cfg then
        return
    end
    if cfg['slots'] then
        local max = utils.loadSettingsRaw()['maxPlayers']
        local current = #net.get_player_list() + 1
        if current >= (max - cfg['slots']) then
            if not is_vip(ucid) then
                return false, config.MESSAGE_SERVER_FULL
            end
        end
    end
end

function slotblock.onPlayerTryChangeSlot(playerID, side, slotID)
    log.write('DCSServerBot', log.DEBUG, 'Slotblocking: onPlayerTryChangeSlot()')
    local player = net.get_player_info(playerID, 'ucid')
    local unit_name = DCS.getUnitProperty(slotID, DCS.UNIT_NAME)
    local group_name = DCS.getUnitProperty(slotID, DCS.UNIT_GROUPNAME)
    local unit_type = DCS.getUnitType(slotID)
    local points
    if not dcsbot.params or not dcsbot.params['slotblocking'] or not dcsbot.params['slotblocking']['restricted'] then
        return
    end
    -- check levels if any
    for id, unit in pairs(dcsbot.params['slotblocking']['restricted']) do
        if (unit['unit_type'] and unit['unit_type'] == unit_type)
                or (unit['unit_name'] and string.match(unit_name, unit['unit_name']) ~= nil)
                or (unit['group_name'] and string.match(group_name, unit['group_name']) ~= nil) then
            -- blocking slots by points // check multicrew
            if tonumber(slotID) then
                points = unit['points']
            else
                points = unit['crew']
            end
            if points then
                if not dcsbot.userInfo[player].points then
                    log.write('DCSServerBot', log.ERROR, 'Slotblocking: User has no points, but points are configured. Check your creditsystem.json and make sure a campaign is running.')
                    return
                end
                if dcsbot.userInfo[player].points < points then
                    local message = 'You need at least ' .. points .. ' points to enter this slot. You currently have ' .. dcsbot.userInfo[player].points .. ' points.'
                    net.send_chat_to(message, playerID)
                    return false
                end
            end
            if unit['ucid'] and player ~= unit['ucid'] then
                local message = unit['message'] or 'This slot is only accessible to a certain user.'
                net.send_chat_to(message, playerID)
                return false
            elseif unit['ucids'] and has_value(unit['ucids'], player) == false then
                local message = unit['message'] or 'This slot is only accessible to certain users.'
                net.send_chat_to(message, playerID)
                return false
            -- blocking slots by discord groups
            elseif unit['discord'] and has_value(dcsbot.userInfo[player].roles, unit['discord']) == false then
                local message = unit['message'] or 'This slot is only accessible to members with the ' .. unit['discord'] .. ' role.'
                net.send_chat_to(message, playerID)
                return false
            elseif unit['VIP'] and not unit['VIP'] == is_vip(player) then
                local message = unit['message'] or 'This slot is only accessible to VIP users.'
                net.send_chat_to(message, playerID)
                return false
            end
        end
    end
end

DCS.setUserCallbacks(slotblock)
