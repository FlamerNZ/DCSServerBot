import asyncio
import logging
import os
import shutil
import uvicorn

from contextlib import closing
from core import Plugin, DEFAULT_TAG
from datetime import datetime, timedelta, time
from fastapi import FastAPI, APIRouter, Form, Response
from psycopg.rows import dict_row
from services import DCSServerBot
from typing import Optional
from uvicorn import Config
from core import const, report, Status, Server, utils, ServiceRegistry
import json, sys

app: Optional[FastAPI] = None


class RestAPI(Plugin):

    def __init__(self, bot: DCSServerBot):
        global app

        super().__init__(bot)
        self.router = APIRouter()
        self.router.add_api_route("/topkills", self.topkills, methods=["GET"])
        self.router.add_api_route("/topkdr", self.topkdr, methods=["GET"])
        self.router.add_api_route("/servers", self.servers, methods=["GET"])
        self.router.add_api_route("/getuser", self.getuser, methods=["POST"])
        self.router.add_api_route("/missilepk", self.missilepk, methods=["POST"])
        self.router.add_api_route("/stats", self.stats, methods=["POST"])
        self.app = app
        cfg = self.locals[DEFAULT_TAG]
        self.config = Config(app=self.app, host=cfg['listen'], port=cfg['port'], log_level=logging.ERROR,
                             use_colors=False)
        self.server: uvicorn.Server = uvicorn.Server(config=self.config)
        self.task = None

    async def cog_load(self) -> None:
        await super().cog_load()
        self.task = asyncio.create_task(self.server.serve())

    async def cog_unload(self):
        self.server.should_exit = True
        await self.task
        await super().cog_unload()

    def topkills(self):
        with self.pool.connection() as conn:
            with closing(conn.cursor(row_factory=dict_row)) as cursor:
                return cursor.execute("""
                    SELECT p.name AS "fullNickname", SUM(pvp) AS "AAkills", SUM(deaths) AS "deaths", 
                        CASE WHEN SUM(deaths) = 0 THEN SUM(pvp) ELSE SUM(pvp)/SUM(deaths::DECIMAL) END AS "AAKDR", 
                        CASE WHEN (points) IS NULL THEN 0 ELSE points END AS points,
						CASE WHEN (total_rescues) IS NULL THEN 0 ELSE total_rescues END AS total_rescues,
						CASE WHEN (helicopterused) IS NULL THEN '' ELSE helicopterused END AS fav_chopper
                    FROM statistics s
                    LEFT JOIN players p ON s.player_ucid = p.ucid
                    LEFT JOIN (
                        SELECT init_id, 
                            COALESCE(SUM(points),0) AS points
                        FROM pu_events
                        GROUP BY init_id 
                    ) u ON s.player_ucid = u.init_id
					LEFT JOIN (
						SELECT ucid,
						SUM(savedpilots) AS total_rescues
						FROM csar_events c
						GROUP BY ucid
					) c on s.player_ucid = c.ucid
					LEFT JOIN (
						SELECT ucid, 
							   savedpilots,
							   helicopterused
						FROM(
							SELECT Row_Number() OVER(partition by ucid ORDER BY savedpilots DESC) AS 
						   row_number, *
						FROM (
							SELECT ucid, helicopterused,
							SUM(savedpilots) AS savedpilots
							FROM csar_events
							GROUP BY ucid, helicopterused
							)
						) 
						 Where row_number = 1
					) h on s.player_ucid = h.ucid
                    GROUP BY 1, points, total_rescues, helicopterused ORDER BY points DESC LIMIT 10
                """).fetchall()

    def topkdr(self):
        with self.pool.connection() as conn:
            with closing(conn.cursor(row_factory=dict_row)) as cursor:
                return cursor.execute("""
                    SELECT p.name AS "fullNickname", SUM(pvp) AS "AAkills", SUM(deaths) AS "deaths", 
                           CASE WHEN SUM(deaths) = 0 THEN SUM(pvp) ELSE SUM(pvp)/SUM(deaths::DECIMAL) END AS "AAKDR" 
                    FROM statistics s, players p 
                    WHERE s.player_ucid = p.ucid 
                    GROUP BY 1 ORDER BY 4 DESC LIMIT 10
                """).fetchall()
    
    def servers(self):
        def is_time_between(begin_time, end_time, check_time=None):
            # If check time is not given, default to current local time
            check_time = check_time or datetime.now().time()
            if begin_time < end_time:
                return check_time >= begin_time and check_time <= end_time
            else: # crosses midnight
                return check_time >= begin_time or check_time <= end_time

        servers_data = []
        # get all servers, and their status, and add them to the servers_data list as JSON
        scheduler = self.bot.cogs.get('Scheduler')
        for server in self.bot.servers.values():
            server_name = f"{server.name}"
            server_status = f"{server.status}"
            active_players = f"{len(server.players) + 1}"
            max_players = f"{server.settings['maxPlayers']}"
            ip_addr = f"{server.node.public_ip}:{server.settings['port']}"
            current_mission = ""
            mission_time = ""
            password = ""
            seconds_till_restart = None
            server_display_name = server.display_name
            server_maintenance = server.maintenance
            # Weather
            try:
                weather = server.current_mission.weather
            except:
                weather = None

            # Current mission
            try:
                current_map = f"{server.current_mission.map}"
                mission_date = f"{server.current_mission.date}"
            except:
                current_map = None
                mission_date = None

            if server.current_mission:
                current_mission = f"{server.current_mission.name}"
                mission_time = server.current_mission.mission_time
                config = scheduler.get_config(server)
                if 'restart' in config:
                    rconf = config['restart']
                    if 'local_times' in rconf:
                        previous_t = time(*[int(i) for i in rconf['local_times'][-1].split(':')])
                        for t in rconf['local_times']: 
                            t = time(*[int(i) for i in t.split(':')]) 
                            if is_time_between(previous_t,t):
                                now = datetime.now()
                                seconds_till_restart = \
                                    int((timedelta(hours=24) - \
                                    (now - now.replace(hour=t.hour, minute=t.minute,\
                                     second=0, microsecond=0))).total_seconds()\
                                    % (24 * 3600))
                                break
                            previous_t = t
                    elif 'mission_time' in rconf:
                        seconds_till_restart = int(rconf['mission_time'] * 60 - mission_time)

            if server.settings['password']:
                password = server.settings['password']
            server_data = {
                "server_name": server_name,
                "data": {
                    "server_display_name": server_display_name,
                    "server_status": server_status,
                    "current_map": current_map,
                    "mission_date": mission_date,
                    "active_players": active_players,
                    "max_players": max_players,
                    "ip_addr": ip_addr,
                    "current_mission": current_mission,
                    "mission_time": mission_time,
                    "password": password,
                    "server_maintenance": server_maintenance,
                    "seconds_till_restart": seconds_till_restart
                },
                "weather": weather
            }
            servers_data.append(server_data)

        # convert the list to a JSON array
        servers_json = json.dumps(servers_data)

        # return the JSON array as a string
        return Response(content=servers_json, media_type="application/json")


    def getuser(self, nick: str = Form(default=None)):
        with self.pool.connection() as conn:
            with closing(conn.cursor(row_factory=dict_row)) as cursor:
                return cursor.execute("""
                    SELECT name AS \"nick\", last_seen AS \"date\" FROM players WHERE name ILIKE %s ORDER BY 2 DESC
                """, ('%' + nick + '%', )).fetchall()

    def missilepk(self, nick: str = Form(default=None), date: str = Form(default=None)):
        with self.pool.connection() as conn:
            with closing(conn.cursor(row_factory=dict_row)) as cursor:
                return {
                    "missilePK": dict([(row['weapon'], row['pk']) for row in cursor.execute("""
                        SELECT weapon, shots, hits, 
                               ROUND(CASE WHEN shots = 0 THEN 0 ELSE hits/shots::DECIMAL END, 2) AS "pk"
                        FROM (
                            SELECT weapon, SUM(CASE WHEN event='S_EVENT_SHOT' THEN 1 ELSE 0 END) AS "shots", 
                                   SUM(CASE WHEN event='S_EVENT_HIT' THEN 1 ELSE 0 END) AS "hits" 
                            FROM missionstats 
                            WHERE init_id = (SELECT ucid FROM players WHERE name = %s AND last_seen = %s)
                            AND weapon IS NOT NULL
                            GROUP BY weapon
                        ) x
                        ORDER BY 4 DESC
                    """, (nick, datetime.fromisoformat(date))).fetchall()])
                }

    def stats(self, nick: str = Form(default=None), date: str = Form(default=None)):
        with self.pool.connection() as conn:
            with closing(conn.cursor(row_factory=dict_row)) as cursor:
                ucid = cursor.execute("SELECT ucid FROM players WHERE name = %s AND last_seen = %s",
                                      (nick, datetime.fromisoformat(date))).fetchone()['ucid']
                data = cursor.execute("""
                    SELECT overall.deaths, overall.aakills, 
                           ROUND(CASE WHEN overall.deaths = 0 
                                      THEN overall.aakills 
                                      ELSE overall.aakills/overall.deaths::DECIMAL END, 2) AS "aakdr", 
                           lastsession.kills AS "lastSessionKills", lastsession.deaths AS "lastSessionDeaths"
                    FROM (
                        SELECT SUM(deaths) AS "deaths", SUM(pvp) AS "aakills"
                        FROM statistics
                        WHERE player_ucid = %s
                    ) overall, (
                        SELECT SUM(pvp) AS "kills", SUM(deaths) AS "deaths"
                        FROM statistics
                        WHERE (player_ucid, mission_id) = (
                            SELECT player_ucid, max(mission_id) FROM statistics WHERE player_ucid = %s GROUP BY 1
                        )
                    ) lastsession
                """, (ucid, ucid)).fetchone()
                data['killsByModule'] = cursor.execute("""
                    SELECT slot AS "module", SUM(pvp) AS "kills" 
                    FROM statistics 
                    WHERE player_ucid = %s 
                    GROUP BY 1 HAVING SUM(pvp) > 1 
                    ORDER BY 2 DESC
                """, (ucid, )).fetchall()
                data['kdrByModule'] = cursor.execute("""
                    SELECT slot AS "module", 
                           CASE WHEN SUM(deaths) = 0 THEN SUM(pvp) ELSE SUM(pvp) / SUM(deaths::DECIMAL) END AS "kdr" 
                    FROM statistics 
                    WHERE player_ucid = %s 
                    GROUP BY 1 HAVING SUM(pvp) > 1 
                    ORDER BY 2 DESC
                """, (ucid, )).fetchall()
                return data


async def setup(bot: DCSServerBot):
    global app

    if not os.path.exists('config/plugins/restapi.yaml'):
        bot.log.info('No restapi.yaml found, copying the sample.')
        shutil.copyfile('config/samples/plugins/restapi.yaml', 'config/plugins/restapi.yaml')
    app = FastAPI()
    restapi = RestAPI(bot)
    await bot.add_cog(restapi)
    app.include_router(restapi.router)
