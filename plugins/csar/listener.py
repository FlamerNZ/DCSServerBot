import json
import time

from psycopg.rows import dict_row
from contextlib import closing

from core import EventListener, Server, event, chat_command, Player


class CsarEventListener(EventListener):
    """
    A class where your DCS events will be handled.

    Methods
    -------
    registerDCSServer(data)
        Called on registration of any DCS server.

    sample(data)
        Called whenever ".sample" is called in discord (see commands.py).
    """

    def get_csar_wounded(self) -> list[dict]:
        with self.pool.connection() as conn:
            with closing(conn.cursor(row_factory=dict_row)) as cursor:
                return list(cursor.execute("""
                    SELECT id, coalition, country, pos, coordinates, typename, unitname, playername, freq FROM csar_wounded
                """).fetchall())

    # @event(name="registerDCSServer")
    # async def registerDCSServer(self, server: Server, data: dict) -> None:
    #     self.log.debug(f"CSAR: I've received a registration event from server {server.name}!")

    @event(name="csarStatData")
    async def csarStatData(self, server: Server, data: dict):
        stats = json.loads(data['data'])
        if stats:
            playernames = stats.keys()
            with self.pool.connection() as conn:
                with conn.transaction():
                    for playername in playernames:
                        ucid, name = self.bot.get_ucid_by_name(playername)
                        if ucid is None:
                            ucid = -1
                            name = playername
                        timestamps = stats[playername].keys()
                        for ts in timestamps:
                            conn.execute("""
                                INSERT INTO csar_events (ts, playername, ucid, savedpilots, helicopterused) 
                                VALUES (%s, %s, %s, %s, %s)
                            """, (ts, name, ucid, stats[playername][ts]['savedPilots'], stats[playername][ts]['helicopterUsed']))
        return data

    @event(name="csarSavePersistentData")
    async def savePersistentData(self, server: Server, data: dict):
        wounded = json.loads(data['data'])
        currentids = ""
        # {"coalition":"blue","coordinates":"120.1, 241.0","unitname":"csar 1-1","country":"usa","playername":"Agaarin","freq":"4201","typename":"MI-8MTV2","pos":"120.1, 241.0"}
        with self.pool.connection() as conn:
            with conn.transaction():
                for w in wounded:
                    currentids += "'" + w['id'] + "', "
                    playername = ""
                    if 'playername' in w:
                        playername = w['playername']
                    else:
                        playername = w['unitname']
                    row = conn.execute("""
                        SELECT id FROM csar_wounded WHERE id = %s
                        """,(w['id'], )).fetchone()
                    if row:
                        conn.execute("""
                            UPDATE csar_wounded SET (coalition, country, pos, coordinates, typename, unitname, playername, freq) = (%s, %s, %s, %s, %s, %s, %s, %s)
                            WHERE id = %s
                            """, (w['coalition'], w['country'], json.dumps(w['pos']), w['coordinates'], w['typename'], w['unitname'], playername, w['freq'], w['id']))
                    else:
                        conn.execute("""
                            INSERT INTO csar_wounded (id, coalition, country, pos, coordinates, typename, unitname, playername, freq) 
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """, (w['id'], w['coalition'], w['country'], json.dumps(w['pos']), w['coordinates'], w['typename'], w['unitname'], playername, w['freq']))
                if currentids:
                    delete = "DELETE FROM csar_wounded WHERE id NOT IN (" + currentids.strip(', ') + ")"
                else:
                     delete = "DELETE FROM csar_wounded"
                conn.execute(delete)
        return data

    @event(name="csarGetPersistentData")
    async def getPersistentData(self, server: Server, data: dict):
        data = self.get_csar_wounded()
        if not data:
            self.log.debug(f"CSAR: No wounded pilots in database")
            return
        i = 0
        concat = ""
        for row in data:
            if concat == "":
                concat = "[" + json.dumps(row)
            else:
                concat += ", " + json.dumps(row)
            i += 1
            if i >= 5:
                concat += "]"
                server.send_to_dcs({
                    'command': 'csarUpdatePersistentData',
                    'data': concat
                })
                concat = ""
                i = 0
                time.sleep(1)
        if concat != "":
            concat += "]"
            server.send_to_dcs({
                'command': 'csarUpdatePersistentData',
                'data': concat
            })
        return

    @chat_command(name="csar", roles=['DCS Admin'], help="A sample command")
    async def csar(self, server: Server, player: Player, params: list[str]):
        player.sendChatMessage("This is a csar command!")

    @event(name="rescuedPilot")
    async def rescuedPilot(self, server: Server, data: dict):
        self.log.debug(f"CSAR: A pilot was rescued!")
        playername = data['playername']
        unittype = data['unittype']
        pilotname = data['pilotname']
        ucid, name = self.bot.get_ucid_by_name(playername)
        player: Player = server.get_player(id=ucid)
        if player:
            player.sendChatMessage("You have been rescued by {} flying a {}".format(pilotname, unittype))