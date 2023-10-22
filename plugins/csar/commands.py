import discord
import psycopg

from core import Plugin, utils, Server, TEventListener, Status, command, DEFAULT_TAG
from discord import app_commands
from services import DCSServerBot
from typing import Type
from discord.ext import tasks

from .listener import CsarEventListener


class Csar(Plugin):
    """
    A class where all your discord commands should go.

    If you need a specific initialization, make sure that you call super().__init__() after it, to
    assure a proper initialization of the plugin.

    Attributes
    ----------
    :param bot: DCSServerBot
        The discord bot instance.
    :param listener: EventListener
        A listener class to receive events from DCS.

    Methods
    -------
    sample(ctx, text)
        Send the text to DCS, which will return the same text again (echo).
    """

    def __init__(self, bot: DCSServerBot, listener: Type[TEventListener]):
        super().__init__(bot, listener)
        self.csar_config = self.locals.get(DEFAULT_TAG, {}).get('expire_after')
        self.prune.add_exception_type(psycopg.DatabaseError)
        self.prune.start()
        # Do whatever is needed to initialize your plugin.
        # You usually don't need to implement this function.

    def rename(self, conn: psycopg.Connection, old_name: str, new_name: str):
        # If a server rename takes place, you might want to update data in your created tables
        # if they contain a server_name value. You usually don't need to implement this function.
        pass

    @command(description='This is a csar command.')
    @app_commands.guild_only()
    @utils.app_has_role('DCS')
    async def csar(self, interaction: discord.Interaction,
                     server: app_commands.Transform[Server, utils.ServerTransformer(status=[
                         Status.RUNNING, Status.PAUSED, Status.STOPPED])
                     ], text: str):
        await interaction.response.defer(thinking=True)
        # Calls can be done async (default) or synchronous, which means we will wait for a response from DCS
        data = await server.send_to_dcs_sync({
            "command": "csar",    # command name
            "message": text         # the message to transfer
        })
        await interaction.followup.send(f"Response: {data['message']}")

    @tasks.loop(hours=1.0)
    async def prune(self):
        if self.csar_config:
            self.log.debug('CSAR: Pruning aged CSARS from DB')
            with self.pool.connection() as conn:
                with conn.transaction():
                    for d in self.csar_config:
                        conn.execute("""
                            DELETE FROM csar_wounded
                            WHERE datestamp < now() - INTERVAL %s
                        """, (d.expire_after))

async def setup(bot: DCSServerBot):
    await bot.add_cog(Csar(bot, CsarEventListener))
