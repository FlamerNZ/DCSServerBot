import discord
import psycopg

from contextlib import closing
from core import Plugin, utils, Server, TEventListener, Status, command, DEFAULT_TAG, Report, ReportEnv
from discord import app_commands
from services import DCSServerBot
from typing import Type
from discord.ext import tasks
from typing import Optional, Union
from psycopg.rows import dict_row

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
        self.expire_after = self.locals.get(DEFAULT_TAG, {}).get('expire_after')
        self.prune.add_exception_type(psycopg.DatabaseError)
        self.prune.start()
        # Do whatever is needed to initialize your plugin.
        # You usually don't need to implement this function.

    def rename(self, conn: psycopg.Connection, old_name: str, new_name: str):
        # If a server rename takes place, you might want to update data in your created tables
        # if they contain a server_name value. You usually don't need to implement this function.
        pass

    @command(description='Shows your CSAR stats')
    @app_commands.guild_only()
    @utils.app_has_role('DCS')
    async def csar(self, interaction: discord.Interaction,
                     user: Optional[app_commands.Transform[Union[str, discord.Member], utils.UserTransformer]]):
        if not user:
            user = interaction.user
        if isinstance(user, str):
            ucid = user
            user = self.bot.get_member_or_name_by_ucid(ucid)
            if isinstance(user, discord.Member):
                name = user.display_name
            else:
                name = user
        else:
            ucid = self.bot.get_ucid_by_member(user)
            name = user.display_name
        
        report = Report(self.bot, self.plugin_name, 'rescues.json')
        env = await report.render(name=name, ucid=ucid)  # params={"rescues": rescues}
        await interaction.response.send_message(embed=env.embed)

    @tasks.loop(hours=1.0)
    async def prune(self):
        if self.expire_after:
            self.log.debug('CSAR: Pruning aged CSARS from DB')
            with self.pool.connection() as conn:
                with conn.transaction():
                    conn.execute("""
                        DELETE FROM csar_wounded
                        WHERE datestamp < NOW() - INTERVAL %s
                    """, (self.expire_after))

async def setup(bot: DCSServerBot):
    await bot.add_cog(Csar(bot, CsarEventListener))
