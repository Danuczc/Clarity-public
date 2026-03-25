import os
import asyncio
import traceback
from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN = os.getenv("DISCORD_BOT_TOKEN")
if not BOT_TOKEN:
    raise RuntimeError("Token not set.")

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DB URL not set.")

SYNC_GUILD_ID = os.getenv("SYNC_GUILD_ID")
SYNC_COMMANDS = os.getenv("SYNC_COMMANDS", "true").lower() != "false"
ELO_BANNER_URL = os.getenv("ELO_BANNER_URL", "")
DISABLE_CHALLENGE_COOLDOWN = os.getenv("DISABLE_CHALLENGE_COOLDOWN", "").lower() in ("1", "true", "yes")

import discord
from discord import app_commands
from discord.ext import commands

from utils.db import init_pool, init_db, get_db, return_db, get_open_matches

from views.shared_views import (
    RefSignupView, DodgeMatchView, ScheduleProposalView,
    NoShowView, LeaderboardView
)
from views.league_dashboard import LeagueDashboardView

from utils.helpers import log_command_use


class LoggingCommandTree(app_commands.CommandTree):

    async def call(self, interaction: discord.Interaction):
        await super().call(interaction)
        asyncio.create_task(log_command_use(interaction))


intents = discord.Intents.default()
intents.members = True
intents.message_content = True

EXTENSIONS = [
    "cogs.admin",
    "cogs.teams",
    "cogs.matches",
    "cogs.elo",
    "cogs.league",
    "cogs.cooldowns",
    "cogs.audit",
    "tasks.lifecycle",
]


class ClarityBot(commands.Bot):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._commands_synced = False

    async def setup_hook(self):
        for ext in EXTENSIONS:
            try:
                await self.load_extension(ext)
                print(f"[SETUP] Loaded extension: {ext}")
            except Exception as e:
                print(f"[SETUP ERROR] Failed to load {ext}: {e}")
                traceback.print_exc()

        if not self._commands_synced and SYNC_COMMANDS:
            self._commands_synced = True
            try:
                if SYNC_GUILD_ID:
                    guild_id = int(SYNC_GUILD_ID)
                    guild = discord.Object(id=guild_id)
                    self.tree.copy_global_to(guild=guild)
                    synced = await self.tree.sync(guild=guild)
                    print(f"[SYNC] Synced {len(synced)} commands to guild {guild_id}")
                else:
                    synced = await self.tree.sync()
                    print(f"[SYNC] Synced {len(synced)} global commands")

                for cmd in self.tree.get_commands():
                    if cmd.name in ("bo3", "bo5"):
                        param_names = [p.name for p in cmd.parameters]
                        print(f"[SYNC] /{cmd.name} registered with {len(cmd.parameters)} options: {param_names}")

            except Exception as e:
                print(f"[SYNC ERROR] Failed to sync commands: {e}")


bot = ClarityBot(
    command_prefix="!",
    intents=intents,
    tree_cls=LoggingCommandTree
)


@bot.event
async def on_ready():
    print(f"Clarity Bot logged in as {bot.user}")
    if DISABLE_CHALLENGE_COOLDOWN:
        print("[TEST] Challenge cooldown disabled")
    init_db()

    # Re-register persistent views
    try:
        for match in get_open_matches():
            if match.get("ref_signup_message_id"):
                bot.add_view(RefSignupView(match["match_id"]))
            if match.get("dodge_allowed") and match.get("status") == "OPEN":
                bot.add_view(DodgeMatchView(match["match_id"], match["challenged_team_role_id"]))
            if match.get("pending_schedule_message_id") and match.get("pending_schedule_by_team_role_id"):
                proposing_team = match["pending_schedule_by_team_role_id"]
                if proposing_team == match["team1_role_id"]:
                    responding_team = match["team2_role_id"]
                else:
                    responding_team = match["team1_role_id"]
                bot.add_view(ScheduleProposalView(
                    match_id=match["match_id"],
                    challenger_team_role_id=match["challenger_team_role_id"],
                    challenged_team_role_id=responding_team
                ))
    except Exception as e:
        print(f"[STARTUP ERROR] Failed to re-register match views: {e}")

    try:
        conn = get_db()
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT id, match_id FROM no_shows WHERE status IN ('PENDING', 'CONFIRMED')")
                pending_no_shows = cursor.fetchall()
        except Exception:
            conn.rollback()
            raise
        finally:
            return_db(conn)

        for ns in pending_no_shows:
            bot.add_view(NoShowView(ns["id"], ns["match_id"]))
    except Exception as e:
        print(f"[STARTUP ERROR] Failed to re-register no-show views: {e}")

    try:
        bot.add_view(LeaderboardView(current_page=0))
    except Exception as e:
        print(f"[STARTUP ERROR] Failed to re-register leaderboard view: {e}")

    try:
        bot.add_view(LeagueDashboardView())
    except Exception as e:
        print(f"[STARTUP ERROR] Failed to re-register league dashboard view: {e}")


if __name__ == "__main__":
    init_pool()
    bot.run(BOT_TOKEN)
