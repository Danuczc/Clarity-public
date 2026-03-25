import traceback
from datetime import datetime, timedelta

import discord
from discord import app_commands
from discord.ext import commands

from utils.db import (
    get_db, return_db, get_config, get_team,
    get_cooldown, set_cooldown, clear_expired_cooldowns
)
from utils.helpers import EMBED_COLOR, utc_now, safe_defer, team_autocomplete
from utils.permissions import is_team_staff


class CooldownsCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @app_commands.command(name="cooldown", description="Set global matchup cooldown duration (Team Perms only)")
    @app_commands.describe(hours="Cooldown duration in hours (0 to disable, max 168)")
    async def cooldown_cmd(self, interaction: discord.Interaction, hours: int):
        """Set the global cooldown duration for matchups after Elo-updating matches."""
        await safe_defer(interaction, ephemeral=True)

        try:
            if not is_team_staff(interaction.user):
                await interaction.followup.send(
                    content="Only Team Perms staff can use this command.",
                    ephemeral=True
                )
                return

            # Validate hours
            if hours < 0:
                await interaction.followup.send(
                    content="Hours must be 0 or greater.",
                    ephemeral=True
                )
                return

            if hours > 168:  # 1 week max
                await interaction.followup.send(
                    content="Hours cannot exceed 168 (1 week).",
                    ephemeral=True
                )
                return

            # Update config
            from utils.db import update_config
            update_config(cooldown_hours=hours)

            if hours == 0:
                await interaction.followup.send(
                    content="Matchup cooldowns **disabled**. Teams can rematch immediately.",
                    ephemeral=True
                )
            else:
                await interaction.followup.send(
                    content=f"Cooldown duration set to **{hours} hours**.\n"
                            f"This applies to future /bo3 and /bo5 matches.",
                    ephemeral=True
                )

        except Exception as e:
            print(f"[COOLDOWN CMD ERROR] Failed to set cooldown: {e}")
            print(traceback.format_exc())
            await interaction.followup.send(
                content="Failed to update cooldown settings.",
                ephemeral=True
            )

    @app_commands.command(name="remove-cooldown", description="Remove cooldowns for matchups (Team Perms only)")
    @app_commands.describe(
        all="Remove ALL matchup cooldowns",
        team1="First team (required if not using all)",
        team2="Second team (required if not using all)"
    )
    async def remove_cooldown_cmd(
        self,
        interaction: discord.Interaction,
        all: bool = False,
        team1: discord.Role = None,
        team2: discord.Role = None
    ):
        """Remove cooldowns either for all matchups or a specific team matchup."""
        await safe_defer(interaction, ephemeral=True)

        try:
            if not is_team_staff(interaction.user):
                await interaction.followup.send(
                    content="Only Team Perms staff can use this command.",
                    ephemeral=True
                )
                return

            conn = get_db()
            try:
                with conn.cursor() as cur:
                    if all:
                        cur.execute("DELETE FROM challenge_cooldowns")
                        deleted_count = cur.rowcount
                        conn.commit()

                        await interaction.followup.send(
                            content=f"Removed cooldowns for **ALL matchups** ({deleted_count} total).",
                            ephemeral=True
                        )

                    elif team1 and team2:
                        team1_id = team1.id
                        team2_id = team2.id

                        if not get_team(team1_id):
                            await interaction.followup.send(
                                content=f"{team1.mention} is not a registered team.",
                                ephemeral=True
                            )
                            return

                        if not get_team(team2_id):
                            await interaction.followup.send(
                                content=f"{team2.mention} is not a registered team.",
                                ephemeral=True
                            )
                            return

                        cur.execute(
                            """DELETE FROM challenge_cooldowns
                               WHERE (challenger_team_role_id = %s AND challenged_team_role_id = %s)
                                  OR (challenger_team_role_id = %s AND challenged_team_role_id = %s)""",
                            (team1_id, team2_id, team2_id, team1_id)
                        )
                        deleted_count = cur.rowcount
                        conn.commit()

                        if deleted_count > 0:
                            await interaction.followup.send(
                                content=f"Removed cooldown for **{team1.mention}** vs **{team2.mention}**.",
                                ephemeral=True
                            )
                        else:
                            await interaction.followup.send(
                                content=f"No active cooldown found for **{team1.mention}** vs **{team2.mention}**.",
                                ephemeral=True
                            )

                    else:
                        await interaction.followup.send(
                            content="Provide either `all=True` OR both `team1` and `team2`.",
                            ephemeral=True
                        )
                        return

            finally:
                return_db(conn)

        except Exception as e:
            print(f"[REMOVE-COOLDOWN CMD ERROR] Failed: {e}")
            print(traceback.format_exc())
            await interaction.followup.send(
                content="Failed to remove cooldowns.",
                ephemeral=True
            )

    @app_commands.command(name="set-cooldown", description="Manage challenge cooldowns (Team Perms only)")
    @app_commands.describe(
        team1="First team",
        team2="Second team",
        action="Clear cooldown or set custom duration",
        hours="Custom cooldown duration in hours (default: 24)"
    )
    @app_commands.choices(action=[
        app_commands.Choice(name="Clear Cooldown", value="clear"),
        app_commands.Choice(name="Set Custom Cooldown", value="set")
    ])
    async def set_cooldown_cmd(
        self,
        interaction: discord.Interaction,
        team1: discord.Role,
        team2: discord.Role,
        action: str,
        hours: int = 24
    ):
        """
        Manage challenge cooldowns for tournament mode or special events.
        Allows staff to clear cooldowns or set custom durations.
        """
        await safe_defer(interaction, ephemeral=False)

        try:
            if not is_team_staff(interaction.user):
                await interaction.followup.send(content="Only Team Perms staff can use this command.")
                return

            # Validate teams
            team1_data = get_team(team1.id)
            team2_data = get_team(team2.id)

            if not team1_data:
                await interaction.followup.send(content="First team is not registered.")
                return
            if not team2_data:
                await interaction.followup.send(content="Second team is not registered.")
                return

            if team1.id == team2.id:
                await interaction.followup.send(content="Teams must be different.")
                return

            if action == "clear":
                # Clear cooldown
                conn = get_db()
                try:
                    cursor = conn.cursor()
                    cursor.execute(
                        "DELETE FROM challenge_cooldowns WHERE (challenger_team_role_id = %s AND challenged_team_role_id = %s) OR (challenger_team_role_id = %s AND challenged_team_role_id = %s)",
                        (team1.id, team2.id, team2.id, team1.id)
                    )
                    rows_deleted = cursor.rowcount
                    conn.commit()
                    cursor.close()
                finally:
                    return_db(conn)

                # Transaction channel
                from utils.helpers import post_transaction
                await post_transaction(
                    guild=interaction.guild,
                    action="Cooldown Cleared",
                    staff=interaction.user,
                    details={
                        "Teams": f"{team1.mention} vs {team2.mention}"
                    }
                )

                if rows_deleted > 0:
                    await interaction.followup.send(
                        content=f"Cooldown cleared between {team1.mention} and {team2.mention}."
                    )
                else:
                    await interaction.followup.send(
                        content=f"No active cooldown existed between {team1.mention} and {team2.mention}."
                    )

            elif action == "set":
                if hours < 1 or hours > 168:  # Max 7 days
                    await interaction.followup.send(content="Hours must be between 1 and 168 (7 days).")
                    return

                # Set custom cooldown
                expires_at = utc_now() + timedelta(hours=hours)
                set_cooldown(team1.id, team2.id, expires_at)
                set_cooldown(team2.id, team1.id, expires_at)

                # Transaction channel
                from utils.helpers import post_transaction
                unix_timestamp = int(expires_at.timestamp())
                await post_transaction(
                    guild=interaction.guild,
                    action="Cooldown Set",
                    staff=interaction.user,
                    details={
                        "Teams": f"{team1.mention} vs {team2.mention}",
                        "Duration": f"{hours} hours",
                        "Expires": f"<t:{unix_timestamp}:R>"
                    }
                )

                await interaction.followup.send(
                    content=f"{hours}-hour cooldown set between {team1.mention} and {team2.mention}.\n"
                            f"Expires: <t:{unix_timestamp}:R>"
                )

        except Exception as e:
            print(f"[SET_COOLDOWN ERROR] {e}")
            print(traceback.format_exc())
            await interaction.followup.send(content=f"Error managing cooldown: {str(e)[:200]}")


async def setup(bot: commands.Bot):
    await bot.add_cog(CooldownsCog(bot))
