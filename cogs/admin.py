import traceback

import discord
from discord import app_commands
from discord.ext import commands

from utils.db import get_config, update_config, get_db, return_db, get_team, get_all_teams, invalidate_config_cache, get_user_captain_teams, get_user_vice_captain_teams, get_player_team, remove_roster_member, remove_vice_captain, update_team
from utils.helpers import EMBED_COLOR, utc_now, safe_defer, update_leaderboard, build_leaderboard_embed, coerce_dt, post_transaction
from utils.permissions import is_team_staff, is_elo_staff
from views.shared_views import EphemeralLeaderboardView


class AdminCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @app_commands.command(name="setup", description="Configure bot roles and channels (Administrator only)")
    @app_commands.describe(
        leaderboard_channel="Channel for leaderboard",
        elo_updates_channel="Channel for ELO updates",
        match_category="Category for match channels",
        transaction_channel="Channel for transaction logs",
        referee_channel="Channel for ref signups",
        logs_channel="Channel for command/error logs",
        ref_role="Referee role",
        head_of_refs_role="Head of Referees role",
        team_perms_role="Team Perms staff role",
        elo_perms_role="ELO Perms staff role",
        league_perms_role="League Perms staff role (full league control)",
        captain_role="Shared Captain role",
        vice_captain_role="Shared Vice Captain role",
        suspended_role="Suspended role"
    )
    async def setup_cmd(
        self,
        interaction: discord.Interaction,
        leaderboard_channel: discord.TextChannel = None,
        elo_updates_channel: discord.TextChannel = None,
        match_category: discord.CategoryChannel = None,
        transaction_channel: discord.TextChannel = None,
        referee_channel: discord.TextChannel = None,
        logs_channel: discord.TextChannel = None,
        ref_role: discord.Role = None,
        head_of_refs_role: discord.Role = None,
        team_perms_role: discord.Role = None,
        elo_perms_role: discord.Role = None,
        league_perms_role: discord.Role = None,
        captain_role: discord.Role = None,
        vice_captain_role: discord.Role = None,
        suspended_role: discord.Role = None
    ):
        await safe_defer(interaction, ephemeral=True)

        try:
            if not interaction.user.guild_permissions.administrator:
                await interaction.followup.send(content="You must have Administrator permissions to use /setup.")
                return

            updates = {}
            if leaderboard_channel:
                updates["leaderboard_channel_id"] = leaderboard_channel.id
            if elo_updates_channel:
                updates["elo_updates_channel_id"] = elo_updates_channel.id
            if match_category:
                updates["match_category_id"] = match_category.id
            if transaction_channel:
                updates["transaction_channel_id"] = transaction_channel.id
            if referee_channel:
                updates["referee_channel_id"] = referee_channel.id
            if logs_channel:
                updates["logs_channel_id"] = logs_channel.id
            if ref_role:
                updates["ref_role_id"] = ref_role.id
            if head_of_refs_role:
                updates["head_of_refs_role_id"] = head_of_refs_role.id
            if team_perms_role:
                updates["team_perms_role_id"] = team_perms_role.id
            if elo_perms_role:
                updates["elo_perms_role_id"] = elo_perms_role.id
            if league_perms_role:
                updates["league_perms_role_id"] = league_perms_role.id
            if captain_role:
                updates["captain_role_id"] = captain_role.id
            if vice_captain_role:
                updates["vice_captain_role_id"] = vice_captain_role.id
            if suspended_role:
                updates["suspended_role_id"] = suspended_role.id

            if updates:
                update_config(**updates)
                await interaction.followup.send(content=f"Configuration updated: {', '.join(updates.keys())}")
            else:
                await interaction.followup.send(content="No changes specified.")
        except Exception as e:
            await interaction.followup.send(content=f"Error: {str(e)[:200]}")

    @app_commands.command(name="leaderboard", description="View the team leaderboard")
    async def leaderboard_cmd(self, interaction: discord.Interaction):
        await safe_defer(interaction, ephemeral=True)

        try:
            embed, total_pages = build_leaderboard_embed(0)
            view = EphemeralLeaderboardView(user_id=interaction.user.id, current_page=0)
            await interaction.followup.send(embed=embed, view=view, ephemeral=True)
        except Exception as e:
            await interaction.followup.send(
                content=f"Error loading leaderboard: {str(e)[:200]}",
                ephemeral=True
            )

    @app_commands.command(name="admin-dashboard", description="View league statistics (Staff only)")
    async def admin_dashboard_cmd(self, interaction: discord.Interaction):
        await safe_defer(interaction, ephemeral=True)

        try:
            # Permission check
            if not (is_team_staff(interaction.user) or is_elo_staff(interaction.user)):
                await interaction.followup.send(content="Only staff can use this command.", ephemeral=True)
                return

            conn = get_db()
            try:
                cursor = conn.cursor()

                # Pending matches count
                cursor.execute("SELECT COUNT(*) as cnt FROM matches WHERE status IN ('OPEN', 'SCHEDULED')")
                pending_matches = cursor.fetchone()["cnt"]

                # Average match duration 
                cursor.execute("""
                    SELECT AVG(EXTRACT(EPOCH FROM (finished_at_utc - created_at_utc))/3600) as avg_hours
                    FROM matches
                    WHERE status = 'FINISHED'
                      AND created_at_utc IS NOT NULL
                      AND finished_at_utc IS NOT NULL
                """)
                avg_result = cursor.fetchone()
                avg_duration = avg_result["avg_hours"] if avg_result and avg_result["avg_hours"] else None

                # Busiest match hours 
                cursor.execute("""
                    SELECT EXTRACT(HOUR FROM created_at_utc) as hour, COUNT(*) as cnt
                    FROM matches
                    WHERE created_at_utc IS NOT NULL
                    GROUP BY hour
                    ORDER BY cnt DESC
                    LIMIT 3
                """)
                busiest_hours = cursor.fetchall()

                # Referee leaderboard 
                cursor.execute("""
                    SELECT ref_user_id, COUNT(*) as claims, MAX(awarded_at_utc) as last_active
                    FROM ref_activity_awards
                    WHERE awarded_at_utc >= NOW() - INTERVAL '30 days'
                    GROUP BY ref_user_id
                    ORDER BY claims DESC
                    LIMIT 10
                """)
                ref_leaders = cursor.fetchall()

                # Inactive teams 
                cursor.execute("""
                    SELECT t.team_role_id
                    FROM teams t
                    WHERE NOT EXISTS (
                        SELECT 1 FROM matches m
                        WHERE (m.team1_role_id = t.team_role_id OR m.team2_role_id = t.team_role_id)
                          AND m.created_at_utc >= NOW() - INTERVAL '14 days'
                    )
                """)
                inactive_teams = [r["team_role_id"] for r in cursor.fetchall()]

                # High forfeit rate 
                cursor.execute("""
                    SELECT team_role_id, COUNT(*) as forfeits
                    FROM forfeit_events
                    WHERE created_at_utc >= NOW() - INTERVAL '7 days'
                    GROUP BY team_role_id
                    HAVING COUNT(*) >= 3
                """)
                high_forfeit_teams = cursor.fetchall()

                cursor.close()
            finally:
                return_db(conn)

            # Build embed
            embed = discord.Embed(
                title="Admin Dashboard",
                description="Real-time league statistics",
                color=0x00FFFF,
                timestamp=utc_now()
            )

            # Pending matches
            embed.add_field(
                name="Pending Matches",
                value=f"{pending_matches} matches (OPEN + SCHEDULED)",
                inline=False
            )

            # Average match duration
            if avg_duration:
                embed.add_field(
                    name="Average Match Duration",
                    value=f"{avg_duration:.1f} hours",
                    inline=False
                )
            else:
                embed.add_field(
                    name="Average Match Duration",
                    value="N/A (no finished matches with timestamps)",
                    inline=False
                )

            # Busiest hours
            if busiest_hours:
                hours_text = "\n".join([
                    f"Hour {int(h['hour'])}:00 UTC - {h['cnt']} matches"
                    for h in busiest_hours
                ])
                embed.add_field(
                    name="Busiest Match Hours (Top 3)",
                    value=hours_text or "No data",
                    inline=False
                )

            # Referee leaderboard
            if ref_leaders:
                ref_text = "\n".join([
                    f"<@{r['ref_user_id']}>: {r['claims']} claims (last: <t:{int(coerce_dt(r['last_active']).timestamp())}:R>)"
                    for r in ref_leaders[:5]  # Show top 5
                ])
                embed.add_field(
                    name="Top Referees (Last 30 Days)",
                    value=ref_text,
                    inline=False
                )
            else:
                embed.add_field(
                    name="Top Referees (Last 30 Days)",
                    value="No referee activity",
                    inline=False
                )

            # Teams needing attention
            attention_text = []
            if inactive_teams:
                attention_text.append(f"**{len(inactive_teams)} inactive teams** (no matches in 14 days)")
            if high_forfeit_teams:
                attention_text.append(f"**{len(high_forfeit_teams)} teams** with high forfeit rate (3+ in 7 days)")

            if attention_text:
                embed.add_field(
                    name="Teams Needing Attention",
                    value="\n".join(attention_text),
                    inline=False
                )
            else:
                embed.add_field(
                    name="Teams Status",
                    value="No teams need attention",
                    inline=False
                )

            embed.set_footer(text=f"Dashboard - {interaction.guild.name}")

            await interaction.followup.send(embed=embed, ephemeral=True)

        except Exception as e:
            print(f"[ADMIN-DASHBOARD ERROR] {e}")
            print(traceback.format_exc())
            await interaction.followup.send(content=f"Dashboard error: {str(e)[:200]}", ephemeral=True)

    @app_commands.command(name="reload-config", description="Reload bot configuration from database (Staff only)")
    async def reload_config_cmd(self, interaction: discord.Interaction):
        await safe_defer(interaction, ephemeral=True)

        try:
            # Permission check
            if not (is_team_staff(interaction.user) or is_elo_staff(interaction.user)):
                await interaction.followup.send(content="Only staff can use this command.", ephemeral=True)
                return

            # Invalidate cache
            invalidate_config_cache()

            # Force reload
            new_config = get_config()

            await interaction.followup.send(
                content="**Configuration reloaded successfully.**\n"
                        f"Cached at <t:{int(utc_now().timestamp())}:T>",
                ephemeral=True
            )

        except Exception as e:
            print(f"[RELOAD-CONFIG ERROR] {e}")
            await interaction.followup.send(content=f"Reload failed: {str(e)[:200]}", ephemeral=True)

    @app_commands.command(name="suspend", description="Suspend a user (Team Perms only)")
    @app_commands.describe(user="The user to suspend", reason="Optional reason for suspension")
    async def suspend_cmd(self, interaction: discord.Interaction, user: discord.Member, reason: str = None):
        await safe_defer(interaction, ephemeral=True)

        try:
            if not is_team_staff(interaction.user):
                await interaction.followup.send(content="Only Team Perms staff can use this command.")
                return

            config = get_config()
            suspended_role_id = config.get("suspended_role_id")

            if not suspended_role_id:
                await interaction.followup.send(content="Suspended role not configured.")
                return

            suspended_role = interaction.guild.get_role(suspended_role_id)
            if not suspended_role:
                await interaction.followup.send(content="Suspended role not found.")
                return

            # Check for suspension 
            if suspended_role in user.roles:
                await interaction.followup.send(content=f"{user.mention} is already suspended.")
                return

            # CAPTAIN LOCK: 
            captain_teams = get_user_captain_teams(user.id)
            if captain_teams:
                team_mentions = []
                for team_role_id in captain_teams:
                    team_role = interaction.guild.get_role(team_role_id)
                    if team_role:
                        team_mentions.append(team_role.mention)
                teams_str = ", ".join(team_mentions) if team_mentions else "their team(s)"
                await interaction.followup.send(
                    content=f"You cannot remove the captain from their team. Transfer ownership with /set-captain or disband the team.\n\n{user.mention} is currently captain of: {teams_str}",
                    ephemeral=True
                )
                return

            captain_role_id = config.get("captain_role_id")
            vice_captain_role_id = config.get("vice_captain_role_id")

            # Get user's team affiliation
            vice_captain_teams = get_user_vice_captain_teams(user.id)
            player_team = get_player_team(user.id)

            # Collect all team roles to remove
            team_roles_to_remove = set()

            # Captain removal
            if captain_teams:
                for team_role_id in captain_teams:
                    team_discord_role = interaction.guild.get_role(team_role_id)
                    if team_discord_role:
                        team_roles_to_remove.add(team_discord_role)
                    # Remove roster entry if they are on the roster
                    if get_player_team(user.id) == team_role_id:
                        remove_roster_member(team_role_id, user.id)
                    # Set captain to None in DB
                    update_team(team_role_id, captain_user_id=None)

            # Handle vice captain removal
            if vice_captain_teams:
                for team_role_id in vice_captain_teams:
                    team_discord_role = interaction.guild.get_role(team_role_id)
                    if team_discord_role:
                        team_roles_to_remove.add(team_discord_role)
                    # Remove from vice_captains table
                    remove_vice_captain(team_role_id, user.id)

            # Handle roster removal 
            if player_team and player_team not in captain_teams:
                team_discord_role = interaction.guild.get_role(player_team)
                if team_discord_role:
                    team_roles_to_remove.add(team_discord_role)
                remove_roster_member(player_team, user.id)

            # Build list of roles to remove
            roles_to_remove = [suspended_role]  
            actual_roles_to_remove = list(team_roles_to_remove)

            # Remove captain role if user is no longer captain of any team
            if captain_role_id and captain_teams:
                captain_discord_role = interaction.guild.get_role(captain_role_id)
                if captain_discord_role and captain_discord_role in user.roles:
                    # Check if they're still captain of other teams (they shouldn't be after we cleared them)
                    remaining_captain_teams = get_user_captain_teams(user.id)
                    if not remaining_captain_teams:
                        actual_roles_to_remove.append(captain_discord_role)

            # Remove vice captain role 
            if vice_captain_role_id and vice_captain_teams:
                vc_discord_role = interaction.guild.get_role(vice_captain_role_id)
                if vc_discord_role and vc_discord_role in user.roles:
                    remaining_vc_teams = get_user_vice_captain_teams(user.id)
                    if not remaining_vc_teams:
                        actual_roles_to_remove.append(vc_discord_role)

            # Apply role changes
            try:
                # Add suspended role
                await user.add_roles(suspended_role)

                # Remove team-related roles
                if actual_roles_to_remove:
                    await user.remove_roles(*actual_roles_to_remove)
            except discord.Forbidden:
                await interaction.followup.send(content="I don't have permission to modify roles.")
                return

            # Post to transaction channel
            details = {"User": user.mention}
            if reason:
                details["Reason"] = reason
            await post_transaction(
                guild=interaction.guild,
                action="User Suspended",
                staff=interaction.user,
                details=details
            )

            await interaction.followup.send(content=f"{user.mention} has been suspended.")
        except Exception as e:
            await interaction.followup.send(content=f"Error suspending user: {str(e)[:200]}")

    @app_commands.command(name="unsuspend", description="Remove suspension from a user (Team Perms only)")
    @app_commands.describe(user="The user to unsuspend")
    async def unsuspend_cmd(self, interaction: discord.Interaction, user: discord.Member):
        await safe_defer(interaction, ephemeral=False)

        try:
            if not is_team_staff(interaction.user):
                await interaction.followup.send(content="Only Team Perms staff can use this command.")
                return

            config = get_config()
            suspended_role_id = config.get("suspended_role_id")

            if not suspended_role_id:
                await interaction.followup.send(content="Suspended role not configured.")
                return

            suspended_role = interaction.guild.get_role(suspended_role_id)
            if not suspended_role:
                await interaction.followup.send(content="Suspended role not found.")
                return

            if suspended_role not in user.roles:
                await interaction.followup.send(content=f"{user.mention} is not suspended.")
                return
            await user.remove_roles(suspended_role)
            await interaction.followup.send(content=f"{user.mention} has been unsuspended.")
        except discord.Forbidden:
            await interaction.followup.send(content="I don't have permission to remove the suspended role.")
        except Exception as e:
            await interaction.followup.send(content=f"Error unsuspending user: {str(e)[:200]}")


async def setup(bot: commands.Bot):
    await bot.add_cog(AdminCog(bot))
