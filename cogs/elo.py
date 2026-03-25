import traceback
from datetime import datetime

import discord
from discord import app_commands
from discord.ext import commands

from utils.db import (
    get_db, return_db, get_config, get_team, update_team,
    get_all_teams, get_roster, get_vice_captains
)
from utils.helpers import (
    EMBED_COLOR, utc_now, safe_defer, coerce_dt,
    post_transaction, update_leaderboard
)
from utils.permissions import is_elo_staff, is_team_staff
from views.shared_views import MatchHistoryPaginationView


class EloCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @app_commands.command(name="set-elo", description="Manually set a team's ELO (Elo Perms only)")
    @app_commands.describe(
        team_role="The team",
        new_elo="New ELO value",
        reason="Reason for adjustment (required, min 5 chars)"
    )
    async def set_elo_cmd(self, interaction: discord.Interaction, team_role: discord.Role, new_elo: int, reason: str):
        await safe_defer(interaction, ephemeral=True)

        try:
            if not is_elo_staff(interaction.user):
                await interaction.followup.send(content="Only Elo Perms staff can use this command.")
                return
            if len(reason.strip()) < 5:
                await interaction.followup.send(content="Reason must be at least 5 characters long.")
                return

            team_role_id = team_role.id
            team = get_team(team_role_id)
            if not team:
                await interaction.followup.send(content="That team is not registered.")
                return

            old_elo = team["elo"]

            # Update team Elo
            update_team(team_role_id, elo=new_elo)

            conn = get_db()
            try:
                cursor = conn.cursor()
                cursor.execute(
                    """INSERT INTO elo_adjustments
                       (team_role_id, old_elo, new_elo, staff_user_id, reason)
                       VALUES (%s, %s, %s, %s, %s)""",
                    (team_role_id, old_elo, new_elo, interaction.user.id, reason.strip())
                )
                conn.commit()
                cursor.close()
            finally:
                return_db(conn)

            team_discord_role = interaction.guild.get_role(team_role_id)
            team_name = team_discord_role.name if team_discord_role else "Unknown Team"

            delta = new_elo - old_elo
            delta_str = f"{delta:+d}" 

            adjustment_embed = discord.Embed(
                title="Elo Adjustment",
                color=0x00FFFF,  
                timestamp=discord.utils.utcnow()
            )

            adjustment_embed.add_field(
                name="Team",
                value=team_discord_role.mention if team_discord_role else team_name,
                inline=False
            )

            adjustment_embed.add_field(
                name="Change",
                value=f"{old_elo} → {new_elo} ({delta_str})",
                inline=False
            )

            adjustment_embed.add_field(
                name="Reason",
                value=reason.strip(),
                inline=False
            )

            adjustment_embed.add_field(
                name="Staff",
                value=interaction.user.mention,
                inline=False
            )

            adjustment_embed.set_footer(text="Adjustment logged")

            config = get_config()
            transaction_channel_id = config.get("transaction_channel_id")
            if transaction_channel_id:
                transaction_channel = interaction.guild.get_channel(transaction_channel_id)
                if transaction_channel:
                    try:
                        await transaction_channel.send(embed=adjustment_embed)
                    except Exception as e:
                        print(f"[SET-ELO] Failed to post to transaction channel: {e}")
                else:
                    await interaction.followup.send(
                        content="Transaction channel not configured or not found.",
                        ephemeral=True
                    )
            else:
                await interaction.followup.send(
                    content="Transaction channel not configured.",
                    ephemeral=True
                )

            await update_leaderboard(interaction.guild)

            await interaction.followup.send(
                content=f"ELO for {team_discord_role.mention if team_discord_role else 'the team'} adjusted to {new_elo} (was {old_elo})."
            )
        except Exception as e:
            await interaction.followup.send(content=f"Error setting ELO: {str(e)[:200]}")

    @app_commands.command(name="team-stats", description="View detailed team statistics (Team Perms only)")
    @app_commands.describe(team_role="The team to view stats for")
    async def team_stats_cmd(self, interaction: discord.Interaction, team_role: discord.Role):
        await safe_defer(interaction, ephemeral=True)

        try:
            if not is_team_staff(interaction.user):
                await interaction.followup.send(content="Only Team Perms staff can use this command.")
                return

            team_role_id = team_role.id
            team = get_team(team_role_id)

            if not team:
                await interaction.followup.send(content="That team is not registered.")
                return

            team_discord_role = interaction.guild.get_role(team_role_id)
            team_name = team_discord_role.name if team_discord_role else "Unknown"

            roster = get_roster(team_role_id)
            captain_id = team["captain_user_id"]
            vice_captains = get_vice_captains(team_role_id)

            # Match history
            conn = get_db()
            try:
                cursor = conn.cursor()
                cursor.execute(
                    """SELECT m.match_id, m.team1_role_id, m.team2_role_id, m.team1_score, m.team2_score,
                              m.status, m.scheduled_time_utc, m.finished_at_utc, m.created_at_utc
                       FROM matches m
                       WHERE (m.team1_role_id = %s OR m.team2_role_id = %s)
                         AND m.status = 'FINISHED'
                       ORDER BY m.finished_at_utc DESC
                       LIMIT 50""",
                    (team_role_id, team_role_id)
                )
                matches = cursor.fetchall()

                cursor.execute(
                    """SELECT m.match_id, m.team1_role_id, m.team2_role_id, m.scheduled_time_utc
                       FROM matches m
                       WHERE (m.team1_role_id = %s OR m.team2_role_id = %s)
                         AND m.status IN ('OPEN', 'SCHEDULED')
                       ORDER BY m.created_at_utc ASC
                       LIMIT 1""",
                    (team_role_id, team_role_id)
                )
                upcoming = cursor.fetchone()

                cursor.close()
            finally:
                return_db(conn)

            wins = 0
            losses = 0
            elo_history = []  
            opponent_counts = {}  
            results_sequence = []  

            for match in matches:
                m_id, t1_id, t2_id, t1_score, t2_score, status, sched, finished, created = match

                opponent_id = t2_id if t1_id == team_role_id else t1_id

                opponent_counts[opponent_id] = opponent_counts.get(opponent_id, 0) + 1

                if t1_id == team_role_id:
                    won = t1_score > t2_score
                else:
                    won = t2_score > t1_score

                if won:
                    wins += 1
                    results_sequence.append("W")
                else:
                    losses += 1
                    results_sequence.append("L")

            results_sequence.reverse()

            current_streak = 0
            streak_type = None
            if results_sequence:
                streak_type = results_sequence[-1]  
                for result in reversed(results_sequence):
                    if result == streak_type:
                        current_streak += 1
                    else:
                        break

            sparkline = ""
            if len(results_sequence) >= 10:
                recent_10 = results_sequence[-10:]
                for r in recent_10:
                    sparkline += "▲" if r == "W" else "▼"
            else:
                sparkline = "Insufficient data"

            favorite_opponents = []
            if opponent_counts:
                sorted_opponents = sorted(opponent_counts.items(), key=lambda x: x[1], reverse=True)
                for opp_id, count in sorted_opponents[:3]:
                    opp_role = interaction.guild.get_role(opp_id)
                    opp_name = opp_role.name if opp_role else "Unknown"
                    favorite_opponents.append(f"{opp_name} ({count}x)")

            recent_form = "".join(results_sequence[-5:]) if results_sequence else "N/A"

            last_active = None
            if matches:
                last_match = matches[0]  
                if last_match["finished_at_utc"]:
                    last_active = coerce_dt(last_match["finished_at_utc"])

            embed = discord.Embed(
                title=f"Team Statistics: {team_name}",
                color=EMBED_COLOR
            )

            embed.add_field(
                name="Current Elo",
                value=f"**{team['elo']}**",
                inline=True
            )
            embed.add_field(
                name="Record",
                value=f"**{wins}W - {losses}L**",
                inline=True
            )

            if team.get('no_show_count', 0) > 0:
                embed.add_field(
                    name="No-Shows",
                    value=f"**{team['no_show_count']}**",
                    inline=True
                )

            embed.add_field(
                name="Elo Trend (Last 10)",
                value=f"`{sparkline}`",
                inline=False
            )

            if current_streak > 0:
                embed.add_field(
                    name="Current Streak",
                    value=f"**{current_streak}{streak_type}**",
                    inline=True
                )

            embed.add_field(
                name="Recent Form (L5)",
                value=f"**{recent_form}**",
                inline=True
            )

            if favorite_opponents:
                embed.add_field(
                    name="Most Played",
                    value="\n".join(favorite_opponents),
                    inline=True
                )

            if upcoming:
                opp_id = upcoming["team2_role_id"] if upcoming["team1_role_id"] == team_role_id else upcoming["team1_role_id"]
                opp_role = interaction.guild.get_role(opp_id)
                opp_name = opp_role.name if opp_role else "Unknown"
                upcoming_text = f"vs {opp_name}"
                if upcoming["scheduled_time_utc"]:
                    sched_dt = coerce_dt(upcoming["scheduled_time_utc"])
                    unix_ts = int(sched_dt.timestamp())
                    upcoming_text += f"\n<t:{unix_ts}:R>"
                embed.add_field(
                    name="Next Match",
                    value=upcoming_text,
                    inline=False
                )

            if last_active:
                days_ago = (utc_now() - last_active).days
                embed.add_field(
                    name="Last Active",
                    value=f"{days_ago} days ago",
                    inline=True
                )

            roster_text = ""
            for member_data in roster[:10]:  
                user_id = member_data["user_id"]
                joined = member_data.get("joined_at_utc")
                if user_id == captain_id:
                    roster_text += f"[C] <@{user_id}>"
                elif user_id in vice_captains:
                    roster_text += f"[VC] <@{user_id}>"
                else:
                    roster_text += f"• <@{user_id}>"

                if joined:
                    joined_dt = coerce_dt(joined)
                    days_ago = (utc_now() - joined_dt).days
                    roster_text += f" _(joined {days_ago}d ago)_"
                roster_text += "\n"

            if len(roster) > 10:
                roster_text += f"... and {len(roster) - 10} more"

            embed.add_field(
                name=f"Roster ({len(roster)} members)",
                value=roster_text if roster_text else "No members",
                inline=False
            )

            if matches:
                history_text = ""
                for match in matches[:5]:  
                    m_id, t1_id, t2_id, t1_score, t2_score, status, sched, finished, created = match

                    opponent_id = t2_id if t1_id == team_role_id else t1_id
                    opponent_role = interaction.guild.get_role(opponent_id)
                    opponent_name = opponent_role.name if opponent_role else "Unknown"

                    if t1_id == team_role_id:
                        result = "W" if t1_score > t2_score else "L"
                        score = f"{t1_score}-{t2_score}"
                    else:
                        result = "W" if t2_score > t1_score else "L"
                        score = f"{t2_score}-{t1_score}"

                    if finished:
                        finished_dt = coerce_dt(finished)
                        days_ago = (utc_now() - finished_dt).days
                        date_str = f"{days_ago}d ago"
                    else:
                        date_str = "Unknown"

                    history_text += f"{result} vs {opponent_name} ({score}) - _{date_str}_\n"

                if len(matches) > 5:
                    history_text += f"\n_Use buttons below to view all {len(matches)} matches_"

                embed.add_field(
                    name="Recent Matches",
                    value=history_text,
                    inline=False
                )
            else:
                embed.add_field(
                    name="Recent Matches",
                    value="No matches played yet",
                    inline=False
                )

            if team.get("created_at_utc"):
                created_dt = coerce_dt(team["created_at_utc"])
                unix_timestamp = int(created_dt.timestamp())
                embed.set_footer(text=f"Team created on")
                embed.timestamp = created_dt

            view = None
            if len(matches) > 5:
                view = MatchHistoryPaginationView(team_role_id, interaction.guild)

            await interaction.followup.send(embed=embed, view=view)

        except Exception as e:
            print(f"[TEAM-STATS ERROR] team_role={team_role}, guild={interaction.guild.id}")
            print(traceback.format_exc())
            await interaction.followup.send(content="Failed to retrieve team stats. Please try again.")


async def setup(bot: commands.Bot):
    await bot.add_cog(EloCog(bot))
