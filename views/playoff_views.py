import traceback
from datetime import timedelta

import discord

from utils.db import (
    get_db, return_db, get_team, get_league_state,
    check_duplicate_playoff_matchup,
    award_ref_activity_for_match
)
from utils.helpers import (
    EMBED_COLOR, utc_now, coerce_dt
)

class CreatePlayoffMatchModal(discord.ui.Modal, title="Create Playoff Match"):

    bracket = discord.ui.TextInput(
        label="Bracket (WINNERS or LOSERS)",
        placeholder="WINNERS or LOSERS",
        required=True,
        max_length=10
    )

    team1_id = discord.ui.TextInput(
        label="Team 1 Role ID",
        placeholder="Team 1 Discord role ID",
        required=True,
        max_length=20
    )

    team2_id = discord.ui.TextInput(
        label="Team 2 Role ID",
        placeholder="Team 2 Discord role ID",
        required=True,
        max_length=20
    )

    round_num = discord.ui.TextInput(
        label="Round Number",
        placeholder="e.g., 1, 2, 3",
        required=True,
        max_length=3
    )

    def __init__(self, guild: discord.Guild):
        super().__init__()
        self.guild = guild

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True, thinking=True)

        try:
            bracket_val = self.bracket.value.strip().upper()
            team1_id_val = int(self.team1_id.value)
            team2_id_val = int(self.team2_id.value)
            round_num_val = int(self.round_num.value)

            if bracket_val not in ["WINNERS", "LOSERS"]:
                await interaction.followup.send(content="Bracket must be WINNERS or LOSERS.", ephemeral=True)
                return

            team1 = self.guild.get_role(team1_id_val)
            team2 = self.guild.get_role(team2_id_val)

            if not team1 or not team2:
                await interaction.followup.send(content="One or both teams not found.", ephemeral=True)
                return

            if team1.id == team2.id:
                await interaction.followup.send(content="Teams must be different.", ephemeral=True)
                return

            team1_data = get_team(team1.id)
            team2_data = get_team(team2.id)

            if not team1_data or not team2_data:
                await interaction.followup.send(content="One or both teams not registered.", ephemeral=True)
                return

            state = get_league_state()
            conn = get_db()
            try:
                cursor = conn.cursor()

                cursor.execute("""
                    SELECT MAX(league_round) as max_round FROM matches
                    WHERE mode = 'LEAGUE' AND bracket = 'WINNERS'
                """)
                result = cursor.fetchone()
                max_winners_round = result["max_round"] if result and result["max_round"] else 0

                if check_duplicate_playoff_matchup(team1.id, team2.id, bracket_val, round_num_val):
                    embed = discord.Embed(
                        title="Duplicate Playoff Match Blocked",
                        description=f"These teams already have a match in this bracket/round.",
                        color=discord.Color.red()
                    )
                    embed.add_field(name="Bracket", value=bracket_val, inline=True)
                    embed.add_field(name="Round", value=round_num_val, inline=True)
                    embed.add_field(name="Teams", value=f"{team1.mention} vs {team2.mention}", inline=False)
                    await interaction.followup.send(embed=embed, ephemeral=True)
                    cursor.close()
                    return

                if bracket_val == "LOSERS" and round_num_val == max_winners_round:
                    series_format = "BO5"  
                elif bracket_val == "WINNERS" and round_num_val == max_winners_round + 1:
                    series_format = "BO5"  
                else:
                    series_format = "BO3"

                if state.get("league_deadline_utc"):
                    deadline = coerce_dt(state["league_deadline_utc"])
                else:
                    await interaction.followup.send(
                        content="No global league deadline set.",
                        ephemeral=True
                    )
                    cursor.close()
                    return

                cursor.execute("""
                    INSERT INTO matches (
                        team1_role_id, team2_role_id,
                        challenger_team_role_id, challenged_team_role_id,
                        team1_elo_locked, team2_elo_locked, elo_diff_locked,
                        mode, bracket, league_round, series_format, deadline_utc,
                        dodge_allowed, status, created_at_utc
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING match_id
                """, (
                    team1.id, team2.id, team1.id, team2.id,
                    team1_data["elo"], team2_data["elo"], abs(team1_data["elo"] - team2_data["elo"]),
                    'LEAGUE', bracket_val, round_num_val, series_format, deadline,
                    False, 'OPEN', utc_now()
                ))

                result = cursor.fetchone()
                match_id = result["match_id"]
                conn.commit()
                cursor.close()

            finally:
                return_db(conn)

            await interaction.followup.send(
                content=f"**Playoff Match Created** (ID: {match_id})\n"
                        f"{team1.mention} vs {team2.mention}\n"
                        f"{bracket_val} • Round {round_num_val} • {series_format}",
                ephemeral=True
            )

            from views.league_dashboard import update_league_dashboard, log_league_action
            await update_league_dashboard(self.guild)

            await log_league_action(
                self.guild,
                "Playoff Match Created",
                interaction.user,
                {
                    "Match ID": match_id,
                    "Teams": f"{team1.mention} vs {team2.mention}",
                    "Bracket": bracket_val,
                    "Round": round_num_val,
                    "Format": series_format
                }
            )

        except ValueError:
            await interaction.followup.send(content="Invalid number format.", ephemeral=True)
        except Exception as e:
            print(f"[CREATE PLAYOFF MATCH ERROR] {e}")
            print(traceback.format_exc())
            await interaction.followup.send(content="An error occurred.", ephemeral=True)
