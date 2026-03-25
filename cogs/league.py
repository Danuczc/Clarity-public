import traceback
from datetime import datetime, timedelta

import discord
from discord import app_commands
from discord.ext import commands

from utils.db import (
    get_db, return_db, get_config, get_team, get_all_teams,
    get_league_state, update_league_state,
    create_league_group, add_team_to_group,
    get_group_standings, get_group_teams,
    update_league_standings, replace_team_in_group,
    check_duplicate_league_matchup, check_duplicate_playoff_matchup,
    get_match, update_match, award_ref_activity_for_match
)
from utils.helpers import (
    EMBED_COLOR, utc_now, coerce_dt, safe_defer,
    post_transaction, team_autocomplete
)
from utils.permissions import (
    has_league_perms, check_league_perms, is_team_staff, is_elo_staff
)
from views.league_dashboard import update_league_dashboard, log_league_action
from views.shared_views import CaptainPanelView



def validate_match_allowed(mode: str) -> tuple[bool, str]:
    state = get_league_state()
    if mode == "LEAGUE":
        if not state.get("season_active"):
            return False, "League season is not active."
        if state.get("season_locked"):
            return False, "League season is locked."
    return True, ""


def validate_playoff_format(bracket: str, round_num: int) -> str:
    if bracket == "WINNERS":
        return "BO3" if round_num <= 2 else "BO5"
    else:  
        return "BO3"


def get_user_team_authority(user_id: int, team_role_id: int) -> str:
    """Get user's authority level for a team."""
    conn = get_db()
    try:
        cursor = conn.cursor()

        cursor.execute("SELECT 1 FROM teams WHERE captain_user_id = %s AND team_role_id = %s",
                      (user_id, team_role_id))
        if cursor.fetchone():
            cursor.close()
            return "Captain"

        cursor.execute("SELECT 1 FROM vice_captains WHERE user_id = %s AND team_role_id = %s",
                      (user_id, team_role_id))
        if cursor.fetchone():
            cursor.close()
            return "Vice Captain"

        cursor.execute("SELECT 1 FROM roster WHERE user_id = %s AND team_role_id = %s",
                      (user_id, team_role_id))
        if cursor.fetchone():
            cursor.close()
            return "Roster"

        cursor.close()
        return "Unknown"
    finally:
        return_db(conn)


async def process_match_outcome(
    interaction: discord.Interaction,
    guild: discord.Guild,
    match: dict,
    winner_id: int,
    set_scores: list,
    fmt: str,
    ref_user: discord.User
):
    match_id = match["match_id"]

    update_match(match_id, status="FINISHED")

    if match["mode"] == "LEAGUE":
        team1_wins = sum(1 for _, _, w in set_scores if w == "team1")
        team2_wins = sum(1 for _, _, w in set_scores if w == "team2")

        group_id = match.get("group_id")
        if group_id:
            update_league_standings(
                group_id,
                match["team1_role_id"],
                match["team2_role_id"],
                team1_wins,
                team2_wins
            )


class LeagueCog(commands.Cog):
    """Cog for league management commands."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @app_commands.command(name="captain-panel", description="Open quick actions panel for team captains")
    async def captain_panel_cmd(self, interaction: discord.Interaction):
        await safe_defer(interaction, ephemeral=True)

        try:
            user_id = interaction.user.id

            conn = get_db()
            try:
                cursor = conn.cursor()

                cursor.execute("SELECT team_role_id FROM teams WHERE captain_user_id = %s LIMIT 1", (user_id,))
                team_row = cursor.fetchone()

                if team_row:
                    team_role_id = team_row["team_role_id"]
                else:
                    cursor.execute("SELECT team_role_id FROM vice_captains WHERE user_id = %s LIMIT 1", (user_id,))
                    vc_row = cursor.fetchone()

                    if vc_row:
                        team_role_id = vc_row["team_role_id"]
                    else:
                        cursor.execute("SELECT team_role_id FROM roster WHERE user_id = %s LIMIT 1", (user_id,))
                        roster_row = cursor.fetchone()

                        if roster_row:
                            team_role_id = roster_row["team_role_id"]
                        else:
                            await interaction.followup.send(content="You are not affiliated with any team.")
                            cursor.close()
                            return

                cursor.close()
            finally:
                return_db(conn)

            team = get_team(team_role_id)
            if not team:
                await interaction.followup.send(content="Team not found.")
                return

            team_role = interaction.guild.get_role(team_role_id)
            team_name = team_role.name if team_role else "Unknown"
            authority = get_user_team_authority(user_id, team_role_id)

            view = CaptainPanelView(team_role_id, user_id)

            await interaction.followup.send(
                content=f"**Captain Panel: {team_name}**\nYour role: {authority}",
                view=view,
                ephemeral=True
            )

        except Exception as e:
            print(f"[CAPTAIN-PANEL ERROR] {e}")
            print(traceback.format_exc())
            await interaction.followup.send(content="An error occurred.", ephemeral=True)

    @app_commands.command(name="league-setup", description="Initialize League Dashboard (League Perms only)")
    @app_commands.describe(
        dashboard_channel="Channel for the league dashboard",
        season_name="Name of the season (e.g., 'Spring 2026')"
    )
    async def league_setup_cmd(
        self,
        interaction: discord.Interaction,
        dashboard_channel: discord.TextChannel,
        season_name: str
    ):
        await safe_defer(interaction, ephemeral=True)

        try:
            if not await check_league_perms(interaction):
                return

            update_league_state(
                dashboard_channel_id=dashboard_channel.id,
                season_name=season_name,
                season_active=False,  
                season_locked=False,
                current_stage=None,
                current_round=0,
                roster_lock_enabled=False
            )

            await update_league_dashboard(interaction.guild)

            await interaction.followup.send(
                content=f"League dashboard initialized in {dashboard_channel.mention}\n"
                        f"Season: **{season_name}**\n"
                        f"State: **OFF-SEASON**",
                ephemeral=True
            )

        except Exception as e:
            print(f"[LEAGUE-SETUP ERROR] {e}")
            print(traceback.format_exc())
            await interaction.followup.send(content="An error occurred.", ephemeral=True)

    @app_commands.command(name="league-set-deadline", description="Set global league deadline (League Perms only)")
    @app_commands.describe(
        days="Days from now",
        hours="Hours from now (added to days)"
    )
    async def league_set_deadline_cmd(
        self,
        interaction: discord.Interaction,
        days: int = 0,
        hours: int = 0
    ):
        """Set the global league deadline."""
        await safe_defer(interaction, ephemeral=True)

        try:
            if not await check_league_perms(interaction):
                return

            if days == 0 and hours == 0:
                await interaction.followup.send(content="Specify at least some time (days or hours).", ephemeral=True)
                return

            deadline = utc_now() + timedelta(days=days, hours=hours)
            update_league_state(league_deadline_utc=deadline)

            unix_ts = int(deadline.timestamp())

            await interaction.followup.send(
                content=f"Global league deadline set to: <t:{unix_ts}:F> (<t:{unix_ts}:R>)",
                ephemeral=True
            )

            await update_league_dashboard(interaction.guild)

            state = get_league_state()
            dashboard_channel = interaction.guild.get_channel(state["dashboard_channel_id"])
            if dashboard_channel:
                await dashboard_channel.send(
                    content=f"**Global Deadline Set** by {interaction.user.mention}\n"
                            f"Deadline: <t:{unix_ts}:F>"
                )

        except Exception as e:
            print(f"[LEAGUE-SET-DEADLINE ERROR] {e}")
            print(traceback.format_exc())
            await interaction.followup.send(content="An error occurred.", ephemeral=True)

    @app_commands.command(name="league-match", description="Create a league match (League Perms only)")
    @app_commands.describe(
        team1="First team",
        team2="Second team",
        group_id="Group ID",
        round_num="Round number",
        deadline_hours="Deadline in hours from now (optional, uses global deadline if not set)"
    )
    async def league_match_cmd(
        self,
        interaction: discord.Interaction,
        team1: discord.Role,
        team2: discord.Role,
        group_id: int,
        round_num: int,
        deadline_hours: int = None
    ):
        """Create a league match with group, round, and deadline."""
        await safe_defer(interaction, ephemeral=False)

        try:

            if not await check_league_perms(interaction):
                return

            allowed, error_msg = validate_match_allowed("LEAGUE")
            if not allowed:
                await interaction.followup.send(content=error_msg, ephemeral=True)
                return

            if team1.id == team2.id:
                await interaction.followup.send(content="Teams must be different.", ephemeral=True)
                return

            team1_data = get_team(team1.id)
            team2_data = get_team(team2.id)

            if not team1_data:
                await interaction.followup.send(content="Team 1 is not registered.", ephemeral=True)
                return
            if not team2_data:
                await interaction.followup.send(content="Team 2 is not registered.", ephemeral=True)
                return

            conn = get_db()
            try:
                cursor = conn.cursor()
                cursor.execute("SELECT stage_id FROM league_groups WHERE group_id = %s", (group_id,))
                group = cursor.fetchone()
                cursor.close()
            finally:
                return_db(conn)

            if not group:
                await interaction.followup.send(content="Group not found.", ephemeral=True)
                return

            stage_id = group["stage_id"]

            group_teams = get_group_teams(group_id)
            if team1.id not in group_teams:
                await interaction.followup.send(content="Team 1 is not in this group.", ephemeral=True)
                return
            if team2.id not in group_teams:
                await interaction.followup.send(content="Team 2 is not in this group.", ephemeral=True)
                return

            if check_duplicate_league_matchup(team1.id, team2.id, stage_id, round_num):
                await interaction.followup.send(
                    content="These teams already have a match in this stage/round.",
                    ephemeral=True
                )
                return

            if deadline_hours is not None:
                deadline = utc_now() + timedelta(hours=deadline_hours)
            else:
                state = get_league_state()
                if state.get("league_deadline_utc"):
                    deadline = coerce_dt(state["league_deadline_utc"])
                else:
                    await interaction.followup.send(
                        content="No global league deadline is set. Either:\n"
                                "• Set a global deadline with `/league-set-deadline`\n"
                                "• Provide `deadline_hours` parameter for this match",
                        ephemeral=True
                    )
                    return

            conn = get_db()
            try:
                cursor = conn.cursor()
                cursor.execute(
                    """INSERT INTO matches (
                        team1_role_id, team2_role_id,
                        challenger_team_role_id, challenged_team_role_id,
                        team1_elo_locked, team2_elo_locked, elo_diff_locked,
                        mode, group_id, league_round, deadline_utc,
                        dodge_allowed, status, created_at_utc
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING match_id""",
                    (
                        team1.id, team2.id,
                        team1.id, team2.id,  
                        team1_data["elo"], team2_data["elo"], 0,
                        "LEAGUE", group_id, round_num, deadline,
                        False,  
                        "OPEN", utc_now()
                    )
                )
                match_id = cursor.fetchone()["match_id"]
                conn.commit()
                cursor.close()
            finally:
                return_db(conn)

            unix_ts = int(deadline.timestamp())
            await interaction.followup.send(
                content=f"**League Match Created**\n"
                        f"**Match #{match_id}**\n"
                        f"{team1.mention} vs {team2.mention}\n"
                        f"Group: **{group_id}** | Round: **{round_num}**\n"
                        f"Deadline: <t:{unix_ts}:F> (<t:{unix_ts}:R>)\n\n"
                        f"_BO3 format, no dodge allowed_",
                ephemeral=False
            )

        except Exception as e:
            print(f"[LEAGUE-MATCH ERROR] {e}")
            print(traceback.format_exc())
            await interaction.followup.send(content="An error occurred.", ephemeral=True)

    @app_commands.command(name="league-create-group", description="Create a league group (League Perms only)")
    @app_commands.describe(
        group_name="Name of the group (e.g., 'Group A')",
        stage_id="Stage identifier (e.g., 'GROUPS', 'PLAYOFFS')"
    )
    async def league_create_group_cmd(
        self,
        interaction: discord.Interaction,
        group_name: str,
        stage_id: str
    ):
        await safe_defer(interaction, ephemeral=True)

        try:
            if not await check_league_perms(interaction):
                return

            group_id = create_league_group(group_name, stage_id)

            await interaction.followup.send(
                content=f"Group created: **{group_name}** (ID: {group_id})\nStage: **{stage_id}**",
                ephemeral=True
            )

        except Exception as e:
            print(f"[CREATE-GROUP ERROR] {e}")
            print(traceback.format_exc())
            await interaction.followup.send(content="An error occurred.", ephemeral=True)

    @app_commands.command(name="league-add-team", description="Add a team to a league group (League Perms only)")
    @app_commands.describe(
        group_id="Group ID",
        team="Team to add"
    )
    async def league_add_team_cmd(
        self,
        interaction: discord.Interaction,
        group_id: int,
        team: discord.Role
    ):
        """Add a team to a league group."""
        await safe_defer(interaction, ephemeral=True)

        try:
            if not await check_league_perms(interaction):
                return

            team_data = get_team(team.id)
            if not team_data:
                await interaction.followup.send(content="Team is not registered.", ephemeral=True)
                return

            add_team_to_group(group_id, team.id)

            await interaction.followup.send(
                content=f"{team.mention} added to Group {group_id}.",
                ephemeral=True
            )

        except Exception as e:
            print(f"[ADD-TEAM ERROR] {e}")
            print(traceback.format_exc())
            await interaction.followup.send(content="An error occurred.", ephemeral=True)

    @app_commands.command(name="league-standings", description="View standings for a league group")
    @app_commands.describe(group_id="Group ID")
    async def league_standings_cmd(
        self,
        interaction: discord.Interaction,
        group_id: int
    ):
        """View standings for a league group."""
        await safe_defer(interaction, ephemeral=True)

        try:
            standings = get_group_standings(group_id)

            if not standings:
                await interaction.followup.send(content="No standings found for this group.", ephemeral=True)
                return

            output = f"**Group {group_id} Standings**\n\n"
            output += "```\n"
            output += f"{'Rank':<6}{'Team':<20}{'W':<4}{'L':<4}{'Diff':<6}\n"
            output += "-" * 40 + "\n"

            for idx, standing in enumerate(standings, 1):
                team_role = interaction.guild.get_role(standing["team_role_id"])
                team_name = team_role.name if team_role else "Unknown"
                team_name = team_name[:18]  # Truncate long names

                sets_won = standing["sets_won"]
                sets_lost = standing["sets_lost"]
                set_diff = standing["set_diff"]

                output += f"{idx:<6}{team_name:<20}{sets_won:<4}{sets_lost:<4}{set_diff:+<6}\n"

            output += "```"

            await interaction.followup.send(content=output, ephemeral=True)

        except Exception as e:
            print(f"[STANDINGS ERROR] {e}")
            print(traceback.format_exc())
            await interaction.followup.send(content="An error occurred.", ephemeral=True)

    @app_commands.command(name="league-replace-team", description="Replace a team in a group (League Perms only)")
    @app_commands.describe(
        group_id="Group ID",
        old_team="Team to replace",
        new_team="Replacement team"
    )
    async def league_replace_team_cmd(
        self,
        interaction: discord.Interaction,
        group_id: int,
        old_team: discord.Role,
        new_team: discord.Role
    ):
        await safe_defer(interaction, ephemeral=True)

        try:
            # Permission check
            if not await check_league_perms(interaction):
                return

            new_team_data = get_team(new_team.id)
            if not new_team_data:
                await interaction.followup.send(content="Replacement team is not registered.", ephemeral=True)
                return

            replace_team_in_group(group_id, old_team.id, new_team.id)

            await interaction.followup.send(
                content=f"**Team Replaced in Group {group_id}**\n"
                        f"Old: {old_team.mention}\n"
                        f"New: {new_team.mention}\n\n"
                        f"_Existing results unchanged, new team starts with 0-0-0_",
                ephemeral=True
            )

        except Exception as e:
            print(f"[REPLACE-TEAM ERROR] {e}")
            print(traceback.format_exc())
            await interaction.followup.send(content="An error occurred.", ephemeral=True)

    @app_commands.command(name="league-playoff-match", description="Create a playoff match (League Perms only)")
    @app_commands.describe(
        team1="First team",
        team2="Second team",
        bracket="Bracket (WINNERS or LOSERS)",
        round_num="Round number",
        deadline_hours="Deadline in hours from now (optional, uses global deadline if not set)"
    )
    @app_commands.choices(bracket=[
        app_commands.Choice(name="Winners Bracket", value="WINNERS"),
        app_commands.Choice(name="Losers Bracket", value="LOSERS")
    ])
    async def league_playoff_match_cmd(
        self,
        interaction: discord.Interaction,
        team1: discord.Role,
        team2: discord.Role,
        bracket: app_commands.Choice[str],
        round_num: int,
        deadline_hours: int = None
    ):
        """Create a playoff match with automatic BO3/BO5 enforcement."""
        await safe_defer(interaction, ephemeral=False)

        try:
            if not await check_league_perms(interaction):
                return

            allowed, error_msg = validate_match_allowed("LEAGUE")
            if not allowed:
                await interaction.followup.send(content=error_msg, ephemeral=True)
                return

            state = get_league_state()
            if state.get("current_stage") != "PLAYOFFS":
                await interaction.followup.send(content="Must be in PLAYOFFS stage to create playoff matches.", ephemeral=True)
                return

            if team1.id == team2.id:
                await interaction.followup.send(content="Teams must be different.", ephemeral=True)
                return

            team1_data = get_team(team1.id)
            team2_data = get_team(team2.id)

            if not team1_data:
                await interaction.followup.send(content="Team 1 is not registered.", ephemeral=True)
                return
            if not team2_data:
                await interaction.followup.send(content="Team 2 is not registered.", ephemeral=True)
                return

            bracket_value = bracket.value

            series_format = validate_playoff_format(bracket_value, round_num)

            if deadline_hours is not None:
                deadline = utc_now() + timedelta(hours=deadline_hours)
            else:
                state = get_league_state()
                if state.get("league_deadline_utc"):
                    deadline = coerce_dt(state["league_deadline_utc"])
                else:
                    await interaction.followup.send(
                        content="No global league deadline is set. Either:\n"
                                "• Set a global deadline with `/league-set-deadline`\n"
                                "• Provide `deadline_hours` parameter for this match",
                        ephemeral=True
                    )
                    return

            conn = get_db()
            try:
                cursor = conn.cursor()
                cursor.execute(
                    """INSERT INTO matches (
                        team1_role_id, team2_role_id,
                        challenger_team_role_id, challenged_team_role_id,
                        team1_elo_locked, team2_elo_locked, elo_diff_locked,
                        mode, bracket, league_round, deadline_utc, series_format,
                        dodge_allowed, status, created_at_utc
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING match_id""",
                    (
                        team1.id, team2.id,
                        team1.id, team2.id,  
                        team1_data["elo"], team2_data["elo"], 0,
                        "LEAGUE", bracket_value, round_num, deadline, series_format,
                        False,  
                        "OPEN", utc_now()
                    )
                )
                match_id = cursor.fetchone()["match_id"]
                conn.commit()
                cursor.close()
            finally:
                return_db(conn)

            unix_ts = int(deadline.timestamp())
            await interaction.followup.send(
                content=f"**Playoff Match Created**\n"
                        f"**Match #{match_id}**\n"
                        f"{team1.mention} vs {team2.mention}\n"
                        f"Bracket: **{bracket_value}** | Round: **{round_num}**\n"
                        f"Format: **{series_format}**\n"
                        f"Deadline: <t:{unix_ts}:F> (<t:{unix_ts}:R>)\n\n"
                        f"_No dodge allowed_",
                ephemeral=False
            )

        except Exception as e:
            print(f"[PLAYOFF-MATCH ERROR] {e}")
            print(traceback.format_exc())
            await interaction.followup.send(content="An error occurred.", ephemeral=True)

    @app_commands.command(name="league-force-complete", description="Force complete a league match (League Perms only)")
    @app_commands.describe(
        match_id="Match ID to force complete",
        winner="Winning team",
        set1_team1="Set 1: Team 1 score",
        set1_team2="Set 1: Team 2 score",
        set2_team1="Set 2: Team 1 score",
        set2_team2="Set 2: Team 2 score",
        set3_team1="Set 3: Team 1 score (0 if not played)",
        set3_team2="Set 3: Team 2 score (0 if not played)",
        set4_team1="Set 4: Team 1 score (BO5 only, 0 if not played)",
        set4_team2="Set 4: Team 2 score (BO5 only, 0 if not played)",
        set5_team1="Set 5: Team 1 score (BO5 only, 0 if not played)",
        set5_team2="Set 5: Team 2 score (BO5 only, 0 if not played)"
    )
    @app_commands.choices(winner=[
        app_commands.Choice(name="Team 1", value="team1"),
        app_commands.Choice(name="Team 2", value="team2")
    ])
    async def league_force_complete_cmd(
        self,
        interaction: discord.Interaction,
        match_id: int,
        winner: app_commands.Choice[str],
        set1_team1: int,
        set1_team2: int,
        set2_team1: int,
        set2_team2: int,
        set3_team1: int = 0,
        set3_team2: int = 0,
        set4_team1: int = 0,
        set4_team2: int = 0,
        set5_team1: int = 0,
        set5_team2: int = 0
    ):
        
        await safe_defer(interaction, ephemeral=False)

        try:
            # Permission check
            if not await check_league_perms(interaction):
                return

            match = get_match(match_id)
            if not match:
                await interaction.followup.send(content="Match not found.", ephemeral=True)
                return

            if match.get("mode") != "LEAGUE":
                await interaction.followup.send(content="This command only works for LEAGUE matches.", ephemeral=True)
                return

            if match["status"] == "FINISHED":
                await interaction.followup.send(content="Match is already finished.", ephemeral=True)
                return

            series_format = match.get("series_format", "BO3")

            set_scores = []
            errors = []

            if set1_team1 == 0 and set1_team2 == 0:
                errors.append("Set 1 cannot be skipped (both scores are 0).")
            else:
                set_scores.append((set1_team1, set1_team2, "team1" if set1_team1 > set1_team2 else "team2"))

            if set2_team1 == 0 and set2_team2 == 0:
                errors.append("Set 2 cannot be skipped (both scores are 0).")
            else:
                set_scores.append((set2_team1, set2_team2, "team1" if set2_team1 > set2_team2 else "team2"))

            if series_format == "BO3":
                if not (set3_team1 == 0 and set3_team2 == 0):
                    set_scores.append((set3_team1, set3_team2, "team1" if set3_team1 > set3_team2 else "team2"))

                if not (set4_team1 == 0 and set4_team2 == 0 and set5_team1 == 0 and set5_team2 == 0):
                    errors.append("BO3 matches cannot have sets 4 or 5.")

            elif series_format == "BO5":
                if set3_team1 == 0 and set3_team2 == 0:
                    errors.append("Set 3 cannot be skipped in BO5 (both scores are 0).")
                else:
                    set_scores.append((set3_team1, set3_team2, "team1" if set3_team1 > set3_team2 else "team2"))

                if not (set4_team1 == 0 and set4_team2 == 0):
                    set_scores.append((set4_team1, set4_team2, "team1" if set4_team1 > set4_team2 else "team2"))
                if not (set5_team1 == 0 and set5_team2 == 0):
                    set_scores.append((set5_team1, set5_team2, "team1" if set5_team1 > set5_team2 else "team2"))

            if errors:
                await interaction.followup.send(content="\n".join(errors), ephemeral=True)
                return

            team1_set_wins = sum(1 for _, _, w in set_scores if w == "team1")
            team2_set_wins = sum(1 for _, _, w in set_scores if w == "team2")

            winner_value = winner.value
            if series_format == "BO3":
                if winner_value == "team1" and team1_set_wins < 2:
                    await interaction.followup.send(
                        content=f"Winner is 'team1' but they only won {team1_set_wins} set(s). Need 2 to win BO3.",
                        ephemeral=True
                    )
                    return
                if winner_value == "team2" and team2_set_wins < 2:
                    await interaction.followup.send(
                        content=f"Winner is 'team2' but they only won {team2_set_wins} set(s). Need 2 to win BO3.",
                        ephemeral=True
                    )
                    return
            elif series_format == "BO5":
                if winner_value == "team1" and team1_set_wins < 3:
                    await interaction.followup.send(
                        content=f"Winner is 'team1' but they only won {team1_set_wins} set(s). Need 3 to win BO5.",
                        ephemeral=True
                    )
                    return
                if winner_value == "team2" and team2_set_wins < 3:
                    await interaction.followup.send(
                        content=f"Winner is 'team2' but they only won {team2_set_wins} set(s). Need 3 to win BO5.",
                        ephemeral=True
                    )
                    return

            team1_role_id = match["team1_role_id"]
            team2_role_id = match["team2_role_id"]
            guild = interaction.guild

            await process_match_outcome(
                interaction=interaction,
                guild=guild,
                match=match,
                winner_id=team1_role_id if winner_value == "team1" else team2_role_id,
                set_scores=set_scores,
                fmt=series_format.lower(),
                ref_user=interaction.user
            )

            print(f"[LEAGUE-FORCE] Match {match_id} completed by {interaction.user} ({series_format})")

        except Exception as e:
            print(f"[LEAGUE-FORCE ERROR] {e}")
            print(traceback.format_exc())
            await interaction.followup.send(content="An error occurred.", ephemeral=True)

    @app_commands.command(name="extend-deadline", description="Extend deadline for a specific match (Staff only)")
    @app_commands.describe(
        match_id="Match ID to extend",
        hours="Hours to add to current deadline"
    )
    async def extend_deadline_cmd(
        self,
        interaction: discord.Interaction,
        match_id: int,
        hours: int
    ):

        await safe_defer(interaction, ephemeral=False)

        try:
            if not is_team_staff(interaction.user):
                await interaction.followup.send(content="Only staff can extend match deadlines.", ephemeral=True)
                return

            if hours <= 0:
                await interaction.followup.send(content="Hours must be positive.", ephemeral=True)
                return

            match = get_match(match_id)
            if not match:
                await interaction.followup.send(content="Match not found.", ephemeral=True)
                return

            if not match.get("deadline_utc"):
                await interaction.followup.send(content="This match does not have a deadline set.", ephemeral=True)
                return

            if match["status"] not in ("OPEN", "SCHEDULED"):
                await interaction.followup.send(content="Can only extend deadlines for active matches.", ephemeral=True)
                return

            old_deadline = coerce_dt(match["deadline_utc"])
            new_deadline = old_deadline + timedelta(hours=hours)

            update_match(match_id, deadline_utc=new_deadline)

            team1_role = interaction.guild.get_role(match["team1_role_id"])
            team2_role = interaction.guild.get_role(match["team2_role_id"])
            team1_name = team1_role.mention if team1_role else f"Team {match['team1_role_id']}"
            team2_name = team2_role.mention if team2_role else f"Team {match['team2_role_id']}"

            match_mode = match.get("mode", "ELO")
            mode_icon = "League" if match_mode == "LEAGUE" else "Elo"

            old_ts = int(old_deadline.timestamp())
            new_ts = int(new_deadline.timestamp())

            await interaction.followup.send(
                content=f"{mode_icon} **Deadline Extended**\n"
                        f"**Match #{match_id}:** {team1_name} vs {team2_name}\n"
                        f"**Old Deadline:** <t:{old_ts}:F>\n"
                        f"**New Deadline:** <t:{new_ts}:F> (<t:{new_ts}:R>)\n"
                        f"**Extended by:** {hours} hours\n\n"
                        f"_Extended by {interaction.user.mention}_",
                ephemeral=False
            )

            print(f"[EXTEND-DEADLINE] Match {match_id} deadline extended by {hours}h by {interaction.user}")

        except Exception as e:
            print(f"[EXTEND-DEADLINE ERROR] {e}")
            print(traceback.format_exc())
            await interaction.followup.send(content="An error occurred.", ephemeral=True)

    @app_commands.command(name="league-extend-deadlines", description="Extend all active league match deadlines (League Perms only)")
    @app_commands.describe(hours="Hours to add to all league match deadlines")
    async def league_extend_deadlines_cmd(
        self,
        interaction: discord.Interaction,
        hours: int
    ):

        await safe_defer(interaction, ephemeral=False)

        try:
            if not await check_league_perms(interaction):
                return

            if hours <= 0:
                await interaction.followup.send(content="Hours must be positive.", ephemeral=True)
                return

            conn = get_db()
            try:
                cursor = conn.cursor()
                cursor.execute(
                    """SELECT match_id, deadline_utc FROM matches
                       WHERE mode = 'LEAGUE'
                         AND status IN ('OPEN', 'SCHEDULED')
                         AND deadline_utc IS NOT NULL"""
                )
                matches = cursor.fetchall()
                cursor.close()
            finally:
                return_db(conn)

            if not matches:
                await interaction.followup.send(
                    content="No active league matches with deadlines found.",
                    ephemeral=True
                )
                return

            extension = timedelta(hours=hours)
            updated_count = 0

            for match in matches:
                match_id = match["match_id"]
                old_deadline = coerce_dt(match["deadline_utc"])
                new_deadline = old_deadline + extension
                update_match(match_id, deadline_utc=new_deadline)
                updated_count += 1

            await interaction.followup.send(
                content=f"**League Deadlines Extended**\n\n"
                        f"**Matches Updated:** {updated_count}\n"
                        f"**Extension:** +{hours} hours\n\n"
                        f"_All active league match deadlines have been extended by {hours} hours._\n"
                        f"Extended by {interaction.user.mention}",
                ephemeral=False
            )

            state = get_league_state()
            dashboard_channel = interaction.guild.get_channel(state["dashboard_channel_id"])
            if dashboard_channel:
                await dashboard_channel.send(
                    content=f"**Deadlines Extended** by {interaction.user.mention}\n"
                            f"{updated_count} league match(es) extended by {hours} hours"
                )

            print(f"[LEAGUE-EXTEND] {updated_count} matches extended by {hours}h by {interaction.user}")

        except Exception as e:
            print(f"[LEAGUE-EXTEND ERROR] {e}")
            print(traceback.format_exc())
            await interaction.followup.send(content="An error occurred.", ephemeral=True)


async def setup(bot: commands.Bot):
    await bot.add_cog(LeagueCog(bot))
