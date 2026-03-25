import asyncio
import re
import traceback
from datetime import datetime, timedelta
from typing import Literal, Tuple, List

import discord
from discord import app_commands
from discord.ext import commands

from utils.db import (
    get_config, get_db, return_db, get_team, get_match_by_channel, get_match,
    update_match, create_match, delete_match, get_open_matches, get_cooldown,
    set_cooldown, set_dodge_cooldown, get_dodge_cooldown_remaining,
    is_user_ref_for_match, remove_match_ref, clear_expired_cooldowns,
    get_no_show_by_match, create_no_show, get_match_refs, update_league_standings,
    get_user_authority_teams, get_user_team_authority, award_ref_activity_for_match
)
from utils.helpers import (
    EMBED_COLOR, utc_now, safe_defer, update_leaderboard, coerce_dt,
    parse_schedule_input, is_team_captain, is_team_vice, has_team_authority,
    is_team_staff, get_match_leadership_user_ids, build_match_created_embed,
    build_compact_elo_embed, post_elo_update, refresh_match_info_message,
    check_command_rate_limit, check_challenge_rate_limit, is_suspended,
    validate_match_allowed, update_elo, build_rich_error, create_match_channel,
    remove_ref_from_channel, notify_refs_reschedule, upsert_ref_signup_message
)
from utils.permissions import is_elo_staff, is_ref
from views.shared_views import (
    ScheduleProposalView, DodgeMatchView, ConfirmationView, NoShowView
)
from views.league_dashboard import update_league_dashboard

SeriesFormat = Literal["bo3", "bo5"]
WinnerChoice = Literal["team1", "team2"]


def validate_set_scores(
    team1_score: int,
    team2_score: int,
    set_winner: WinnerChoice,
    set_num: int
) -> Tuple[bool, str]:
    """Returns (is_valid, error_message)."""
    if team1_score < 0 or team2_score < 0:
        return False, f"Set {set_num}: Scores cannot be negative."

    if team1_score > team2_score:
        expected_winner = "team1"
    elif team2_score > team1_score:
        expected_winner = "team2"
    else:
        return False, f"Set {set_num}: Scores are tied ({team1_score}-{team2_score}). There must be a winner."

    if set_winner != expected_winner:
        return False, f"Set {set_num}: Winner is '{set_winner}' but scores show {team1_score}-{team2_score}."

    return True, ""


def is_set_skipped(team1_score: int, team2_score: int) -> Tuple[bool, bool]:
    """Returns (is_skipped, is_partial_zero_error)."""
    if team1_score == 0 and team2_score == 0:
        return True, False
    if team1_score == 0 or team2_score == 0:
        return False, True
    return False, False


async def update_ref_signup_embed(bot: commands.Bot, guild: discord.Guild, match_id: int):
    match = get_match(match_id)
    if not match:
        return

    team1_role = guild.get_role(match["team1_role_id"])
    team2_role = guild.get_role(match["team2_role_id"])

    scheduled_time = None
    if match.get("scheduled_time_utc"):
        scheduled_time = coerce_dt(match["scheduled_time_utc"])

    success, error_msg = await upsert_ref_signup_message(
        guild=guild,
        match_id=match_id,
        team1_role=team1_role,
        team2_role=team2_role,
        scheduled_time_utc=scheduled_time,
        channel_id=match.get("channel_id")
    )

    if not success:
        raise Exception(f"Referee signup message update failed: {error_msg}")


async def process_match_outcome(
    interaction: discord.Interaction,
    match: dict,
    winner_choice: WinnerChoice,
    team1_role_id: int,
    team2_role_id: int,
    fmt: SeriesFormat,
    set_scores: List[Tuple[int, int, str]]
):
    if winner_choice == "team1":
        winner_role_id = team1_role_id
        loser_role_id = team2_role_id
    else:
        winner_role_id = team2_role_id
        loser_role_id = team1_role_id

    match_mode = match.get("mode", "ELO")

    if match_mode == "LEAGUE":
        try:
            update_match(match["match_id"], status="FINISHED", finished_at_utc=utc_now().isoformat())

            award_ref_activity_for_match(interaction.guild.id, match["match_id"])

            group_id = match.get("group_id")
            if group_id:
                team1_sets = 0
                team2_sets = 0
                for team1_score, team2_score, set_winner in set_scores:
                    if set_winner == "team1":
                        team1_sets += 1
                    else:
                        team2_sets += 1

                update_league_standings(group_id, team1_role_id, team1_sets, team2_sets)
                update_league_standings(group_id, team2_role_id, team2_sets, team1_sets)

            winner_role = interaction.guild.get_role(winner_role_id)
            loser_role = interaction.guild.get_role(loser_role_id)
            winner_name = winner_role.name if winner_role else "Unknown Team"
            loser_name = loser_role.name if loser_role else "Unknown Team"

            result_text = f"**League Match Recorded**\n"
            result_text += f"Winner: {winner_role.mention if winner_role else winner_name}\n"
            result_text += f"Loser: {loser_role.mention if loser_role else loser_name}\n"
            if group_id:
                result_text += f"\n**Standings Updated**\n"
                result_text += f"{winner_role.name if winner_role else 'Winner'}: +{team1_sets if winner_role_id == team1_role_id else team2_sets} sets won\n"
                result_text += f"{loser_role.name if loser_role else 'Loser'}: +{team2_sets if winner_role_id == team1_role_id else team1_sets} sets won"

            await interaction.followup.send(content=result_text)

            try:
                await update_league_dashboard(interaction.guild)
            except Exception as dash_err:
                print(f"[LEAGUE DASHBOARD] Failed to refresh after match completion: {dash_err}")

            return

        except Exception as db_err:
            print(f"[LEAGUE MATCH ERROR] DB update failed for match_id={match['match_id']}")
            print(traceback.format_exc())
            await interaction.followup.send(
                content=f"**Error Code: LEAGUE_DB_UPDATE_FAIL**\n"
                        f"Database update failed. Please report this to staff."
            )
            return

    old_a_int = int(match["team1_elo_locked"])
    old_b_int = int(match["team2_elo_locked"])

    if winner_role_id == match["team1_role_id"]:
        winner_side = "A"
    else:
        winner_side = "B"

    new_a_float, new_b_float, delta_a_float = update_elo(
        float(old_a_int),
        float(old_b_int),
        winner_side,
        fmt
    )

    new_a_int = int(round(new_a_float))

    delta_a_int = new_a_int - old_a_int
    new_b_int = old_b_int - delta_a_int

    winner_team_data = get_team(winner_role_id)
    loser_team_data = get_team(loser_role_id)
    winner_current = winner_team_data["elo"]
    loser_current = loser_team_data["elo"]

    if winner_role_id == match["team1_role_id"]:
        winner_elo_change = delta_a_int      
        loser_elo_change = -delta_a_int      
    else:
        winner_elo_change = -delta_a_int     
        loser_elo_change = delta_a_int       

    final_winner_elo = winner_current + winner_elo_change
    final_loser_elo = loser_current + loser_elo_change

    try:
        from utils.db import update_team
        update_team(winner_role_id, elo=final_winner_elo, wins=winner_team_data["wins"] + 1)
        update_team(loser_role_id, elo=final_loser_elo, losses=loser_team_data["losses"] + 1)

        update_match(match["match_id"], status="FINISHED", finished_at_utc=utc_now().isoformat())

        award_ref_activity_for_match(interaction.guild.id, match["match_id"])

        config = get_config()
        cooldown_hours = config.get("cooldown_hours", 24)  

        if cooldown_hours > 0:
            cooldown_expires = utc_now() + timedelta(hours=cooldown_hours)
            set_cooldown(team1_role_id, team2_role_id, cooldown_expires)
            print(f"[COOLDOWN] Applied {cooldown_hours}h cooldown for teams {team1_role_id} vs {team2_role_id}, expires at {cooldown_expires}")

    except Exception as db_err:
        print(f"[MATCH RESULT ERROR] DB update failed for match_id={match['match_id']}")
        print(f"[MATCH RESULT ERROR] Exception type: {type(db_err).__name__}")
        print(f"[MATCH RESULT ERROR] Exception message: {str(db_err)}")
        print(traceback.format_exc())

        error_msg = str(db_err)[:200]
        await interaction.followup.send(
            content=f"**Error Code: DB_UPDATE_FAIL**\n"
                    f"Exception: `{type(db_err).__name__}`\n"
                    f"Details: {error_msg}\n\n"
                    f"Database update failed. Please report this to staff."
        )
        return

    winner_role = interaction.guild.get_role(winner_role_id)
    loser_role = interaction.guild.get_role(loser_role_id)
    winner_name = winner_role.name if winner_role else "Unknown Team"
    loser_name = loser_role.name if loser_role else "Unknown Team"

    warnings = []

    team1_role = interaction.guild.get_role(team1_role_id)
    team2_role = interaction.guild.get_role(team2_role_id)
    team1_name = team1_role.name if team1_role else "Unknown Team"
    team2_name = team2_role.name if team2_role else "Unknown Team"

    if winner_role_id == team1_role_id:
        team1_old_elo = winner_current
        team1_new_elo = final_winner_elo
        team2_old_elo = loser_current
        team2_new_elo = final_loser_elo
    else:
        team1_old_elo = loser_current
        team1_new_elo = final_loser_elo
        team2_old_elo = winner_current
        team2_new_elo = final_winner_elo

    compact_embed = build_compact_elo_embed(
        fmt=fmt,
        team1_name=team1_name,
        team2_name=team2_name,
        winner=winner_choice,
        set_scores=set_scores,
        team1_old_elo=team1_old_elo,
        team1_new_elo=team1_new_elo,
        team2_old_elo=team2_old_elo,
        team2_new_elo=team2_new_elo,
        staff_user=interaction.user,
        team1_locked_elo=old_a_int,
        team2_locked_elo=old_b_int,
        interaction=interaction,
        team1_role_id=team1_role_id,
        team2_role_id=team2_role_id
    )

    try:
        await interaction.channel.send(embed=compact_embed)
    except Exception as ch_err:
        print(f"[MATCH RESULT WARNING] Failed to send compact embed to match channel for match_id={match['match_id']}")
        print(traceback.format_exc())

    try:
        await post_elo_update(interaction.guild, compact_embed)
    except Exception as elo_err:
        print(f"[MATCH RESULT WARNING] Failed to post ELO update for match_id={match['match_id']}")
        print(traceback.format_exc())
        warnings.append("Could not post to Elo Updates channel")

    try:
        await update_leaderboard(interaction.guild)
    except Exception as lb_err:
        print(f"[MATCH RESULT WARNING] Failed to update leaderboard for match_id={match['match_id']}")
        print(traceback.format_exc())
        warnings.append("Could not update leaderboard")

    try:
        await refresh_match_info_message(match["match_id"], interaction.guild)
    except Exception as refresh_err:
        print(f"[MATCH RESULT WARNING] Failed to refresh match info for match_id={match['match_id']}")
        print(traceback.format_exc())

    try:
        cooldown_expires = utc_now() + timedelta(hours=24)
        set_cooldown(match["challenger_team_role_id"], match["challenged_team_role_id"], cooldown_expires)
    except Exception as cd_err:
        print(f"[MATCH RESULT WARNING] Failed to set cooldown for match_id={match['match_id']}")
        print(traceback.format_exc())

    staff_message = "Match recorded and Elo updated."
    if warnings:
        staff_message += f"\n{'; '.join(warnings)}"

    try:
        await interaction.followup.send(content=staff_message)
    except Exception as followup_err:
        print(f"[MATCH RESULT WARNING] Failed to send ephemeral followup for match_id={match['match_id']}")
        print(f"[MATCH RESULT WARNING] Exception: {type(followup_err).__name__}: {str(followup_err)}")

    await asyncio.sleep(30)
    try:
        await interaction.channel.delete(reason="Match finished")
    except Exception as del_err:
        print(f"[MATCH RESULT WARNING] Failed to delete channel for match_id={match['match_id']}")
        print(traceback.format_exc())


class MatchesCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @app_commands.command(name="bo3", description="Record a Best of 3 match result (Elo Perms only)")
    @app_commands.describe(
        team1="First team",
        team2="Second team",
        winner="Match winner (team1 or team2)",
        team1_s1_score="Team 1 score in Set 1",
        team2_s1_score="Team 2 score in Set 1",
        set1_winner="Winner of Set 1",
        team1_s2_score="Team 1 score in Set 2",
        team2_s2_score="Team 2 score in Set 2",
        set2_winner="Winner of Set 2",
        team1_s3_score="Team 1 score in Set 3 (0-0 = not played)",
        team2_s3_score="Team 2 score in Set 3 (0-0 = not played)",
        set3_winner="Winner of Set 3 (ignored if Set 3 not played)"
    )
    @app_commands.choices(winner=[
        app_commands.Choice(name="Team 1", value="team1"),
        app_commands.Choice(name="Team 2", value="team2")
    ])
    @app_commands.choices(set1_winner=[
        app_commands.Choice(name="Team 1", value="team1"),
        app_commands.Choice(name="Team 2", value="team2")
    ])
    @app_commands.choices(set2_winner=[
        app_commands.Choice(name="Team 1", value="team1"),
        app_commands.Choice(name="Team 2", value="team2")
    ])
    @app_commands.choices(set3_winner=[
        app_commands.Choice(name="Team 1", value="team1"),
        app_commands.Choice(name="Team 2", value="team2")
    ])
    async def bo3_cmd(
        self,
        interaction: discord.Interaction,
        team1: discord.Role,
        team2: discord.Role,
        winner: WinnerChoice,
        team1_s1_score: int,
        team2_s1_score: int,
        set1_winner: WinnerChoice,
        team1_s2_score: int,
        team2_s2_score: int,
        set2_winner: WinnerChoice,
        team1_s3_score: int,
        team2_s3_score: int,
        set3_winner: WinnerChoice
    ):
        await safe_defer(interaction, ephemeral=True)

        try:
            if not is_elo_staff(interaction.user):
                await interaction.followup.send(
                    content="You do not have permission to use this command."
                )
                return

            team1_role_id = team1.id
            team2_role_id = team2.id

            if team1_role_id == team2_role_id:
                await interaction.followup.send(content="team1 and team2 must be different teams.")
                return

            team1_data = get_team(team1_role_id)
            team2_data = get_team(team2_role_id)

            if not team1_data:
                await interaction.followup.send(content="Team 1 is not registered.")
                return
            if not team2_data:
                await interaction.followup.send(content="Team 2 is not registered.")
                return

            match = get_match_by_channel(interaction.channel.id)
            if not match:
                await interaction.followup.send(
                    content="This command must be used in a match channel."
                )
                return

            if match["status"] in ("FINISHED", "CANCELLED"):
                await interaction.followup.send(content="This match has already been finalized.")
                return

            if match["status"] != "SCHEDULED":
                await interaction.followup.send(
                    content="This match is not in SCHEDULED status."
                )
                return

            match_teams = {match["team1_role_id"], match["team2_role_id"]}
            provided_teams = {team1_role_id, team2_role_id}
            if match_teams != provided_teams:
                await interaction.followup.send(
                    content="The provided teams do not match the teams in this match channel."
                )
                return

            sets_played = []
            errors = []

            skipped, partial_err = is_set_skipped(team1_s1_score, team2_s1_score)
            if skipped:
                errors.append("Set 1 cannot be skipped (both scores are 0).")
            elif partial_err:
                errors.append("Set 1: Invalid - only one score is 0.")
            else:
                valid, err = validate_set_scores(team1_s1_score, team2_s1_score, set1_winner, 1)
                if not valid:
                    errors.append(err)
                else:
                    sets_played.append({"set": 1, "winner": set1_winner})

            skipped, partial_err = is_set_skipped(team1_s2_score, team2_s2_score)
            if skipped:
                errors.append("Set 2 cannot be skipped (both scores are 0).")
            elif partial_err:
                errors.append("Set 2: Invalid - only one score is 0.")
            else:
                valid, err = validate_set_scores(team1_s2_score, team2_s2_score, set2_winner, 2)
                if not valid:
                    errors.append(err)
                else:
                    sets_played.append({"set": 2, "winner": set2_winner})

            skipped_s3, partial_err_s3 = is_set_skipped(team1_s3_score, team2_s3_score)
            if partial_err_s3:
                errors.append("Set 3: Invalid - only one score is 0. Both must be 0 to skip.")
            elif not skipped_s3:
                valid, err = validate_set_scores(team1_s3_score, team2_s3_score, set3_winner, 3)
                if not valid:
                    errors.append(err)
                else:
                    sets_played.append({"set": 3, "winner": set3_winner})

            if errors:
                await interaction.followup.send(
                    content="**Validation Errors:**\n" + "\n".join(f"• {e}" for e in errors)
                )
                return

            team1_set_wins = sum(1 for s in sets_played if s["winner"] == "team1")
            team2_set_wins = sum(1 for s in sets_played if s["winner"] == "team2")

            if winner == "team1" and team1_set_wins < 2:
                await interaction.followup.send(
                    content=f"Winner is 'team1' but they only won {team1_set_wins} set(s). Need 2 to win BO3."
                )
                return
            if winner == "team2" and team2_set_wins < 2:
                await interaction.followup.send(
                    content=f"Winner is 'team2' but they only won {team2_set_wins} set(s). Need 2 to win BO3."
                )
                return

            set_scores_list = [
                (team1_s1_score, team2_s1_score, set1_winner),
                (team1_s2_score, team2_s2_score, set2_winner)
            ]
            if not (team1_s3_score == 0 and team2_s3_score == 0):
                set_scores_list.append((team1_s3_score, team2_s3_score, set3_winner))

            await process_match_outcome(
                interaction=interaction,
                match=match,
                winner_choice=winner,
                team1_role_id=team1_role_id,
                team2_role_id=team2_role_id,
                fmt="bo3",
                set_scores=set_scores_list
            )
        except Exception as e:
            print(f"[BO3 ERROR] Unexpected error for match channel={interaction.channel.id}")
            print(f"[BO3 ERROR] Exception type: {type(e).__name__}")
            print(f"[BO3 ERROR] Exception message: {str(e)}")
            print(traceback.format_exc())

            error_msg = str(e)[:200]  
            await interaction.followup.send(
                content=f"**Error Code: BO3_RECORD_FAIL**\n"
                        f"Exception: `{type(e).__name__}`\n"
                        f"Details: {error_msg}\n\n"
                        f"Please report this to staff with the error code."
            )

    @app_commands.command(name="bo5", description="Record a Best of 5 match result (Elo Perms only)")
    @app_commands.describe(
        team1="First team",
        team2="Second team",
        winner="Match winner (team1 or team2)",
        team1_s1_score="Team 1 score in Set 1",
        team2_s1_score="Team 2 score in Set 1",
        set1_winner="Winner of Set 1",
        team1_s2_score="Team 1 score in Set 2",
        team2_s2_score="Team 2 score in Set 2",
        set2_winner="Winner of Set 2",
        team1_s3_score="Team 1 score in Set 3",
        team2_s3_score="Team 2 score in Set 3",
        set3_winner="Winner of Set 3",
        team1_s4_score="Team 1 score in Set 4 (0-0 = not played)",
        team2_s4_score="Team 2 score in Set 4 (0-0 = not played)",
        set4_winner="Winner of Set 4 (ignored if not played)",
        team1_s5_score="Team 1 score in Set 5 (0-0 = not played)",
        team2_s5_score="Team 2 score in Set 5 (0-0 = not played)",
        set5_winner="Winner of Set 5 (ignored if not played)"
    )
    @app_commands.choices(winner=[
        app_commands.Choice(name="Team 1", value="team1"),
        app_commands.Choice(name="Team 2", value="team2")
    ])
    @app_commands.choices(set1_winner=[
        app_commands.Choice(name="Team 1", value="team1"),
        app_commands.Choice(name="Team 2", value="team2")
    ])
    @app_commands.choices(set2_winner=[
        app_commands.Choice(name="Team 1", value="team1"),
        app_commands.Choice(name="Team 2", value="team2")
    ])
    @app_commands.choices(set3_winner=[
        app_commands.Choice(name="Team 1", value="team1"),
        app_commands.Choice(name="Team 2", value="team2")
    ])
    @app_commands.choices(set4_winner=[
        app_commands.Choice(name="Team 1", value="team1"),
        app_commands.Choice(name="Team 2", value="team2")
    ])
    @app_commands.choices(set5_winner=[
        app_commands.Choice(name="Team 1", value="team1"),
        app_commands.Choice(name="Team 2", value="team2")
    ])
    async def bo5_cmd(
        self,
        interaction: discord.Interaction,
        team1: discord.Role,
        team2: discord.Role,
        winner: WinnerChoice,
        team1_s1_score: int,
        team2_s1_score: int,
        set1_winner: WinnerChoice,
        team1_s2_score: int,
        team2_s2_score: int,
        set2_winner: WinnerChoice,
        team1_s3_score: int,
        team2_s3_score: int,
        set3_winner: WinnerChoice,
        team1_s4_score: int,
        team2_s4_score: int,
        set4_winner: WinnerChoice,
        team1_s5_score: int,
        team2_s5_score: int,
        set5_winner: WinnerChoice
    ):
        await safe_defer(interaction, ephemeral=True)

        try:
            if not is_elo_staff(interaction.user):
                await interaction.followup.send(
                    content="You do not have permission to use this command."
                )
                return

            team1_role_id = team1.id
            team2_role_id = team2.id

            if team1_role_id == team2_role_id:
                await interaction.followup.send(content="team1 and team2 must be different teams.")
                return

            team1_data = get_team(team1_role_id)
            team2_data = get_team(team2_role_id)

            if not team1_data:
                await interaction.followup.send(content="Team 1 is not registered.")
                return
            if not team2_data:
                await interaction.followup.send(content="Team 2 is not registered.")
                return

            match = get_match_by_channel(interaction.channel.id)
            if not match:
                await interaction.followup.send(
                    content="This command must be used in a match channel."
                )
                return

            if match["status"] in ("FINISHED", "CANCELLED"):
                await interaction.followup.send(content="This match has already been finalized.")
                return

            if match["status"] != "SCHEDULED":
                await interaction.followup.send(
                    content="This match is not in SCHEDULED status."
                )
                return

            match_teams = {match["team1_role_id"], match["team2_role_id"]}
            provided_teams = {team1_role_id, team2_role_id}
            if match_teams != provided_teams:
                await interaction.followup.send(
                    content="The provided teams do not match the teams in this match channel."
                )
                return

            sets_played = []
            errors = []

            skipped, partial_err = is_set_skipped(team1_s1_score, team2_s1_score)
            if skipped:
                errors.append("Set 1 cannot be skipped (both scores are 0).")
            elif partial_err:
                errors.append("Set 1: Invalid - only one score is 0.")
            else:
                valid, err = validate_set_scores(team1_s1_score, team2_s1_score, set1_winner, 1)
                if not valid:
                    errors.append(err)
                else:
                    sets_played.append({"set": 1, "winner": set1_winner})

            skipped, partial_err = is_set_skipped(team1_s2_score, team2_s2_score)
            if skipped:
                errors.append("Set 2 cannot be skipped (both scores are 0).")
            elif partial_err:
                errors.append("Set 2: Invalid - only one score is 0.")
            else:
                valid, err = validate_set_scores(team1_s2_score, team2_s2_score, set2_winner, 2)
                if not valid:
                    errors.append(err)
                else:
                    sets_played.append({"set": 2, "winner": set2_winner})

            skipped, partial_err = is_set_skipped(team1_s3_score, team2_s3_score)
            if skipped:
                errors.append("Set 3 cannot be skipped in BO5 (both scores are 0).")
            elif partial_err:
                errors.append("Set 3: Invalid - only one score is 0.")
            else:
                valid, err = validate_set_scores(team1_s3_score, team2_s3_score, set3_winner, 3)
                if not valid:
                    errors.append(err)
                else:
                    sets_played.append({"set": 3, "winner": set3_winner})

            skipped_s4, partial_err_s4 = is_set_skipped(team1_s4_score, team2_s4_score)
            if partial_err_s4:
                errors.append("Set 4: Invalid - only one score is 0. Both must be 0 to skip.")
            elif not skipped_s4:
                valid, err = validate_set_scores(team1_s4_score, team2_s4_score, set4_winner, 4)
                if not valid:
                    errors.append(err)
                else:
                    sets_played.append({"set": 4, "winner": set4_winner})

            skipped_s5, partial_err_s5 = is_set_skipped(team1_s5_score, team2_s5_score)
            if skipped_s4 and not skipped_s5:
                errors.append("Set 5 cannot be played if Set 4 was skipped.")
            elif partial_err_s5:
                errors.append("Set 5: Invalid - only one score is 0. Both must be 0 to skip.")
            elif not skipped_s5 and not skipped_s4:
                valid, err = validate_set_scores(team1_s5_score, team2_s5_score, set5_winner, 5)
                if not valid:
                    errors.append(err)
                else:
                    sets_played.append({"set": 5, "winner": set5_winner})

            if errors:
                await interaction.followup.send(
                    content="**Validation Errors:**\n" + "\n".join(f"• {e}" for e in errors)
                )
                return

            team1_set_wins = sum(1 for s in sets_played if s["winner"] == "team1")
            team2_set_wins = sum(1 for s in sets_played if s["winner"] == "team2")

            if winner == "team1" and team1_set_wins < 3:
                await interaction.followup.send(
                    content=f"Winner is 'team1' but they only won {team1_set_wins} set(s). Need 3 to win BO5."
                )
                return
            if winner == "team2" and team2_set_wins < 3:
                await interaction.followup.send(
                    content=f"Winner is 'team2' but they only won {team2_set_wins} set(s). Need 3 to win BO5."
                )
                return

            set_scores_list = [
                (team1_s1_score, team2_s1_score, set1_winner),
                (team1_s2_score, team2_s2_score, set2_winner),
                (team1_s3_score, team2_s3_score, set3_winner)
            ]
            if not (team1_s4_score == 0 and team2_s4_score == 0):
                set_scores_list.append((team1_s4_score, team2_s4_score, set4_winner))
            if not (team1_s5_score == 0 and team2_s5_score == 0):
                set_scores_list.append((team1_s5_score, team2_s5_score, set5_winner))

            await process_match_outcome(
                interaction=interaction,
                match=match,
                winner_choice=winner,
                team1_role_id=team1_role_id,
                team2_role_id=team2_role_id,
                fmt="bo5",
                set_scores=set_scores_list
            )
        except Exception as e:
            print(f"[BO5 ERROR] Unexpected error for match channel={interaction.channel.id}")
            print(f"[BO5 ERROR] Exception type: {type(e).__name__}")
            print(f"[BO5 ERROR] Exception message: {str(e)}")
            print(traceback.format_exc())

            error_msg = str(e)[:200]  
            await interaction.followup.send(
                content=f"**Error Code: BO5_RECORD_FAIL**\n"
                        f"Exception: `{type(e).__name__}`\n"
                        f"Details: {error_msg}\n\n"
                        f"Please report this to staff with the error code."
            )

    @app_commands.command(name="forfeit", description="Manually award a win/loss for a forfeit (Elo Perms only)")
    @app_commands.describe(
        winner="The team that wins by forfeit",
        loser="The team that forfeits"
    )
    async def forfeit_cmd(self, interaction: discord.Interaction, winner: discord.Role, loser: discord.Role):
        """
        Manually award a forfeit result:
        - Winner: +30 Elo, +1 win
        - Loser: -30 Elo, +1 loss
        - Posts Elo update embed (same style as /bo3 and /bo5)
        """
        await safe_defer(interaction, ephemeral=True)

        try:
            if not is_elo_staff(interaction.user):
                await interaction.followup.send(
                    content="You do not have permission to use this command."
                )
                return

            if winner.id == loser.id:
                await interaction.followup.send(content="Winner and loser must be different teams.")
                return

            winner_data = get_team(winner.id)
            loser_data = get_team(loser.id)

            if not winner_data:
                await interaction.followup.send(content="Winner team is not registered.")
                return
            if not loser_data:
                await interaction.followup.send(content="Loser team is not registered.")
                return

            match = get_match_by_channel(interaction.channel.id)
            if not match:
                await interaction.followup.send(
                    content="This command must be used in a match channel."
                )
                return

            match_teams = {match["team1_role_id"], match["team2_role_id"]}
            forfeit_teams = {winner.id, loser.id}
            if match_teams != forfeit_teams:
                await interaction.followup.send(
                    content="The winner and loser must be the two teams in this match."
                )
                return

            if match["status"] == "FINISHED":
                await interaction.followup.send(
                    content="This match has already been finalized."
                )
                return

            config = get_config()
            elo_updates_channel_id = config.get("elo_updates_channel_id")
            if not elo_updates_channel_id:
                await interaction.followup.send(
                    content="Elo Updates channel is not configured. Please run `/setup` first."
                )
                return

            match_mode = match.get("mode", "ELO")  

            if match_mode == "LEAGUE":
                try:
                    update_match(match["match_id"], status="FINISHED", finished_at_utc=utc_now().isoformat())

                    award_ref_activity_for_match(interaction.guild.id, match["match_id"])

                    group_id = match.get("group_id")
                    if group_id:
                        update_league_standings(group_id, winner.id, 2, 0)
                        update_league_standings(group_id, loser.id, 0, 2)

                    result_text = f"**League Forfeit Recorded** (2-0)\n"
                    result_text += f"Winner: {winner.mention}\n"
                    result_text += f"Forfeiter: {loser.mention}\n"
                    if group_id:
                        result_text += f"\n**Standings Updated**\n"
                        result_text += f"{winner.name}: +2 sets won\n"
                        result_text += f"{loser.name}: +2 sets lost"

                    await interaction.followup.send(content=result_text)
                    return

                except Exception as db_err:
                    print(f"[LEAGUE FORFEIT ERROR] DB update failed for match_id={match['match_id']}")
                    print(traceback.format_exc())
                    await interaction.followup.send(
                        content=f"**Error Code: LEAGUE_FORFEIT_FAIL**\n"
                                f"Database update failed. Please report this to staff."
                    )
                    return

            FORFEIT_ELO_CHANGE = 30

            winner_old_elo = winner_data["elo"]
            winner_old_wins = winner_data["wins"]
            winner_new_elo = winner_old_elo + FORFEIT_ELO_CHANGE
            winner_new_wins = winner_old_wins + 1

            loser_old_elo = loser_data["elo"]
            loser_old_losses = loser_data["losses"]
            loser_new_elo = loser_old_elo - FORFEIT_ELO_CHANGE
            loser_new_losses = loser_old_losses + 1

            conn = get_db()
            try:
                cursor = conn.cursor()
                cursor.execute("BEGIN")

                try:
                    cursor.execute(
                        "UPDATE teams SET elo = %s, wins = %s WHERE team_role_id = %s",
                        (winner_new_elo, winner_new_wins, winner.id)
                    )

                    cursor.execute(
                        "UPDATE teams SET elo = %s, losses = %s WHERE team_role_id = %s",
                        (loser_new_elo, loser_new_losses, loser.id)
                    )

                    cursor.execute(
                        """SELECT COUNT(*) as cnt FROM forfeit_events
                           WHERE team_role_id = %s
                           AND created_at_utc > NOW() - INTERVAL '7 days'""",
                        (loser.id,)
                    )
                    forfeit_row = cursor.fetchone()
                    forfeits_last_7_days = forfeit_row["cnt"] if forfeit_row else 0

                    cursor.execute(
                        "INSERT INTO forfeit_events (team_role_id, created_at_utc) VALUES (%s, NOW())",
                        (loser.id,)
                    )

                    n = forfeits_last_7_days + 1

                    if n == 1:
                        forfeit_penalty = 0
                        forfeit_warning = "Warning: next forfeit within 7 days adds extra Elo loss."
                    elif n == 2:
                        forfeit_penalty = 20
                        forfeit_warning = None
                    elif n == 3:
                        forfeit_penalty = 30
                        forfeit_warning = None
                    elif n == 4:
                        forfeit_penalty = 40
                        forfeit_warning = None
                    else:  # n >= 5
                        forfeit_penalty = 50
                        forfeit_warning = None

                    if forfeit_penalty > 0:
                        cursor.execute(
                            "UPDATE teams SET elo = GREATEST(0, elo - %s) WHERE team_role_id = %s",
                            (forfeit_penalty, loser.id)
                        )
                        loser_new_elo = max(0, loser_new_elo - forfeit_penalty)

                    cursor.execute(
                        "UPDATE matches SET status = 'FINISHED', finished_at_utc = %s WHERE match_id = %s",
                        (utc_now(), match["match_id"])
                    )

                    cursor.execute("COMMIT")

                except Exception as db_err:
                    cursor.execute("ROLLBACK")
                    raise db_err

            finally:
                return_db(conn)

            winner_mention = winner.mention
            loser_mention = loser.mention
            winner_display = winner.name
            loser_display = loser.name

            title = f"**{winner_display}** vs **{loser_display}**"

            description_lines = [
                f"**Forfeit Result**",
                "",
                f"{winner_mention}  **1**  –  **0**  {loser_mention}",
                "",
            ]
            description = "\n".join(description_lines)

            embed = discord.Embed(
                title=title,
                description=description,
                color=EMBED_COLOR  
            )

            forfeit_lines = [
                f"**Winner:** {winner_mention} (Forfeit Victory)",
                f"**Loser:** {loser_mention} (Forfeit)"
            ]
            embed.add_field(
                name="Match Details",
                value="\n".join(forfeit_lines),
                inline=False
            )

            winner_delta = winner_new_elo - winner_old_elo
            loser_delta = loser_new_elo - loser_old_elo
            winner_delta_str = f"{winner_delta:+d}"
            loser_delta_str = f"{loser_delta:+d}"

            elo_lines = [
                f"**{winner_mention}**",
                f"{winner_old_elo} → **{winner_new_elo}** ({winner_delta_str})",
                "",
                f"**{loser_mention}**",
                f"{loser_old_elo} → **{loser_new_elo}** ({loser_delta_str})",
            ]

            if forfeit_penalty > 0:
                elo_lines.append("")
                elo_lines.append(f"**Repeat Forfeit Penalty (#{n} in 7 days):** -{forfeit_penalty} Elo")
            elif forfeit_warning:
                elo_lines.append("")
                elo_lines.append(forfeit_warning)

            embed.add_field(
                name="Rating Changes",
                value="\n".join(elo_lines),
                inline=False
            )

            embed.set_footer(text=f"Recorded by {interaction.user.display_name}")

            embed_send_failed = False
            try:
                await post_elo_update(interaction.guild, embed)
            except Exception as elo_err:
                print(f"[FORFEIT WARNING] Failed to post Elo update: {elo_err}")
                print(traceback.format_exc())
                embed_send_failed = True

            try:
                await update_leaderboard(interaction.guild)
            except Exception as lb_err:
                print(f"[FORFEIT] Failed to update leaderboard: {lb_err}")

            if embed_send_failed:
                await interaction.followup.send(
                    content=f"Forfeit recorded: {winner_mention} (+{FORFEIT_ELO_CHANGE}) / {loser_mention} (-{FORFEIT_ELO_CHANGE})\n"
                            f"However, failed to post in Elo Updates channel."
                )
            else:
                await interaction.followup.send(
                    content=f"Forfeit recorded: {winner_mention} (+{FORFEIT_ELO_CHANGE}) / {loser_mention} (-{FORFEIT_ELO_CHANGE})"
                )

        except Exception as e:
            print(f"[FORFEIT ERROR] Unexpected error")
            print(f"[FORFEIT ERROR] Exception type: {type(e).__name__}")
            print(f"[FORFEIT ERROR] Exception message: {str(e)}")
            print(traceback.format_exc())
            print(f"[FORFEIT_FAIL] {repr(e)} winner_role_id={winner.id} loser_role_id={loser.id} channel_id={interaction.channel_id}")

            error_msg = str(e)[:200]
            await interaction.followup.send(
                content=f"**Error Code: FORFEIT_FAIL**\n"
                        f"Exception: `{type(e).__name__}`\n"
                        f"Details: {error_msg}\n\n"
                        f"Please report this to staff with the error code."
            )

    @app_commands.command(name="challenge", description="Challenge another team")
    @app_commands.describe(opponent_team="The team to challenge")
    async def challenge_cmd(self, interaction: discord.Interaction, opponent_team: discord.Role):
        await safe_defer(interaction, ephemeral=True)

        try:
            allowed, blocked_until = check_command_rate_limit(interaction.user.id)
            if not allowed:
                error_msg = build_rich_error(
                    "Rate Limit Exceeded",
                    "You are sending commands too quickly.",
                    retry_ts=blocked_until,
                    suggestion="Slow down and try again in a moment."
                )
                await interaction.followup.send(content=error_msg, ephemeral=True)
                return

            if is_suspended(interaction.user):
                error_msg = build_rich_error(
                    "Account Suspended",
                    "Suspended users cannot issue challenges.",
                    suggestion="Contact staff to resolve your suspension."
                )
                await interaction.followup.send(content=error_msg, ephemeral=True)
                return

            allowed, error_msg = validate_match_allowed("ELO")
            if not allowed:
                await interaction.followup.send(content=error_msg, ephemeral=True)
                return

            opponent_team_role_id = opponent_team.id
            opponent_team_data = get_team(opponent_team_role_id)

            if not opponent_team_data:
                await interaction.followup.send(content="That team is not registered.")
                return

            user_authority_teams = get_user_authority_teams(interaction.user)

            if len(user_authority_teams) == 0:
                await interaction.followup.send(
                    content="You must be a captain or vice captain of a team to challenge."
                )
                return

            if len(user_authority_teams) > 1:
                team_mentions = [f"<@&{tid}>" for tid in user_authority_teams]
                await interaction.followup.send(
                    content=f"You have authority over multiple teams: {', '.join(team_mentions)}.\n"
                    "Please have only one team leadership role to issue challenges, or ask staff for assistance."
                )
                return

            challenger_team_role_id = user_authority_teams[0]

            if challenger_team_role_id == opponent_team_role_id:
                await interaction.followup.send(content="You cannot challenge your own team.")
                return

            user_opponent_authority = get_user_team_authority(interaction.user.id, opponent_team_role_id)
            if user_opponent_authority:
                error_msg = build_rich_error(
                    "Affiliation Conflict",
                    f"You cannot challenge a team you are affiliated with ({user_opponent_authority}).",
                    suggestion="Contact staff if this is a database error."
                )
                await interaction.followup.send(content=error_msg, ephemeral=True)
                return

            challenge_allowed, blocked_until = check_challenge_rate_limit(challenger_team_role_id)
            if not challenge_allowed:
                error_msg = build_rich_error(
                    "Challenge Limit Exceeded",
                    "Your team has issued too many challenges recently (5 per hour max).",
                    retry_ts=blocked_until,
                    suggestion="Wait for cooldown to expire, or schedule existing matches."
                )
                await interaction.followup.send(content=error_msg, ephemeral=True)
                return

            clear_expired_cooldowns()
            cooldown = get_cooldown(challenger_team_role_id, opponent_team_role_id)
            if cooldown and cooldown > utc_now():
                error_msg = build_rich_error(
                    "Cooldown Active",
                    "These teams recently played and are on cooldown.",
                    retry_ts=cooldown,
                    suggestion="Challenge other teams while waiting, or check /leaderboard for available opponents."
                )
                await interaction.followup.send(content=error_msg, ephemeral=True)
                return

            dodge_remaining = get_dodge_cooldown_remaining(challenger_team_role_id, opponent_team_role_id)
            if dodge_remaining:
                dodge_expiry = utc_now() + dodge_remaining
                error_msg = build_rich_error(
                    "Dodge Cooldown",
                    "This team dodged your last challenge.",
                    retry_ts=dodge_expiry,
                    suggestion="Challenge other teams while waiting."
                )
                await interaction.followup.send(content=error_msg, ephemeral=True)
                return

            open_matches = get_open_matches()
            for match in open_matches:
                teams_in_match = {match["team1_role_id"], match["team2_role_id"]}
                if teams_in_match == {challenger_team_role_id, opponent_team_role_id}:
                    await interaction.followup.send(
                        content="There's already an open match between these teams."
                    )
                    return

            challenger_team = get_team(challenger_team_role_id)
            challenger_role = interaction.guild.get_role(challenger_team_role_id)
            opponent_role = interaction.guild.get_role(opponent_team_role_id)

            team1_elo = challenger_team["elo"]
            team2_elo = opponent_team_data["elo"]
            elo_diff = team1_elo - team2_elo

            dodge_allowed = (team2_elo - team1_elo) >= 150


            match_id = None
            channel = None

            try:
                match_id = create_match(
                    channel_id=0,  
                    team1_role_id=challenger_team_role_id,
                    team2_role_id=opponent_team_role_id,
                    challenger_team_role_id=challenger_team_role_id,
                    challenged_team_role_id=opponent_team_role_id,
                    team1_elo_locked=team1_elo,
                    team2_elo_locked=team2_elo,
                    elo_diff_locked=elo_diff,
                    dodge_allowed=dodge_allowed,  
                    status="OPEN"
                )
            except Exception as db_err:
                print(f"[CHALLENGE ERROR] DB insert failed for guild={interaction.guild.id}, challenger={challenger_team_role_id}, opponent={opponent_team_role_id}")
                print(traceback.format_exc())
                await interaction.followup.send(content="Failed to create match: database error.")
                return

            try:
                channel = await create_match_channel(interaction.guild, challenger_role, opponent_role, match_id)

                update_match(match_id, channel_id=channel.id)

                new_name = f"match-{match_id}-{challenger_role.name[:10]}-vs-{opponent_role.name[:10]}".lower()
                new_name = re.sub(r'[^a-z0-9-]', '', new_name)
                try:
                    await channel.edit(name=new_name)
                except:
                    pass

            except Exception as channel_err:
                print(f"[CHALLENGE ERROR] Channel creation failed for match_id={match_id}, guild={interaction.guild.id}")
                print(traceback.format_exc())
                try:
                    delete_match(match_id)
                except:
                    pass
                await interaction.followup.send(content="Failed to create match channel. Match cancelled.")
                return

            try:
                match_embed = build_match_created_embed(
                    match_id=match_id,
                    team1_name=challenger_role.name,
                    team2_name=opponent_role.name,
                    team1_elo=team1_elo,
                    team2_elo=team2_elo,
                    status="OPEN",
                    scheduled_time_utc=None,
                    refs_claimed=0,
                    invoker_name=interaction.user.display_name
                )
                match_info_msg = await channel.send(
                    content=f"{challenger_role.mention} vs {opponent_role.mention}",
                    embed=match_embed
                )
                update_match(match_id, match_info_message_id=match_info_msg.id)

                await channel.send(content="You have **3 days** to finish this match.")

                if dodge_allowed:
                    dodge_view = DodgeMatchView(match_id=match_id, challenged_team_role_id=opponent_team_role_id)
                    await channel.send(
                        content=f"**Dodge allowed** for {opponent_role.mention} due to Elo difference.",
                        view=dodge_view
                    )
                    await channel.send(content="Dodge window: 24 hours from match creation.")

                await interaction.followup.send(content=f"Challenge sent! Match channel: {channel.mention}")

            except Exception as msg_err:
                print(f"[CHALLENGE WARNING] Messaging failed for match_id={match_id}, channel={channel.id}")
                print(traceback.format_exc())
                try:
                    await channel.send(
                        f"**Match #{match_id} created but encountered an error during setup.**\n"
                        f"Teams: {challenger_role.mention} vs {opponent_role.mention}\n"
                        f"Please contact staff if this match is not working correctly."
                    )
                except:
                    pass
                await interaction.followup.send(
                    content=f"Match created but setup messages failed. Check {channel.mention}."
                )

        except Exception as e:
            print(f"[CHALLENGE ERROR] Unexpected error in /challenge for guild={interaction.guild.id}")
            print(traceback.format_exc())
            await interaction.followup.send(content="Failed to create challenge. Please try again.")

    @app_commands.command(name="schedule", description="Schedule the match time")
    @app_commands.describe(time="Time in DD MM HH:MM format (CET is implicit). Example: 12 03 19:30")
    async def schedule_cmd(self, interaction: discord.Interaction, time: str):
        await safe_defer(interaction, ephemeral=True)

        try:
            match = get_match_by_channel(interaction.channel.id)
            if not match:
                await interaction.followup.send(content="This command must be used in a match channel.")
                return

            if match["status"] not in ("OPEN", "SCHEDULED"):
                await interaction.followup.send(content="This match cannot be scheduled.")
                return

            if not (has_team_authority(match["team1_role_id"], interaction.user) or
                    has_team_authority(match["team2_role_id"], interaction.user)):
                await interaction.followup.send(
                    content="Only a captain or vice captain of either team can propose a schedule.",
                    ephemeral=True
                )
                return

            if match["status"] in ("FINISHED", "CANCELLED"):
                await interaction.followup.send(
                    content="Cannot schedule a finished or cancelled match.",
                    ephemeral=True
                )
                return

            utc_time = parse_schedule_input(time)
            if not utc_time:
                await interaction.followup.send(
                    content="Invalid time. Use format: DD MM HH:MM (example: 07 02 19:30). Time must be in the future.",
                    ephemeral=True
                )
                return

            user_is_team1_leadership = is_team_captain(match["team1_role_id"], interaction.user) or \
                                      is_team_vice(match["team1_role_id"], interaction.user)
            user_is_team2_leadership = is_team_captain(match["team2_role_id"], interaction.user) or \
                                      is_team_vice(match["team2_role_id"], interaction.user)

            print(f"[SCHEDULE PROPOSE DEBUG] match_id={match['match_id']}, user_id={interaction.user.id}")
            print(f"  team1_role_id={match['team1_role_id']}, team2_role_id={match['team2_role_id']}")
            print(f"  user_is_team1_leadership={user_is_team1_leadership}")
            print(f"  user_is_team2_leadership={user_is_team2_leadership}")

            proposing_team_role_id = None
            if user_is_team1_leadership:
                proposing_team_role_id = match["team1_role_id"]
            elif user_is_team2_leadership:
                proposing_team_role_id = match["team2_role_id"]
            elif is_team_staff(interaction.user):
                proposing_team_role_id = match["challenger_team_role_id"]
                print(f"  staff override: proposing_team_role_id={proposing_team_role_id}")
            else:
                await interaction.followup.send(
                    content="Only a captain or vice captain of either team can propose a schedule.",
                    ephemeral=True
                )
                return

            print(f"  final proposing_team_role_id={proposing_team_role_id}")

            if proposing_team_role_id == match["team1_role_id"]:
                other_team_role_id = match["team2_role_id"]
            else:
                other_team_role_id = match["team1_role_id"]

            unix_timestamp = int(utc_time.timestamp())
            discord_timestamp = f"<t:{unix_timestamp}:F>"  

            team1_role = interaction.guild.get_role(match["team1_role_id"])
            team2_role = interaction.guild.get_role(match["team2_role_id"])
            other_team_role = interaction.guild.get_role(other_team_role_id)

            if match.get("pending_schedule_message_id"):
                try:
                    old_message = await interaction.channel.fetch_message(match["pending_schedule_message_id"])
                    await old_message.delete()
                except Exception:
                    pass

            embed = discord.Embed(
                title="Schedule Proposal",
                description=f"{team1_role.mention if team1_role else 'Team 1'} vs {team2_role.mention if team2_role else 'Team 2'}",
                color=0x00FFFF
            )
            embed.add_field(name="Proposed Time", value=discord_timestamp, inline=False)
            embed.add_field(name="Proposed by", value=interaction.user.mention, inline=False)

            view = ScheduleProposalView(
                match_id=match["match_id"],
                challenger_team_role_id=match["challenger_team_role_id"],
                challenged_team_role_id=other_team_role_id
            )

            leadership_ids = get_match_leadership_user_ids(match["team1_role_id"], match["team2_role_id"])
            ping_content = "**Schedule Proposal** " + " ".join(f"<@{uid}>" for uid in sorted(leadership_ids))

            proposal_msg = await interaction.channel.send(
                content=ping_content,
                embed=embed,
                view=view,
                allowed_mentions=discord.AllowedMentions(users=True, roles=False)
            )

            print(f"[SCHEDULE DB UPDATE] Storing proposal: match_id={match['match_id']}, proposer={proposing_team_role_id}")
            update_match(
                match["match_id"],
                pending_schedule_time_utc=utc_time.isoformat(),
                pending_schedule_by_team_role_id=proposing_team_role_id,
                pending_schedule_message_id=proposal_msg.id,
                pending_created_at_utc=utc_now().isoformat(),
                schedule_pending=True
            )
            print(f"[SCHEDULE DB UPDATE] Proposal stored successfully")

            await interaction.followup.send(content="Schedule proposal sent!", ephemeral=True)
        except Exception as e:
            print(f"[SCHEDULE ERROR] Failed for channel={interaction.channel.id}, guild={interaction.guild.id}")
            print(traceback.format_exc())
            await interaction.followup.send(content="Failed to schedule match. Please try again.")

    @app_commands.command(name="reschedule", description="Propose a reschedule with accept/deny buttons")
    @app_commands.describe(time="New time in DD MM HH:MM format (CET is implicit). Example: 12 03 19:30")
    async def reschedule_cmd(self, interaction: discord.Interaction, time: str):
        await safe_defer(interaction, ephemeral=True)

        try:
            match = get_match_by_channel(interaction.channel.id)
            if not match:
                await interaction.followup.send(content="This command must be used in a match channel.")
                return

            if match["status"] != "SCHEDULED":
                await interaction.followup.send(content="This match is not scheduled yet.")
                return

            user_is_team1_leadership = is_team_captain(match["team1_role_id"], interaction.user) or \
                                      is_team_vice(match["team1_role_id"], interaction.user)
            user_is_team2_leadership = is_team_captain(match["team2_role_id"], interaction.user) or \
                                      is_team_vice(match["team2_role_id"], interaction.user)

            print(f"[RESCHEDULE PROPOSE DEBUG] match_id={match['match_id']}, user_id={interaction.user.id}")
            print(f"  team1_role_id={match['team1_role_id']}, team2_role_id={match['team2_role_id']}")
            print(f"  user_is_team1_leadership={user_is_team1_leadership}")
            print(f"  user_is_team2_leadership={user_is_team2_leadership}")

            proposing_team_role_id = None
            if user_is_team1_leadership:
                proposing_team_role_id = match["team1_role_id"]
                print(f"  proposing_team_role_id={proposing_team_role_id} (team1 leadership)")
            elif user_is_team2_leadership:
                proposing_team_role_id = match["team2_role_id"]
                print(f"  proposing_team_role_id={proposing_team_role_id} (team2 leadership)")
            elif is_team_staff(interaction.user):
                print(f"  staff forcing immediate reschedule")
                utc_time = parse_schedule_input(time)
                if not utc_time:
                    await interaction.followup.send(
                        content="Invalid time. Use format: DD MM HH:MM (example: 05 02 19:30). Time must be in the future.",
                        ephemeral=True
                    )
                    return

                old_time_utc = coerce_dt(match["scheduled_time_utc"]) if match.get("scheduled_time_utc") else None

                update_match(
                    match["match_id"],
                    scheduled_time_utc=utc_time.isoformat(),
                    pending_schedule_time_utc=None,
                    pending_schedule_by_team_role_id=None,
                    pending_schedule_message_id=None,
                    pending_created_at_utc=None,
                    schedule_pending=False,
                    reminded_captains=False,
                    reminded_refs=False,
                    reminded_captains_15m=False,
                    reminded_refs_15m=False
                )
                unix_timestamp = int(utc_time.timestamp())
                discord_timestamp = f"<t:{unix_timestamp}:F>"
                await interaction.followup.send(content=f"Match rescheduled to {discord_timestamp} by staff.")
                await update_ref_signup_embed(self.bot, interaction.guild, match["match_id"])

                if old_time_utc:
                    await notify_refs_reschedule(self.bot, match, old_time_utc, utc_time, interaction.guild)

                return
            else:
                await interaction.followup.send(content="You don't have authority for this match.", ephemeral=True)
                return

            utc_time = parse_schedule_input(time)
            if not utc_time:
                await interaction.followup.send(
                    content="Invalid time. Use format: DD MM HH:MM (example: 07 02 19:30). Time must be in the future.",
                    ephemeral=True
                )
                return

            if proposing_team_role_id == match["team1_role_id"]:
                other_team_role_id = match["team2_role_id"]
            else:
                other_team_role_id = match["team1_role_id"]

            unix_timestamp = int(utc_time.timestamp())
            discord_timestamp = f"<t:{unix_timestamp}:F>" 

            team1_role = interaction.guild.get_role(match["team1_role_id"])
            team2_role = interaction.guild.get_role(match["team2_role_id"])

            if match.get("pending_schedule_message_id"):
                try:
                    old_message = await interaction.channel.fetch_message(match["pending_schedule_message_id"])
                    await old_message.delete()
                except Exception:
                    pass

            embed = discord.Embed(
                title="Schedule Proposal",
                description=f"{team1_role.mention if team1_role else 'Team 1'} vs {team2_role.mention if team2_role else 'Team 2'}",
                color=0x00FFFF
            )
            embed.add_field(name="Proposed Time", value=discord_timestamp, inline=False)
            embed.add_field(name="Proposed by", value=interaction.user.mention, inline=False)

            view = ScheduleProposalView(
                match_id=match["match_id"],
                challenger_team_role_id=match["challenger_team_role_id"],
                challenged_team_role_id=other_team_role_id
            )

            leadership_ids = get_match_leadership_user_ids(match["team1_role_id"], match["team2_role_id"])
            ping_content = "**Schedule Proposal** " + " ".join(f"<@{uid}>" for uid in sorted(leadership_ids))

            proposal_msg = await interaction.channel.send(
                content=ping_content,
                embed=embed,
                view=view,
                allowed_mentions=discord.AllowedMentions(users=True, roles=False)
            )

            print(f"[RESCHEDULE DB UPDATE] Storing proposal: match_id={match['match_id']}, proposer={proposing_team_role_id}")
            update_match(
                match["match_id"],
                pending_schedule_time_utc=utc_time.isoformat(),
                pending_schedule_by_team_role_id=proposing_team_role_id,
                pending_schedule_message_id=proposal_msg.id,
                pending_created_at_utc=utc_now().isoformat(),
                schedule_pending=True
            )
            print(f"[RESCHEDULE DB UPDATE] Proposal stored successfully")

            await interaction.followup.send(content="Reschedule proposal sent!", ephemeral=True)
        except Exception as e:
            print(f"[RESCHEDULE ERROR] Failed for channel={interaction.channel.id}, guild={interaction.guild.id}")
            print(traceback.format_exc())
            await interaction.followup.send(content="Failed to reschedule match. Please try again.")

    @app_commands.command(name="dodge", description="Dodge a match (only if dodge is allowed for your team)")
    async def dodge_cmd(self, interaction: discord.Interaction):
        await safe_defer(interaction, ephemeral=True)

        try:
            match = get_match_by_channel(interaction.channel.id)
            if not match:
                await interaction.followup.send(content="This command must be used in a match channel.")
                return

            if match["status"] != "OPEN":
                await interaction.followup.send(content="This match can no longer be dodged (not in OPEN status).")
                return

            if not match.get("dodge_allowed"):
                await interaction.followup.send(content="Dodging is not allowed for this match.")
                return

            created_at = match.get("created_at_utc")
            if created_at:
                created_at = coerce_dt(created_at)
                expiry = created_at + timedelta(hours=24)
                if utc_now() > expiry:
                    await interaction.followup.send(content="The 24-hour window for dodging has passed.")
                    return

            challenger_team_role_id = match["challenger_team_role_id"]
            challenged_team_role_id = match["challenged_team_role_id"]
            user = interaction.user

            challenger_role = interaction.guild.get_role(challenger_team_role_id)
            if challenger_role and challenger_role in user.roles:
                await interaction.followup.send(content="Only the challenged team may dodge this match.")
                return

            is_authorized = False

            if is_team_captain(challenged_team_role_id, user):
                is_authorized = True
            elif is_team_vice(challenged_team_role_id, user):
                is_authorized = True

            if not is_authorized:
                challenged_role = interaction.guild.get_role(challenged_team_role_id)
                team_name = challenged_role.name if challenged_role else "the challenged team"
                await interaction.followup.send(
                    content=f"Only the captain or vice captain of **{team_name}** (the challenged team) can dodge."
                )
                return

            try:
                update_match(match["match_id"], status="CANCELLED")
                set_dodge_cooldown(challenger_team_role_id, challenged_team_role_id, hours=24)
            except Exception as db_err:
                print(f"[DODGE CMD ERROR] DB update failed for match_id={match['match_id']}")
                print(traceback.format_exc())
                await interaction.followup.send(content="Failed to dodge match. Database error.")
                return

            challenger_role = interaction.guild.get_role(challenger_team_role_id)
            challenged_role = interaction.guild.get_role(challenged_team_role_id)
            challenger_mention = challenger_role.mention if challenger_role else "Challenger"
            challenged_mention = challenged_role.mention if challenged_role else "Challenged team"

            await refresh_match_info_message(match["match_id"], interaction.guild)
            await interaction.channel.send(
                content=f"**Match dodged** by {challenged_mention}. {challenger_mention} cannot challenge {challenged_mention} for 24 hours."
            )

            await interaction.followup.send(content="Match dodged successfully.")

            async def delete_channel_later():
                await asyncio.sleep(10)
                try:
                    await interaction.channel.delete(reason="Match dodged")
                except Exception as del_err:
                    print(f"[DODGE CMD WARNING] Failed to delete channel for match_id={match['match_id']}")
                    print(traceback.format_exc())

            asyncio.create_task(delete_channel_later())

        except Exception as e:
            print(f"[DODGE CMD ERROR] Unexpected error for channel={interaction.channel.id}, user={interaction.user.id}")
            print(traceback.format_exc())
            await interaction.followup.send(content="Failed to dodge match. Please try again.")

    @app_commands.command(name="cancel-match", description="Cancel a match manually (Elo Perms only, no penalties)")
    async def cancel_match_cmd(self, interaction: discord.Interaction):
        """Staff command to cancel a match without Elo changes or penalties."""
        await safe_defer(interaction, ephemeral=True)

        try:
            if not is_elo_staff(interaction.user):
                await interaction.followup.send(content="Only Elo Perms staff can use this command.")
                return

            match = get_match_by_channel(interaction.channel.id)
            if not match:
                await interaction.followup.send(content="This command must be used in a match channel.")
                return

            if match["status"] in ("FINISHED", "CANCELLED"):
                await interaction.followup.send(
                    content=f"This match is already {match['status']}. Cannot cancel."
                )
                return

            async def execute_cancellation(confirm_interaction: discord.Interaction):
                """Execute the match cancellation after confirmation."""
                try:
                    update_match(match["match_id"], status="CANCELLED")
                except Exception as db_err:
                    print(f"[CANCEL-MATCH ERROR] DB update failed for match_id={match['match_id']}")
                    print(traceback.format_exc())
                    await interaction.followup.send(content="Failed to cancel match. Database error.")
                    return

                await refresh_match_info_message(match["match_id"], interaction.guild)

                await interaction.channel.send(
                    content=f"**Match cancelled** by staff ({interaction.user.mention})."
                )

                await interaction.followup.send(content="Match cancelled successfully.")

            team1_name = interaction.guild.get_role(match["team1_role_id"]).name if interaction.guild.get_role(match["team1_role_id"]) else "Team 1"
            team2_name = interaction.guild.get_role(match["team2_role_id"]).name if interaction.guild.get_role(match["team2_role_id"]) else "Team 2"

            warning_text = (
                f"**WARNING:** This will cancel the match between **{team1_name}** vs **{team2_name}**\n"
                f"• No Elo changes will occur\n"
                f"• Match status will be set to CANCELLED\n"
                f"• This action cannot be undone"
            )

            view = ConfirmationView(
                action_name="Cancel Match",
                warning_text=warning_text,
                on_confirm_callback=execute_cancellation,
                interaction_context=interaction
            )

            await interaction.followup.send(
                content=f"**Cancel this match?**\n{warning_text}",
                view=view,
                ephemeral=True
            )

        except Exception as e:
            print(f"[CANCEL-MATCH ERROR] Unexpected error for channel={interaction.channel.id}, user={interaction.user.id}")
            print(traceback.format_exc())
            await interaction.followup.send(content="Failed to cancel match. Please try again.")

    @app_commands.command(name="match-info", description="View match information")
    async def match_info_cmd(self, interaction: discord.Interaction):
        await safe_defer(interaction, ephemeral=True)

        try:
            match = get_match_by_channel(interaction.channel.id)
            if not match:
                await interaction.followup.send(content="This command must be used in a match channel.")
                return

            team1_role = interaction.guild.get_role(match["team1_role_id"])
            team2_role = interaction.guild.get_role(match["team2_role_id"])

            refs = get_match_refs(match["match_id"])
            ref1 = next((r for r in refs if r["team_side"] == 1), None)
            ref2 = next((r for r in refs if r["team_side"] == 2), None)

            status_colors = {
                "SCHEDULED": 0x00FF00,  
                "OPEN": 0xFFFF00,       
                "FINISHED": 0x808080,   
                "CANCELLED": 0xFF0000   
            }
            embed_color = status_colors.get(match["status"], EMBED_COLOR)

            status_label = {
                "SCHEDULED": "Scheduled",
                "OPEN": "Open",
                "FINISHED": "Finished",
                "CANCELLED": "Cancelled"
            }
            status_display = f"{status_label.get(match['status'], 'Unknown')} **{match['status']}**"

            embed = discord.Embed(
                title=f"Match #{match['match_id']} Info",
                color=embed_color
            )

            if match["scheduled_time_utc"]:
                scheduled_dt = coerce_dt(match["scheduled_time_utc"])
                unix_timestamp = int(scheduled_dt.timestamp())
                embed.add_field(
                    name="Scheduled Time",
                    value=f"<t:{unix_timestamp}:F>\n(<t:{unix_timestamp}:R>)",
                    inline=False
                )
            else:
                embed.add_field(
                    name="Scheduled Time",
                    value="Not scheduled yet",
                    inline=False
                )

            embed.add_field(
                name="Teams",
                value=f"{team1_role.mention if team1_role else 'Team 1'} **vs** {team2_role.mention if team2_role else 'Team 2'}",
                inline=False
            )

            refs_claimed = len(refs)
            ref_status = "2/2 Claimed" if refs_claimed == 2 else f"{refs_claimed}/2 Claimed"
            ref1_text = f'<@{ref1["ref_user_id"]}>' if ref1 else 'Unclaimed'
            ref2_text = f'<@{ref2["ref_user_id"]}>' if ref2 else 'Unclaimed'
            embed.add_field(
                name=f"Referees - {ref_status}",
                value=f"**Team 1 Side:** {ref1_text}\n**Team 2 Side:** {ref2_text}",
                inline=False
            )

            embed.add_field(name="Match Status", value=status_display, inline=True)

            embed.add_field(
                name="Locked ELO",
                value=f"{match['team1_elo_locked']} | {match['team2_elo_locked']}",
                inline=True
            )

            await interaction.followup.send(embed=embed)
        except Exception as e:
            await interaction.followup.send(content=f"Error: {str(e)[:200]}")

    @app_commands.command(name="ref-withdraw", description="Withdraw from refereeing a match")
    async def ref_withdraw_cmd(self, interaction: discord.Interaction):
        await safe_defer(interaction, ephemeral=True)

        try:
            match = get_match_by_channel(interaction.channel.id)
            if not match:
                await interaction.edit_original_response(content="This command must be used in a match channel.")
                return

            if not is_user_ref_for_match(match["match_id"], interaction.user.id):
                await interaction.edit_original_response(content="You are not a ref for this match.")
                return

            try:
                remove_match_ref(match["match_id"], interaction.user.id)
            except Exception as db_err:
                print(f"[REF WITHDRAW ERROR] DB delete failed for match_id={match['match_id']}, user={interaction.user.id}")
                print(traceback.format_exc())
                await interaction.edit_original_response(content="Failed to withdraw. Database error.")
                return

            channel_remove_failed = False
            try:
                await remove_ref_from_channel(interaction.channel, interaction.user)
            except Exception as perm_err:
                print(f"[REF WITHDRAW WARNING] Channel permission removal failed for match_id={match['match_id']}, user={interaction.user.id}")
                print(traceback.format_exc())
                channel_remove_failed = True

            try:
                await update_ref_signup_embed(self.bot, interaction.guild, match["match_id"])
            except Exception as embed_err:
                print(f"[REF WITHDRAW WARNING] Ref signup embed update failed for match_id={match['match_id']}")
                print(traceback.format_exc())

            try:
                await refresh_match_info_message(match["match_id"], interaction.guild)
            except Exception as refresh_err:
                print(f"[REF WITHDRAW WARNING] Match info message update failed for match_id={match['match_id']}")
                print(traceback.format_exc())

            if channel_remove_failed:
                await interaction.edit_original_response(
                    content="You have withdrawn from refereeing this match.\n"
                    "Could not remove your channel access - you may need to leave manually."
                )
            else:
                await interaction.edit_original_response(content="You have withdrawn from refereeing this match.")
        except Exception as e:
            print(f"[REF WITHDRAW ERROR] Unexpected error for channel={interaction.channel.id}, user={interaction.user.id}")
            print(traceback.format_exc())
            await interaction.edit_original_response(content="Failed to withdraw. Please try again.")

    @app_commands.command(name="report-noshow", description="Report a no-show")
    @app_commands.describe(loser_team="The team that didn't show up", reason="Reason for the report")
    async def report_noshow_cmd(self, interaction: discord.Interaction, loser_team: discord.Role, reason: str):
        await safe_defer(interaction, ephemeral=False)

        try:
            match = get_match_by_channel(interaction.channel.id)
            if not match:
                await interaction.followup.send(content="This command must be used in a match channel.")
                return

            if match["status"] != "SCHEDULED":
                await interaction.followup.send(content="Match must be in SCHEDULED status.")
                return

            loser_role_id = loser_team.id

            if loser_role_id not in (match["team1_role_id"], match["team2_role_id"]):
                await interaction.followup.send(content="Team must be part of this match.")
                return

            reporter_team = None
            if has_team_authority(match["team1_role_id"], interaction.user):
                reporter_team = match["team1_role_id"]
            elif has_team_authority(match["team2_role_id"], interaction.user):
                reporter_team = match["team2_role_id"]

            if not reporter_team and not is_team_staff(interaction.user):
                await interaction.followup.send(content="You must be a captain/VC of a team in this match or staff.")
                return

            if reporter_team == loser_role_id and not is_team_staff(interaction.user):
                await interaction.followup.send(content="You cannot report your own team.")
                return

            if match["scheduled_time_utc"]:
                scheduled_dt = coerce_dt(match["scheduled_time_utc"])
                grace_end = scheduled_dt + timedelta(minutes=15)
                if utc_now() < grace_end:
                    remaining = (grace_end - utc_now()).total_seconds() / 60
                    await interaction.followup.send(
                        content=f"Grace period not over. Wait {remaining:.0f} more minutes."
                    )
                    return

            existing = get_no_show_by_match(match["match_id"])
            if existing and existing["status"] == "PENDING":
                await interaction.followup.send(content="There's already a pending no-show report for this match.")
                return

            no_show_id = create_no_show(match["match_id"], loser_role_id, interaction.user.id, reason)

            config = get_config()
            elo_perms_role_id = config.get("elo_perms_role_id")
            elo_role = interaction.guild.get_role(elo_perms_role_id) if elo_perms_role_id else None
            accused_role = interaction.guild.get_role(loser_role_id)

            embed = discord.Embed(
                title=f"No-Show Report - Match #{match['match_id']}",
                color=EMBED_COLOR
            )
            embed.add_field(name="Accused Team", value=accused_role.mention if accused_role else "Unknown", inline=True)
            embed.add_field(name="Status", value="PENDING", inline=True)
            embed.add_field(name="Reason", value=reason, inline=False)
            embed.add_field(
                name="Evidence Required",
                value="1) In-game scoreboard screenshot\n2) Proof opponent not in stage/team VC",
                inline=False
            )
            embed.add_field(name="Reported By", value=interaction.user.mention, inline=True)

            view = NoShowView(no_show_id, match["match_id"])

            content = f"{elo_role.mention}" if elo_role else ""
            await interaction.followup.send(
                content=content,
                embed=embed,
                view=view,
                allowed_mentions=discord.AllowedMentions(roles=True)
            )
        except Exception as e:
            await interaction.followup.send(content=f"Error reporting no-show: {str(e)[:200]}")


async def setup(bot: commands.Bot):
    await bot.add_cog(MatchesCog(bot))
