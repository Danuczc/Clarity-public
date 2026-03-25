from typing import Optional, Tuple, List

import discord

from utils.db import (
    get_db, return_db, get_config, get_league_state
)
from utils.helpers import get_user_team_authority


def is_team_staff(user: discord.Member) -> bool:
    config = get_config()
    team_perms_role_id = config.get("team_perms_role_id")
    if not team_perms_role_id:
        return False
    return any(role.id == team_perms_role_id for role in user.roles)


def is_elo_staff(user: discord.Member) -> bool:
    config = get_config()
    elo_perms_role_id = config.get("elo_perms_role_id")
    if not elo_perms_role_id:
        return False
    return any(role.id == elo_perms_role_id for role in user.roles)


def has_league_perms(user: discord.Member) -> bool:
    config = get_config()
    league_perms_role_id = config.get("league_perms_role_id")
    if not league_perms_role_id:
        return False
    return any(role.id == league_perms_role_id for role in user.roles)


async def check_league_perms(interaction: discord.Interaction) -> bool:
    config = get_config()
    if not config.get("league_perms_role_id"):
        await interaction.followup.send(
            content="**League Perms role not configured.**\n"
                    "An administrator must set it using `/setup league_perms_role:<role>`",
            ephemeral=True
        )
        return False

    if not has_league_perms(interaction.user):
        await interaction.followup.send(
            content="You need the League Perms role to use this command.",
            ephemeral=True
        )
        return False

    return True


def validate_playoff_format(bracket: str, round_num: int) -> str:
    if bracket == "WINNERS":
        return "BO3"
    elif bracket == "LOSERS":
        if round_num >= 3:
            return "BO5"
        else:
            return "BO3"

    return "BO3"


def get_unfinished_playoff_matches(bracket: str) -> List[dict]:
    conn = get_db()
    try:
        cursor = conn.cursor()
        cursor.execute(
            """SELECT match_id, team1_role_id, team2_role_id, league_round, series_format
               FROM matches
               WHERE mode = 'LEAGUE'
                 AND bracket = %s
                 AND status IN ('OPEN', 'SCHEDULED')
               ORDER BY league_round, match_id""",
            (bracket,)
        )
        matches = cursor.fetchall()
        cursor.close()
        return [dict(m) for m in matches]
    finally:
        return_db(conn)


def check_team_active(team_role_id: int) -> bool:
    from utils.db import get_team
    team = get_team(team_role_id)
    return team is not None


def is_league_team(team_role_id: int) -> bool:
    conn = get_db()
    try:
        cursor = conn.cursor()
        cursor.execute(
            """SELECT 1 FROM league_group_teams WHERE team_role_id = %s LIMIT 1""",
            (team_role_id,)
        )
        exists = cursor.fetchone() is not None
        cursor.close()
        return exists
    finally:
        return_db(conn)


def is_roster_locked() -> bool:
    state = get_league_state()
    return state.get("roster_lock_enabled", False)


def validate_roster_change(team_role_id: int, user: discord.Member, action: str) -> tuple[bool, Optional[str], bool]:
    if not is_league_team(team_role_id):
        return True, None, False

    if not is_roster_locked():
        return True, None, False

    if is_team_staff(user):
        return True, None, True
    else:
        return False, "**Roster Lock Active**\n\nRoster changes are locked during the league season. Contact staff for assistance.", False


def is_team_captain(team_role_id: int, user: discord.Member) -> bool:
    authority = get_user_team_authority(user.id, team_role_id)
    return authority == "CAPTAIN"


def is_team_vice(team_role_id: int, user: discord.Member) -> bool:
    authority = get_user_team_authority(user.id, team_role_id)
    return authority == "VICE"


def has_team_authority(team_role_id: int, user: discord.Member) -> bool:
    if is_team_staff(user):
        return True
    authority = get_user_team_authority(user.id, team_role_id)
    return authority in ("CAPTAIN", "VICE")


def is_suspended(user: discord.Member) -> bool:
    config = get_config()
    suspended_role_id = config.get("suspended_role_id")
    if not suspended_role_id:
        return False
    return any(role.id == suspended_role_id for role in user.roles)


def is_ref(user: discord.Member) -> bool:
    config = get_config()
    ref_role_id = config.get("ref_role_id")
    if not ref_role_id:
        return False
    return any(role.id == ref_role_id for role in user.roles)


def can_modify_roster(user: discord.Member, team_role_id: int) -> Tuple[bool, str]:
    config = get_config()
    transactions_open = config.get("transactions_open", 0)

    if is_team_staff(user):
        return True, ""

    if not transactions_open:
        return False, "Transaction window is closed. Only staff can modify rosters."

    if has_team_authority(team_role_id, user):
        return True, ""

    return False, "You don't have authority over this team."


def get_user_authority_teams(user: discord.Member) -> List[int]:
    from utils.db import get_all_teams

    authority_teams = []
    for team in get_all_teams():
        team_role_id = team["team_role_id"]
        authority = get_user_team_authority(user.id, team_role_id)
        if authority in ("CAPTAIN", "VICE"):
            authority_teams.append(team_role_id)
    return authority_teams
