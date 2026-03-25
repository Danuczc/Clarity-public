import os
import re
import traceback
from datetime import datetime, timedelta
from dataclasses import dataclass
from typing import Optional, List, Tuple, Dict, Any, Literal

import discord
from discord import app_commands
import pytz

from utils.db import (
    get_db, return_db, get_config, get_team, get_all_teams,
    get_match, get_match_refs, get_roster, get_match_by_channel,
    get_user_captain_teams, get_user_vice_captain_teams,
    get_player_team, update_match, get_vice_captains, update_config
)

EMBED_COLOR = 0x00FFFF
SeriesFormat = Literal["bo3", "bo5"]


@dataclass(frozen=True)
class EloConfig:
    K0: float = 100.0
    D: float = 300.0
    Mcap: float = 5.0
    L_bo3: float = 1.00
    L_bo5: float = 1.30
    delta_max: float | None = 450.0


def expected_score(r_a: float, r_b: float) -> float:
    return 1.0 / (1.0 + 10 ** ((r_b - r_a) / 400.0))


def gap_multiplier(r_a: float, r_b: float, cfg: EloConfig) -> float:
    diff = abs(r_a - r_b)
    m = 1.0 + min(cfg.Mcap - 1.0, diff / cfg.D)
    return m


def series_multiplier(fmt: SeriesFormat, cfg: EloConfig) -> float:
    if fmt == "bo3":
        return cfg.L_bo3
    if fmt == "bo5":
        return cfg.L_bo5
    raise ValueError(f"Unknown series format: {fmt}")


def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def update_elo(
    r_a: float,
    r_b: float,
    winner: Literal["A", "B"],
    fmt: SeriesFormat = "bo3",
    cfg: EloConfig = EloConfig(),
) -> Tuple[float, float, float]:
    e_a = expected_score(r_a, r_b)
    s_a = 1.0 if winner == "A" else 0.0
    k = cfg.K0 * gap_multiplier(r_a, r_b, cfg) * series_multiplier(fmt, cfg)
    delta_a = k * (s_a - e_a)
    if cfg.delta_max is not None:
        delta_a = clamp(delta_a, -cfg.delta_max, cfg.delta_max)
    new_a = r_a + delta_a
    new_b = r_b - delta_a
    return new_a, new_b, delta_a


CET_TZ = pytz.timezone("Europe/Zurich")
UTC_TZ = pytz.UTC
SCHEDULE_TIME_FORMAT = r"^\d{2} \d{2} \d{2}:\d{2}$"  


def coerce_dt(value):
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        return datetime.fromisoformat(value)
    raise TypeError(f"Unexpected datetime type: {type(value)}")


def utc_now() -> datetime:
    return datetime.now(UTC_TZ)


def parse_schedule_input(date_str: str) -> Optional[datetime]:
    if not re.match(SCHEDULE_TIME_FORMAT, date_str):
        return None

    try:
        parts = date_str.split()
        day = int(parts[0])
        month = int(parts[1])
        time_part = parts[2]
        hour, minute = map(int, time_part.split(':'))

        now_cet = utc_now().astimezone(CET_TZ)
        current_year = now_cet.year

        try:
            naive_dt = datetime(current_year, month, day, hour, minute)
            cet_dt = CET_TZ.localize(naive_dt)
        except ValueError:
            return None

        now_utc = utc_now()
        if cet_dt.astimezone(UTC_TZ) <= now_utc:
            try:
                naive_dt = datetime(current_year + 1, month, day, hour, minute)
                cet_dt = CET_TZ.localize(naive_dt)
            except ValueError:
                return None

            if cet_dt.astimezone(UTC_TZ) <= now_utc:
                return None

        utc_result = cet_dt.astimezone(UTC_TZ)
        if (utc_result - now_utc).days > 3:
            return None

        return utc_result

    except (ValueError, IndexError, AttributeError):
        return None


def utc_to_cet_str(utc_dt: datetime) -> str:
    if utc_dt.tzinfo is None:
        utc_dt = UTC_TZ.localize(utc_dt)
    cet_dt = utc_dt.astimezone(CET_TZ)
    return cet_dt.strftime("%Y-%m-%d %H:%M")


VALID_POSITIONS = ["Setter", "Libero", "Wing Spiker", "Defensive Specialist"]
VALID_RANKS = ["Starter", "Substitute"]

STARTER_LIMITS = {
    "Setter": 1,
    "Libero": 1,
    "Wing Spiker": 2,
    "Defensive Specialist": 2
}


def validate_roster_addition(team_role_id: int, position: str, rank: str) -> Tuple[bool, str]:
    roster = get_roster(team_role_id)

    starters = [m for m in roster if m["rank"] == "Starter"]
    substitutes = [m for m in roster if m["rank"] == "Substitute"]

    if rank == "Starter":
        if len(starters) >= 6:
            return False, "Team already has maximum 6 starters."

        position_count = len([m for m in starters if m["position"] == position])
        limit = STARTER_LIMITS.get(position, 0)

        if position_count >= limit:
            return False, f"Team already has maximum {limit} starter(s) for {position}."

    elif rank == "Substitute":
        if len(substitutes) >= 6:
            return False, "Team already has maximum 6 substitutes."

    return True, ""


async def notify_refs_reschedule(
    bot,
    match: dict,
    old_time_utc: datetime,
    new_time_utc: datetime,
    guild: discord.Guild
):
    match_id = match["match_id"]
    refs = get_match_refs(match_id)

    if not refs:
        return 
    
    team1_role = guild.get_role(match["team1_role_id"])
    team2_role = guild.get_role(match["team2_role_id"])
    team1_mention = f"<@&{match['team1_role_id']}>"
    team2_mention = f"<@&{match['team2_role_id']}>"

    old_cet = utc_to_cet_str(old_time_utc)
    new_cet = utc_to_cet_str(new_time_utc)

    jump_link = ""
    if match.get("channel_id"):
        jump_link = f"\n**Match Channel:** https://discord.com/channels/{guild.id}/{match['channel_id']}"

    message = (
        f"**Match Rescheduled - Match #{match_id}**\n\n"
        f"**Teams:** {team1_mention} vs {team2_mention}\n"
        f"**Old Time:** {old_cet} CET\n"
        f"**New Time:** {new_cet} CET{jump_link}\n\n"
        f"Please update your availability accordingly."
    )

    for ref_data in refs:
        ref_user_id = ref_data["ref_user_id"]
        user = bot.get_user(ref_user_id)
        if not user:
            try:
                user = await bot.fetch_user(ref_user_id)
            except:
                continue
        if user:
            await safe_dm_user(user, message)


async def safe_defer(interaction: discord.Interaction, ephemeral: bool = True):
    if interaction.response.is_done():
        return
    await interaction.response.defer(ephemeral=ephemeral, thinking=True)


LEADERBOARD_TEAMS_PER_PAGE = 10


def build_leaderboard_embed(page: int = 0) -> Tuple[discord.Embed, int]:
    teams = get_all_teams()  
    total_teams = len(teams)
    total_pages = max(1, (total_teams + LEADERBOARD_TEAMS_PER_PAGE - 1) // LEADERBOARD_TEAMS_PER_PAGE)

    page = max(0, min(page, total_pages - 1))

    now_cet = utc_now().astimezone(CET_TZ)
    timestamp_str = now_cet.strftime("%Y-%m-%d %H:%M") + " CET"

    embed = discord.Embed(
        title="Leaderboard",
        color=EMBED_COLOR
    )

    if not teams:
        embed.description = "No teams registered yet."
        embed.set_footer(text=f"Page 1/1 • Updated: {timestamp_str}")
        return embed, 1

    start_idx = page * LEADERBOARD_TEAMS_PER_PAGE
    end_idx = start_idx + LEADERBOARD_TEAMS_PER_PAGE
    page_teams = teams[start_idx:end_idx]

    lines = []
    for i, team in enumerate(page_teams):
        rank = start_idx + i + 1
        team_role_id = team["team_role_id"]
        captain_user_id = team["captain_user_id"]
        elo = team["elo"]
        wins = team["wins"]
        losses = team["losses"]

        medal = ""
        if rank == 1:
            medal = ""
        elif rank == 2:
            medal = ""
        elif rank == 3:
            medal = ""

        team_mention = f"<@&{team_role_id}>"
        captain_mention = f"<@{captain_user_id}>" if captain_user_id else "No Captain"

        entry = (
            f"{medal}**#{rank}** {team_mention} - Captain:{captain_mention}\n"
            f"Elo: **{elo}**\n"
            f"Record: **{wins}**W - **{losses}**L"
        )
        lines.append(entry)

    embed.description = "\n\n".join(lines)
    embed.set_footer(text=f"Page {page + 1}/{total_pages} • Updated: {timestamp_str}")

    return embed, total_pages


async def update_leaderboard(guild: discord.Guild):
    """Update the leaderboard message with embed and pagination."""
    from views.shared_views import LeaderboardView

    config = get_config()
    channel_id = config.get("leaderboard_channel_id")
    message_id = config.get("leaderboard_message_id")

    if not channel_id:
        return

    channel = guild.get_channel(channel_id)
    if not channel:
        return

    embed, total_pages = build_leaderboard_embed(0)
    view = LeaderboardView(current_page=0)

    if message_id:
        try:
            message = await channel.fetch_message(message_id)
            await message.edit(content=None, embed=embed, view=view)
            return
        except discord.NotFound:
            pass

    message = await channel.send(embed=embed, view=view)
    try:
        await message.pin()
    except discord.Forbidden:
        pass
    update_config(leaderboard_message_id=message.id)


async def post_elo_update(guild: discord.Guild, embed: discord.Embed):

    config = get_config()
    channel_id = config.get("elo_updates_channel_id")
    if not channel_id:
        return

    channel = guild.get_channel(channel_id)
    if channel:
        await channel.send(embed=embed)


def build_elo_update_embed(
    event_type: str,
    team_a_name: str,
    team_a_old_elo: int,
    team_a_new_elo: int,
    team_a_old_wins: int,
    team_a_old_losses: int,
    team_a_new_wins: int,
    team_a_new_losses: int,
    team_b_name: Optional[str] = None,
    team_b_old_elo: Optional[int] = None,
    team_b_new_elo: Optional[int] = None,
    team_b_old_wins: Optional[int] = None,
    team_b_old_losses: Optional[int] = None,
    team_b_new_wins: Optional[int] = None,
    team_b_new_losses: Optional[int] = None,
    match_id: Optional[int] = None,
    match_format: Optional[str] = None,
    scheduled_time_utc: Optional[datetime] = None,
    channel_id: Optional[int] = None,
    guild_id: Optional[int] = None
) -> discord.Embed:
    
    titles = {
        "BO3": "Match Result (BO3)",
        "BO5": "Match Result (BO5)",
        "FORFEIT": "Match Forfeit (No-Show)",
        "ADJUSTMENT": "Elo Adjustment"
    }
    title = titles.get(event_type, "Elo Update")

    if team_b_name:
        description = f"**{team_a_name}** vs **{team_b_name}**"
    else:
        description = f"**{team_a_name}**"

    embed = discord.Embed(
        title=title,
        description=description,
        color=EMBED_COLOR
    )

    team_a_delta = team_a_new_elo - team_a_old_elo
    team_a_delta_str = f"{team_a_delta:+d}"  

    team_a_value = (
        f"**Elo:** {team_a_old_elo} → {team_a_new_elo} ({team_a_delta_str})\n"
        f"**Record:** {team_a_old_wins}W-{team_a_old_losses}L → {team_a_new_wins}W-{team_a_new_losses}L"
    )
    embed.add_field(name=team_a_name, value=team_a_value, inline=True)

    if team_b_name:
        team_b_delta = team_b_new_elo - team_b_old_elo
        team_b_delta_str = f"{team_b_delta:+d}"
        team_b_value = (
            f"**Elo:** {team_b_old_elo} → {team_b_new_elo} ({team_b_delta_str})\n"
            f"**Record:** {team_b_old_wins}W-{team_b_old_losses}L → {team_b_new_wins}W-{team_b_new_losses}L"
        )
        embed.add_field(name=team_b_name, value=team_b_value, inline=True)

    if match_id:
        embed.add_field(name="Match ID", value=f"`{match_id}`", inline=True)

    if match_format:
        embed.add_field(name="Format", value=match_format.upper(), inline=True)

    if scheduled_time_utc:
        scheduled_str = utc_to_cet_str(scheduled_time_utc) + " CET"
        embed.add_field(name="Scheduled", value=scheduled_str, inline=True)

    if channel_id and guild_id:
        jump_link = f"https://discord.com/channels/{guild_id}/{channel_id}"
        embed.add_field(name="Match Channel", value=f"[Go to Match]({jump_link})", inline=False)

    return embed


def build_match_created_embed(
    match_id: int,
    team1_name: str,
    team2_name: str,
    team1_elo: int,
    team2_elo: int,
    status: str = "OPEN",
    scheduled_time_utc: Optional[datetime] = None,
    refs_claimed: int = 0,
    invoker_name: str = "Unknown"
) -> discord.Embed:
    embed = discord.Embed(
        title="Match Created",
        description=f"**{team1_name}** vs **{team2_name}**",
        color=EMBED_COLOR
    )

    embed.add_field(name="Match ID", value=str(match_id), inline=True)
    embed.add_field(name="Status", value=status, inline=True)

    if scheduled_time_utc:
        scheduled_str = utc_to_cet_str(scheduled_time_utc) + " CET"
    else:
        scheduled_str = "Not scheduled"
    embed.add_field(name="Scheduled Time (CET)", value=scheduled_str, inline=True)

    embed.add_field(name="Refs", value=f"Needed: 2 • Claimed: {refs_claimed}/2", inline=True)

    embed.add_field(
        name="Locked Elo",
        value=f"{team1_name}: {team1_elo} | {team2_name}: {team2_elo}",
        inline=False
    )

    embed.add_field(
        name="Next Steps",
        value="Use `/schedule` to set time • Refs claim via the ref signup channel",
        inline=False
    )

    embed.set_footer(text=f"CET times only • Created by {invoker_name}")

    return embed


def build_ref_signup_embed(
    match_id: int,
    team1_role: Optional[discord.Role],
    team2_role: Optional[discord.Role],
    scheduled_time_utc: Optional[datetime] = None,
    ref1_user_id: Optional[int] = None,
    ref2_user_id: Optional[int] = None,
    channel_id: Optional[int] = None,
    guild_id: Optional[int] = None
) -> discord.Embed:
    team1_name = team1_role.name if team1_role else "Team 1"
    team2_name = team2_role.name if team2_role else "Team 2"
    team1_mention = team1_role.mention if team1_role else "Team 1"
    team2_mention = team2_role.mention if team2_role else "Team 2"

    embed = discord.Embed(
        title=f"Match #{match_id} - Ref Signup",
        color=EMBED_COLOR
    )

    embed.add_field(name="Teams", value=f"{team1_mention} vs {team2_mention}", inline=False)

    if scheduled_time_utc:
        scheduled_dt = coerce_dt(scheduled_time_utc)
        scheduled_str = utc_to_cet_str(scheduled_dt) + " CET"
    else:
        scheduled_str = "Not scheduled yet"
    embed.add_field(name="Scheduled", value=scheduled_str, inline=False)

    ref1_text = f"<@{ref1_user_id}>" if ref1_user_id else "Unclaimed"
    ref2_text = f"<@{ref2_user_id}>" if ref2_user_id else "Unclaimed"
    embed.add_field(name=f"Team 1 Ref ({team1_name})", value=ref1_text, inline=True)
    embed.add_field(name=f"Team 2 Ref ({team2_name})", value=ref2_text, inline=True)

    if channel_id:
        if guild_id:
            jump_link = f"https://discord.com/channels/{guild_id}/{channel_id}"
            embed.add_field(name="Match Channel", value=f"[Go to Match]({jump_link})", inline=False)
        else:
            embed.add_field(name="Match Channel", value=f"<#{channel_id}>", inline=False)

    return embed


async def refresh_match_info_message(match_id: int, guild: discord.Guild):
    try:
        match = get_match(match_id)
        if not match:
            return  

        channel_id = match.get("channel_id")
        if not channel_id:
            return  

        channel = guild.get_channel(channel_id)
        if not channel:
            return  

        team1_role = guild.get_role(match["team1_role_id"])
        team2_role = guild.get_role(match["team2_role_id"])
        team1_name = team1_role.name if team1_role else f"Team {match['team1_role_id']}"
        team2_name = team2_role.name if team2_role else f"Team {match['team2_role_id']}"

        refs = get_match_refs(match_id)
        refs_claimed = len(refs)

        embed = build_match_created_embed(
            match_id=match_id,
            team1_name=team1_name,
            team2_name=team2_name,
            team1_elo=match["team1_elo_locked"],
            team2_elo=match["team2_elo_locked"],
            status=match["status"],
            scheduled_time_utc=match.get("scheduled_time_utc"),
            refs_claimed=refs_claimed,
            invoker_name="System"
        )

        match_info_message_id = match.get("match_info_message_id")

        if match_info_message_id:
            try:
                message = await channel.fetch_message(match_info_message_id)
                await message.edit(embed=embed)
                return  
            except discord.NotFound:
                pass
            except Exception as e:
                print(f"[REFRESH_MATCH_INFO] Failed to edit message {match_info_message_id}: {e}")


        mention_content = f"<@&{match['team1_role_id']}> vs <@&{match['team2_role_id']}>"
        new_message = await channel.send(content=mention_content, embed=embed)

        update_match(match_id, match_info_message_id=new_message.id)

    except Exception as e:
        print(f"[REFRESH_MATCH_INFO] Error for match_id={match_id}: {e}")
        import traceback
        traceback.print_exc()


async def log_command_use(interaction: discord.Interaction):
    try:
        if not interaction.guild or not interaction.command:
            return

        config = get_config()
        logs_channel_id = config.get("logs_channel_id")
        if not logs_channel_id:
            return

        logs_channel = interaction.guild.get_channel(logs_channel_id)
        if not logs_channel:
            return

        command_name = interaction.command.name

        ALLOWED_LOG_COMMANDS = {
            # Admin
            "setup",
            # Team Perms
            "register", "disband", "set-captain", "vice-captain",
            "cooldown", "remove-cooldown", "set-cooldown",
            "add-member", "remove-member", "swap-member", "promote-sub", "leave-team",
            "open-transactions", "close-transactions",
            "challenge", "schedule", "reschedule", "dodge", "cancel-match",
            "ref-withdraw", "referee-activity", "audit-roles", "team-stats",
            "suspend", "unsuspend", "report-noshow",
            # Elo Perms
            "bo3", "bo5", "forfeit", "set-elo"
        }

        if command_name not in ALLOWED_LOG_COMMANDS:
            return

        params = {}
        if interaction.data and "options" in interaction.data:
            for option in interaction.data["options"]:
                option_name = option.get("name")
                option_value = option.get("value")
                option_type = option.get("type")

                if option_type == 8:  
                    role = interaction.guild.get_role(int(option_value))
                    params[option_name] = role if role else f"Role ID: {option_value}"
                elif option_type == 6:  
                    user = interaction.guild.get_member(int(option_value))
                    params[option_name] = user if user else f"User ID: {option_value}"
                elif option_type == 7:  
                    channel = interaction.guild.get_channel(int(option_value))
                    params[option_name] = channel if channel else f"Channel ID: {option_value}"
                else:
                    params[option_name] = option_value

        embed = discord.Embed(
            title="📝 Command Log",
            color=0x00FFFF,
            timestamp=discord.utils.utcnow()
        )

        embed.add_field(
            name="Command",
            value=f"`/{command_name}`",
            inline=False
        )

        embed.add_field(
            name="User",
            value=f"{interaction.user.mention} (ID: `{interaction.user.id}`)",
            inline=False
        )

        channel_mention = interaction.channel.mention if hasattr(interaction.channel, 'mention') else "DM"
        channel_id = interaction.channel.id if interaction.channel else "N/A"
        embed.add_field(
            name="Channel",
            value=f"{channel_mention} (ID: `{channel_id}`)",
            inline=False
        )

        if params:
            inputs_lines = []
            for key, value in params.items():
                if value is None:
                    formatted_value = "None"
                elif isinstance(value, discord.Role):
                    formatted_value = f"@{value.name} (ID: {value.id})"
                elif isinstance(value, (discord.Member, discord.User)):
                    formatted_value = f"@{value.name} (ID: {value.id})"
                elif isinstance(value, discord.abc.GuildChannel):
                    formatted_value = f"#{value.name} (ID: {value.id})"
                else:
                    str_value = str(value)
                    if len(str_value) > 200:
                        formatted_value = str_value[:197] + "..."
                    else:
                        formatted_value = str_value

                inputs_lines.append(f"{key}: {formatted_value}")

            inputs_text = "\n".join(inputs_lines)
            if len(inputs_text) > 1000:
                inputs_text = inputs_text[:997] + "..."

            embed.add_field(
                name="Inputs",
                value=f"```\n{inputs_text}\n```",
                inline=False
            )
        else:
            embed.add_field(
                name="Inputs",
                value="```\nNone\n```",
                inline=False
            )

        embed.set_footer(
            text=f"{interaction.guild.name} (ID: {interaction.guild.id})",
            icon_url=interaction.guild.icon.url if interaction.guild.icon else None
        )

        await logs_channel.send(embed=embed)

    except Exception as e:
        command_name = interaction.command.name if interaction.command else "unknown"
        guild_id = interaction.guild.id if interaction.guild else "N/A"
        channel_id = interaction.channel.id if interaction.channel else "N/A"
        print(f"[LOGGING ERROR] Failed to log command '{command_name}'")
        print(f"[LOGGING ERROR] Guild ID: {guild_id}, Channel ID: {channel_id}")
        print(f"[LOGGING ERROR] Exception: {type(e).__name__}: {e}")
        import traceback
        print(traceback.format_exc())


async def log_error_code(
    guild: discord.Guild,
    error_code: str,
    exception_type: str,
    exception_message: str,
    command_name: str = "Unknown",
    user_id: int = None,
    channel_id: int = None
):
    try:
        config = get_config()
        logs_channel_id = config.get("logs_channel_id")
        if not logs_channel_id:
            return  

        logs_channel = guild.get_channel(logs_channel_id)
        if not logs_channel:
            return 
        
        log_lines = [
            f"🚨 **Error Code:** `{error_code}`",
            f"**Exception Type:** `{exception_type}`",
            f"**Command:** `{command_name}`",
            f"**Guild:** {guild.name} (ID: {guild.id})"
        ]

        if user_id:
            log_lines.append(f"**User:** <@{user_id}> (ID: {user_id})")

        if channel_id:
            log_lines.append(f"**Channel:** <#{channel_id}> (ID: {channel_id})")

        if exception_message:
            msg = exception_message[:300] if len(exception_message) > 300 else exception_message
            log_lines.append(f"**Error Details:** {msg}")

        log_message = "\n".join(log_lines)

        await logs_channel.send(log_message[:2000])  

    except Exception as e:
        print(f"[LOGGING WARNING] Failed to log error code {error_code}: {e}")


async def upsert_ref_signup_message(
    guild: discord.Guild,
    match_id: int,
    team1_role: Optional[discord.Role],
    team2_role: Optional[discord.Role],
    scheduled_time_utc: Optional[datetime] = None,
    channel_id: Optional[int] = None
) -> Tuple[bool, str]:
    from views.shared_views import RefSignupView

    config = get_config()
    referee_channel_id = config.get("referee_channel_id")
    ref_role_id = config.get("ref_role_id")

    if not referee_channel_id:
        return False, "Referee channel is not configured. Run /setup."

    ref_channel = guild.get_channel(referee_channel_id)
    if not ref_channel:
        return False, "Referee channel not found (may have been deleted)."

    # Check permissions
    bot_member = guild.me
    if not ref_channel.permissions_for(bot_member).send_messages:
        return False, "Cannot post in the referee channel (missing permissions)."

    # Get current refs
    refs = get_match_refs(match_id)
    ref1 = next((r for r in refs if r["team_side"] == 1), None)
    ref2 = next((r for r in refs if r["team_side"] == 2), None)
    ref1_user_id = ref1["ref_user_id"] if ref1 else None
    ref2_user_id = ref2["ref_user_id"] if ref2 else None

    embed = build_ref_signup_embed(
        match_id=match_id,
        team1_role=team1_role,
        team2_role=team2_role,
        scheduled_time_utc=scheduled_time_utc,
        ref1_user_id=ref1_user_id,
        ref2_user_id=ref2_user_id,
        channel_id=channel_id,
        guild_id=guild.id
    )

    view = RefSignupView(match_id)

    match = get_match(match_id)
    existing_message_id = match.get("ref_signup_message_id") if match else None

    ref_role = guild.get_role(ref_role_id) if ref_role_id else None

    try:
        if existing_message_id:
            try:
                message = await ref_channel.fetch_message(existing_message_id)
                await message.edit(embed=embed, view=view)
                return True, ""
            except discord.NotFound:
                content = f"{ref_role.mention}" if ref_role else ""
                ref_msg = await ref_channel.send(content=content, embed=embed, view=view)
                update_match(match_id, ref_signup_message_id=ref_msg.id)
                return True, ""
        else:
            content = f"{ref_role.mention}" if ref_role else ""
            ref_msg = await ref_channel.send(content=content, embed=embed, view=view)
            update_match(match_id, ref_signup_message_id=ref_msg.id)
            return True, ""
    except discord.Forbidden:
        print(f"[REF SIGNUP ERROR] Forbidden: cannot post in referee channel")
        return False, "Cannot post in the referee channel (forbidden)."
    except Exception as e:
        print(f"[REF SIGNUP ERROR] Failed to upsert ref signup message for match_id={match_id}")
        print(traceback.format_exc())
        return False, f"Could not update ref signup message: {str(e)}"


async def post_transaction(guild: discord.Guild, action: str, staff: discord.Member, details: dict = None):
    """Post transaction to transaction channel as an embed."""
    config = get_config()
    channel_id = config.get("transaction_channel_id")
    if not channel_id:
        return

    channel = guild.get_channel(channel_id)
    if not channel:
        return

    embed = discord.Embed(
        title="Transaction",
        color=0x00FFFF,  # Cyan
        timestamp=discord.utils.utcnow()
    )

    embed.add_field(
        name="Action",
        value=action,
        inline=False
    )

    embed.add_field(
        name="Staff",
        value=f"{staff.mention} (ID: `{staff.id}`)",
        inline=False
    )

    if details:
        for field_name, field_value in details.items():
            embed.add_field(
                name=field_name,
                value=str(field_value),
                inline=False
            )

    try:
        await channel.send(embed=embed)
    except Exception as e:
        print(f"[TRANSACTION] Failed to post transaction: {e}")


async def safe_dm_user(user: discord.User, content: str):
    """Safely send a DM to a user. Silently fails if DMs are disabled."""
    try:
        await user.send(content)
    except discord.Forbidden:
        pass  
    except Exception as e:
        print(f"Error sending DM to {user.id}: {e}")


async def send_roster_signup_dm(
    user: discord.Member,
    team_role_id: int,
    position: str,
    rank: str,
    signer: Optional[discord.User] = None
):
    try:
        guild = user.guild
        role = guild.get_role(team_role_id) if guild else None
        team_name = role.name if role else f"Team ({team_role_id})"

        signer_text = f" by {signer.mention}" if signer else ""
        rank_text = f" (Rank: **{rank}**)" if rank else ""

        dm_content = (
            f"You were signed to **{team_name}** as **{position}**{rank_text}{signer_text}.\n\n"
            f"If you were force-signed and want to leave the team, you have **24 hours** to open a support ticket and request to be unsigned."
        )

        await user.send(dm_content)
    except discord.Forbidden:
        print(f"[DM] Could not DM user {user.id} for roster signup.")
    except Exception as e:
        print(f"[DM] Could not DM user {user.id} for roster signup. Error: {e}")


async def send_captain_assignment_dm(
    user: discord.Member,
    team_role_id: int,
    staff: discord.User
):
    try:
        guild = user.guild
        role = guild.get_role(team_role_id) if guild else None
        team_name = role.name if role else f"Team ({team_role_id})"

        dm_content = f"You have been set as **Captain** of **{team_name}** by {staff.mention}."
        await user.send(dm_content)
    except discord.Forbidden:
        print(f"[DM] Could not DM user {user.id} for captain assignment.")
    except Exception as e:
        print(f"[DM] Could not DM user {user.id} for captain assignment. Error: {e}")


def get_match_leadership_user_ids(team1_role_id: int, team2_role_id: int) -> set:
    user_ids = set()

    team1 = get_team(team1_role_id)
    team2 = get_team(team2_role_id)

    if team1 and team1.get("captain_user_id"):
        user_ids.add(team1["captain_user_id"])
    if team2 and team2.get("captain_user_id"):
        user_ids.add(team2["captain_user_id"])

    for vc_id in get_vice_captains(team1_role_id):
        user_ids.add(vc_id)
    for vc_id in get_vice_captains(team2_role_id):
        user_ids.add(vc_id)

    return user_ids


async def create_match_channel(
    guild: discord.Guild,
    team1_role: discord.Role,
    team2_role: discord.Role,
    match_id: int
) -> discord.TextChannel:
    config = get_config()
    category_id = config.get("match_category_id")
    team_perms_role_id = config.get("team_perms_role_id")

    category = guild.get_channel(category_id) if category_id else None
    team_perms_role = guild.get_role(team_perms_role_id) if team_perms_role_id else None

    overwrites = {
        guild.default_role: discord.PermissionOverwrite(view_channel=False),
        guild.me: discord.PermissionOverwrite(
            view_channel=True,
            send_messages=True,
            manage_channels=True,
            manage_messages=True,
            embed_links=True,
            attach_files=True,
            read_message_history=True
        ),
        team1_role: discord.PermissionOverwrite(
            view_channel=True,
            send_messages=True,
            embed_links=True,
            attach_files=True,
            read_message_history=True
        ),
        team2_role: discord.PermissionOverwrite(
            view_channel=True,
            send_messages=True,
            embed_links=True,
            attach_files=True,
            read_message_history=True
        ),
    }

    if team_perms_role:
        overwrites[team_perms_role] = discord.PermissionOverwrite(
            view_channel=True,
            send_messages=True,
            read_message_history=True,
            embed_links=True,
            attach_files=True
        )

    channel_name = f"match-{match_id}-{team1_role.name[:10]}-vs-{team2_role.name[:10]}".lower()
    channel_name = re.sub(r'[^a-z0-9-]', '', channel_name)

    channel = await guild.create_text_channel(
        name=channel_name,
        category=category,
        overwrites=overwrites
    )

    return channel


async def add_ref_to_channel(channel: discord.TextChannel, user: discord.Member):
    await channel.set_permissions(
        user,
        view_channel=True,
        read_message_history=True,
        send_messages=True
    )


async def remove_ref_from_channel(channel: discord.TextChannel, user: discord.Member):
    await channel.set_permissions(user, overwrite=None)


async def team_autocomplete(interaction: discord.Interaction, current: str) -> List[app_commands.Choice[str]]:
    teams = get_all_teams()
    choices = []
    for team in teams:
        role = interaction.guild.get_role(team["team_role_id"])
        if role and (current.lower() in role.name.lower() or not current):
            choices.append(app_commands.Choice(name=role.name, value=str(role.id)))
        if len(choices) >= 25:
            break
    return choices


async def position_autocomplete(interaction: discord.Interaction, current: str) -> List[app_commands.Choice[str]]:
    return [
        app_commands.Choice(name=pos, value=pos)
        for pos in VALID_POSITIONS
        if current.lower() in pos.lower() or not current
    ]


async def rank_autocomplete(interaction: discord.Interaction, current: str) -> List[app_commands.Choice[str]]:
    return [
        app_commands.Choice(name=rank, value=rank)
        for rank in VALID_RANKS
        if current.lower() in rank.lower() or not current
    ]


def build_rich_error(reason_title: str, reason_text: str, retry_ts: Optional[datetime] = None, suggestion: Optional[str] = None) -> str:
    msg = f"**{reason_title}**\n{reason_text}"

    if retry_ts:
        unix_ts = int(retry_ts.timestamp())
        msg += f"\nTry again <t:{unix_ts}:R>"

    if suggestion:
        msg += f"\n**Suggestion:** {suggestion}"

    return msg


def get_user_team_authority(user_id: int, team_role_id: int) -> Optional[str]:
    conn = get_db()
    try:
        cursor = conn.cursor()

        cursor.execute("SELECT captain_user_id FROM teams WHERE team_role_id = %s", (team_role_id,))
        team = cursor.fetchone()
        if team and team["captain_user_id"] == user_id:
            return "CAPTAIN"

        cursor.execute("SELECT 1 FROM vice_captains WHERE team_role_id = %s AND user_id = %s", (team_role_id, user_id))
        if cursor.fetchone():
            return "VICE"

        cursor.execute("SELECT 1 FROM roster WHERE team_role_id = %s AND user_id = %s", (team_role_id, user_id))
        if cursor.fetchone():
            return "ROSTER"

        cursor.close()
        return None
    finally:
        return_db(conn)


def check_command_rate_limit(user_id: int) -> tuple[bool, Optional[datetime]]:
    conn = get_db()
    try:
        cursor = conn.cursor()
        now = utc_now()

        cursor.execute("SELECT * FROM command_rate_limits WHERE user_id = %s", (user_id,))
        record = cursor.fetchone()

        if not record:
            cursor.execute(
                "INSERT INTO command_rate_limits (user_id, command_count, window_start_utc) VALUES (%s, 1, %s)",
                (user_id, now)
            )
            conn.commit()
            cursor.close()
            return True, None

        window_start = coerce_dt(record["window_start_utc"])
        if window_start.tzinfo is None:
            window_start = UTC_TZ.localize(window_start)

        if record.get("blocked_until_utc"):
            blocked_until = coerce_dt(record["blocked_until_utc"])
            if blocked_until.tzinfo is None:
                blocked_until = UTC_TZ.localize(blocked_until)
            if now < blocked_until:
                cursor.close()
                return False, blocked_until

        if (now - window_start).total_seconds() >= 60:
            cursor.execute(
                "UPDATE command_rate_limits SET command_count = 1, window_start_utc = %s, blocked_until_utc = NULL WHERE user_id = %s",
                (now, user_id)
            )
            conn.commit()
            cursor.close()
            return True, None

        new_count = record["command_count"] + 1
        if new_count > 10:
            blocked_until = now + timedelta(minutes=1)
            cursor.execute(
                "UPDATE command_rate_limits SET command_count = %s, blocked_until_utc = %s WHERE user_id = %s",
                (new_count, blocked_until, user_id)
            )
            conn.commit()
            cursor.close()
            return False, blocked_until
        else:
            cursor.execute(
                "UPDATE command_rate_limits SET command_count = %s WHERE user_id = %s",
                (new_count, user_id)
            )
            conn.commit()
            cursor.close()
            return True, None
    finally:
        return_db(conn)


def check_challenge_rate_limit(team_role_id: int) -> tuple[bool, Optional[datetime]]:
    """Check if team has exceeded challenge rate limit (5 challenges/hour)."""
    conn = get_db()
    try:
        cursor = conn.cursor()
        now = utc_now()

        cursor.execute("SELECT * FROM challenge_rate_limits WHERE team_role_id = %s", (team_role_id,))
        record = cursor.fetchone()

        if not record:
            cursor.execute(
                "INSERT INTO challenge_rate_limits (team_role_id, challenge_count, window_start_utc) VALUES (%s, 1, %s)",
                (team_role_id, now)
            )
            conn.commit()
            cursor.close()
            return True, None

        window_start = coerce_dt(record["window_start_utc"])
        if window_start.tzinfo is None:
            window_start = UTC_TZ.localize(window_start)

        if record.get("blocked_until_utc"):
            blocked_until = coerce_dt(record["blocked_until_utc"])
            if blocked_until.tzinfo is None:
                blocked_until = UTC_TZ.localize(blocked_until)
            if now < blocked_until:
                cursor.close()
                return False, blocked_until

        if (now - window_start).total_seconds() >= 3600:
            cursor.execute(
                "UPDATE challenge_rate_limits SET challenge_count = 1, window_start_utc = %s, blocked_until_utc = NULL WHERE team_role_id = %s",
                (now, team_role_id)
            )
            conn.commit()
            cursor.close()
            return True, None

        new_count = record["challenge_count"] + 1
        if new_count > 5:
            blocked_until = now + timedelta(hours=1)
            cursor.execute(
                "UPDATE challenge_rate_limits SET challenge_count = %s, blocked_until_utc = %s WHERE team_role_id = %s",
                (new_count, blocked_until, team_role_id)
            )
            conn.commit()
            cursor.close()
            return False, blocked_until
        else:
            cursor.execute(
                "UPDATE challenge_rate_limits SET challenge_count = %s WHERE team_role_id = %s",
                (new_count, team_role_id)
            )
            conn.commit()
            cursor.close()
            return True, None
    finally:
        return_db(conn)


def update_leaderboard_cache():
    conn = get_db()
    try:
        cursor = conn.cursor()

        cursor.execute("DELETE FROM leaderboard_cache")

        cursor.execute("SELECT team_role_id, elo, wins, losses FROM teams ORDER BY elo DESC")
        teams = cursor.fetchall()

        for rank, team in enumerate(teams, start=1):
            cursor.execute(
                "INSERT INTO leaderboard_cache (team_role_id, elo, wins, losses, rank, updated_at_utc) VALUES (%s, %s, %s, %s, %s, %s)",
                (team["team_role_id"], team["elo"], team["wins"], team["losses"], rank, utc_now())
            )

        conn.commit()
        cursor.close()
    finally:
        return_db(conn)


def get_leaderboard_from_cache(limit: int = None, offset: int = 0) -> List[dict]:
    conn = get_db()
    try:
        cursor = conn.cursor()
        if limit:
            cursor.execute(
                "SELECT * FROM leaderboard_cache ORDER BY rank ASC LIMIT %s OFFSET %s",
                (limit, offset)
            )
        else:
            cursor.execute("SELECT * FROM leaderboard_cache ORDER BY rank ASC")
        rows = cursor.fetchall()
        cursor.close()
        return [dict(row) for row in rows]
    finally:
        return_db(conn)
