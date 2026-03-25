import traceback
from datetime import datetime, timedelta
from typing import List, Optional

import discord

from utils.db import (
    get_db, return_db, get_config, get_league_state, update_league_state,
    update_match, get_group_standings, get_group_teams, get_team,
    check_duplicate_league_matchup, check_duplicate_playoff_matchup,
    update_league_standings, award_ref_activity_for_match
)
from utils.helpers import (
    EMBED_COLOR, utc_now, coerce_dt
)
from utils.permissions import (
    has_league_perms, is_league_team, get_unfinished_playoff_matches
)
from views.group_picker import GroupMatchGroupSelect
from views.playoff_views import CreatePlayoffMatchModal



async def update_league_dashboard(guild: discord.Guild):
    state = get_league_state()
    dashboard_channel_id = state.get("dashboard_channel_id")

    if not dashboard_channel_id:
        return

    dashboard_channel = guild.get_channel(dashboard_channel_id)
    if not dashboard_channel:
        return

    embed = discord.Embed(
        title="League Dashboard",
        color=EMBED_COLOR,
        timestamp=utc_now()
    )

    config = get_config()
    league_perms_configured = "Configured" if config.get("league_perms_role_id") else "Not Configured"

    embed.add_field(
        name="League Perms",
        value=league_perms_configured,
        inline=True
    )

    season_status = "ACTIVE" if state["season_active"] else "OFF-SEASON"
    if state["season_locked"]:
        season_status = "LOCKED"

    embed.add_field(
        name="Season Status",
        value=season_status,
        inline=True
    )

    if state["season_name"]:
        embed.add_field(
            name="Season",
            value=state["season_name"],
            inline=True
        )

    if state["current_stage"]:
        embed.add_field(
            name="Stage",
            value=state["current_stage"],
            inline=True
        )

    if state["current_stage"] == "PLAYOFFS" and state.get("current_bracket"):
        embed.add_field(
            name="Bracket",
            value=state["current_bracket"],
            inline=True
        )

    if state["current_round"]:
        step_display = f"Round {state['current_round']}"
        if state["current_stage"] == "GROUPS":
            step_display = f"G_R{state['current_round']}"
        elif state["current_stage"] == "PLAYOFFS" and state.get("current_bracket"):
            bracket_prefix = "W" if state["current_bracket"] == "WINNERS" else "L"
            step_display = f"{bracket_prefix}_R{state['current_round']}"

        embed.add_field(
            name="Current Step",
            value=step_display,
            inline=True
        )

    roster_lock = "Locked" if state["roster_lock_enabled"] else "Open"
    embed.add_field(
        name="Roster Lock",
        value=roster_lock,
        inline=True
    )

    if state["league_deadline_utc"]:
        deadline = coerce_dt(state["league_deadline_utc"])
        unix_ts = int(deadline.timestamp())
        embed.add_field(
            name="Global Deadline",
            value=f"<t:{unix_ts}:R>",
            inline=True
        )
    else:
        embed.add_field(
            name="Global Deadline",
            value="NOT SET",
            inline=True
        )

    current_round = state.get("current_round", 0)
    current_stage = state.get("current_stage")
    current_bracket = state.get("current_bracket")

    if current_stage and current_round > 0:
        conn = get_db()
        try:
            cursor = conn.cursor()

            if current_stage == "PLAYOFFS" and current_bracket:
                cursor.execute("""
                    SELECT
                        COUNT(*) FILTER (WHERE status = 'FINISHED') as finished,
                        COUNT(*) as total
                    FROM matches
                    WHERE mode = 'LEAGUE'
                      AND league_round = %s
                      AND bracket = %s
                """, (current_round, current_bracket))
            else:
                cursor.execute("""
                    SELECT
                        COUNT(*) FILTER (WHERE status = 'FINISHED') as finished,
                        COUNT(*) as total
                    FROM matches
                    WHERE mode = 'LEAGUE'
                      AND league_round = %s
                """, (current_round,))

            result = cursor.fetchone()
            cursor.close()

            if result and result["total"] > 0:
                finished = result["finished"] or 0
                total = result["total"]
                progress_label = "Done" if finished == total else "Pending"
                embed.add_field(
                    name="Match Progress",
                    value=f"[{progress_label}] {finished}/{total} completed",
                    inline=True
                )
        except Exception as e:
            print(f"[DASHBOARD] Error querying match progress: {e}")
        finally:
            return_db(conn)

    if current_stage == "GROUPS":
        conn_checklist = get_db()
        try:
            cursor_checklist = conn_checklist.cursor()
            cursor_checklist.execute("""
                SELECT
                    g.group_id,
                    g.group_name,
                    g.stage_id,
                    COUNT(DISTINCT gt.team_role_id) as team_count,
                    COUNT(DISTINCT m.match_id) as match_count
                FROM league_groups g
                LEFT JOIN group_teams gt ON g.group_id = gt.group_id
                LEFT JOIN matches m ON m.group_id = g.group_id AND m.mode = 'LEAGUE'
                GROUP BY g.group_id, g.group_name, g.stage_id
                ORDER BY g.group_name
            """)
            groups_checklist = cursor_checklist.fetchall()
            cursor_checklist.close()

            if groups_checklist:
                checklist_lines = []
                for grp in groups_checklist:
                    team_count = grp["team_count"]
                    match_count = grp["match_count"]
                    expected_matches = (team_count * (team_count - 1)) // 2 if team_count > 1 else 0
                    status_mark = "[done]" if match_count >= expected_matches else "[incomplete]"
                    checklist_lines.append(f"{status_mark} {grp['group_name']}: {match_count}/{expected_matches} matches")

                if checklist_lines:
                    embed.add_field(
                        name="Expected Matches",
                        value="\n".join(checklist_lines[:5]),  
                        inline=False
                    )
        except Exception as e:
            print(f"[DASHBOARD] Error generating expected matches checklist: {e}")
        finally:
            return_db(conn_checklist)

    issues = []
    conn = get_db()
    try:
        cursor = conn.cursor()

        if current_stage and current_round > 0:
            if current_stage == "PLAYOFFS" and current_bracket:
                cursor.execute("""
                    SELECT COUNT(*) as cnt
                    FROM matches
                    WHERE mode = 'LEAGUE'
                      AND status IN ('OPEN', 'SCHEDULED')
                      AND bracket = %s
                      AND league_round = %s
                """, (current_bracket, current_round))
            else:
                cursor.execute("""
                    SELECT COUNT(*) as cnt
                    FROM matches
                    WHERE mode = 'LEAGUE'
                      AND status IN ('OPEN', 'SCHEDULED')
                      AND league_round = %s
                """, (current_round,))

            unfinished_result = cursor.fetchone()
            unfinished_count = unfinished_result["cnt"] if unfinished_result else 0

            if unfinished_count > 0:
                issues.append(f"{unfinished_count} unfinished match(es) in current step")

        cursor.execute("""
            SELECT COUNT(*) as cnt
            FROM matches
            WHERE mode = 'LEAGUE'
              AND status IN ('OPEN', 'SCHEDULED')
              AND deadline_utc IS NOT NULL
              AND deadline_utc + INTERVAL '2 hours' < NOW()
        """)
        overdue_result = cursor.fetchone()
        overdue_count = overdue_result["cnt"] if overdue_result else 0

        if overdue_count > 0:
            issues.append(f"{overdue_count} overdue match(es) (>2h)")

        if current_stage == "GROUPS":
            ready, tie_issues = check_playoff_readiness()
            if not ready:
                for issue in tie_issues:
                    if "Tie" in issue or "tie" in issue:
                        issues.append(issue)

        cursor.close()
    except Exception as e:
        print(f"[DASHBOARD] Error checking issues: {e}")
    finally:
        return_db(conn)

    if issues:
        embed.add_field(
            name="Issues",
            value="\n".join(issues[:5]),  
            inline=False
        )

    embed.set_footer(text="League Dashboard")

    view = LeagueDashboardView()

    try:
        async for message in dashboard_channel.history(limit=50):
            if message.author == guild.me and message.embeds:
                if message.embeds[0].title == "League Dashboard":
                    await message.edit(embed=embed, view=view)
                    return
    except Exception as e:
        print(f"[LEAGUE DASHBOARD] Error finding existing message: {e}")

    try:
        msg = await dashboard_channel.send(embed=embed, view=view)
        await msg.pin()
    except Exception as e:
        print(f"[LEAGUE DASHBOARD] Error posting dashboard: {e}")


async def log_league_action(guild: discord.Guild, action: str, actor: discord.Member, details: dict):
    """
    Log league actions to dashboard channel.
    Creates formatted embed with action details.
    """
    state = get_league_state()
    dashboard_channel_id = state.get("dashboard_channel_id")

    if not dashboard_channel_id:
        return

    dashboard_channel = guild.get_channel(dashboard_channel_id)
    if not dashboard_channel:
        return

    embed = discord.Embed(
        title=f"{action}",
        color=EMBED_COLOR,
        timestamp=utc_now()
    )

    embed.add_field(name="Actor", value=f"{actor.mention} (ID: {actor.id})", inline=False)

    for key, value in details.items():
        embed.add_field(name=key, value=str(value), inline=True)

    try:
        await dashboard_channel.send(embed=embed)
    except Exception as e:
        print(f"[ACTION LOG] Failed to log action: {e}")


def check_playoff_readiness() -> tuple[bool, list[str]]:
    issues = []

    conn = get_db()
    try:
        cursor = conn.cursor()

        cursor.execute("""
            SELECT COUNT(*) as cnt FROM matches
            WHERE mode = 'LEAGUE'
              AND group_id IS NOT NULL
              AND status IN ('OPEN', 'SCHEDULED')
        """)
        result = cursor.fetchone()
        unfinished_count = result["cnt"] if result else 0

        if unfinished_count > 0:
            issues.append(f"{unfinished_count} unfinished group match(es)")

        cursor.execute("SELECT DISTINCT group_id FROM league_groups")
        groups = cursor.fetchall()

        for group in groups:
            group_id = group["group_id"]
            standings = get_group_standings(group_id)

            if not standings or len(standings) < 2:
                continue

            sets_played_counts = [s["sets_played"] for s in standings]
            if len(set(sets_played_counts)) > 1:
                min_played = min(sets_played_counts)
                max_played = max(sets_played_counts)
                issues.append(f"Group {group_id}: Unequal match counts ({min_played}-{max_played} sets played) - missing matches")

        cursor.execute("SELECT DISTINCT group_id FROM league_groups")
        all_groups = cursor.fetchall()

        for group in all_groups:
            group_id = group["group_id"]
            standings = get_group_standings(group_id)

            if not standings or len(standings) < 2:
                continue

            standings_sorted = sorted(
                standings,
                key=lambda x: (x["sets_won"] - x["sets_lost"]),
                reverse=True
            )

            top2_diff = standings_sorted[0]["sets_won"] - standings_sorted[0]["sets_lost"]

            teams_at_top = []
            for s in standings_sorted:
                if (s["sets_won"] - s["sets_lost"]) == top2_diff:
                    teams_at_top.append(s)

            if len(teams_at_top) >= 3:
                issues.append(f"Group {group_id}: {len(teams_at_top)}-way tie at top (manual resolution required)")
            elif len(teams_at_top) == 2:
                if len(standings_sorted) >= 3:
                    third_diff = standings_sorted[2]["sets_won"] - standings_sorted[2]["sets_lost"]
                    if third_diff == top2_diff:
                        issues.append(f"Group {group_id}: 3+ team tie at top (manual resolution required)")
                    else:
                        issues.append(f"Group {group_id}: Tie in top 2 - tiebreak match required (set diff: {top2_diff})")
                else:
                    issues.append(f"Group {group_id}: Tie in top 2 - tiebreak match required (set diff: {top2_diff})")

            if len(standings_sorted) >= 3:
                top1_diff = standings_sorted[0]["sets_won"] - standings_sorted[0]["sets_lost"]
                top2_diff = standings_sorted[1]["sets_won"] - standings_sorted[1]["sets_lost"]
                top3_diff = standings_sorted[2]["sets_won"] - standings_sorted[2]["sets_lost"]

                if top1_diff > top2_diff and top2_diff == top3_diff:
                    issues.append(f"Group {group_id}: Tie between #2 and #3 - tiebreak required (set diff: {top2_diff})")

        cursor.close()
    finally:
        return_db(conn)

    return (len(issues) == 0, issues)


class SetDeadlineModal(discord.ui.Modal, title="Set Global League Deadline"):
    """Modal for setting global deadline."""

    days = discord.ui.TextInput(
        label="Days",
        placeholder="0-30",
        required=True,
        max_length=2,
        default="0"
    )

    hours = discord.ui.TextInput(
        label="Hours",
        placeholder="0-23",
        required=True,
        max_length=2,
        default="0"
    )

    def __init__(self, guild: discord.Guild):
        super().__init__()
        self.guild = guild

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True, thinking=True)

        try:
            days_val = int(self.days.value)
            hours_val = int(self.hours.value)

            if days_val < 0 or days_val > 30:
                await interaction.followup.send(content="Error: Days must be between 0 and 30.", ephemeral=True)
                return

            if hours_val < 0 or hours_val > 23:
                await interaction.followup.send(content="Error: Hours must be between 0 and 23.", ephemeral=True)
                return

            if days_val == 0 and hours_val == 0:
                await interaction.followup.send(content="Error: Must specify at least some time.", ephemeral=True)
                return

            deadline = utc_now() + timedelta(days=days_val, hours=hours_val)
            update_league_state(league_deadline_utc=deadline)

            unix_ts = int(deadline.timestamp())

            await interaction.followup.send(
                content=f"Global league deadline set to: <t:{unix_ts}:F> (<t:{unix_ts}:R>)",
                ephemeral=True
            )

            await update_league_dashboard(self.guild)

            await log_league_action(
                self.guild,
                "Global Deadline Updated",
                interaction.user,
                {
                    "New Deadline": f"<t:{unix_ts}:F>",
                    "Relative": f"<t:{unix_ts}:R>"
                }
            )

        except ValueError:
            await interaction.followup.send(content="Error: Invalid number format.", ephemeral=True)
        except Exception as e:
            print(f"[SET DEADLINE MODAL ERROR] {e}")
            await interaction.followup.send(content="Error: An error occurred.", ephemeral=True)


class ExtendDeadlinesModal(discord.ui.Modal, title="Extend All League Deadlines"):

    hours = discord.ui.TextInput(
        label="Hours to Extend",
        placeholder="1-72",
        required=True,
        max_length=2
    )

    def __init__(self, guild: discord.Guild):
        super().__init__()
        self.guild = guild

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True, thinking=True)

        try:
            hours_val = int(self.hours.value)

            if hours_val < 1 or hours_val > 72:
                await interaction.followup.send(content="Error: Hours must be between 1 and 72.", ephemeral=True)
                return

            conn = get_db()
            try:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT match_id, deadline_utc FROM matches
                    WHERE mode = 'LEAGUE'
                      AND status IN ('OPEN', 'SCHEDULED')
                      AND deadline_utc IS NOT NULL
                """)
                matches = cursor.fetchall()
                cursor.close()
            finally:
                return_db(conn)

            if not matches:
                await interaction.followup.send(
                    content="No active league matches with deadlines to extend.",
                    ephemeral=True
                )
                return

            for match in matches:
                old_deadline = coerce_dt(match["deadline_utc"])
                new_deadline = old_deadline + timedelta(hours=hours_val)
                update_match(match["match_id"], deadline_utc=new_deadline, overdue_warned=False)

            await interaction.followup.send(
                content=f"Extended {len(matches)} match deadline(s) by {hours_val} hours.",
                ephemeral=True
            )

            await update_league_dashboard(self.guild)

            await log_league_action(
                self.guild,
                "Deadlines Extended",
                interaction.user,
                {
                    "Matches Updated": len(matches),
                    "Hours Extended": hours_val
                }
            )

        except ValueError:
            await interaction.followup.send(content="Error: Invalid number format.", ephemeral=True)
        except Exception as e:
            print(f"[EXTEND DEADLINES MODAL ERROR] {e}")
            await interaction.followup.send(content="Error: An error occurred.", ephemeral=True)


class StandingsView(discord.ui.View):

    def __init__(self):
        super().__init__(timeout=60)
        self.add_item(StandingsGroupSelect())


class StandingsGroupSelect(discord.ui.Select):

    def __init__(self):
        # Get all groups
        conn = get_db()
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT group_id, group_name FROM league_groups ORDER BY group_name")
            groups = cursor.fetchall()
            cursor.close()
        finally:
            return_db(conn)

        if not groups:
            options = [discord.SelectOption(label="No groups exist", value="none")]
        else:
            options = [
                discord.SelectOption(label=f"{g['group_name']} (ID: {g['group_id']})", value=str(g['group_id']))
                for g in groups[:25]  # Max 25 options
            ]

        super().__init__(
            placeholder="Select a group to view standings...",
            options=options,
            min_values=1,
            max_values=1
        )

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True, thinking=True)

        if self.values[0] == "none":
            await interaction.followup.send(content="No groups exist yet.", ephemeral=True)
            return

        group_id = int(self.values[0])

        standings = get_group_standings(group_id)

        if not standings:
            await interaction.followup.send(
                content=f"No standings data for Group {group_id}.",
                ephemeral=True
            )
            return

        embed = discord.Embed(
            title=f"Group {group_id} Standings",
            color=EMBED_COLOR,
            timestamp=utc_now()
        )

        standings_sorted = sorted(
            standings,
            key=lambda x: (x["sets_won"] - x["sets_lost"]),
            reverse=True
        )

        table_lines = []
        table_lines.append("```")
        table_lines.append(f"{'#':<3} {'Team':<20} {'W':<4} {'L':<4} {'Diff':<6}")
        table_lines.append("-" * 45)

        for idx, s in enumerate(standings_sorted, 1):
            team_role = interaction.guild.get_role(s["team_role_id"])
            team_name = team_role.name if team_role else f"Team {s['team_role_id']}"
            team_name = team_name[:20]  # Truncate long names

            diff = s["sets_won"] - s["sets_lost"]
            diff_str = f"+{diff}" if diff > 0 else str(diff)

            table_lines.append(
                f"{idx:<3} {team_name:<20} {s['sets_won']:<4} {s['sets_lost']:<4} {diff_str:<6}"
            )

        table_lines.append("```")

        embed.description = "\n".join(table_lines)
        embed.set_footer(text="Sorted by Set Differential")

        await interaction.followup.send(embed=embed, ephemeral=True)


class LeagueDashboardView(discord.ui.View):

    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="Change State", style=discord.ButtonStyle.danger, row=0, custom_id="league_change_state")
    async def change_state_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True, thinking=True)

        config = get_config()
        if not config.get("league_perms_role_id"):
            await interaction.followup.send(
                content="**League Perms role not configured.** An administrator must set it using `/setup`.",
                ephemeral=True
            )
            return

        if not has_league_perms(interaction.user):
            await interaction.followup.send(content="You need the League Perms role to use this.", ephemeral=True)
            return

        state = get_league_state()
        current_status = "LOCKED" if state["season_locked"] else ("ACTIVE" if state["season_active"] else "OFF-SEASON")

        view = LeagueStateSelectionView()
        await interaction.followup.send(
            content=f"**Current State:** {current_status}\n\nSelect new state:",
            view=view,
            ephemeral=True
        )

    @discord.ui.button(label="Roster Lock", style=discord.ButtonStyle.secondary, row=0, custom_id="league_toggle_roster")
    async def toggle_roster_lock_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True, thinking=True)

        config = get_config()
        if not config.get("league_perms_role_id"):
            await interaction.followup.send(
                content="**League Perms role not configured.** An administrator must set it using `/setup`.",
                ephemeral=True
            )
            return

        if not has_league_perms(interaction.user):
            await interaction.followup.send(content="You need the League Perms role to use this.", ephemeral=True)
            return

        state = get_league_state()
        new_value = not state["roster_lock_enabled"]

        update_league_state(roster_lock_enabled=new_value)

        status = "LOCKED" if new_value else "OPEN"
        await interaction.followup.send(content=f"Roster lock: **{status}**", ephemeral=True)

        await update_league_dashboard(interaction.guild)

        dashboard_channel = interaction.guild.get_channel(state["dashboard_channel_id"])
        if dashboard_channel:
            await dashboard_channel.send(
                content=f"**Roster Lock Changed** by {interaction.user.mention}\nNew status: **{status}**"
            )

    @discord.ui.button(label="Set Deadline", style=discord.ButtonStyle.secondary, row=0, custom_id="league_set_deadline")
    async def set_deadline_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        config = get_config()
        if not config.get("league_perms_role_id"):
            await interaction.response.send_message(
                content="**League Perms role not configured.** An administrator must set it using `/setup`.",
                ephemeral=True
            )
            return

        if not has_league_perms(interaction.user):
            await interaction.response.send_message(
                content="You need the League Perms role to use this.",
                ephemeral=True
            )
            return

        modal = SetDeadlineModal(interaction.guild)
        await interaction.response.send_modal(modal)

    @discord.ui.button(label="Start Round", style=discord.ButtonStyle.primary, row=1, custom_id="league_start_round")
    async def start_round_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Start next round (validates no unfinished matches). """
        await interaction.response.defer(ephemeral=True, thinking=True)

        config = get_config()
        if not config.get("league_perms_role_id"):
            await interaction.followup.send(
                content="**League Perms role not configured.** An administrator must set it using `/setup`.",
                ephemeral=True
            )
            return

        if not has_league_perms(interaction.user):
            await interaction.followup.send(content="You need the League Perms role to use this.", ephemeral=True)
            return

        state = get_league_state()
        current_stage = state.get("current_stage")
        current_bracket = state.get("current_bracket")

        if current_stage == "PLAYOFFS" and current_bracket:
            unfinished = get_unfinished_playoff_matches(current_bracket)

            if unfinished:
                match_list = "\n".join([f"• Match #{m['match_id']}" for m in unfinished[:10]])
                if len(unfinished) > 10:
                    match_list += f"\n... and {len(unfinished) - 10} more"

                await interaction.followup.send(
                    content=f"**Cannot Start Next Round**\n\n{len(unfinished)} unfinished {current_bracket} bracket matches:\n{match_list}\n\nAll matches must be completed first.",
                    ephemeral=True
                )
                return

        else:
            conn = get_db()
            try:
                cursor = conn.cursor()
                cursor.execute(
                    """SELECT match_id FROM matches
                       WHERE mode = 'LEAGUE' AND status IN ('OPEN', 'SCHEDULED')"""
                )
                unfinished = cursor.fetchall()
                cursor.close()
            finally:
                return_db(conn)

            if unfinished:
                match_list = "\n".join([f"• Match #{m['match_id']}" for m in unfinished[:10]])
                if len(unfinished) > 10:
                    match_list += f"\n... and {len(unfinished) - 10} more"

                await interaction.followup.send(
                    content=f"**Cannot Start Next Round**\n\n{len(unfinished)} unfinished matches:\n{match_list}\n\nAll matches must be completed first.",
                    ephemeral=True
                )
                return

        new_round = state["current_round"] + 1
        bracket_info = f" ({current_bracket} bracket)" if current_stage == "PLAYOFFS" and current_bracket else ""
        view = ConfirmStartRoundView(new_round, current_bracket if current_stage == "PLAYOFFS" else None)
        await interaction.followup.send(
            content=f"**Start Round {new_round}{bracket_info}?**\n\nThis will advance to Round {new_round}.\nConfirm to proceed.",
            view=view,
            ephemeral=True
        )

    @discord.ui.button(label="Extend Deadlines", style=discord.ButtonStyle.secondary, row=1, custom_id="league_extend_deadlines")
    async def extend_deadlines_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        config = get_config()
        if not config.get("league_perms_role_id"):
            await interaction.response.send_message(
                content="**League Perms role not configured.** An administrator must set it using `/setup`.",
                ephemeral=True
            )
            return

        if not has_league_perms(interaction.user):
            await interaction.response.send_message(
                content="You need the League Perms role to use this.",
                ephemeral=True
            )
            return

        modal = ExtendDeadlinesModal(interaction.guild)
        await interaction.response.send_modal(modal)

    @discord.ui.button(label="Create Match", style=discord.ButtonStyle.success, row=1, custom_id="league_create_match")
    async def create_match_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Create a league match. Context-aware modal."""
        config = get_config()
        if not config.get("league_perms_role_id"):
            await interaction.response.send_message(
                content="**League Perms role not configured.** An administrator must set it using `/setup`.",
                ephemeral=True
            )
            return

        if not has_league_perms(interaction.user):
            await interaction.response.send_message(
                content="You need the League Perms role to use this.",
                ephemeral=True
            )
            return

        state = get_league_state()
        current_stage = state.get("current_stage")

        if current_stage == "PLAYOFFS":
            modal = CreatePlayoffMatchModal(interaction.guild)
            await interaction.response.send_modal(modal)
        else:
            view = discord.ui.View(timeout=60)
            view.add_item(GroupMatchGroupSelect(interaction.guild))
            await interaction.response.send_message(
                content="**Create Group Match**\n\n**Step 1:** Select a group:",
                view=view,
                ephemeral=True
            )

    @discord.ui.button(label="Standings", style=discord.ButtonStyle.secondary, row=2, custom_id="league_view_standings")
    async def view_standings_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """View current standings. Uses Select menu."""
        view = StandingsView()
        await interaction.response.send_message(
            content="**Select a group to view standings:**",
            view=view,
            ephemeral=True
        )

    @discord.ui.button(label="Stage Info", style=discord.ButtonStyle.secondary, row=2, custom_id="league_stage_info")
    async def stage_info_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """View current stage information. Shows bracket info."""
        await interaction.response.defer(ephemeral=True, thinking=True)

        state = get_league_state()

        info = f"**Season:** {state['season_name'] or 'Not set'}\n"
        info += f"**Stage:** {state['current_stage'] or 'Not set'}\n"

        if state['current_stage'] == 'PLAYOFFS' and state.get('current_bracket'):
            info += f"**Bracket:** {state['current_bracket']}\n"

        info += f"**Round:** {state['current_round']}\n"

        season_status = "ACTIVE" if state["season_active"] else "OFF-SEASON"
        if state["season_locked"]:
            season_status = "LOCKED"
        info += f"**Status:** {season_status}\n"

        roster_lock = "Locked" if state["roster_lock_enabled"] else "Open"
        info += f"**Roster Lock:** {roster_lock}\n"

        if state["league_deadline_utc"]:
            deadline = coerce_dt(state["league_deadline_utc"])
            unix_ts = int(deadline.timestamp())
            info += f"**Global Deadline:** <t:{unix_ts}:F>\n"

        await interaction.followup.send(content=info, ephemeral=True)

    @discord.ui.button(label="Begin Playoffs", style=discord.ButtonStyle.danger, row=2, custom_id="league_begin_playoffs")
    async def begin_playoffs_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Begin playoffs with strict validation. Model A implementation."""
        await interaction.response.defer(ephemeral=True, thinking=True)

        config = get_config()
        if not config.get("league_perms_role_id"):
            await interaction.followup.send(
                content="**League Perms role not configured.** An administrator must set it using `/setup`.",
                ephemeral=True
            )
            return

        if not has_league_perms(interaction.user):
            await interaction.followup.send(content="You need the League Perms role to use this.", ephemeral=True)
            return

        state = get_league_state()
        current_stage = state.get("current_stage")

        if current_stage != "GROUPS":
            await interaction.followup.send(
                content="This button is only available during GROUPS stage.",
                ephemeral=True
            )
            return

        ready, issues = check_playoff_readiness()

        if not ready:
            embed = discord.Embed(
                title="Cannot Begin Playoffs",
                description="The following issues must be resolved:",
                color=discord.Color.red(),
                timestamp=utc_now()
            )

            for issue in issues:
                embed.add_field(name=issue, value="", inline=False)

            await interaction.followup.send(embed=embed, ephemeral=True)
            return

        view = ConfirmBeginPlayoffsView()
        await interaction.followup.send(
            content="**All checks passed.**\n\n"
                    "Ready to begin PLAYOFFS stage.\n"
                    "This will transition from GROUPS to PLAYOFFS (Winners R1).\n\n"
                    "**Confirm to proceed.**",
            view=view,
            ephemeral=True
        )

    @discord.ui.button(label="Create Tiebreak Match", style=discord.ButtonStyle.primary, row=3, custom_id="league_create_tiebreak")
    async def create_tiebreak_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True, thinking=True)

        config = get_config()
        if not config.get("league_perms_role_id"):
            await interaction.followup.send(
                content="**League Perms role not configured.** An administrator must set it using `/setup`.",
                ephemeral=True
            )
            return

        if not has_league_perms(interaction.user):
            await interaction.followup.send(content="You need the League Perms role to use this.", ephemeral=True)
            return

        state = get_league_state()
        current_stage = state.get("current_stage")

        if current_stage != "GROUPS":
            await interaction.followup.send(
                content="Tiebreak matches can only be created during GROUPS stage.",
                ephemeral=True
            )
            return

        ready, issues = check_playoff_readiness()
        tie_issues = [issue for issue in issues if "Tie" in issue or "tie" in issue]

        if not tie_issues:
            await interaction.followup.send(
                content="No ties detected. This button is only useful when there are ties in standings.",
                ephemeral=True
            )
            return

        tie_info = "**Detected Ties:**\n\n" + "\n".join(f"• {issue}" for issue in tie_issues)
        tie_info += "\n\n**Use the 'Create Match' button to create a tiebreak match** for the tied teams."
        tie_info += "\n\nTiebreak matches follow the same process as regular matches."

        await interaction.followup.send(content=tie_info, ephemeral=True)


class LeagueStateSelectionView(discord.ui.View):

    def __init__(self):
        super().__init__(timeout=300)

    @discord.ui.button(label="Active", style=discord.ButtonStyle.success)
    async def active_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True, thinking=True)

        update_league_state(season_active=True, season_locked=False)
        await interaction.followup.send(content="Season: **ACTIVE**", ephemeral=True)
        await update_league_dashboard(interaction.guild)

        state = get_league_state()
        dashboard_channel = interaction.guild.get_channel(state["dashboard_channel_id"])
        if dashboard_channel:
            await dashboard_channel.send(
                content=f"**Season State Changed** by {interaction.user.mention}\nNew state: **ACTIVE**"
            )

    @discord.ui.button(label="Off-Season", style=discord.ButtonStyle.secondary)
    async def off_season_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True, thinking=True)

        update_league_state(season_active=False, season_locked=False)
        await interaction.followup.send(content="Season: **OFF-SEASON**", ephemeral=True)
        await update_league_dashboard(interaction.guild)

        state = get_league_state()
        dashboard_channel = interaction.guild.get_channel(state["dashboard_channel_id"])
        if dashboard_channel:
            await dashboard_channel.send(
                content=f"**Season State Changed** by {interaction.user.mention}\nNew state: **OFF-SEASON**"
            )

    @discord.ui.button(label="Locked", style=discord.ButtonStyle.danger)
    async def locked_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True, thinking=True)

        update_league_state(season_locked=True, season_active=False)
        await interaction.followup.send(content="Season: **LOCKED**", ephemeral=True)
        await update_league_dashboard(interaction.guild)

        state = get_league_state()
        dashboard_channel = interaction.guild.get_channel(state["dashboard_channel_id"])
        if dashboard_channel:
            await dashboard_channel.send(
                content=f"**Season State Changed** by {interaction.user.mention}\nNew state: **LOCKED**"
            )


class ConfirmBeginPlayoffsView(discord.ui.View):

    def __init__(self):
        super().__init__(timeout=300)

    @discord.ui.button(label="Confirm Begin Playoffs", style=discord.ButtonStyle.danger)
    async def confirm_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True, thinking=True)

        ready, issues = check_playoff_readiness()

        if not ready:
            await interaction.followup.send(
                content=f"**Cannot Begin Playoffs**\n\n" + "\n".join(issues),
                ephemeral=True
            )
            return

        update_league_state(
            current_stage="PLAYOFFS",
            current_bracket="WINNERS",
            current_round=1
        )

        await interaction.followup.send(
            content="**PLAYOFFS BEGUN**\n\nStage: PLAYOFFS\nBracket: WINNERS\nRound: 1",
            ephemeral=True
        )

        await update_league_dashboard(interaction.guild)

        await log_league_action(
            interaction.guild,
            "Playoffs Begun",
            interaction.user,
            {
                "Stage": "PLAYOFFS",
                "Bracket": "WINNERS",
                "Round": "1"
            }
        )

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_message(content="Cancelled.", ephemeral=True)


class ConfirmStartRoundView(discord.ui.View):

    def __init__(self, new_round: int, bracket: Optional[str] = None):
        super().__init__(timeout=300)
        self.new_round = new_round
        self.bracket = bracket  

    @discord.ui.button(label="Confirm Start Round", style=discord.ButtonStyle.danger)
    async def confirm_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True, thinking=True)

        if self.bracket:
            unfinished = get_unfinished_playoff_matches(self.bracket)
            unfinished_count = len(unfinished)
        else:
            conn = get_db()
            try:
                cursor = conn.cursor()
                cursor.execute(
                    """SELECT COUNT(*) as cnt FROM matches
                       WHERE mode = 'LEAGUE' AND status IN ('OPEN', 'SCHEDULED')"""
                )
                count_row = cursor.fetchone()
                unfinished_count = count_row["cnt"] if count_row else 0
                cursor.close()
            finally:
                return_db(conn)

        if unfinished_count > 0:
            bracket_msg = f" in {self.bracket} bracket" if self.bracket else ""
            await interaction.followup.send(
                content=f"Cannot start - {unfinished_count} unfinished matches{bracket_msg} appeared.",
                ephemeral=True
            )
            return

        update_league_state(current_round=self.new_round)
        bracket_info = f" ({self.bracket} bracket)" if self.bracket else ""
        await interaction.followup.send(content=f"Advanced to **Round {self.new_round}{bracket_info}**.", ephemeral=True)
        await update_league_dashboard(interaction.guild)

        state = get_league_state()
        dashboard_channel = interaction.guild.get_channel(state["dashboard_channel_id"])
        if dashboard_channel:
            await dashboard_channel.send(
                content=f"**Round Started** by {interaction.user.mention}\nNow in: **Round {self.new_round}{bracket_info}**"
            )

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_message(content="Cancelled.", ephemeral=True)
