"""Group match picker flow for creating group stage matches."""

import traceback
from datetime import timedelta

import discord

from utils.db import (
    get_db, return_db, get_config, get_team, get_league_state,
    get_group_teams, check_duplicate_league_matchup,
    update_league_standings, award_ref_activity_for_match
)
from utils.helpers import (
    EMBED_COLOR, utc_now, coerce_dt
)
from utils.permissions import has_league_perms


async def update_league_dashboard(guild: discord.Guild):
    """Update the league dashboard embed and buttons. Shows match progress."""
    from bot import update_league_dashboard as _update_league_dashboard
    await _update_league_dashboard(guild)


async def log_league_action(guild: discord.Guild, action: str, actor: discord.Member, details: dict):
    """
    Log league actions to dashboard channel.
    Creates formatted embed with action details.
    """
    from bot import log_league_action as _log_league_action
    await _log_league_action(guild, action, actor, details)


class GroupMatchRoundModal(discord.ui.Modal, title="Match Details"):
    """Final modal for group match round and deadline."""

    round_num = discord.ui.TextInput(
        label="Round Number",
        placeholder="e.g., 1, 2, 3",
        required=True,
        max_length=3
    )

    deadline_hours = discord.ui.TextInput(
        label="Deadline Hours (optional)",
        placeholder="Leave empty to use global deadline",
        required=False,
        max_length=4
    )

    def __init__(self, guild: discord.Guild, group_id: int, team1_id: int, team2_id: int):
        super().__init__()
        self.guild = guild
        self.group_id = group_id
        self.team1_id = team1_id
        self.team2_id = team2_id

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True, thinking=True)

        try:
            round_num_val = int(self.round_num.value)
            deadline_hours_val = int(self.deadline_hours.value) if self.deadline_hours.value.strip() else None

            # Get teams
            team1 = self.guild.get_role(self.team1_id)
            team2 = self.guild.get_role(self.team2_id)

            team1_data = get_team(team1.id)
            team2_data = get_team(team2.id)

            # Get group and validate
            conn = get_db()
            try:
                cursor = conn.cursor()
                cursor.execute("SELECT stage_id FROM league_groups WHERE group_id = %s", (self.group_id,))
                group = cursor.fetchone()

                if not group:
                    await interaction.followup.send(content="Group not found.", ephemeral=True)
                    cursor.close()
                    return

                stage_id = group["stage_id"]

                # Check for duplicate matchup (Hard block)
                if check_duplicate_league_matchup(team1.id, team2.id, stage_id, round_num_val):
                    embed = discord.Embed(
                        title="Duplicate Match Blocked",
                        description=f"These teams already have a match in this stage/round.",
                        color=discord.Color.red()
                    )
                    embed.add_field(name="Stage", value=stage_id, inline=True)
                    embed.add_field(name="Round", value=round_num_val, inline=True)
                    embed.add_field(name="Teams", value=f"{team1.mention} vs {team2.mention}", inline=False)
                    await interaction.followup.send(embed=embed, ephemeral=True)
                    cursor.close()
                    return

                # Calculate deadline
                if deadline_hours_val is not None:
                    deadline = utc_now() + timedelta(hours=deadline_hours_val)
                else:
                    state = get_league_state()
                    if state.get("league_deadline_utc"):
                        deadline = coerce_dt(state["league_deadline_utc"])
                    else:
                        await interaction.followup.send(
                            content="No global league deadline set. Please specify deadline_hours.",
                            ephemeral=True
                        )
                        cursor.close()
                        return

                # Create match
                cursor.execute("""
                    INSERT INTO matches (
                        team1_role_id, team2_role_id,
                        challenger_team_role_id, challenged_team_role_id,
                        team1_elo_locked, team2_elo_locked, elo_diff_locked,
                        mode, group_id, league_round, deadline_utc,
                        dodge_allowed, status, created_at_utc
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING match_id
                """, (
                    team1.id, team2.id, team1.id, team2.id,
                    team1_data["elo"], team2_data["elo"], abs(team1_data["elo"] - team2_data["elo"]),
                    'LEAGUE', self.group_id, round_num_val, deadline,
                    False, 'OPEN', utc_now()
                ))

                result = cursor.fetchone()
                match_id = result["match_id"]
                conn.commit()
                cursor.close()

            finally:
                return_db(conn)

            await interaction.followup.send(
                content=f"**Match Created** (ID: {match_id})\n"
                        f"{team1.mention} vs {team2.mention}\n"
                        f"Group {self.group_id} • Round {round_num_val}",
                ephemeral=True
            )

            # Refresh dashboard
            await update_league_dashboard(self.guild)

            # Log action
            await log_league_action(
                self.guild,
                "Group Match Created",
                interaction.user,
                {
                    "Match ID": match_id,
                    "Teams": f"{team1.mention} vs {team2.mention}",
                    "Group": self.group_id,
                    "Round": round_num_val
                }
            )

        except ValueError:
            await interaction.followup.send(content="Invalid number format.", ephemeral=True)
        except Exception as e:
            print(f"[GROUP MATCH MODAL ERROR] {e}")
            print(traceback.format_exc())
            await interaction.followup.send(content="An error occurred.", ephemeral=True)


class GroupMatchTeam2Select(discord.ui.Select):
    """Step 3 - Select Team 2 for group match."""

    def __init__(self, guild: discord.Guild, group_id: int, team1_id: int):
        self.guild = guild
        self.group_id = group_id
        self.team1_id = team1_id

        # Get teams in group, excluding team1
        group_teams = get_group_teams(group_id)
        options = []

        for team_id in group_teams:
            if team_id == team1_id:
                continue
            team_role = guild.get_role(team_id)
            if team_role:
                options.append(discord.SelectOption(label=team_role.name, value=str(team_id)))

        if not options:
            options = [discord.SelectOption(label="No other teams in group", value="none")]

        super().__init__(
            placeholder="Select Team 2...",
            options=options[:25],
            min_values=1,
            max_values=1
        )

    async def callback(self, interaction: discord.Interaction):
        if self.values[0] == "none":
            await interaction.response.send_message(
                content="No other teams available in this group.",
                ephemeral=True
            )
            return

        team2_id = int(self.values[0])

        # Open modal for round and deadline
        modal = GroupMatchRoundModal(self.guild, self.group_id, self.team1_id, team2_id)
        await interaction.response.send_modal(modal)


class GroupMatchTeam1Select(discord.ui.Select):
    """Step 2 - Select Team 1 for group match."""

    def __init__(self, guild: discord.Guild, group_id: int):
        self.guild = guild
        self.group_id = group_id

        # Get teams in group
        group_teams = get_group_teams(group_id)
        options = []

        for team_id in group_teams:
            team_role = guild.get_role(team_id)
            if team_role:
                options.append(discord.SelectOption(label=team_role.name, value=str(team_id)))

        if not options:
            options = [discord.SelectOption(label="No teams in group", value="none")]

        super().__init__(
            placeholder="Select Team 1...",
            options=options[:25],
            min_values=1,
            max_values=1
        )

    async def callback(self, interaction: discord.Interaction):
        if self.values[0] == "none":
            await interaction.response.send_message(
                content="No teams in this group.",
                ephemeral=True
            )
            return

        team1_id = int(self.values[0])

        # Show team2 selector
        view = discord.ui.View(timeout=60)
        view.add_item(GroupMatchTeam2Select(self.guild, self.group_id, team1_id))

        team1_role = self.guild.get_role(team1_id)
        await interaction.response.send_message(
            content=f"**Team 1:** {team1_role.mention}\n\n**Now select Team 2:**",
            view=view,
            ephemeral=True
        )


class GroupMatchGroupSelect(discord.ui.Select):
    """Step 1 - Select group for group match creation."""

    def __init__(self, guild: discord.Guild):
        self.guild = guild

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
                for g in groups[:25]
            ]

        super().__init__(
            placeholder="Select a group...",
            options=options,
            min_values=1,
            max_values=1
        )

    async def callback(self, interaction: discord.Interaction):
        if self.values[0] == "none":
            await interaction.response.send_message(
                content="No groups exist. Create a group first.",
                ephemeral=True
            )
            return

        group_id = int(self.values[0])

        # Show team1 selector
        view = discord.ui.View(timeout=60)
        view.add_item(GroupMatchTeam1Select(self.guild, group_id))

        await interaction.response.send_message(
            content=f"**Group {group_id} selected.**\n\n**Now select Team 1:**",
            view=view,
            ephemeral=True
        )


class CreateGroupMatchModal(discord.ui.Modal, title="Create Group Stage Match"):
    """Modal for creating group match. Deprecated - use picker flow instead."""

    group_id = discord.ui.TextInput(
        label="Group ID",
        placeholder="Enter group ID",
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

    deadline_hours = discord.ui.TextInput(
        label="Deadline Hours (optional)",
        placeholder="Leave empty to use global deadline",
        required=False,
        max_length=4
    )

    def __init__(self, guild: discord.Guild):
        super().__init__()
        self.guild = guild

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True, thinking=True)

        try:
            group_id_val = int(self.group_id.value)
            team1_id_val = int(self.team1_id.value)
            team2_id_val = int(self.team2_id.value)
            round_num_val = int(self.round_num.value)
            deadline_hours_val = int(self.deadline_hours.value) if self.deadline_hours.value.strip() else None

            # Validate teams exist
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

            # Validate group exists
            conn = get_db()
            try:
                cursor = conn.cursor()
                cursor.execute("SELECT stage_id FROM league_groups WHERE group_id = %s", (group_id_val,))
                group = cursor.fetchone()

                if not group:
                    await interaction.followup.send(content="Group not found.", ephemeral=True)
                    cursor.close()
                    return

                stage_id = group["stage_id"]

                # Validate both teams are in group
                group_teams = get_group_teams(group_id_val)
                if team1.id not in group_teams or team2.id not in group_teams:
                    await interaction.followup.send(content="Both teams must be in the group.", ephemeral=True)
                    cursor.close()
                    return

                # Check for duplicate matchup
                if check_duplicate_league_matchup(team1.id, team2.id, stage_id, round_num_val):
                    await interaction.followup.send(
                        content="These teams already have a match in this stage/round.",
                        ephemeral=True
                    )
                    cursor.close()
                    return

                # Calculate deadline
                if deadline_hours_val is not None:
                    deadline = utc_now() + timedelta(hours=deadline_hours_val)
                else:
                    state = get_league_state()
                    if state.get("league_deadline_utc"):
                        deadline = coerce_dt(state["league_deadline_utc"])
                    else:
                        await interaction.followup.send(
                            content="No global league deadline set. Please specify deadline_hours.",
                            ephemeral=True
                        )
                        cursor.close()
                        return

                # Create match
                cursor.execute("""
                    INSERT INTO matches (
                        team1_role_id, team2_role_id,
                        challenger_team_role_id, challenged_team_role_id,
                        team1_elo_locked, team2_elo_locked, elo_diff_locked,
                        mode, group_id, league_round, deadline_utc,
                        dodge_allowed, status, created_at_utc
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING match_id
                """, (
                    team1.id, team2.id, team1.id, team2.id,
                    team1_data["elo"], team2_data["elo"], abs(team1_data["elo"] - team2_data["elo"]),
                    'LEAGUE', group_id_val, round_num_val, deadline,
                    False, 'OPEN', utc_now()
                ))

                result = cursor.fetchone()
                match_id = result["match_id"]
                conn.commit()
                cursor.close()

            finally:
                return_db(conn)

            await interaction.followup.send(
                content=f"**Match Created** (ID: {match_id})\n"
                        f"{team1.mention} vs {team2.mention}\n"
                        f"Group {group_id_val} • Round {round_num_val}",
                ephemeral=True
            )

            # Refresh dashboard
            await update_league_dashboard(self.guild)

            # Log action
            await log_league_action(
                self.guild,
                "Group Match Created",
                interaction.user,
                {
                    "Match ID": match_id,
                    "Teams": f"{team1.mention} vs {team2.mention}",
                    "Group": group_id_val,
                    "Round": round_num_val
                }
            )

        except ValueError:
            await interaction.followup.send(content="Invalid number format.", ephemeral=True)
        except Exception as e:
            print(f"[CREATE GROUP MATCH ERROR] {e}")
            print(traceback.format_exc())
            await interaction.followup.send(content="An error occurred.", ephemeral=True)
