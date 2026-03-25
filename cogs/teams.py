import traceback
from datetime import datetime, timedelta

import discord
from discord import app_commands
from discord.ext import commands

from utils.db import (
    get_db, return_db, get_config, update_config,
    get_team, create_team, update_team, delete_team, disband_team_full,
    get_roster, get_player_team, add_roster_member, remove_roster_member, update_roster_member,
    get_vice_captains, add_vice_captain, remove_vice_captain,
    get_user_affiliation, check_affiliation_allowed, add_affiliation, remove_affiliation,
    update_affiliation_type, clear_team_affiliations,
    get_user_captain_teams, get_user_vice_captain_teams,
    get_all_teams
)
from utils.helpers import (
    EMBED_COLOR, utc_now, safe_defer, post_transaction,
    team_autocomplete, position_autocomplete, rank_autocomplete,
    build_rich_error, get_user_team_authority,
    send_roster_signup_dm, send_captain_assignment_dm,
    validate_roster_addition,
    update_leaderboard, update_leaderboard_cache,
    VALID_POSITIONS, VALID_RANKS, STARTER_LIMITS
)
from utils.permissions import (
    is_team_staff, has_team_authority, is_suspended,
    validate_roster_change, is_league_team, is_roster_locked,
    can_modify_roster, is_team_captain
)
from views.shared_views import ConfirmationView, RosterLockOverrideView


class TeamsCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @app_commands.command(name="register", description="Register a new team (Team Perms only)")
    @app_commands.describe(
        team_role="The team's Discord role",
        captain="The team captain",
        starting_elo="Starting ELO (default 1000)"
    )
    async def register_cmd(
        self,
        interaction: discord.Interaction,
        team_role: discord.Role,
        captain: discord.Member,
        starting_elo: int = 1000
    ):
        await safe_defer(interaction, ephemeral=False)

        try:
            if not is_team_staff(interaction.user):
                await interaction.followup.send(content="Only Team Perms staff can use this command.")
                return

            if is_suspended(captain):
                await interaction.followup.send(content="Cannot assign a suspended user as captain.")
                return

            allowed, error_msg = check_affiliation_allowed(captain.id, team_role.id, interaction.guild)
            if not allowed:
                await interaction.followup.send(content=error_msg)
                return

            if get_team(team_role.id):
                await interaction.followup.send(content="This team is already registered.")
                return
            create_team(team_role.id, captain.id, starting_elo)

            add_affiliation(captain.id, team_role.id, "CAPTAIN")

            config = get_config()
            captain_role_id = config.get("captain_role_id")
            if captain_role_id:
                captain_discord_role = interaction.guild.get_role(captain_role_id)
                if captain_discord_role:
                    try:
                        await captain.add_roles(captain_discord_role, team_role)
                    except discord.Forbidden:
                        pass

            await update_leaderboard(interaction.guild)

            await post_transaction(
                guild=interaction.guild,
                action="Team Registered",
                staff=interaction.user,
                details={
                    "Team": team_role.mention,
                    "Captain": captain.mention,
                    "Starting ELO": starting_elo
                }
            )

            await interaction.followup.send(
                content=f"Team {team_role.mention} registered with captain {captain.mention} at {starting_elo} ELO."
            )
        except Exception as e:
            await interaction.followup.send(content=f"Error registering team: {str(e)[:200]}")

    @app_commands.command(name="set-captain", description="Change team captain (Team Perms only)")
    @app_commands.describe(team_role="The team", new_captain="The new captain")
    async def set_captain_cmd(
        self,
        interaction: discord.Interaction,
        team_role: discord.Role,
        new_captain: discord.Member
    ):
        await safe_defer(interaction, ephemeral=False)

        try:
            if not is_team_staff(interaction.user):
                await interaction.followup.send(content="Only Team Perms staff can use this command.")
                return

            team_role_id = team_role.id
            team = get_team(team_role_id)
            if not team:
                await interaction.followup.send(content="That team is not registered.")
                return

            if is_suspended(new_captain):
                await interaction.followup.send(content="Cannot assign a suspended user as captain.")
                return

            allowed, error_msg = check_affiliation_allowed(new_captain.id, team_role_id, interaction.guild)
            if not allowed:
                await interaction.followup.send(content=error_msg)
                return
            old_captain_id = team["captain_user_id"]
            config = get_config()
            captain_role_id = config.get("captain_role_id")
            team_discord_role = interaction.guild.get_role(team_role_id)

            update_team(team_role_id, captain_user_id=new_captain.id)

            old_captain_has_affiliation = False
            if old_captain_id:
                roster = get_roster(team_role_id)
                vice_captains = get_vice_captains(team_role_id)
                if old_captain_id in [m["user_id"] for m in roster]:
                    update_affiliation_type(old_captain_id, "ROSTER")
                    old_captain_has_affiliation = True
                elif old_captain_id in vice_captains:
                    update_affiliation_type(old_captain_id, "VICE")
                    old_captain_has_affiliation = True
                else:
                    remove_affiliation(old_captain_id)
                    old_captain_has_affiliation = False

            if old_captain_id and captain_role_id:
                old_captain = interaction.guild.get_member(old_captain_id)
                captain_discord_role = interaction.guild.get_role(captain_role_id)
                if old_captain and captain_discord_role:
                    other_teams = [t for t in get_all_teams() if t["captain_user_id"] == old_captain_id and t["team_role_id"] != team_role_id]
                    if not other_teams:
                        try:
                            await old_captain.remove_roles(captain_discord_role)
                        except discord.Forbidden:
                            pass

            if old_captain_id and not old_captain_has_affiliation:
                old_captain = interaction.guild.get_member(old_captain_id)
                if old_captain and team_discord_role:
                    try:
                        await old_captain.remove_roles(team_discord_role)
                    except discord.Forbidden:
                        print(f"[SET-CAPTAIN] No permission to remove team role from old captain {old_captain_id}")
                    except Exception as e:
                        print(f"[SET-CAPTAIN] Error removing team role from old captain: {e}")

            add_affiliation(new_captain.id, team_role_id, "CAPTAIN")

            if captain_role_id:
                captain_discord_role = interaction.guild.get_role(captain_role_id)
                if captain_discord_role:
                    try:
                        await new_captain.add_roles(captain_discord_role, team_discord_role)
                    except discord.Forbidden:
                        pass

            await send_captain_assignment_dm(
                user=new_captain,
                team_role_id=team_role_id,
                staff=interaction.user
            )

            await post_transaction(
                guild=interaction.guild,
                action="Captain Changed",
                staff=interaction.user,
                details={
                    "Team": team_discord_role.mention if team_discord_role else 'Unknown',
                    "New Captain": new_captain.mention
                }
            )

            await interaction.followup.send(content=f"Captain of {team_discord_role.mention if team_discord_role else 'the team'} is now {new_captain.mention}.")
        except Exception as e:
            await interaction.followup.send(content=f"Error changing captain: {str(e)[:200]}")

    @app_commands.command(name="vice-captain", description="Add or remove a vice captain")
    @app_commands.describe(
        team_role="The team",
        user="The user to add/remove as vice captain",
        action="Add or remove"
    )
    @app_commands.choices(action=[
        app_commands.Choice(name="Add", value="add"),
        app_commands.Choice(name="Remove", value="remove")
    ])
    async def vice_captain_cmd(
        self,
        interaction: discord.Interaction,
        team_role: discord.Role,
        user: discord.Member,
        action: str
    ):
        await safe_defer(interaction, ephemeral=False)

        try:
            team_role_id = team_role.id
            team = get_team(team_role_id)
            if not team:
                await interaction.followup.send(content="That team is not registered.")
                return

            if not is_team_staff(interaction.user) and not is_team_captain(team_role_id, interaction.user):
                await interaction.followup.send(content="Only the captain or staff can manage vice captains.")
                return

            if action == "add":
                if is_suspended(user):
                    await interaction.followup.send(content="Cannot assign a suspended user as vice captain.")
                    return

                allowed, error_msg = check_affiliation_allowed(user.id, team_role_id, interaction.guild)
                if not allowed:
                    await interaction.followup.send(content=error_msg)
                    return
            config = get_config()
            vice_captain_role_id = config.get("vice_captain_role_id")
            team_discord_role = interaction.guild.get_role(team_role_id)

            if action == "add":
                add_vice_captain(team_role_id, user.id)

                current_aff = get_user_affiliation(user.id)
                if current_aff is None:
                    add_affiliation(user.id, team_role_id, "VICE")
                elif current_aff["affiliation_type"] == "ROSTER":
                    update_affiliation_type(user.id, "VICE")

                if vice_captain_role_id:
                    vc_role = interaction.guild.get_role(vice_captain_role_id)
                    if vc_role:
                        try:
                            await user.add_roles(vc_role, team_discord_role)
                        except discord.Forbidden:
                            pass

                await interaction.followup.send(content=f"{user.mention} is now a vice captain of {team_discord_role.mention if team_discord_role else 'the team'}.")

            else:  
                remove_vice_captain(team_role_id, user.id)

                remaining_authority = get_user_team_authority(user.id, team_role_id)
                if remaining_authority == "CAPTAIN":
                    pass
                elif remaining_authority == "ROSTER":
                    update_affiliation_type(user.id, "ROSTER")
                else:
                    remove_affiliation(user.id)

                if vice_captain_role_id:
                    all_vc_teams = []
                    for t in get_all_teams():
                        if user.id in get_vice_captains(t["team_role_id"]):
                            all_vc_teams.append(t["team_role_id"])

                    if not all_vc_teams:
                        vc_role = interaction.guild.get_role(vice_captain_role_id)
                        if vc_role:
                            try:
                                await user.remove_roles(vc_role)
                            except discord.Forbidden:
                                pass

                await interaction.followup.send(content=f"{user.mention} is no longer a vice captain of {team_discord_role.mention if team_discord_role else 'the team'}.")
        except Exception as e:
            await interaction.followup.send(content=f"Error managing vice captain: {str(e)[:200]}")

    @app_commands.command(name="disband", description="Disband a team (Team Perms only)")
    @app_commands.describe(team_role="The team to disband")
    async def disband_cmd(self, interaction: discord.Interaction, team_role: discord.Role):
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
            member_count = len(roster)

            conn = get_db()
            try:
                cursor = conn.cursor()
                cursor.execute(
                    """SELECT match_id, team1_role_id, team2_role_id, status
                       FROM matches
                       WHERE (team1_role_id = %s OR team2_role_id = %s)
                         AND status IN ('OPEN', 'SCHEDULED')""",
                    (team_role_id, team_role_id)
                )
                active_matches = cursor.fetchall()

                cursor.execute(
                    """SELECT COUNT(*) as recent_count FROM no_show_log
                       WHERE team_role_id = %s
                         AND logged_at_utc >= NOW() - INTERVAL '7 days'""",
                    (team_role_id,)
                )
                no_show_row = cursor.fetchone()
                recent_no_shows = no_show_row["recent_count"] if no_show_row else 0

                cursor.close()
            finally:
                return_db(conn)

            protection_warnings = []
            if active_matches:
                match_ids = ", ".join([f"#{m['match_id']}" for m in active_matches[:5]])
                protection_warnings.append(f"**{len(active_matches)} active match(es):** {match_ids}")

            if recent_no_shows > 0:
                protection_warnings.append(f"**{recent_no_shows} no-show(s) in last 7 days**")

            if protection_warnings:
                warning_msg = "\n".join(protection_warnings)
                warning_msg += "\n\n**These issues should be resolved before disbanding.**\nProceed anyway?"

                pass 

            async def execute_disband(confirm_interaction: discord.Interaction):
                roster = get_roster(team_role_id)
                vice_captains = get_vice_captains(team_role_id)
                captain_id = team["captain_user_id"]

                config = get_config()
                captain_role_id = config.get("captain_role_id")
                vice_captain_role_id = config.get("vice_captain_role_id")

                success, error_msg, channel_ids_to_delete = disband_team_full(team_role_id)

                if not success:
                    print(f"[DISBAND ERROR] Failed to disband team {team_role_id}: {error_msg}")
                    await interaction.followup.send(
                        content=f"Failed to disband team: database error. Please contact an administrator.\nError: {error_msg}"
                    )
                    return

                cleanup_warnings = []

                for channel_id in channel_ids_to_delete:
                    channel = interaction.guild.get_channel(channel_id)
                    if channel:
                        try:
                            await channel.delete(reason=f"Team {team_name} disbanded")
                        except discord.Forbidden:
                            cleanup_warnings.append(f"Could not delete channel <#{channel_id}>")
                        except Exception as e:
                            print(f"[DISBAND] Failed to delete channel {channel_id}: {e}")
                            cleanup_warnings.append(f"Failed to delete match channel")

                role_cleanup_failed = False

                for member_data in roster:
                    member = interaction.guild.get_member(member_data["user_id"])
                    if member and team_discord_role:
                        try:
                            await member.remove_roles(team_discord_role)
                        except discord.Forbidden:
                            role_cleanup_failed = True
                        except Exception:
                            role_cleanup_failed = True

                if captain_id and captain_role_id:
                    captain = interaction.guild.get_member(captain_id)
                    captain_discord_role = interaction.guild.get_role(captain_role_id)
                    if captain and captain_discord_role:
                        other_captain_teams = [t for t in get_all_teams() if t["captain_user_id"] == captain_id]
                        if not other_captain_teams:
                            try:
                                await captain.remove_roles(captain_discord_role)
                            except discord.Forbidden:
                                role_cleanup_failed = True
                            except Exception:
                                role_cleanup_failed = True
                        if team_discord_role:
                            try:
                                await captain.remove_roles(team_discord_role)
                            except discord.Forbidden:
                                role_cleanup_failed = True
                            except Exception:
                                role_cleanup_failed = True

                if vice_captain_role_id:
                    vc_role = interaction.guild.get_role(vice_captain_role_id)
                    for vc_id in vice_captains:
                        vc_member = interaction.guild.get_member(vc_id)
                        if vc_member:
                            is_vc_elsewhere = False
                            for t in get_all_teams():
                                if vc_id in get_vice_captains(t["team_role_id"]):
                                    is_vc_elsewhere = True
                                    break
                            if not is_vc_elsewhere and vc_role:
                                try:
                                    await vc_member.remove_roles(vc_role)
                                except discord.Forbidden:
                                    role_cleanup_failed = True
                                except Exception:
                                    role_cleanup_failed = True
                            if team_discord_role:
                                try:
                                    await vc_member.remove_roles(team_discord_role)
                                except discord.Forbidden:
                                    role_cleanup_failed = True
                                except Exception:
                                    role_cleanup_failed = True

                if role_cleanup_failed:
                    cleanup_warnings.append("Some member roles could not be removed")

                team_role_deleted = False
                if team_discord_role:
                    try:
                        await team_discord_role.delete(reason="Team disbanded")
                        team_role_deleted = True
                    except discord.Forbidden:
                        print(f"[DISBAND] No permission to delete role {team_role_id}")
                        cleanup_warnings.append("Could not delete team role (no permission)")
                    except Exception as e:
                        print(f"[DISBAND] Failed to delete role {team_role_id}: {e}")
                        cleanup_warnings.append("Failed to delete team role")

                try:
                    update_leaderboard_cache()
                    await update_leaderboard(interaction.guild)
                except Exception as lb_err:
                    print(f"[DISBAND] Failed to update leaderboard: {lb_err}")

                try:
                    await post_transaction(
                        guild=interaction.guild,
                        action="Team Disbanded",
                        staff=interaction.user,
                        details={"Team": team_name}
                    )
                except Exception as tx_err:
                    print(f"[DISBAND] Failed to post transaction: {tx_err}")

                if cleanup_warnings:
                    await interaction.followup.send(
                        content=f"Team **{team_name}** has been disbanded from the database.\n"
                        f"Some cleanup issues: {'; '.join(cleanup_warnings)}"
                    )
                else:
                    await interaction.followup.send(content=f"Team **{team_name}** has been disbanded.")

            warning_text = (
                f"**Warning:** This will permanently disband **{team_name}**\n"
                f"• {member_count} member(s) will lose their team role\n"
                f"• All active matches will be cancelled\n"
                f"• Team channels will be deleted\n"
                f"• This action cannot be undone"
            )

            if protection_warnings:
                warning_text += "\n\n" + "\n".join(protection_warnings)

            view = ConfirmationView(
                action_name="Disband Team",
                warning_text=warning_text,
                on_confirm_callback=execute_disband,
                interaction_context=interaction
            )

            await interaction.followup.send(
                content=f"**Disband {team_name}?**\n{warning_text}",
                view=view,
                ephemeral=True
            )

        except Exception as e:
            print(f"[DISBAND ERROR] Failed for team_role={team_role}, guild={interaction.guild.id}")
            print(traceback.format_exc())
            await interaction.followup.send(content="Failed to disband team. Please try again or contact an administrator.")

    @app_commands.command(name="add-member", description="Add a member to the roster")
    @app_commands.describe(
        team_role="The team",
        user="The user to add",
        position="Player position",
        rank="Starter or Substitute"
    )
    @app_commands.autocomplete(position=position_autocomplete, rank=rank_autocomplete)
    async def add_member_cmd(
        self,
        interaction: discord.Interaction,
        team_role: discord.Role,
        user: discord.Member,
        position: str,
        rank: str
    ):
        await safe_defer(interaction, ephemeral=False)

        try:
            team_role_id = team_role.id
            team = get_team(team_role_id)
            if not team:
                await interaction.followup.send(content="That team is not registered.")
                return

            allowed, reason = can_modify_roster(interaction.user, team_role_id)
            if not allowed:
                await interaction.followup.send(content=reason)
                return

            if position not in VALID_POSITIONS:
                await interaction.followup.send(content=f"Invalid position. Choose from: {', '.join(VALID_POSITIONS)}")
                return
            if rank not in VALID_RANKS:
                await interaction.followup.send(content=f"Invalid rank. Choose from: {', '.join(VALID_RANKS)}")
                return

            if is_suspended(user):
                await interaction.followup.send(content="**Cannot sign a suspended user.**\n\nSuspended players cannot be added to rosters, even during roster lock override.")
                return

            roster_allowed, roster_error, needs_override = validate_roster_change(team_role_id, interaction.user, "add member")
            if not roster_allowed:
                await interaction.followup.send(content=roster_error, ephemeral=True)
                return

            if needs_override:
                async def confirm_add_member(confirmation_interaction: discord.Interaction):
                    allowed_aff, error_msg = check_affiliation_allowed(user.id, team_role_id, interaction.guild)
                    if not allowed_aff:
                        await confirmation_interaction.followup.send(content=error_msg, ephemeral=True)
                        return

                    valid, err = validate_roster_addition(team_role_id, position, rank)
                    if not valid:
                        await confirmation_interaction.followup.send(content=err, ephemeral=True)
                        return

                    add_roster_member(team_role_id, user.id, position, rank)
                    current_aff = get_user_affiliation(user.id)
                    if current_aff is None:
                        add_affiliation(user.id, team_role_id, "ROSTER")

                    team_discord_role = interaction.guild.get_role(team_role_id)
                    if team_discord_role:
                        try:
                            await user.add_roles(team_discord_role)
                        except discord.Forbidden:
                            pass

                    await send_roster_signup_dm(
                        user=user,
                        team_role_id=team_role_id,
                        position=position,
                        rank=rank,
                        signer=interaction.user
                    )

                    await post_transaction(
                        guild=interaction.guild,
                        action="Player Signed (Roster Lock Override)",
                        staff=interaction.user,
                        details={
                            "Player": user.mention,
                            "Team": team_discord_role.mention if team_discord_role else 'Unknown',
                            "Position": position,
                            "Rank": rank
                        }
                    )

                    await confirmation_interaction.followup.send(
                        content=f"**Roster Lock Override**\n{user.mention} added to {team_discord_role.mention if team_discord_role else 'the team'} as {rank} {position}.",
                        ephemeral=False
                    )

                team_discord_role = interaction.guild.get_role(team_role_id)
                team_name = team_discord_role.name if team_discord_role else f"Team {team_role_id}"
                view = RosterLockOverrideView(
                    action_description=f"Add {user.mention} to **{team_name}** as {rank} {position}",
                    on_confirm_callback=confirm_add_member,
                    original_interaction=interaction
                )
                await interaction.followup.send(
                    content=f"**Roster Lock Override Required**\n\n"
                            f"**Action:** Add {user.mention} to **{team_name}**\n"
                            f"**Position:** {rank} {position}\n\n"
                            f"Roster lock is currently enabled for league teams.\n"
                            f"Confirm to override and proceed with this roster change.",
                    view=view,
                    ephemeral=True
                )
                return

            allowed_aff, error_msg = check_affiliation_allowed(user.id, team_role_id, interaction.guild)
            if not allowed_aff:
                await interaction.followup.send(content=error_msg)
                return

            valid, err = validate_roster_addition(team_role_id, position, rank)
            if not valid:
                await interaction.followup.send(content=err)
                return
            
            add_roster_member(team_role_id, user.id, position, rank)

            current_aff = get_user_affiliation(user.id)
            if current_aff is None:
                add_affiliation(user.id, team_role_id, "ROSTER")

            team_discord_role = interaction.guild.get_role(team_role_id)
            if team_discord_role:
                try:
                    await user.add_roles(team_discord_role)
                except discord.Forbidden:
                    pass

            await send_roster_signup_dm(
                user=user,
                team_role_id=team_role_id,
                position=position,
                rank=rank,
                signer=interaction.user
            )

            await post_transaction(
                guild=interaction.guild,
                action="Player Signed",
                staff=interaction.user,
                details={
                    "Player": user.mention,
                    "Team": team_discord_role.mention if team_discord_role else 'Unknown',
                    "Position": position,
                    "Rank": rank
                }
            )

            await interaction.followup.send(
                content=f"{user.mention} added to {team_discord_role.mention if team_discord_role else 'the team'} as {rank} {position}."
            )
        except Exception as e:
            await interaction.followup.send(content=f"Error adding member: {str(e)[:200]}")

    @app_commands.command(name="remove-member", description="Remove a member from the roster")
    @app_commands.describe(team_role="The team", user="The user to remove")
    async def remove_member_cmd(self, interaction: discord.Interaction, team_role: discord.Role, user: discord.Member):
        await safe_defer(interaction, ephemeral=False)

        try:
            team_role_id = team_role.id
            team = get_team(team_role_id)
            if not team:
                await interaction.followup.send(content="That team is not registered.")
                return

            allowed, reason = can_modify_roster(interaction.user, team_role_id)
            if not allowed:
                await interaction.followup.send(content=reason)
                return

            existing = get_player_team(user.id)
            if existing != team_role_id:
                await interaction.followup.send(content=f"{user.mention} is not on this team.")
                return

            if is_team_captain(team_role_id, user):
                await interaction.followup.send(
                    content="You cannot remove the captain from their team. Transfer ownership with /set-captain or disband the team.",
                    ephemeral=True
                )
                return

            roster_allowed, roster_error, needs_override = validate_roster_change(team_role_id, interaction.user, "remove member")
            if not roster_allowed:
                await interaction.followup.send(content=roster_error, ephemeral=True)
                return

            if needs_override:
                async def confirm_remove_member(confirmation_interaction: discord.Interaction):
                    remove_roster_member(team_role_id, user.id)

                    remaining_authority = get_user_team_authority(user.id, team_role_id)
                    if remaining_authority == "CAPTAIN":
                        pass
                    elif remaining_authority == "VICE":
                        update_affiliation_type(user.id, "VICE")
                    else:
                        remove_affiliation(user.id)

                    team_discord_role = interaction.guild.get_role(team_role_id)
                    if team_discord_role:
                        try:
                            await user.remove_roles(team_discord_role)
                        except discord.Forbidden:
                            pass

                    await post_transaction(
                        guild=interaction.guild,
                        action="Player Released (Roster Lock Override)",
                        staff=interaction.user,
                        details={
                            "Player": user.mention,
                            "Team": team_discord_role.mention if team_discord_role else 'Unknown'
                        }
                    )

                    await confirmation_interaction.followup.send(
                        content=f"**Roster Lock Override**\n{user.mention} removed from {team_discord_role.mention if team_discord_role else 'the team'}.",
                        ephemeral=False
                    )

                team_discord_role = interaction.guild.get_role(team_role_id)
                team_name = team_discord_role.name if team_discord_role else f"Team {team_role_id}"
                view = RosterLockOverrideView(
                    action_description=f"Remove {user.mention} from **{team_name}**",
                    on_confirm_callback=confirm_remove_member,
                    original_interaction=interaction
                )
                await interaction.followup.send(
                    content=f"**Roster Lock Override Required**\n\n"
                            f"**Action:** Remove {user.mention} from **{team_name}**\n\n"
                            f"Roster lock is currently enabled for league teams.\n"
                            f"Confirm to override and proceed with this roster change.",
                    view=view,
                    ephemeral=True
                )
                return

            remove_roster_member(team_role_id, user.id)

            remaining_authority = get_user_team_authority(user.id, team_role_id)
            if remaining_authority == "CAPTAIN":
                pass
            elif remaining_authority == "VICE":
                update_affiliation_type(user.id, "VICE")
            else:
                remove_affiliation(user.id)

            team_discord_role = interaction.guild.get_role(team_role_id)
            if team_discord_role:
                try:
                    await user.remove_roles(team_discord_role)
                except discord.Forbidden:
                    pass

            await post_transaction(
                guild=interaction.guild,
                action="Player Released",
                staff=interaction.user,
                details={
                    "Player": user.mention,
                    "Team": team_discord_role.mention if team_discord_role else 'Unknown'
                }
            )

            await interaction.followup.send(content=f"{user.mention} removed from {team_discord_role.mention if team_discord_role else 'the team'}.")
        except Exception as e:
            await interaction.followup.send(content=f"Error removing member: {str(e)[:200]}")

    @app_commands.command(name="leave-team", description="Leave your current team")
    async def leave_team_cmd(self, interaction: discord.Interaction):
        """Allow a player to voluntarily leave their team (captains cannot use this)."""
        await safe_defer(interaction, ephemeral=True)

        try:
            user = interaction.user

            player_team_role_id = get_player_team(user.id)
            if not player_team_role_id:
                await interaction.followup.send(
                    content="You are not currently signed to a team.",
                    ephemeral=True
                )
                return

            team = get_team(player_team_role_id)
            if not team:
                await interaction.followup.send(
                    content="Team not found in database.",
                    ephemeral=True
                )
                return

            if is_team_captain(player_team_role_id, user):
                await interaction.followup.send(
                    content="Team captains cannot leave their team. Transfer ownership with /set-captain or disband the team.",
                    ephemeral=True
                )
                return

            conn = get_db()
            try:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT match_id, status
                    FROM matches
                    WHERE (team1_role_id = %s OR team2_role_id = %s)
                      AND status IN ('OPEN', 'SCHEDULED')
                    LIMIT 1
                """, (player_team_role_id, player_team_role_id))
                active_match = cursor.fetchone()
                cursor.close()
            finally:
                return_db(conn)

            if active_match:
                await interaction.followup.send(
                    content="You cannot leave your team while a match is active.",
                    ephemeral=True
                )
                return

            team_discord_role = interaction.guild.get_role(player_team_role_id)
            team_name = team_discord_role.name if team_discord_role else f"Team ID {player_team_role_id}"

            vice_captains = get_vice_captains(player_team_role_id)
            is_vice = user.id in vice_captains

            remove_roster_member(player_team_role_id, user.id)

            if is_vice:
                remove_vice_captain(player_team_role_id, user.id)

            remove_affiliation(user.id)

            if team_discord_role:
                try:
                    await user.remove_roles(team_discord_role)
                except discord.Forbidden:
                    pass  

            if is_vice:
                config = get_config()
                vice_captain_role_id = config.get("vice_captain_role_id")
                if vice_captain_role_id:
                    all_vc_teams = []
                    for t in get_all_teams():
                        if user.id in get_vice_captains(t["team_role_id"]):
                            all_vc_teams.append(t["team_role_id"])

                    if not all_vc_teams:
                        vc_role = interaction.guild.get_role(vice_captain_role_id)
                        if vc_role and vc_role in user.roles:
                            try:
                                await user.remove_roles(vc_role)
                            except discord.Forbidden:
                                pass

            await post_transaction(
                guild=interaction.guild,
                action="Player Left Team",
                staff=interaction.user,  
                details={
                    "Player": user.mention,
                    "Team": team_name
                }
            )

            await interaction.followup.send(
                content=f"You have successfully left **{team_name}**.",
                ephemeral=True
            )

        except ValueError as ve:
            await interaction.followup.send(
                content=f"{str(ve)}",
                ephemeral=True
            )
        except Exception as e:
            print(f"[LEAVE-TEAM ERROR] Failed for user={interaction.user.id}, guild={interaction.guild.id}")
            print(traceback.format_exc())
            await interaction.followup.send(
                content="Failed to leave team. Please contact staff.",
                ephemeral=True
            )

    @app_commands.command(name="swap-member", description="Swap a roster member's position/rank")
    @app_commands.describe(
        team_role="The team",
        user="The player to swap",
        new_position="New position",
        new_rank="New rank (Starter or Substitute)"
    )
    @app_commands.autocomplete(new_position=position_autocomplete, new_rank=rank_autocomplete)
    async def swap_member_cmd(
        self,
        interaction: discord.Interaction,
        team_role: discord.Role,
        user: discord.Member,
        new_position: str,
        new_rank: str
    ):
        await safe_defer(interaction, ephemeral=False)

        try:
            team_role_id = team_role.id
            team = get_team(team_role_id)
            if not team:
                await interaction.followup.send(content="That team is not registered.")
                return

            allowed, reason = can_modify_roster(interaction.user, team_role_id)
            if not allowed:
                await interaction.followup.send(content=reason)
                return

            roster_allowed, roster_error, needs_override = validate_roster_change(team_role_id, interaction.user, "swap member")
            if not roster_allowed:
                await interaction.followup.send(content=roster_error, ephemeral=True)
                return

            if new_position not in VALID_POSITIONS:
                await interaction.followup.send(content=f"Invalid position. Choose from: {', '.join(VALID_POSITIONS)}")
                return
            if new_rank not in VALID_RANKS:
                await interaction.followup.send(content=f"Invalid rank. Choose from: {', '.join(VALID_RANKS)}")
                return

            existing = get_player_team(user.id)
            if existing != team_role_id:
                await interaction.followup.send(content=f"{user.mention} is not on this team.")
                return

            roster = get_roster(team_role_id)
            member_data = next((m for m in roster if m["user_id"] == user.id), None)
            if not member_data:
                await interaction.followup.send(content="Member not found.")
                return
            old_position = member_data["position"]
            old_rank = member_data["rank"]

            if needs_override:
                async def confirm_swap_member(confirmation_interaction: discord.Interaction):
                    remove_roster_member(team_role_id, user.id)

                    valid, err = validate_roster_addition(team_role_id, new_position, new_rank)
                    if not valid:
                        add_roster_member(team_role_id, user.id, old_position, old_rank)
                        await confirmation_interaction.followup.send(content=err, ephemeral=True)
                        return

                    add_roster_member(team_role_id, user.id, new_position, new_rank)

                    team_discord_role = interaction.guild.get_role(team_role_id)
                    await confirmation_interaction.followup.send(
                        content=f"**Roster Lock Override**\n{user.mention} updated: {old_rank} {old_position} → {new_rank} {new_position}",
                        ephemeral=False
                    )

                team_discord_role = interaction.guild.get_role(team_role_id)
                team_name = team_discord_role.name if team_discord_role else f"Team {team_role_id}"
                view = RosterLockOverrideView(
                    action_description=f"Swap {user.mention}'s position: {old_rank} {old_position} → {new_rank} {new_position}",
                    on_confirm_callback=confirm_swap_member,
                    original_interaction=interaction
                )
                await interaction.followup.send(
                    content=f"**Roster Lock Override Required**\n\n"
                            f"**Action:** Update {user.mention} on **{team_name}**\n"
                            f"**From:** {old_rank} {old_position}\n"
                            f"**To:** {new_rank} {new_position}\n\n"
                            f"Roster lock is currently enabled for league teams.\n"
                            f"Confirm to override and proceed with this roster change.",
                    view=view,
                    ephemeral=True
                )
                return

            remove_roster_member(team_role_id, user.id)

            valid, err = validate_roster_addition(team_role_id, new_position, new_rank)
            if not valid:
                add_roster_member(team_role_id, user.id, old_position, old_rank)
                await interaction.followup.send(content=err)
                return

            add_roster_member(team_role_id, user.id, new_position, new_rank)

            team_discord_role = interaction.guild.get_role(team_role_id)
            await interaction.followup.send(
                content=f"{user.mention} updated: {old_rank} {old_position} → {new_rank} {new_position}"
            )
        except Exception as e:
            await interaction.followup.send(content=f"Error swapping member: {str(e)[:200]}")

    @app_commands.command(name="promote-sub", description="Promote a substitute to starter (swaps with existing starter)")
    @app_commands.describe(team_role="The team", substitute="The substitute to promote")
    async def promote_sub_cmd(self, interaction: discord.Interaction, team_role: discord.Role, substitute: discord.Member):
        await safe_defer(interaction, ephemeral=False)

        try:
            team_role_id = team_role.id
            team = get_team(team_role_id)
            if not team:
                await interaction.followup.send(content="That team is not registered.")
                return

            allowed, reason = can_modify_roster(interaction.user, team_role_id)
            if not allowed:
                await interaction.followup.send(content=reason)
                return

            roster_allowed, roster_error, needs_override = validate_roster_change(team_role_id, interaction.user, "promote substitute")
            if not roster_allowed:
                await interaction.followup.send(content=roster_error, ephemeral=True)
                return

            roster = get_roster(team_role_id)
            sub_data = next((m for m in roster if m["user_id"] == substitute.id), None)

            if not sub_data:
                await interaction.followup.send(content=f"{substitute.mention} is not on this team.")
                return

            if sub_data["rank"] != "Substitute":
                await interaction.followup.send(content=f"{substitute.mention} is not a substitute.")
                return

            position = sub_data["position"]

            starter_to_demote = next(
                (m for m in roster if m["rank"] == "Starter" and m["position"] == position),
                None
            )

            if needs_override:
                async def confirm_promote_sub(confirmation_interaction: discord.Interaction):
                    if not starter_to_demote:
                        starters = [m for m in roster if m["rank"] == "Starter"]
                        position_starters = [m for m in starters if m["position"] == position]

                        if len(position_starters) >= STARTER_LIMITS.get(position, 0):
                            await confirmation_interaction.followup.send(
                                content=f"No starter slot available for {position}. Use /swap-member instead.",
                                ephemeral=True
                            )
                            return

                        update_roster_member(team_role_id, substitute.id, rank="Starter")
                        await confirmation_interaction.followup.send(
                            content=f"**Roster Lock Override**\n{substitute.mention} promoted to Starter {position}.",
                            ephemeral=False
                        )
                        return

                    demoted_user_id = starter_to_demote["user_id"]
                    update_roster_member(team_role_id, substitute.id, rank="Starter")
                    update_roster_member(team_role_id, demoted_user_id, rank="Substitute")

                    demoted_member = interaction.guild.get_member(demoted_user_id)
                    await confirmation_interaction.followup.send(
                        content=f"**Roster Lock Override**\n{substitute.mention} promoted to Starter. {demoted_member.mention if demoted_member else 'Previous starter'} demoted to Substitute.",
                        ephemeral=False
                    )

                team_discord_role = interaction.guild.get_role(team_role_id)
                team_name = team_discord_role.name if team_discord_role else f"Team {team_role_id}"
                if starter_to_demote:
                    demoted_member = interaction.guild.get_member(starter_to_demote["user_id"])
                    action_desc = f"Promote {substitute.mention} to Starter (demotes {demoted_member.mention if demoted_member else 'starter'})"
                else:
                    action_desc = f"Promote {substitute.mention} to Starter {position}"

                view = RosterLockOverrideView(
                    action_description=action_desc,
                    on_confirm_callback=confirm_promote_sub,
                    original_interaction=interaction
                )
                await interaction.followup.send(
                    content=f"**Roster Lock Override Required**\n\n"
                            f"**Action:** {action_desc} on **{team_name}**\n\n"
                            f"Roster lock is currently enabled for league teams.\n"
                            f"Confirm to override and proceed with this roster change.",
                    view=view,
                    ephemeral=True
                )
                return

            if not starter_to_demote:
                starters = [m for m in roster if m["rank"] == "Starter"]
                position_starters = [m for m in starters if m["position"] == position]

                if len(position_starters) >= STARTER_LIMITS.get(position, 0):
                    await interaction.followup.send(
                        content=f"No starter slot available for {position}. Use /swap-member instead."
                    )
                    return

                update_roster_member(team_role_id, substitute.id, rank="Starter")
                await interaction.followup.send(content=f"{substitute.mention} promoted to Starter {position}.")
                return
            
            demoted_user_id = starter_to_demote["user_id"]
            update_roster_member(team_role_id, substitute.id, rank="Starter")
            update_roster_member(team_role_id, demoted_user_id, rank="Substitute")

            demoted_member = interaction.guild.get_member(demoted_user_id)
            await interaction.followup.send(
                content=f"{substitute.mention} promoted to Starter. {demoted_member.mention if demoted_member else 'Previous starter'} demoted to Substitute."
            )
        except Exception as e:
            await interaction.followup.send(content=f"Error promoting substitute: {str(e)[:200]}")

    @app_commands.command(name="list-member", description="List roster for a team")
    @app_commands.describe(team_role="The team to list")
    async def list_member_cmd(self, interaction: discord.Interaction, team_role: discord.Role):
        await safe_defer(interaction, ephemeral=True)

        try:
            team_role_id = team_role.id
            team = get_team(team_role_id)
            if not team:
                await interaction.followup.send(content="That team is not registered.")
                return

            roster = get_roster(team_role_id)
            team_discord_role = interaction.guild.get_role(team_role_id)
            team_name = team_discord_role.name if team_discord_role else "Unknown"

            captain_id = team["captain_user_id"]
            vice_captains = get_vice_captains(team_role_id)

            captain_member = interaction.guild.get_member(captain_id) if captain_id else None

            embed = discord.Embed(title=f"{team_name} Roster", color=EMBED_COLOR)
            embed.add_field(name="ELO", value=str(team["elo"]), inline=True)
            embed.add_field(name="Record", value=f"{team['wins']}W / {team['losses']}L", inline=True)
            embed.add_field(name="Captain", value=captain_member.mention if captain_member else "None", inline=True)

            if vice_captains:
                vc_mentions = [f"<@{vc_id}>" for vc_id in vice_captains]
                embed.add_field(name="Vice Captains", value=", ".join(vc_mentions), inline=False)

            starters = [m for m in roster if m["rank"] == "Starter"]
            substitutes = [m for m in roster if m["rank"] == "Substitute"]

            if starters:
                starter_lines = [f"<@{m['user_id']}> - {m['position']}" for m in starters]
                embed.add_field(name=f"Starters ({len(starters)}/6)", value="\n".join(starter_lines), inline=False)

            if substitutes:
                sub_lines = [f"<@{m['user_id']}> - {m['position']}" for m in substitutes]
                embed.add_field(name=f"Substitutes ({len(substitutes)}/6)", value="\n".join(sub_lines), inline=False)

            if not roster:
                embed.add_field(name="Roster", value="No players signed.", inline=False)

            await interaction.followup.send(embed=embed)
        except Exception as e:
            await interaction.followup.send(content=f"Error listing roster: {str(e)[:200]}")

    @app_commands.command(name="open-transactions", description="Open the transaction window (Team Perms only)")
    async def open_transactions_cmd(self, interaction: discord.Interaction):
        await safe_defer(interaction, ephemeral=False)

        try:
            if not is_team_staff(interaction.user):
                await interaction.followup.send(content="Only Team Perms staff can use this command.")
                return

            update_config(transactions_open=True)
            await interaction.followup.send(content="Transaction window is now **OPEN**. Captains and vice captains can modify rosters.")
        except Exception as e:
            await interaction.followup.send(content=f"Error: {str(e)[:200]}")

    @app_commands.command(name="close-transactions", description="Close the transaction window (Team Perms only)")
    async def close_transactions_cmd(self, interaction: discord.Interaction):
        await safe_defer(interaction, ephemeral=False)

        try:
            if not is_team_staff(interaction.user):
                await interaction.followup.send(content="Only Team Perms staff can use this command.")
                return

            update_config(transactions_open=False)
            await interaction.followup.send(content="Transaction window is now **CLOSED**. Only staff can modify rosters.")
        except Exception as e:
            await interaction.followup.send(content=f"Error: {str(e)[:200]}")

    @app_commands.command(name="transaction-status", description="Check if the transaction window is open")
    async def transaction_status_cmd(self, interaction: discord.Interaction):
        await safe_defer(interaction, ephemeral=True)

        try:
            config = get_config()
            is_open = config.get("transactions_open", 0)
            status = "**OPEN**" if is_open else "**CLOSED**"
            await interaction.followup.send(content=f"Transaction window is {status}")
        except Exception as e:
            await interaction.followup.send(content=f"Error: {str(e)[:200]}")


async def setup(bot: commands.Bot):
    await bot.add_cog(TeamsCog(bot))
