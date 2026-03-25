"""Shared UI views used across multiple commands."""

import asyncio
import traceback
from datetime import datetime, timedelta
from typing import List

import discord
import pytz

from utils.db import (
    get_db, return_db, get_config, get_team, get_match,
    get_match_by_channel, update_match, get_no_show, update_no_show,
    get_match_refs, add_match_ref, remove_match_ref,
    award_ref_activity_for_match
)
from utils.helpers import (
    EMBED_COLOR, utc_now, coerce_dt, build_leaderboard_embed,
    safe_defer, update_elo, refresh_match_info_message,
    get_user_team_authority, get_match_leadership_user_ids,
    build_elo_update_embed, build_ref_signup_embed,
    utc_to_cet_str, update_leaderboard_cache
)
from utils.permissions import (
    is_team_staff, is_elo_staff, is_ref, is_suspended,
    has_team_authority
)
from utils.db import CET_TZ, UTC_TZ



class LeaderboardView(discord.ui.View):

    def __init__(self, current_page: int = 0):
        super().__init__(timeout=None)  
        self.current_page = current_page

    @discord.ui.button(label="Prev", style=discord.ButtonStyle.secondary, custom_id="leaderboard_prev")
    async def prev_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()

        try:
            config = get_config()
            if interaction.message.id != config.get("leaderboard_message_id"):
                await interaction.followup.send(content="This button only works on the official leaderboard.", ephemeral=True)
                return

            _, total_pages = build_leaderboard_embed(self.current_page)
            new_page = self.current_page - 1
            if new_page < 0:
                new_page = total_pages - 1  

            self.current_page = new_page
            embed, _ = build_leaderboard_embed(new_page)

            await interaction.message.edit(embed=embed, view=self)
        except Exception as e:
            await interaction.followup.send(content=f"Error: {str(e)[:200]}", ephemeral=True)

    @discord.ui.button(label="Next", style=discord.ButtonStyle.secondary, custom_id="leaderboard_next")
    async def next_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()

        try:
            config = get_config()
            if interaction.message.id != config.get("leaderboard_message_id"):
                await interaction.followup.send(content="This button only works on the official leaderboard.", ephemeral=True)
                return

            _, total_pages = build_leaderboard_embed(self.current_page)
            new_page = self.current_page + 1
            if new_page >= total_pages:
                new_page = 0  

            self.current_page = new_page
            embed, _ = build_leaderboard_embed(new_page)

            await interaction.message.edit(embed=embed, view=self)
        except Exception as e:
            await interaction.followup.send(content=f"Error: {str(e)[:200]}", ephemeral=True)


class EphemeralLeaderboardView(discord.ui.View):

    def __init__(self, user_id: int, current_page: int = 0):
        super().__init__(timeout=900)  
        self.user_id = user_id
        self.current_page = current_page

    @discord.ui.button(label="Prev", style=discord.ButtonStyle.secondary)
    async def prev_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.user_id:
            await interaction.response.send_message(
                content="These buttons are not for you.",
                ephemeral=True
            )
            return

        await interaction.response.defer()

        try:
            _, total_pages = build_leaderboard_embed(self.current_page)
            new_page = self.current_page - 1
            if new_page < 0:
                new_page = total_pages - 1 

            self.current_page = new_page
            embed, _ = build_leaderboard_embed(new_page)

            await interaction.edit_original_response(embed=embed, view=self)
        except Exception as e:
            await interaction.followup.send(content=f"Error: {str(e)[:200]}", ephemeral=True)

    @discord.ui.button(label="Next", style=discord.ButtonStyle.secondary)
    async def next_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.user_id:
            await interaction.response.send_message(
                content="These buttons are not for you.",
                ephemeral=True
            )
            return

        await interaction.response.defer()

        try:
            _, total_pages = build_leaderboard_embed(self.current_page)
            new_page = self.current_page + 1
            if new_page >= total_pages:
                new_page = 0  

            self.current_page = new_page
            embed, _ = build_leaderboard_embed(new_page)

            await interaction.edit_original_response(embed=embed, view=self)
        except Exception as e:
            await interaction.followup.send(content=f"Error: {str(e)[:200]}", ephemeral=True)



class RefSignupView(discord.ui.View):
    def __init__(self, match_id: int):
        super().__init__(timeout=None)
        self.match_id = match_id

    @discord.ui.button(label="Claim Team 1 Side", style=discord.ButtonStyle.primary, custom_id="ref_claim_1")
    async def claim_team1(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.handle_claim(interaction, 1)

    @discord.ui.button(label="Claim Team 2 Side", style=discord.ButtonStyle.primary, custom_id="ref_claim_2")
    async def claim_team2(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.handle_claim(interaction, 2)

    async def handle_claim(self, interaction: discord.Interaction, team_side: int):
        await interaction.response.defer(ephemeral=True, thinking=True)

        try:
            config = get_config()
            ref_role_id = config.get("ref_role_id")

            if ref_role_id:
                ref_role = interaction.guild.get_role(ref_role_id)
                if ref_role and ref_role not in interaction.user.roles:
                    await interaction.edit_original_response(content="You must have the Ref role to claim.")
                    return
            else:
                await interaction.edit_original_response(content="Ref role not configured.")
                return

            match = get_match(self.match_id)
            if not match:
                await interaction.edit_original_response(content="Match not found.")
                return

            if match["status"] not in ("OPEN", "SCHEDULED"):
                await interaction.edit_original_response(content="This match is no longer open for ref signups.")
                return

            team1_role_id = match["team1_role_id"]
            team2_role_id = match["team2_role_id"]
            user_id = interaction.user.id

            team1_authority = get_user_team_authority(user_id, team1_role_id)
            team2_authority = get_user_team_authority(user_id, team2_role_id)

            if team1_authority or team2_authority:
                is_staff_override = is_elo_staff(interaction.user)

                if not is_staff_override:
                    conflict_team = "Team 1" if team1_authority else "Team 2"
                    await interaction.edit_original_response(
                        content=f"**Conflict of Interest:** You are affiliated with {conflict_team} ({team1_authority or team2_authority}).\n"
                                f"Contact staff if you need to referee this match."
                    )
                    return
                else:
                    conflict_team = "Team 1" if team1_authority else "Team 2"
                    print(f"[REF COI OVERRIDE] User {user_id} ({interaction.user.name}) is affiliated with {conflict_team} "
                          f"but has staff override for match {self.match_id}")

                    try:
                        logs_channel_id = config.get("logs_channel_id")
                        if logs_channel_id:
                            logs_channel = interaction.guild.get_channel(logs_channel_id)
                            if logs_channel:
                                await logs_channel.send(
                                    content=f"**Referee COI Override:** {interaction.user.mention} (Staff) is refereeing "
                                            f"Match #{self.match_id} despite affiliation with {conflict_team} ({team1_authority or team2_authority})"
                                )
                    except Exception as log_err:
                        print(f"[REF COI LOG ERROR] Failed to log override: {log_err}")

            existing_ref = self._get_ref_for_match_side(self.match_id, team_side)
            if existing_ref:
                await interaction.edit_original_response(content=f"Team {team_side} side is already claimed.")
                return

            if self._is_user_ref_for_match(self.match_id, interaction.user.id):
                await interaction.edit_original_response(content="You're already signed up for this match.")
                return

            try:
                add_match_ref(self.match_id, team_side, interaction.user.id)
            except Exception as db_err:
                print(f"[REF CLAIM ERROR] DB insert failed for match_id={self.match_id}, user={interaction.user.id}")
                print(traceback.format_exc())
                await interaction.edit_original_response(content="Could not claim this side. Please try again.")
                return

            channel_access_failed = False
            if match["channel_id"]:
                channel = interaction.guild.get_channel(match["channel_id"])
                if channel:
                    try:
                        await self._add_ref_to_channel(channel, interaction.user)
                    except Exception as perm_err:
                        print(f"[REF CLAIM WARNING] Channel permission failed for match_id={self.match_id}, user={interaction.user.id}")
                        print(traceback.format_exc())
                        channel_access_failed = True

            try:
                await self.update_embed(interaction)
            except Exception as embed_err:
                print(f"[REF CLAIM WARNING] Embed update failed for match_id={self.match_id}")
                print(traceback.format_exc())

            try:
                await refresh_match_info_message(self.match_id, interaction.guild)
            except Exception as refresh_err:
                print(f"[REF CLAIM WARNING] Match info refresh failed for match_id={self.match_id}")
                print(traceback.format_exc())

            if channel_access_failed:
                await interaction.edit_original_response(
                    content=f"You've claimed Team {team_side} side.\n"
                    f"Could not add you to match channel - ask staff for access.\n"
                    f"[Go to match channel](https://discord.com/channels/{interaction.guild.id}/{match['channel_id']})"
                )
            else:
                await interaction.edit_original_response(
                    content=f"You've claimed Team {team_side} side. [Go to match channel](https://discord.com/channels/{interaction.guild.id}/{match['channel_id']})"
                )
        except Exception as e:
            print(f"[REF CLAIM ERROR] Unexpected error for match_id={self.match_id}, user={interaction.user.id}")
            print(traceback.format_exc())
            await interaction.edit_original_response(content="Failed to claim ref slot. Please try again.")

    async def update_embed(self, interaction: discord.Interaction):
        """Update the ref signup embed using the standardized helper."""
        match = get_match(self.match_id)
        if not match:
            return

        team1_role = interaction.guild.get_role(match["team1_role_id"])
        team2_role = interaction.guild.get_role(match["team2_role_id"])

        refs = get_match_refs(self.match_id)
        ref1 = next((r for r in refs if r["team_side"] == 1), None)
        ref2 = next((r for r in refs if r["team_side"] == 2), None)
        ref1_user_id = ref1["ref_user_id"] if ref1 else None
        ref2_user_id = ref2["ref_user_id"] if ref2 else None

        scheduled_time = None
        if match.get("scheduled_time_utc"):
            scheduled_time = coerce_dt(match["scheduled_time_utc"])

        embed = build_ref_signup_embed(
            match_id=self.match_id,
            team1_role=team1_role,
            team2_role=team2_role,
            scheduled_time_utc=scheduled_time,
            ref1_user_id=ref1_user_id,
            ref2_user_id=ref2_user_id,
            channel_id=match.get("channel_id"),
            guild_id=interaction.guild.id
        )

        try:
            await interaction.message.edit(embed=embed, view=self)
        except:
            pass

    def _get_ref_for_match_side(self, match_id: int, team_side: int) -> bool:
        """Check if a side is already claimed."""
        refs = get_match_refs(match_id)
        return any(r["team_side"] == team_side for r in refs)

    def _is_user_ref_for_match(self, match_id: int, user_id: int) -> bool:
        """Check if user is already a ref for this match."""
        refs = get_match_refs(match_id)
        return any(r["ref_user_id"] == user_id for r in refs)

    async def _add_ref_to_channel(self, channel: discord.TextChannel, user: discord.Member):
        """Add ref to match channel."""
        config = get_config()
        ref_role_id = config.get("ref_role_id")
        if ref_role_id:
            ref_role = channel.guild.get_role(ref_role_id)
            if ref_role:
                await channel.set_permissions(user, view_channel=True, send_messages=True, manage_messages=False)



class NoShowView(discord.ui.View):
    def __init__(self, no_show_id: int, match_id: int):
        super().__init__(timeout=None)
        self.no_show_id = no_show_id
        self.match_id = match_id
        self.confirm.custom_id = f"noshow_confirm:{self.no_show_id}"
        self.reject.custom_id = f"noshow_reject:{self.no_show_id}"
        self.forfeit.custom_id = f"noshow_forfeit:{self.no_show_id}"

    @discord.ui.button(label="Confirm", style=discord.ButtonStyle.success, custom_id="noshow_confirm")
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True)

        try:
            no_show = get_no_show(self.no_show_id)
            if not no_show:
                await interaction.followup.send(content="No-show report not found.")
                return

            if no_show["status"] != "PENDING":
                await interaction.followup.send(content="This report is already resolved.")
                return

            match = get_match(self.match_id)
            if not match:
                await interaction.followup.send(content="Match not found.")
                return

            accused_team_role_id = no_show["accused_team_role_id"]
            reporter_team_role_id = (
                match["team1_role_id"] if match["team2_role_id"] == accused_team_role_id
                else match["team2_role_id"]
            )

            reporter_role = interaction.guild.get_role(reporter_team_role_id)
            if reporter_role and reporter_role in interaction.user.roles:
                await interaction.followup.send(content="The reporting team cannot confirm their own report.")
                return

            if not self._is_team_captain(accused_team_role_id, interaction.user) and not self._is_team_vice(accused_team_role_id, interaction.user):
                accused_role = interaction.guild.get_role(accused_team_role_id)
                accused_name = accused_role.name if accused_role else f"Team {accused_team_role_id}"
                await interaction.followup.send(
                    content=f"Only the captain or vice captain of **{accused_name}** can respond to this report."
                )
                return

            try:
                update_no_show(self.no_show_id, status="CONFIRMED", reviewed_by_user_id=interaction.user.id)
            except Exception as db_err:
                print(f"[NOSHOW CONFIRM ERROR] DB update failed for no_show_id={self.no_show_id}")
                print(traceback.format_exc())
                await interaction.followup.send(content="Failed to confirm. Database error.")
                return

            await interaction.followup.send(content="No-show confirmed. Use **Forfeit** to apply penalties.")
            await self.update_embed(interaction, "CONFIRMED")
        except Exception as e:
            print(f"[NOSHOW CONFIRM ERROR] Unexpected error for no_show_id={self.no_show_id}")
            print(traceback.format_exc())
            await interaction.followup.send(content="Failed to confirm. Please try again.")

    @discord.ui.button(label="Reject", style=discord.ButtonStyle.danger, custom_id="noshow_reject")
    async def reject(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True)

        try:
            no_show = get_no_show(self.no_show_id)
            if not no_show:
                await interaction.followup.send(content="No-show report not found.")
                return

            if no_show["status"] != "PENDING":
                await interaction.followup.send(content="This report is already resolved.")
                return

            match = get_match(self.match_id)
            if not match:
                await interaction.followup.send(content="Match not found.")
                return

            accused_team_role_id = no_show["accused_team_role_id"]
            reporter_team_role_id = (
                match["team1_role_id"] if match["team2_role_id"] == accused_team_role_id
                else match["team2_role_id"]
            )

            reporter_role = interaction.guild.get_role(reporter_team_role_id)
            if reporter_role and reporter_role in interaction.user.roles:
                await interaction.followup.send(content="The reporting team cannot reject their own report.")
                return

            if not self._is_team_captain(accused_team_role_id, interaction.user) and not self._is_team_vice(accused_team_role_id, interaction.user):
                accused_role = interaction.guild.get_role(accused_team_role_id)
                accused_name = accused_role.name if accused_role else f"Team {accused_team_role_id}"
                await interaction.followup.send(
                    content=f"Only the captain or vice captain of **{accused_name}** can respond to this report."
                )
                return

            try:
                update_no_show(self.no_show_id, status="REJECTED", reviewed_by_user_id=interaction.user.id)
            except Exception as db_err:
                print(f"[NOSHOW REJECT ERROR] DB update failed for no_show_id={self.no_show_id}")
                print(traceback.format_exc())
                await interaction.followup.send(content="Failed to reject. Database error.")
                return

            await interaction.followup.send(content="No-show rejected.")
            await self.update_embed(interaction, "REJECTED")
        except Exception as e:
            print(f"[NOSHOW REJECT ERROR] Unexpected error for no_show_id={self.no_show_id}")
            print(traceback.format_exc())
            await interaction.followup.send(content="Failed to reject. Please try again.")

    @discord.ui.button(label="Forfeit", style=discord.ButtonStyle.secondary, custom_id="noshow_forfeit")
    async def forfeit(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True)

        try:
            no_show = get_no_show(self.no_show_id)
            if not no_show:
                await interaction.followup.send(content="No-show report not found.")
                return

            if no_show["status"] == "RESOLVED":
                await interaction.followup.send(content="Already processed. ELO has already been applied.")
                return
            if no_show["status"] == "REJECTED":
                await interaction.followup.send(content="This report was rejected. Cannot apply forfeit.")
                return
            if no_show["status"] not in ("PENDING", "CONFIRMED"):
                await interaction.followup.send(content="This report is already resolved.")
                return

            match = get_match(self.match_id)
            if not match:
                await interaction.followup.send(content="Match not found.")
                return

            accused_team_role_id = no_show["accused_team_role_id"]
            reporter_team_role_id = (
                match["team1_role_id"] if match["team2_role_id"] == accused_team_role_id
                else match["team2_role_id"]
            )

            reporter_role = interaction.guild.get_role(reporter_team_role_id)
            if reporter_role and reporter_role in interaction.user.roles:
                await interaction.followup.send(content="The reporting team cannot forfeit their own report.")
                return

            if not self._is_team_captain(accused_team_role_id, interaction.user) and not self._is_team_vice(accused_team_role_id, interaction.user):
                accused_role = interaction.guild.get_role(accused_team_role_id)
                accused_name = accused_role.name if accused_role else f"Team {accused_team_role_id}"
                await interaction.followup.send(
                    content=f"Only the captain or vice captain of **{accused_name}** can respond to this report."
                )
                return

            match = get_match(self.match_id)
            if not match:
                await interaction.followup.send(content="Match not found.")
                return

            if match["status"] == "FINISHED":
                await interaction.followup.send(content="Match is already finished. ELO may have been applied.")
                return

            if no_show["status"] == "PENDING":
                update_no_show(self.no_show_id, status="CONFIRMED", reviewed_by_user_id=interaction.user.id)

            loser_team_role_id = no_show["accused_team_role_id"]
            winner_team_role_id = (
                match["team1_role_id"] if match["team2_role_id"] == loser_team_role_id
                else match["team2_role_id"]
            )

            winner_team = get_team(winner_team_role_id)
            loser_team = get_team(loser_team_role_id)

            if not winner_team or not loser_team:
                await interaction.followup.send(content="Team data not found.")
                return

            new_winner_elo = winner_team["elo"] + 30
            new_loser_elo = loser_team["elo"] - 30

            try:
                self._update_team(winner_team_role_id, elo=new_winner_elo, wins=winner_team["wins"] + 1)
                self._update_team(loser_team_role_id, elo=new_loser_elo, losses=loser_team["losses"] + 1)
                update_no_show(self.no_show_id, status="RESOLVED", resolution="FORFEIT", reviewed_by_user_id=interaction.user.id)
                update_match(self.match_id, status="FINISHED", finished_at_utc=utc_now().isoformat())
            except Exception as db_err:
                print(f"[NOSHOW FORFEIT ERROR] DB update failed for no_show_id={self.no_show_id}, match_id={self.match_id}")
                print(traceback.format_exc())
                await interaction.followup.send(content="Failed to apply forfeit. Database error. Contact staff.")
                return

            warnings = []

            winner_role = interaction.guild.get_role(winner_team_role_id)
            loser_role = interaction.guild.get_role(loser_team_role_id)
            winner_name = winner_role.name if winner_role else "Unknown Team"
            loser_name = loser_role.name if loser_role else "Unknown Team"

            scheduled_dt = None
            if match.get("scheduled_time_utc"):
                scheduled_dt = coerce_dt(match["scheduled_time_utc"])

            try:
                elo_embed = build_elo_update_embed(
                    event_type="FORFEIT",
                    team_a_name=winner_name,
                    team_a_old_elo=winner_team["elo"],
                    team_a_new_elo=new_winner_elo,
                    team_a_old_wins=winner_team["wins"],
                    team_a_old_losses=winner_team["losses"],
                    team_a_new_wins=winner_team["wins"] + 1,
                    team_a_new_losses=winner_team["losses"],
                    team_b_name=loser_name,
                    team_b_old_elo=loser_team["elo"],
                    team_b_new_elo=new_loser_elo,
                    team_b_old_wins=loser_team["wins"],
                    team_b_old_losses=loser_team["losses"],
                    team_b_new_wins=loser_team["wins"],
                    team_b_new_losses=loser_team["losses"] + 1,
                    match_id=self.match_id,
                    match_format="Forfeit",
                    scheduled_time_utc=scheduled_dt,
                    channel_id=match.get("channel_id"),
                    guild_id=interaction.guild.id
                )
                await self._post_elo_update(interaction.guild, elo_embed)
            except Exception as elo_err:
                print(f"[NOSHOW FORFEIT WARNING] Failed to post ELO update for match_id={self.match_id}")
                print(traceback.format_exc())
                warnings.append("Could not post to ELO updates channel")

            try:
                update_leaderboard_cache()
                await self._update_leaderboard(interaction.guild)
            except Exception as lb_err:
                print(f"[NOSHOW FORFEIT WARNING] Failed to update leaderboard for match_id={self.match_id}")
                print(traceback.format_exc())
                warnings.append("Could not update leaderboard")

            try:
                cooldown_expires = utc_now() + timedelta(hours=24)
                self._set_cooldown(match["challenger_team_role_id"], match["challenged_team_role_id"], cooldown_expires)
            except Exception as cd_err:
                print(f"[NOSHOW FORFEIT WARNING] Failed to set cooldown for match_id={self.match_id}")
                print(traceback.format_exc())

            await self.update_embed(interaction, "RESOLVED (FORFEIT)")

            if warnings:
                await interaction.followup.send(
                    content=f"Forfeit applied. Winner: {winner_role.mention if winner_role else 'Unknown'} (+30), "
                    f"Loser: {loser_role.mention if loser_role else 'Unknown'} (-30).\n"
                    f"Note: {'; '.join(warnings)}\n"
                    "Channel will be deleted in 30 seconds."
                )
            else:
                await interaction.followup.send(
                    content=f"Forfeit applied. Winner: {winner_role.mention if winner_role else 'Unknown'} (+30), "
                    f"Loser: {loser_role.mention if loser_role else 'Unknown'} (-30). "
                    "Channel will be deleted in 30 seconds."
                )
        except Exception as e:
            print(f"[NOSHOW FORFEIT ERROR] Unexpected error for no_show_id={self.no_show_id}, match_id={self.match_id}")
            print(traceback.format_exc())
            await interaction.followup.send(content="Failed to apply forfeit. Please try again.")
            return

        await asyncio.sleep(30)
        try:
            channel = interaction.channel
            if channel:
                await channel.delete(reason="Match finished (forfeit)")
        except Exception as del_err:
            print(f"[NOSHOW FORFEIT WARNING] Failed to delete channel for match_id={self.match_id}")
            print(traceback.format_exc())

    async def update_embed(self, interaction: discord.Interaction, new_status: str):
        """Update the no-show embed."""
        no_show = get_no_show(self.no_show_id)
        if not no_show:
            return

        match = get_match(self.match_id)
        accused_role = interaction.guild.get_role(no_show["accused_team_role_id"])

        embed = discord.Embed(
            title=f"No-Show Report - Match #{self.match_id}",
            color=EMBED_COLOR
        )
        embed.add_field(name="Accused Team", value=accused_role.mention if accused_role else "Unknown", inline=True)
        embed.add_field(name="Status", value=new_status, inline=True)
        embed.add_field(name="Reason", value=no_show["reason"], inline=False)
        embed.add_field(
            name="Evidence Required",
            value="1) In-game scoreboard screenshot\n2) Proof opponent not in stage/team VC",
            inline=False
        )

        if no_show["reviewed_by_user_id"]:
            embed.add_field(name="Reviewed By", value=f"<@{no_show['reviewed_by_user_id']}>", inline=True)

        if new_status in ("REJECTED", "RESOLVED (FORFEIT)"):
            for child in self.children:
                child.disabled = True

        try:
            await interaction.message.edit(embed=embed, view=self)
        except:
            pass

    def _is_team_captain(self, team_role_id: int, user: discord.Member) -> bool:
        """Check if user is captain of team."""
        team = get_team(team_role_id)
        if not team:
            return False
        return team["captain_user_id"] == user.id

    def _is_team_vice(self, team_role_id: int, user: discord.Member) -> bool:
        """Check if user is vice captain of team."""
        from utils.db import get_vice_captains
        vice_captains = get_vice_captains(team_role_id)
        return user.id in vice_captains

    def _update_team(self, team_role_id: int, **kwargs):
        """Update team record."""
        from utils.db import update_team
        update_team(team_role_id, **kwargs)

    async def _post_elo_update(self, guild: discord.Guild, embed: discord.Embed):
        """Post ELO update to the ELO updates channel."""
        config = get_config()
        elo_channel_id = config.get("elo_channel_id")
        if elo_channel_id:
            channel = guild.get_channel(elo_channel_id)
            if channel:
                await channel.send(embed=embed)

    async def _update_leaderboard(self, guild: discord.Guild):
        """Update the leaderboard message."""
        config = get_config()
        leaderboard_channel_id = config.get("leaderboard_channel_id")
        leaderboard_message_id = config.get("leaderboard_message_id")

        if leaderboard_channel_id and leaderboard_message_id:
            try:
                channel = guild.get_channel(leaderboard_channel_id)
                if channel:
                    message = await channel.fetch_message(leaderboard_message_id)
                    embed, _ = build_leaderboard_embed(0)
                    await message.edit(embed=embed)
            except:
                pass

    def _set_cooldown(self, team1_id: int, team2_id: int, expires_at: datetime):
        """Set cooldown for a matchup."""
        team_a_id = min(team1_id, team2_id)
        team_b_id = max(team1_id, team2_id)

        conn = get_db()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO challenge_cooldowns (challenger_team_role_id, challenged_team_role_id, expires_at_utc)
                       VALUES (%s, %s, %s)
                       ON CONFLICT (challenger_team_role_id, challenged_team_role_id) DO UPDATE SET expires_at_utc = EXCLUDED.expires_at_utc""",
                    (team_a_id, team_b_id, expires_at)
                )
                conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            return_db(conn)



class DodgeMatchView(discord.ui.View):

    def __init__(self, match_id: int, challenged_team_role_id: int):
        super().__init__(timeout=None)
        self.match_id = match_id
        self.challenged_team_role_id = challenged_team_role_id
        self.dodge_button.custom_id = f"dodge_match:{self.match_id}"

    @discord.ui.button(label="Dodge Match", style=discord.ButtonStyle.danger, custom_id="dodge_match")
    async def dodge_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True)

        try:
            match = get_match(self.match_id)
            if not match:
                await interaction.followup.send(content="Match not found.")
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

            if self._is_team_captain(challenged_team_role_id, user):
                is_authorized = True
            elif self._is_team_vice(challenged_team_role_id, user):
                is_authorized = True

            if not is_authorized:
                challenged_role = interaction.guild.get_role(challenged_team_role_id)
                team_name = challenged_role.name if challenged_role else "the challenged team"
                await interaction.followup.send(
                    content=f"Only the captain or vice captain of **{team_name}** (the challenged team) can dodge."
                )
                return

            try:
                update_match(self.match_id, status="CANCELLED")
                self._set_dodge_cooldown(challenger_team_role_id, challenged_team_role_id, hours=24)
            except Exception as db_err:
                print(f"[DODGE ERROR] DB update failed for match_id={self.match_id}")
                print(traceback.format_exc())
                await interaction.followup.send(content="Failed to dodge match. Database error.")
                return

            button.disabled = True
            button.label = "Match Dodged"
            try:
                await interaction.message.edit(view=self)
            except:
                pass

            challenger_role = interaction.guild.get_role(challenger_team_role_id)
            challenged_role = interaction.guild.get_role(challenged_team_role_id)
            challenger_mention = challenger_role.mention if challenger_role else "Challenger"
            challenged_mention = challenged_role.mention if challenged_role else "Challenged team"

            await interaction.channel.send(
                content=f"**Match dodged** by {challenged_mention}. {challenger_mention} cannot challenge {challenged_mention} for 24 hours."
            )

            await interaction.followup.send(content="Match dodged successfully.")

            async def delete_channel_later():
                await asyncio.sleep(10)
                try:
                    await interaction.channel.delete(reason="Match dodged")
                except Exception as del_err:
                    print(f"[DODGE WARNING] Failed to delete channel for match_id={self.match_id}")
                    print(traceback.format_exc())

            asyncio.create_task(delete_channel_later())

        except Exception as e:
            print(f"[DODGE ERROR] Unexpected error for match_id={self.match_id}, user={interaction.user.id}")
            print(traceback.format_exc())
            await interaction.followup.send(content="Failed to dodge match. Please try again.")

    def _is_team_captain(self, team_role_id: int, user: discord.Member) -> bool:
        """Check if user is captain of team."""
        team = get_team(team_role_id)
        if not team:
            return False
        return team["captain_user_id"] == user.id

    def _is_team_vice(self, team_role_id: int, user: discord.Member) -> bool:
        """Check if user is vice captain of team."""
        from utils.db import get_vice_captains
        vice_captains = get_vice_captains(team_role_id)
        return user.id in vice_captains

    def _set_dodge_cooldown(self, challenger_team_role_id: int, challenged_team_role_id: int, hours: int = 24):
        """Set a directional dodge cooldown in the dodge_cooldowns table."""
        until = utc_now() + timedelta(hours=hours)

        conn = get_db()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO dodge_cooldowns (challenger_team_role_id, challenged_team_role_id, until_utc)
                       VALUES (%s, %s, %s)
                       ON CONFLICT (challenger_team_role_id, challenged_team_role_id) DO UPDATE SET until_utc = EXCLUDED.until_utc""",
                    (challenger_team_role_id, challenged_team_role_id, until)
                )
                conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            return_db(conn)



class ConfirmationView(discord.ui.View):
    def __init__(self, action_name: str, warning_text: str, on_confirm_callback, interaction_context):
        super().__init__(timeout=30)
        self.action_name = action_name
        self.warning_text = warning_text
        self.on_confirm_callback = on_confirm_callback
        self.interaction_context = interaction_context
        self.confirmed = False

    @discord.ui.button(label="Yes, Confirm", style=discord.ButtonStyle.danger)
    async def confirm_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.interaction_context.user.id:
            await interaction.response.send_message("Only the command user can confirm.", ephemeral=True)
            return

        self.confirmed = True
        for item in self.children:
            item.disabled = True
        await interaction.response.edit_message(view=self)

        await self.on_confirm_callback(interaction)

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.interaction_context.user.id:
            await interaction.response.send_message("Only the command user can cancel.", ephemeral=True)
            return

        for item in self.children:
            item.disabled = True
        await interaction.response.edit_message(content=f"{self.action_name} cancelled.", view=self)

    async def on_timeout(self):
        if not self.confirmed:
            for item in self.children:
                item.disabled = True



class RosterLockOverrideView(discord.ui.View):
    def __init__(self, action_description: str, on_confirm_callback, original_interaction):
        super().__init__(timeout=60)
        self.action_description = action_description
        self.on_confirm_callback = on_confirm_callback
        self.original_interaction = original_interaction
        self.confirmed = False

    @discord.ui.button(label="Confirm Override", style=discord.ButtonStyle.danger)
    async def confirm_override_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.original_interaction.user.id:
            await interaction.response.send_message("Only the command user can confirm.", ephemeral=True)
            return

        self.confirmed = True
        for item in self.children:
            item.disabled = True
        await interaction.response.edit_message(view=self)

        await self.on_confirm_callback(interaction)

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.original_interaction.user.id:
            await interaction.response.send_message("Only the command user can cancel.", ephemeral=True)
            return

        for item in self.children:
            item.disabled = True
        await interaction.response.edit_message(content="Roster change cancelled.", view=self)



class ScheduleProposalView(discord.ui.View):
    def __init__(self, match_id: int, challenger_team_role_id: int, challenged_team_role_id: int):
        super().__init__(timeout=None)  
        self.match_id = match_id
        self.challenger_team_role_id = challenger_team_role_id
        self.challenged_team_role_id = challenged_team_role_id

        self.accept_button.custom_id = f"schedule_accept_{match_id}"
        self.deny_button.custom_id = f"schedule_deny_{match_id}"

    @discord.ui.button(label="Accept", style=discord.ButtonStyle.success)
    async def accept_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await safe_defer(interaction, ephemeral=True)

        try:
            match = get_match(self.match_id)
            if not match:
                await interaction.followup.send(content="Match not found.", ephemeral=True)
                return

            proposing_team_role_id = match.get("pending_schedule_by_team_role_id")
            if not proposing_team_role_id:
                await interaction.followup.send(
                    content="No active schedule proposal found.",
                    ephemeral=True
                )
                return

            print(f"[SCHEDULE ACCEPT DEBUG] match_id={self.match_id}")
            print(f"  proposing_team_role_id={proposing_team_role_id}")
            print(f"  team1_role_id={match['team1_role_id']}, team2_role_id={match['team2_role_id']}")
            print(f"  clicking_user_id={interaction.user.id}")

            user_is_team1_leadership = self._is_team_captain(match["team1_role_id"], interaction.user) or \
                                      self._is_team_vice(match["team1_role_id"], interaction.user)
            user_is_team2_leadership = self._is_team_captain(match["team2_role_id"], interaction.user) or \
                                      self._is_team_vice(match["team2_role_id"], interaction.user)

            print(f"  user_is_team1_leadership={user_is_team1_leadership}")
            print(f"  user_is_team2_leadership={user_is_team2_leadership}")

            clicker_team_role_id = None
            if user_is_team1_leadership:
                clicker_team_role_id = match["team1_role_id"]
            elif user_is_team2_leadership:
                clicker_team_role_id = match["team2_role_id"]

            print(f"  clicker_team_role_id={clicker_team_role_id}")

            if clicker_team_role_id is None:
                await interaction.followup.send(
                    content="Only the captain or vice captain of either team can accept/deny this proposal.",
                    ephemeral=True
                )
                return

            if clicker_team_role_id == proposing_team_role_id:
                await interaction.followup.send(
                    content="The proposing team cannot accept/deny their own proposal.",
                    ephemeral=True
                )
                return

            print(f"  Accept allowed: clicker is on other team")

            if match["status"] in ("FINISHED", "CANCELLED"):
                await interaction.followup.send(
                    content="This schedule proposal is no longer active.",
                    ephemeral=True
                )
                return

            if not match.get("pending_schedule_time_utc") or not match.get("schedule_pending"):
                await interaction.followup.send(
                    content="This schedule proposal is no longer active.",
                    ephemeral=True
                )
                return

            if match.get("pending_created_at_utc"):
                created_at = coerce_dt(match["pending_created_at_utc"])
                if created_at.tzinfo is None:
                    created_at = UTC_TZ.localize(created_at)
                age = (utc_now() - created_at).total_seconds() / 3600  
                if age > 2:
                    for item in self.children:
                        item.disabled = True
                    await interaction.message.edit(view=self)
                    await interaction.followup.send(
                        content="This proposal has expired (older than 2 hours).",
                        ephemeral=True
                    )
                    return

            proposed_time = coerce_dt(match["pending_schedule_time_utc"])
            conn = get_db()
            try:
                cursor = conn.cursor()
                cursor.execute("""
                    UPDATE matches
                    SET schedule_pending = FALSE
                    WHERE match_id = %s AND schedule_pending = TRUE
                """, (self.match_id,))
                rows_affected = cursor.rowcount
                conn.commit()
                cursor.close()
            finally:
                return_db(conn)

            if rows_affected == 0:
                await interaction.followup.send(
                    content="This proposal was just accepted by someone else.",
                    ephemeral=True
                )
                return

            update_match(
                self.match_id,
                scheduled_time_utc=proposed_time.isoformat(),
                status="SCHEDULED",
                pending_schedule_time_utc=None,
                pending_schedule_by_team_role_id=None,
                pending_schedule_message_id=None,
                pending_created_at_utc=None,
                reminded_captains=False,
                reminded_refs=False,
                reminded_captains_15m=False,
                reminded_refs_15m=False
            )

            if proposed_time.tzinfo is None:
                proposed_time = UTC_TZ.localize(proposed_time)
            cet_dt = proposed_time.astimezone(CET_TZ)
            cet_str = cet_dt.strftime("%d %m %H:%M")
            team1_role = interaction.guild.get_role(match["team1_role_id"])
            team2_role = interaction.guild.get_role(match["team2_role_id"])

            try:
                await self._update_ref_signup_embed(interaction.guild, self.match_id)
            except Exception as ref_err:
                print(f"[SCHEDULE ACCEPT] Failed to update referee channel: {ref_err}")

            confirm_embed = discord.Embed(
                title="Match Scheduled",
                description=f"{team1_role.mention if team1_role else 'Team 1'} vs {team2_role.mention if team2_role else 'Team 2'}",
                color=0x00FFFF
            )
            confirm_embed.add_field(name="Time", value=cet_str, inline=False)
            confirm_embed.add_field(name="Confirmed by", value=interaction.user.mention, inline=False)

            await interaction.channel.send(
                embed=confirm_embed,
                allowed_mentions=discord.AllowedMentions(roles=False)
            )

            for item in self.children:
                item.disabled = True
            await interaction.message.edit(view=self)

            await refresh_match_info_message(self.match_id, interaction.guild)

            await interaction.followup.send(content="Schedule accepted.", ephemeral=True)

        except Exception as e:
            print(f"[SCHEDULE ACCEPT ERROR] {e}")
            print(traceback.format_exc())
            await interaction.followup.send(content="Failed to accept schedule.", ephemeral=True)

    @discord.ui.button(label="Deny", style=discord.ButtonStyle.danger)
    async def deny_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await safe_defer(interaction, ephemeral=True)

        try:
            match = get_match(self.match_id)
            if not match:
                await interaction.followup.send(content="Match not found.", ephemeral=True)
                return

            proposing_team_role_id = match.get("pending_schedule_by_team_role_id")
            if not proposing_team_role_id:
                await interaction.followup.send(
                    content="No active schedule proposal found.",
                    ephemeral=True
                )
                return

            print(f"[SCHEDULE DENY DEBUG] match_id={self.match_id}")
            print(f"  proposing_team_role_id={proposing_team_role_id}")
            print(f"  team1_role_id={match['team1_role_id']}, team2_role_id={match['team2_role_id']}")
            print(f"  clicking_user_id={interaction.user.id}")

            user_is_team1_leadership = self._is_team_captain(match["team1_role_id"], interaction.user) or \
                                      self._is_team_vice(match["team1_role_id"], interaction.user)
            user_is_team2_leadership = self._is_team_captain(match["team2_role_id"], interaction.user) or \
                                      self._is_team_vice(match["team2_role_id"], interaction.user)

            print(f"  user_is_team1_leadership={user_is_team1_leadership}")
            print(f"  user_is_team2_leadership={user_is_team2_leadership}")

            clicker_team_role_id = None
            if user_is_team1_leadership:
                clicker_team_role_id = match["team1_role_id"]
            elif user_is_team2_leadership:
                clicker_team_role_id = match["team2_role_id"]

            print(f"  clicker_team_role_id={clicker_team_role_id}")

            if clicker_team_role_id is None:
                await interaction.followup.send(
                    content="Only the captain or vice captain of either team can accept/deny this proposal.",
                    ephemeral=True
                )
                return

            if clicker_team_role_id == proposing_team_role_id:
                await interaction.followup.send(
                    content="The proposing team cannot accept/deny their own proposal.",
                    ephemeral=True
                )
                return

            print(f"  Deny allowed: clicker is on other team")

            if match["status"] in ("FINISHED", "CANCELLED"):
                await interaction.followup.send(
                    content="This schedule proposal is no longer active.",
                    ephemeral=True
                )
                return

            if not match.get("pending_schedule_time_utc") or not match.get("schedule_pending"):
                await interaction.followup.send(
                    content="This schedule proposal is no longer active.",
                    ephemeral=True
                )
                return
            
            conn = get_db()
            try:
                cursor = conn.cursor()
                cursor.execute("""
                    UPDATE matches
                    SET schedule_pending = FALSE,
                        pending_schedule_time_utc = NULL,
                        pending_schedule_by_team_role_id = NULL,
                        pending_schedule_message_id = NULL,
                        pending_created_at_utc = NULL
                    WHERE match_id = %s AND schedule_pending = TRUE
                """, (self.match_id,))
                rows_affected = cursor.rowcount
                conn.commit()
                cursor.close()
            finally:
                return_db(conn)

            if rows_affected == 0:
                await interaction.followup.send(
                    content="This proposal was just processed by someone else.",
                    ephemeral=True
                )
                return

            print(f"[SCHEDULE DENY] Proposal denied for match_id={self.match_id}")

            await interaction.channel.send(
                content=f"Schedule proposal denied by {interaction.user.mention}."
            )

            for item in self.children:
                item.disabled = True
            await interaction.message.edit(view=self)

            await interaction.followup.send(
                content="You can propose a different time using /schedule DD MM HH:MM",
                ephemeral=True
            )

        except Exception as e:
            print(f"[SCHEDULE DENY ERROR] {e}")
            print(traceback.format_exc())
            await interaction.followup.send(content="Failed to deny schedule.", ephemeral=True)

    def _is_team_captain(self, team_role_id: int, user: discord.Member) -> bool:
        team = get_team(team_role_id)
        if not team:
            return False
        return team["captain_user_id"] == user.id

    def _is_team_vice(self, team_role_id: int, user: discord.Member) -> bool:
        from utils.db import get_vice_captains
        vice_captains = get_vice_captains(team_role_id)
        return user.id in vice_captains

    async def _update_ref_signup_embed(self, guild: discord.Guild, match_id: int):
        match = get_match(match_id)
        if not match or not match.get("ref_signup_message_id"):
            return

        config = get_config()
        ref_channel_id = config.get("ref_channel_id")
        if not ref_channel_id:
            return

        try:
            channel = guild.get_channel(ref_channel_id)
            if channel:
                message = await channel.fetch_message(match["ref_signup_message_id"])

                team1_role = guild.get_role(match["team1_role_id"])
                team2_role = guild.get_role(match["team2_role_id"])

                refs = get_match_refs(match_id)
                ref1 = next((r for r in refs if r["team_side"] == 1), None)
                ref2 = next((r for r in refs if r["team_side"] == 2), None)
                ref1_user_id = ref1["ref_user_id"] if ref1 else None
                ref2_user_id = ref2["ref_user_id"] if ref2 else None

                scheduled_time = None
                if match.get("scheduled_time_utc"):
                    scheduled_time = coerce_dt(match["scheduled_time_utc"])

                embed = build_ref_signup_embed(
                    match_id=match_id,
                    team1_role=team1_role,
                    team2_role=team2_role,
                    scheduled_time_utc=scheduled_time,
                    ref1_user_id=ref1_user_id,
                    ref2_user_id=ref2_user_id,
                    channel_id=match.get("channel_id"),
                    guild_id=guild.id
                )

                await message.edit(embed=embed)
        except:
            pass



class RefListView(discord.ui.View):
    def __init__(self, matches: List[dict], page: int, guild: discord.Guild):
        super().__init__(timeout=120)
        self.matches = matches
        self.page = page
        self.guild = guild
        self.per_page = 5
        self.max_pages = (len(matches) + self.per_page - 1) // self.per_page

        if self.page > 0:
            self.add_item(RefListPrevButton())
        if self.page < self.max_pages - 1:
            self.add_item(RefListNextButton())

    def get_embed(self) -> discord.Embed:
        start = self.page * self.per_page
        end = start + self.per_page
        page_matches = self.matches[start:end]

        embed = discord.Embed(
            title="Matches Needing Refs",
            color=EMBED_COLOR
        )
        embed.set_footer(text=f"Page {self.page + 1}/{self.max_pages}")

        for match in page_matches:
            team1_role = self.guild.get_role(match["team1_role_id"])
            team2_role = self.guild.get_role(match["team2_role_id"])

            refs = get_match_refs(match["match_id"])
            ref1 = next((r for r in refs if r["team_side"] == 1), None)
            ref2 = next((r for r in refs if r["team_side"] == 2), None)

            ref_status = []
            if not ref1:
                ref_status.append(f"Team 1 ({team1_role.name if team1_role else '?'})")
            if not ref2:
                ref_status.append(f"Team 2 ({team2_role.name if team2_role else '?'})")

            scheduled = "Not scheduled"
            if match["scheduled_time_utc"]:
                scheduled_dt = coerce_dt(match["scheduled_time_utc"])
                scheduled = utc_to_cet_str(scheduled_dt) + " CET"

            value = (
                f"**Teams:** {team1_role.name if team1_role else '?'} vs {team2_role.name if team2_role else '?'}\n"
                f"**Scheduled:** {scheduled}\n"
                f"**Open slots:** {', '.join(ref_status)}\n"
                f"**Channel:** <#{match['channel_id']}>"
            )

            embed.add_field(
                name=f"Match #{match['match_id']}",
                value=value,
                inline=False
            )

        return embed


class RefListPrevButton(discord.ui.Button):
    def __init__(self):
        super().__init__(style=discord.ButtonStyle.secondary, label="Previous")

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.defer()

        try:
            view: RefListView = self.view
            view.page -= 1
            view.clear_items()
            if view.page > 0:
                view.add_item(RefListPrevButton())
            if view.page < view.max_pages - 1:
                view.add_item(RefListNextButton())
            await interaction.message.edit(embed=view.get_embed(), view=view)
        except Exception as e:
            await interaction.followup.send(content=f"Error: {str(e)[:200]}", ephemeral=True)


class RefListNextButton(discord.ui.Button):
    def __init__(self):
        super().__init__(style=discord.ButtonStyle.secondary, label="Next")

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.defer()

        try:
            view: RefListView = self.view
            view.page += 1
            view.clear_items()
            if view.page > 0:
                view.add_item(RefListPrevButton())
            if view.page < view.max_pages - 1:
                view.add_item(RefListNextButton())
            await interaction.message.edit(embed=view.get_embed(), view=view)
        except Exception as e:
            await interaction.followup.send(content=f"Error: {str(e)[:200]}", ephemeral=True)



class RefActivityView(discord.ui.View):
    def __init__(self, guild_id: int, invoker_id: int, mode: str = "7d", timeout: int = 120):
        super().__init__(timeout=timeout)
        self.guild_id = guild_id
        self.invoker_id = invoker_id
        self.mode = mode  # "7d" or "all-time"
        self.page = 0
        self.per_page = 10
        self.ref_stats = []
        self.max_pages = 0
        self.message = None

        # Load initial data
        self._load_data()
        self._update_buttons()

    def _load_data(self):
        conn = get_db()
        cursor = None
        try:
            cursor = conn.cursor()

            if self.mode == "7d":
                cutoff_time = utc_now() - timedelta(days=7)
                query = """
                    SELECT
                        ref_user_id,
                        COUNT(*) as match_count
                    FROM ref_activity_awards
                    WHERE guild_id = %s
                      AND awarded_at_utc >= %s
                    GROUP BY ref_user_id
                    ORDER BY match_count DESC, ref_user_id ASC
                """
                cursor.execute(query, (self.guild_id, cutoff_time))
            else:
                query = """
                    SELECT
                        ref_user_id,
                        COUNT(*) as match_count
                    FROM ref_activity_awards
                    WHERE guild_id = %s
                    GROUP BY ref_user_id
                    ORDER BY match_count DESC, ref_user_id ASC
                """
                cursor.execute(query, (self.guild_id,))

            rows = cursor.fetchall()

            self.ref_stats = []
            for rank, row in enumerate(rows, start=1):
                self.ref_stats.append({
                    "rank": rank,
                    "ref_user_id": row["ref_user_id"],
                    "match_count": row["match_count"]
                })

            self.max_pages = (len(self.ref_stats) + self.per_page - 1) // self.per_page if self.ref_stats else 1

        finally:
            if cursor:
                cursor.close()
            return_db(conn)

    def _update_buttons(self):
        self.prev_button.disabled = (self.page == 0)
        self.next_button.disabled = (self.page >= self.max_pages - 1) or (self.max_pages == 0)

        if self.mode == "7d":
            self.timeframe_button.label = "Show All-Time"
            self.timeframe_button.emoji = None
        else:
            self.timeframe_button.label = "Show 7 Days"
            self.timeframe_button.emoji = None

    def get_embed(self) -> discord.Embed:
        timeframe_text = "Last 7 Days" if self.mode == "7d" else "All-Time"

        embed = discord.Embed(
            title=f"Referee Activity - {timeframe_text}",
            color=0x00FFFF
        )

        if not self.ref_stats:
            embed.description = "No referee activity found in this timeframe."
            return embed

        start_idx = self.page * self.per_page
        end_idx = min(start_idx + self.per_page, len(self.ref_stats))
        page_stats = self.ref_stats[start_idx:end_idx]

        embed.description = f"Page {self.page + 1}/{self.max_pages}"

        lines = []
        for stat in page_stats:
            rank = stat["rank"]
            ref_id = stat["ref_user_id"]
            match_count = stat["match_count"]

            line = f"**{rank}.** <@{ref_id}> - **{match_count}** matches"
            lines.append(line)

        embed.add_field(
            name="Rankings",
            value="\n".join(lines),
            inline=False
        )

        embed.set_footer(text="Activity awarded when match results are recorded via /bo3 or /bo5")
        return embed

    @discord.ui.button(label="Prev", style=discord.ButtonStyle.secondary, row=0)
    async def prev_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Go to previous page."""
        await safe_defer(interaction, ephemeral=True)

        if self.page > 0:
            self.page -= 1
            self._update_buttons()
            await interaction.message.edit(embed=self.get_embed(), view=self)
            await interaction.followup.send(content="Done.", ephemeral=True)
        else:
            await interaction.followup.send(content="Already on first page.", ephemeral=True)

    @discord.ui.button(label="Next", style=discord.ButtonStyle.secondary, row=0)
    async def next_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Go to next page."""
        await safe_defer(interaction, ephemeral=True)

        if self.page < self.max_pages - 1:
            self.page += 1
            self._update_buttons()
            await interaction.message.edit(embed=self.get_embed(), view=self)
            await interaction.followup.send(content="Done.", ephemeral=True)
        else:
            await interaction.followup.send(content="Already on last page.", ephemeral=True)

    @discord.ui.button(label="Show All-Time", style=discord.ButtonStyle.primary, row=1)
    async def timeframe_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Toggle between 7d and all-time view."""
        await safe_defer(interaction, ephemeral=True)

        self.mode = "all-time" if self.mode == "7d" else "7d"
        self.page = 0  

        self._load_data()
        self._update_buttons()

        await interaction.message.edit(embed=self.get_embed(), view=self)
        await interaction.followup.send(content="Timeframe updated.", ephemeral=True)

    async def on_timeout(self):
        """Disable buttons when view times out."""
        for item in self.children:
            item.disabled = True
        try:
            if self.message:
                await self.message.edit(view=self)
        except:
            pass



class MatchHistoryPaginationView(discord.ui.View):

    def __init__(self, team_role_id: int, guild: discord.Guild, page: int = 0, page_size: int = 10):
        super().__init__(timeout=300)
        self.team_role_id = team_role_id
        self.guild = guild
        self.page = page
        self.page_size = page_size

    @discord.ui.button(label="Prev", style=discord.ButtonStyle.secondary)
    async def previous_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.page > 0:
            self.page -= 1
            await self.update_page(interaction)
        else:
            await interaction.response.send_message(content="Already on first page.", ephemeral=True)

    @discord.ui.button(label="Next", style=discord.ButtonStyle.secondary)
    async def next_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.page += 1
        await self.update_page(interaction)

    async def update_page(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=False, thinking=True)

        offset = self.page * self.page_size
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
                   LIMIT %s OFFSET %s""",
                (self.team_role_id, self.team_role_id, self.page_size, offset)
            )
            matches = cursor.fetchall()
            cursor.close()
        finally:
            return_db(conn)

        if not matches:
            await interaction.edit_original_response(content="No more matches to display.", view=None)
            return

        history_text = f"**Match History (Page {self.page + 1})**\n\n"
        for match in matches:
            m_id, t1_id, t2_id, t1_score, t2_score, status, sched, finished, created = match

            opponent_id = t2_id if t1_id == self.team_role_id else t1_id
            opponent_role = self.guild.get_role(opponent_id)
            opponent_name = opponent_role.name if opponent_role else "Unknown"

            if t1_id == self.team_role_id:
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

        await interaction.edit_original_response(content=history_text, view=self)



class CaptainPanelView(discord.ui.View):

    def __init__(self, team_role_id: int, user_id: int):
        super().__init__(timeout=300)  # 5 minute timeout
        self.team_role_id = team_role_id
        self.user_id = user_id

    @discord.ui.button(label="My Team Info", style=discord.ButtonStyle.primary)
    async def team_info_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True, thinking=True)

        authority = get_user_team_authority(self.user_id, self.team_role_id)
        if not authority:
            await interaction.followup.send(content="You no longer have authority over this team.", ephemeral=True)
            return

        team = get_team(self.team_role_id)
        if not team:
            await interaction.followup.send(content="Team not found.", ephemeral=True)
            return

        team_role = interaction.guild.get_role(self.team_role_id)
        team_name = team_role.name if team_role else "Unknown"
        from utils.db import get_roster
        roster = get_roster(self.team_role_id)

        info_msg = (
            f"**Team:** {team_name}\n"
            f"**Elo:** {team['elo']}\n"
            f"**Roster Size:** {len(roster)} members\n"
            f"**Your Role:** {authority}"
        )
        await interaction.followup.send(content=info_msg, ephemeral=True)

    @discord.ui.button(label="Challenge Team", style=discord.ButtonStyle.success)
    async def challenge_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_message(
            content="Use `/challenge` command to challenge another team.",
            ephemeral=True
        )

    @discord.ui.button(label="View Open Matches", style=discord.ButtonStyle.secondary)
    async def view_matches_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True, thinking=True)

        conn = get_db()
        try:
            cursor = conn.cursor()
            cursor.execute(
                """SELECT match_id, team1_role_id, team2_role_id, status, scheduled_time_utc
                   FROM matches
                   WHERE (team1_role_id = %s OR team2_role_id = %s)
                     AND status IN ('OPEN', 'SCHEDULED')
                   ORDER BY created_at_utc DESC
                   LIMIT 5""",
                (self.team_role_id, self.team_role_id)
            )
            matches = cursor.fetchall()
            cursor.close()
        finally:
            return_db(conn)

        if not matches:
            await interaction.followup.send(content="No open matches found.", ephemeral=True)
            return

        match_list = ""
        for match in matches:
            opponent_id = match["team2_role_id"] if match["team1_role_id"] == self.team_role_id else match["team1_role_id"]
            opponent_role = interaction.guild.get_role(opponent_id)
            opponent_name = opponent_role.name if opponent_role else "Unknown"
            match_list += f"• **Match #{match['match_id']}** vs {opponent_name} - {match['status']}\n"

        await interaction.followup.send(content=f"**Open Matches:**\n{match_list}", ephemeral=True)

    @discord.ui.button(label="Roster Status", style=discord.ButtonStyle.secondary)
    async def roster_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True, thinking=True)

        from utils.db import get_roster, get_vice_captains
        roster = get_roster(self.team_role_id)
        vice_captains = get_vice_captains(self.team_role_id)
        team = get_team(self.team_role_id)

        roster_msg = f"**Total Members:** {len(roster)}\n"
        roster_msg += f"**Captain:** <@{team['captain_user_id']}>\n"

        if vice_captains:
            vc_mentions = ", ".join([f"<@{vc}>" for vc in vice_captains[:3]])
            roster_msg += f"**Vice Captains:** {vc_mentions}\n"

        await interaction.followup.send(content=roster_msg, ephemeral=True)

    @discord.ui.button(label="Transaction Window", style=discord.ButtonStyle.secondary)
    async def transaction_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        config = get_config()
        transaction_open = config.get("transaction_window_open", False)

        status = "**OPEN**" if transaction_open else "**CLOSED**"
        await interaction.response.send_message(
            content=f"**Transaction Window Status:** {status}",
            ephemeral=True
        )
