"""Background tasks for match reminders, deadline warnings, and league overdue checks."""

import traceback
from datetime import datetime, timedelta

import discord
from discord.ext import commands, tasks
import pytz

from utils.db import (
    get_db, return_db, get_config, get_match, get_team,
    update_match, get_open_matches, get_match_refs, get_league_state
)
from utils.helpers import (
    utc_now, coerce_dt, safe_dm_user,
    get_match_leadership_user_ids, EMBED_COLOR
)

CET_TZ = pytz.timezone("Europe/Zurich")
UTC_TZ = pytz.UTC


class LifecycleTasks(commands.Cog):

    def __init__(self, bot: commands.Bot):
        self.bot = bot

    async def cog_load(self):
        self.reminder_task.start()
        self.dm_reminder_task.start()
        self.deadline_reminder_task.start()
        self.league_overdue_warning_task.start()

    async def cog_unload(self):
        self.reminder_task.cancel()
        self.dm_reminder_task.cancel()
        self.deadline_reminder_task.cancel()
        self.league_overdue_warning_task.cancel()


    @tasks.loop(minutes=5)
    async def reminder_task(self):
        try:
            for guild in self.bot.guilds:
                config = get_config()

                open_matches = get_open_matches()
                for match in open_matches:
                    if not match["scheduled_time_utc"]:
                        continue

                    scheduled_dt = coerce_dt(match["scheduled_time_utc"])
                    time_until = (scheduled_dt - utc_now()).total_seconds() / 60  

                    # 30 minute reminder 
                    if 25 <= time_until <= 35 and not match["reminded_captains"]:
                        channel = guild.get_channel(match["channel_id"])
                        if channel:
                            team1 = get_team(match["team1_role_id"])
                            team2 = get_team(match["team2_role_id"])

                            mentions = []
                            if team1 and team1["captain_user_id"]:
                                mentions.append(f"<@{team1['captain_user_id']}>")
                            if team2 and team2["captain_user_id"]:
                                mentions.append(f"<@{team2['captain_user_id']}>")

                            if mentions:
                                await channel.send(
                                    f"**30 Minute Reminder**\n"
                                    f"{' '.join(mentions)}\n"
                                    f"Your match starts in approximately 30 minutes!"
                                )

                        update_match(match["match_id"], reminded_captains=True)

                    # 15 minute reminder 
                    if 10 <= time_until <= 20 and not match["reminded_refs"]:
                        refs = get_match_refs(match["match_id"])
                        channel = guild.get_channel(match["channel_id"])

                        if channel and refs:
                            ref_mentions = []
                            for r in refs:
                                if isinstance(r, dict):
                                    ref_user_id = r.get("ref_user_id")
                                else:
                                    ref_user_id = r

                                if ref_user_id:
                                    ref_mentions.append(f"<@{ref_user_id}>")

                            if ref_mentions:
                                await channel.send(
                                    f"**15 Minute Reminder**\n"
                                    f"Refs: {' '.join(ref_mentions)}\n"
                                    f"Match starts in approximately 15 minutes!"
                                )

                        update_match(match["match_id"], reminded_refs=True)

        except Exception as e:
            print(f"[REMINDER_TASK ERROR] Task failed: {e}")
            print(traceback.format_exc())


    @reminder_task.before_loop
    async def before_reminder_task(self):
        await self.bot.wait_until_ready()


    @tasks.loop(seconds=60)
    async def dm_reminder_task(self):
        try:
            now = utc_now()
            remind_time_start = now + timedelta(minutes=15) - timedelta(seconds=45)
            remind_time_end = now + timedelta(minutes=15) + timedelta(seconds=45)

            conn = get_db()
            cursor = None
            try:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT match_id, team1_role_id, team2_role_id, channel_id, scheduled_time_utc,
                           reminded_captains_15m, reminded_refs_15m
                    FROM matches
                    WHERE status = 'SCHEDULED'
                      AND scheduled_time_utc IS NOT NULL
                      AND (reminded_captains_15m = FALSE OR reminded_refs_15m = FALSE)
                      AND scheduled_time_utc >= %s
                      AND scheduled_time_utc <= %s
                """, (remind_time_start, remind_time_end))
                rows = cursor.fetchall()
            finally:
                if cursor:
                    cursor.close()
                return_db(conn)

            if rows:
                print(f"[DM_REMINDER] Found {len(rows)} matches needing 15min DM reminders")

            for row in rows:
                match_id = row["match_id"]
                team1_role_id = row["team1_role_id"]
                team2_role_id = row["team2_role_id"]
                channel_id = row["channel_id"]
                scheduled_time_utc = row["scheduled_time_utc"]
                reminded_captains = row["reminded_captains_15m"]
                reminded_refs = row["reminded_refs_15m"]

                flags_updated = []

                guild_obj = None
                team1_name = "Team 1"
                team2_name = "Team 2"
                guild_name = ""

                for guild in self.bot.guilds:
                    team1_role = guild.get_role(team1_role_id)
                    team2_role = guild.get_role(team2_role_id)
                    if team1_role:
                        team1_name = team1_role.name
                    if team2_role:
                        team2_name = team2_role.name
                    if team1_role and team2_role:
                        guild_obj = guild
                        guild_name = guild.name
                        break

                scheduled_dt = coerce_dt(scheduled_time_utc)
                if scheduled_dt.tzinfo is None:
                    scheduled_dt = UTC_TZ.localize(scheduled_dt)
                cet_dt = scheduled_dt.astimezone(CET_TZ)
                cet_str = cet_dt.strftime("%d %m %H:%M")

                jump_link = ""
                if channel_id:
                    for guild in self.bot.guilds:
                        channel = guild.get_channel(channel_id)
                        if channel:
                            jump_link = f"\n**Match Channel:** https://discord.com/channels/{guild.id}/{channel_id}"
                            break

                team_dm_sent = 0
                team_dm_failed = 0
                if not reminded_captains:
                    team_reminder_msg = (
                        f"**15 Minute Match Reminder**\n\n"
                        f"**Match #{match_id}**{f' in {guild_name}' if guild_name else ''}\n"
                        f"**Opponent:** {team2_name if team1_role_id else team1_name}\n"
                        f"**Scheduled:** {cet_str}{jump_link}"
                    )

                    leadership_ids = get_match_leadership_user_ids(team1_role_id, team2_role_id)

                    for user_id in leadership_ids:
                        user = self.bot.get_user(user_id)
                        if not user:
                            try:
                                user = await self.bot.fetch_user(user_id)
                            except:
                                team_dm_failed += 1
                                continue
                        if user:
                            try:
                                await safe_dm_user(user, team_reminder_msg)
                                team_dm_sent += 1
                            except Exception:
                                team_dm_failed += 1

                    update_match(match_id, reminded_captains_15m=True)
                    flags_updated.append("reminded_captains_15m")

                ref_dm_sent = 0
                ref_dm_failed = 0
                if not reminded_refs:
                    refs = get_match_refs(match_id)

                    if refs:
                        ref_reminder_msg = (
                            f"**15 Minute Referee Reminder**\n\n"
                            f"**Match #{match_id}**{f' in {guild_name}' if guild_name else ''}\n"
                            f"**Teams:** {team1_name} vs {team2_name}\n"
                            f"**Scheduled:** {cet_str}{jump_link}"
                        )

                        for ref in refs:
                            if isinstance(ref, dict):
                                ref_user_id = ref.get("ref_user_id")
                            else:
                                ref_user_id = ref

                            if ref_user_id is None:
                                continue

                            user = self.bot.get_user(ref_user_id)
                            if not user:
                                try:
                                    user = await self.bot.fetch_user(ref_user_id)
                                except:
                                    ref_dm_failed += 1
                                    continue
                            if user:
                                try:
                                    await safe_dm_user(user, ref_reminder_msg)
                                    ref_dm_sent += 1
                                except Exception:
                                    ref_dm_failed += 1

                    update_match(match_id, reminded_refs_15m=True)
                    flags_updated.append("reminded_refs_15m")

                flags_str = f" (updated: {', '.join(flags_updated)})" if flags_updated else ""
                print(f"[DM_REMINDER] Match #{match_id}: Team DMs {team_dm_sent}/{team_dm_sent+team_dm_failed}, Ref DMs {ref_dm_sent}/{ref_dm_sent+ref_dm_failed}{flags_str}")

        except Exception as e:
            print(f"[DM_REMINDER ERROR] {e}")
            print(traceback.format_exc())


    @dm_reminder_task.before_loop
    async def before_dm_reminder_task(self):
        await self.bot.wait_until_ready()


    @tasks.loop(minutes=10)
    async def deadline_reminder_task(self):
        """Send 1-day deadline reminders for matches 2 days after creation."""
        try:
            conn = get_db()
            cursor = None
            try:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT match_id, team1_role_id, team2_role_id, channel_id, created_at_utc
                    FROM matches
                    WHERE status IN ('OPEN', 'SCHEDULED')
                      AND channel_id IS NOT NULL
                      AND reminded_deadline_1d = FALSE
                      AND created_at_utc IS NOT NULL
                      AND created_at_utc <= NOW() - INTERVAL '2 days'
                """)
                matches_needing_reminder = cursor.fetchall()
                cursor.close()
            finally:
                return_db(conn)

            for match in matches_needing_reminder:
                match_id = match["match_id"]
                team1_role_id = match["team1_role_id"]
                team2_role_id = match["team2_role_id"]
                channel_id = match["channel_id"]

                channel = None
                for guild in self.bot.guilds:
                    channel = guild.get_channel(channel_id)
                    if channel:
                        team1_role = guild.get_role(team1_role_id)
                        team2_role = guild.get_role(team2_role_id)

                        team1_mention = team1_role.mention if team1_role else f"<@&{team1_role_id}>"
                        team2_mention = team2_role.mention if team2_role else f"<@&{team2_role_id}>"

                        try:
                            await channel.send(
                                f"**1 day left** to finish this match: {team1_mention} vs {team2_mention}"
                            )
                            print(f"[DEADLINE_REMINDER] Sent reminder for match {match_id}")
                        except Exception as send_err:
                            print(f"[DEADLINE_REMINDER] Failed to send reminder for match {match_id}: {send_err}")

                        break  

                update_match(match_id, reminded_deadline_1d=True)

        except Exception as e:
            print(f"[DEADLINE_REMINDER ERROR] {e}")
            print(traceback.format_exc())


    @deadline_reminder_task.before_loop
    async def before_deadline_reminder_task(self):
        await self.bot.wait_until_ready()


    @tasks.loop(minutes=15)
    async def league_overdue_warning_task(self):
        try:
            state = get_league_state()
            dashboard_channel_id = state.get("dashboard_channel_id")

            if not dashboard_channel_id:
                return  

            conn = get_db()
            cursor = None
            try:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT match_id, team1_role_id, team2_role_id, group_id, league_round, deadline_utc
                    FROM matches
                    WHERE mode = 'LEAGUE'
                      AND status IN ('OPEN', 'SCHEDULED')
                      AND deadline_utc IS NOT NULL
                      AND overdue_warned = FALSE
                      AND deadline_utc + INTERVAL '2 hours' < NOW()
                """)
                overdue_matches = cursor.fetchall()
                cursor.close()
            finally:
                return_db(conn)

            if not overdue_matches:
                return  

            for guild in self.bot.guilds:
                dashboard_channel = guild.get_channel(dashboard_channel_id)
                if not dashboard_channel:
                    continue

                for match in overdue_matches:
                    match_id = match["match_id"]
                    team1_role_id = match["team1_role_id"]
                    team2_role_id = match["team2_role_id"]
                    group_id = match.get("group_id")
                    league_round = match.get("league_round")
                    deadline_utc = coerce_dt(match["deadline_utc"])

                    team1_role = guild.get_role(team1_role_id)
                    team2_role = guild.get_role(team2_role_id)

                    team1_mention = team1_role.mention if team1_role else f"<@&{team1_role_id}>"
                    team2_mention = team2_role.mention if team2_role else f"<@&{team2_role_id}>"

                    if deadline_utc.tzinfo is None:
                        deadline_utc = UTC_TZ.localize(deadline_utc)
                    cet_dt = deadline_utc.astimezone(CET_TZ)
                    deadline_str = cet_dt.strftime("%d %b %H:%M CET")

                    time_overdue = (utc_now() - deadline_utc).total_seconds() / 3600
                    hours_overdue = int(time_overdue)

                    warning_msg = (
                        f"**MATCH OVERDUE**\n"
                        f"**Match ID:** {match_id}\n"
                        f"**Teams:** {team1_mention} vs {team2_mention}\n"
                    )

                    if group_id and league_round:
                        warning_msg += f"**Group {group_id} • Round {league_round}**\n"

                    warning_msg += (
                        f"**Deadline:** {deadline_str}\n"
                        f"**Overdue by:** ~{hours_overdue} hours\n\n"
                        f"_This match should have been reported. Please complete and report ASAP._"
                    )

                    try:
                        await dashboard_channel.send(warning_msg)
                        print(f"[LEAGUE_OVERDUE] Warned about match {match_id} (overdue by {hours_overdue}h)")
                    except Exception as send_err:
                        print(f"[LEAGUE_OVERDUE] Failed to send warning for match {match_id}: {send_err}")

                    update_match(match_id, overdue_warned=True)

                break 

        except Exception as e:
            print(f"[LEAGUE_OVERDUE_WARNING ERROR] {e}")
            print(traceback.format_exc())


    @league_overdue_warning_task.before_loop
    async def before_league_overdue_warning_task(self):
        await self.bot.wait_until_ready()



async def setup(bot: commands.Bot):
    await bot.add_cog(LifecycleTasks(bot))
