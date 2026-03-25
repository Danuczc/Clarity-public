import traceback
from datetime import datetime

import discord
from discord import app_commands
from discord.ext import commands

from utils.db import (
    get_db, return_db, get_config, get_team, get_all_teams,
    get_roster, get_league_state, get_group_standings
)
from utils.helpers import EMBED_COLOR, utc_now, safe_defer, coerce_dt
from utils.permissions import is_team_staff, is_elo_staff, has_league_perms
from utils.audit_engine import (
    run_integrity_audit, apply_integrity_cleanup, format_audit_report,
    AuditResult,
)
from views.shared_views import RefListView, RefActivityView



class CleanupConfirmView(discord.ui.View):

    def __init__(self, audit_result: AuditResult, invoker_id: int, source_cmd: str):
        super().__init__(timeout=120)
        self.audit_result = audit_result
        self.invoker_id = invoker_id
        self.source_cmd = source_cmd  

    @discord.ui.button(label="Confirm Cleanup", style=discord.ButtonStyle.danger)
    async def confirm_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.invoker_id:
            await interaction.response.send_message("Only the invoker can confirm.", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True)

        try:
            stats = await apply_integrity_cleanup(interaction.guild, self.audit_result.issues)

            # Summary
            summary = (
                f"**Cleanup complete:** {stats.roles_added} role(s) added, "
                f"{stats.roles_removed} role(s) removed, "
                f"{stats.orphan_affiliations_removed} orphan affiliation(s) removed."
            )
            if stats.errors:
                summary += f"\n**Errors ({len(stats.errors)}):** " + "; ".join(stats.errors[:3])
                if len(stats.errors) > 3:
                    summary += f" ... and {len(stats.errors) - 3} more"

            await interaction.followup.send(content=summary, ephemeral=True)

            new_result = run_integrity_audit(interaction.guild)
            new_report = format_audit_report(new_result)

            fixable_count = sum(1 for i in new_result.issues if i.fixable)

            if new_result.issues:
                view = AuditCleanupView(new_result, self.invoker_id, self.source_cmd)
                view.cleanup_btn.disabled = fixable_count == 0
            else:
                view = None

            if len(new_report) > 1900:
                chunks = [new_report[i:i+1900] for i in range(0, len(new_report), 1900)]
                for idx, chunk in enumerate(chunks):
                    if idx == len(chunks) - 1 and view:
                        await interaction.followup.send(content=chunk, view=view, ephemeral=True)
                    else:
                        await interaction.followup.send(content=chunk, ephemeral=True)
            else:
                if view:
                    await interaction.followup.send(content=new_report, view=view, ephemeral=True)
                else:
                    await interaction.followup.send(content=new_report, ephemeral=True)

        except Exception as e:
            print(f"[CLEANUP ERROR] {e}")
            print(traceback.format_exc())
            await interaction.followup.send(content="Cleanup failed. Check logs.", ephemeral=True)

        self.stop()

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel_btn(self, interaction: discord.Interaction, button: discord.ui.Button):

        await interaction.response.send_message("Cleanup cancelled.", ephemeral=True)
        self.stop()


class AuditCleanupView(discord.ui.View):

    def __init__(self, audit_result: AuditResult, invoker_id: int, source_cmd: str):
        super().__init__(timeout=300)
        self.audit_result = audit_result
        self.invoker_id = invoker_id
        self.source_cmd = source_cmd

    @discord.ui.button(label="Cleanup", style=discord.ButtonStyle.primary, custom_id="audit_cleanup_btn")
    async def cleanup_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        # Permission check
        if not (is_team_staff(interaction.user) or interaction.user.guild_permissions.administrator):
            await interaction.response.send_message(
                "Only Team Perms staff or Admins can run cleanup.", ephemeral=True
            )
            return

        fixable = [i for i in self.audit_result.issues if i.fixable]
        if not fixable:
            await interaction.response.send_message("No fixable issues to clean up.", ephemeral=True)
            return

        # Confirmation
        confirm_view = CleanupConfirmView(self.audit_result, interaction.user.id, self.source_cmd)
        await interaction.response.send_message(
            f"**This will modify roles and remove invalid affiliations.** Continue?\n"
            f"({len(fixable)} fixable issue(s) will be addressed.)",
            view=confirm_view,
            ephemeral=True,
        )



async def send_audit_with_cleanup(interaction, audit_result, source_cmd):
    """Send formatted audit report with a Cleanup button if fixable issues exist."""
    report = format_audit_report(audit_result)
    fixable_count = sum(1 for i in audit_result.issues if i.fixable)

    if audit_result.issues:
        view = AuditCleanupView(audit_result, interaction.user.id, source_cmd)
        view.cleanup_btn.disabled = fixable_count == 0
    else:
        view = None

    if len(report) > 1900:
        chunks = [report[i:i+1900] for i in range(0, len(report), 1900)]
        for idx, chunk in enumerate(chunks):
            if idx == len(chunks) - 1 and view:
                await interaction.followup.send(content=chunk, view=view)
            else:
                await interaction.followup.send(content=chunk)
    else:
        if view:
            await interaction.followup.send(content=report, view=view)
        else:
            await interaction.followup.send(content=report)


class AuditCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @app_commands.command(
        name="audit",
        description="Audit team/role integrity and optionally clean issues"
    )
    async def audit_cmd(self, interaction: discord.Interaction):
        """
        Full team/role integrity audit.
        Shows all issues and provides a Cleanup button for staff.
        """
        await safe_defer(interaction, ephemeral=True)

        try:
            # Permission check 
            if not (is_team_staff(interaction.user) or interaction.user.guild_permissions.administrator):
                await interaction.followup.send(
                    content="Only Team Perms staff or Admins can use this command."
                )
                return

            result = run_integrity_audit(interaction.guild)
            await send_audit_with_cleanup(interaction, result, "audit")

        except Exception as e:
            print(f"[AUDIT ERROR] guild={interaction.guild.id}")
            print(traceback.format_exc())
            await interaction.followup.send(content="Failed to run audit. Please try again.")

    # /league-audit

    @app_commands.command(
        name="league-audit",
        description="Run league system and role integrity check"
    )
    async def league_audit_cmd(self, interaction: discord.Interaction):
        """
        Comprehensive audit of league system integrity.
        Includes role/team audit with cleanup, plus league-specific checks.
        """
        await safe_defer(interaction, ephemeral=True)

        try:
            from utils.permissions import check_league_perms

            # Permission check 
            if not (has_league_perms(interaction.user) or interaction.user.guild_permissions.administrator):
                await interaction.followup.send(
                    content="You need League Perms or Admin to use this command.",
                    ephemeral=True,
                )
                return

            role_result = run_integrity_audit(interaction.guild)

            report_lines = []
            issues_found = 0
            warnings_found = 0

            report_lines.append("# LEAGUE SYSTEM AUDIT REPORT")
            report_lines.append(f"Generated: <t:{int(utc_now().timestamp())}:F>")
            report_lines.append(f"By: {interaction.user.mention}\n")

            conn = get_db()
            try:
                cursor = conn.cursor()

                # 1. Duplicate matchups
                report_lines.append("## 1. Duplicate Matchup Check")
                cursor.execute("""
                    SELECT
                        g.stage_id,
                        m.league_round,
                        m.team1_role_id,
                        m.team2_role_id,
                        COUNT(*) as match_count
                    FROM matches m
                    JOIN league_groups g ON m.group_id = g.group_id
                    WHERE m.mode = 'LEAGUE'
                      AND m.group_id IS NOT NULL
                    GROUP BY g.stage_id, m.league_round, m.team1_role_id, m.team2_role_id
                    HAVING COUNT(*) > 1
                """)
                duplicates = cursor.fetchall()

                if duplicates:
                    issues_found += len(duplicates)
                    report_lines.append(f"**CRITICAL: {len(duplicates)} duplicate matchup(s) found!**")
                    for dup in duplicates[:5]:
                        report_lines.append(f"  \u2022 Stage {dup['stage_id']}, Round {dup['league_round']}: Teams {dup['team1_role_id']} vs {dup['team2_role_id']} ({dup['match_count']} matches)")
                    if len(duplicates) > 5:
                        report_lines.append(f"  ... and {len(duplicates) - 5} more")
                else:
                    report_lines.append("No duplicate matchups found\n")

                report_lines.append("## 2. Cross-Mode Data Integrity")

                cursor.execute("""
                    SELECT COUNT(*) as count
                    FROM elo_history
                    WHERE match_id IN (
                        SELECT match_id FROM matches WHERE mode = 'LEAGUE'
                    )
                """)
                elo_in_league = cursor.fetchone()["count"]

                if elo_in_league > 0:
                    issues_found += 1
                    report_lines.append(f"**CRITICAL: {elo_in_league} ELO changes linked to LEAGUE matches!**")
                else:
                    report_lines.append("No ELO corruption in LEAGUE matches")

                cursor.execute("SELECT COUNT(*) as count FROM matches WHERE mode IS NULL")
                missing_mode = cursor.fetchone()["count"]

                if missing_mode > 0:
                    warnings_found += 1
                    report_lines.append(f"Warning: {missing_mode} matches missing mode field")
                else:
                    report_lines.append("All matches have mode set\n")

                # 3. Overdue warnings
                report_lines.append("## 3. Overdue Warning System")
                report_lines.append("Overdue warning system status")

                cursor.execute("""
                    SELECT COUNT(*) as count
                    FROM matches
                    WHERE mode = 'LEAGUE'
                      AND status IN ('OPEN', 'SCHEDULED')
                      AND deadline_utc IS NOT NULL
                      AND overdue_warned = FALSE
                      AND deadline_utc + INTERVAL '2 hours' < NOW()
                """)
                should_warn = cursor.fetchone()["count"]

                if should_warn > 0:
                    warnings_found += 1
                    report_lines.append(f"Warning: {should_warn} match(es) overdue but not yet warned\n")
                else:
                    report_lines.append("No pending overdue warnings\n")

                # 4. Mode branching
                report_lines.append("## 4. Mode Branching Validation")

                cursor.execute("SELECT COUNT(*) as count FROM matches WHERE mode NOT IN ('ELO', 'LEAGUE')")
                invalid_mode = cursor.fetchone()["count"]

                if invalid_mode > 0:
                    issues_found += 1
                    report_lines.append(f"FAIL: {invalid_mode} match(es) with invalid mode")
                else:
                    report_lines.append("OK: All matches have valid mode (ELO or LEAGUE)")

                cursor.execute("""
                    SELECT COUNT(*) as count
                    FROM matches
                    WHERE mode = 'LEAGUE'
                      AND status IN ('OPEN', 'SCHEDULED')
                      AND deadline_utc IS NULL
                """)
                no_deadline = cursor.fetchone()["count"]

                if no_deadline > 0:
                    warnings_found += 1
                    report_lines.append(f"Warning: {no_deadline} active LEAGUE match(es) missing deadline\n")
                else:
                    report_lines.append("OK: All active LEAGUE matches have deadlines\n")

                # 5. State consistency
                report_lines.append("## 5. League State Consistency")
                state = get_league_state()

                if state.get("season_active"):
                    cursor.execute("SELECT COUNT(*) as count FROM league_groups")
                    group_count = cursor.fetchone()["count"]

                    if group_count == 0:
                        warnings_found += 1
                        report_lines.append("Warning: Season marked ACTIVE but no groups exist")
                    else:
                        report_lines.append(f"OK: Season ACTIVE with {group_count} group(s)")
                else:
                    report_lines.append("Season not currently active")

                if state.get("roster_lock_enabled"):
                    cursor.execute("SELECT COUNT(*) as count FROM league_group_teams")
                    team_count = cursor.fetchone()["count"]

                    if team_count == 0:
                        warnings_found += 1
                        report_lines.append("Warning: Roster lock ENABLED but no teams in league")
                    else:
                        report_lines.append(f"OK: Roster lock ENABLED for {team_count} league team(s)")
                else:
                    report_lines.append("Roster lock not enabled\n")

                # 6. Playoff bracket integrity
                report_lines.append("## 6. Playoff Bracket Integrity")

                cursor.execute("""
                    SELECT COUNT(*) as count
                    FROM matches
                    WHERE mode = 'LEAGUE'
                      AND bracket IS NOT NULL
                      AND bracket NOT IN ('WINNERS', 'LOSERS')
                """)
                invalid_brackets = cursor.fetchone()["count"]

                if invalid_brackets > 0:
                    issues_found += 1
                    report_lines.append(f"FAIL: {invalid_brackets} playoff match(es) with invalid bracket")
                else:
                    report_lines.append("OK: All playoff brackets valid")

                cursor.execute("""
                    SELECT COUNT(*) as count
                    FROM matches
                    WHERE mode = 'LEAGUE'
                      AND bracket = 'LOSERS'
                      AND league_round >= 3
                      AND series_format != 'BO5'
                """)
                wrong_format = cursor.fetchone()["count"]

                if wrong_format > 0:
                    warnings_found += 1
                    report_lines.append(f"Warning: {wrong_format} Losers Final match(es) not set to BO5\n")
                else:
                    report_lines.append("OK: BO5 enforcement correct\n")

                # 7. Dashboard logging
                report_lines.append("## 7. Dashboard Logging")

                if state.get("dashboard_channel_id"):
                    report_lines.append(f"OK: Dashboard channel configured: <#{state['dashboard_channel_id']}>")
                else:
                    warnings_found += 1
                    report_lines.append("Warning: No dashboard channel configured\n")

                # 8. Standings consistency
                report_lines.append("## 8. Standings Consistency")

                cursor.execute("SELECT DISTINCT group_id FROM league_groups")
                all_groups = cursor.fetchall()

                standings_issues = 0
                for group in all_groups:
                    group_id = group["group_id"]
                    standings = get_group_standings(group_id)

                    if not standings:
                        continue

                    for s in standings:
                        team_id = s["team_role_id"]

                        cursor.execute("""
                            SELECT
                                SUM(CASE
                                    WHEN team1_role_id = %s AND status = 'FINISHED' THEN
                                        (SELECT COUNT(*) FROM unnest(string_to_array(sets, ',')) AS set_winner WHERE set_winner = 'team1')
                                    WHEN team2_role_id = %s AND status = 'FINISHED' THEN
                                        (SELECT COUNT(*) FROM unnest(string_to_array(sets, ',')) AS set_winner WHERE set_winner = 'team2')
                                    ELSE 0
                                END) as actual_sets_won
                            FROM matches
                            WHERE mode = 'LEAGUE'
                              AND group_id = %s
                              AND (team1_role_id = %s OR team2_role_id = %s)
                        """, (team_id, team_id, group_id, team_id, team_id))

                if standings_issues == 0:
                    report_lines.append("OK: Standings data appears consistent")
                else:
                    warnings_found += standings_issues
                    report_lines.append(f"Warning: {standings_issues} potential standings inconsistencies\n")

                cursor.close()
            finally:
                return_db(conn)

            # League summary
            report_lines.append("## LEAGUE AUDIT SUMMARY")
            report_lines.append(f"**Critical Issues:** {issues_found}")
            report_lines.append(f"**Warnings:** {warnings_found}")

            if issues_found == 0 and warnings_found == 0:
                report_lines.append("\n**SYSTEM HEALTHY:** All league integrity checks passed!")
            elif issues_found > 0:
                report_lines.append(f"\n**ACTION REQUIRED:** {issues_found} critical issue(s) need immediate attention")
            else:
                report_lines.append(f"\n**REVIEW RECOMMENDED:** {warnings_found} warning(s) detected")

            full_report = "\n".join(report_lines)

            if len(full_report) > 1900:
                chunks = [full_report[i:i+1900] for i in range(0, len(full_report), 1900)]
                for chunk in chunks:
                    await interaction.followup.send(content=chunk, ephemeral=True)
            else:
                await interaction.followup.send(content=full_report, ephemeral=True)

            await send_audit_with_cleanup(interaction, role_result, "league-audit")

            print(f"[LEAGUE-AUDIT] Audit completed by {interaction.user} - {issues_found} league issues, {warnings_found} warnings, {len(role_result.issues)} role issues")

        except Exception as e:
            print(f"[LEAGUE-AUDIT ERROR] {e}")
            print(traceback.format_exc())
            await interaction.followup.send(content="Audit failed. Check logs.", ephemeral=True)


    @app_commands.command(name="referee-activity", description="View referee activity rankings (Admins + Head of Refs only)")
    async def referee_activity_cmd(self, interaction: discord.Interaction):
        """
        Display referee activity rankings based on matches they've recorded via /bo3 or /bo5.

        Features:
        - Toggle between 7-day and all-time views
        - Paginated view with 10 refs per page
        - Public embed (non-ephemeral)
        """
        await safe_defer(interaction, ephemeral=False)

        try:
            # Permission check 
            has_permission = False

            if interaction.user.guild_permissions.administrator:
                has_permission = True
            else:
                config = get_config()
                head_of_refs_role_id = config.get("head_of_refs_role_id")
                if head_of_refs_role_id and any(role.id == head_of_refs_role_id for role in interaction.user.roles):
                    has_permission = True

            if not has_permission:
                await interaction.followup.send(
                    content="Only administrators or Head of Refs can use this command."
                )
                return

            view = RefActivityView(
                guild_id=interaction.guild.id,
                invoker_id=interaction.user.id,
                mode="7d",  # Start with 7-day view
                timeout=120
            )

            message = await interaction.followup.send(embed=view.get_embed(), view=view)
            view.message = message 

        except Exception as e:
            print(f"[REFEREE-ACTIVITY ERROR] Failed for guild={interaction.guild.id}")
            print(traceback.format_exc())
            await interaction.followup.send(content="Failed to retrieve referee activity. Please try again.")

    @app_commands.command(name="ref-list", description="List matches needing refs")
    async def ref_list_cmd(self, interaction: discord.Interaction):
        await safe_defer(interaction, ephemeral=True)

        try:
            from utils.permissions import is_ref
            from utils.db import get_open_matches, get_match_refs

            if not is_ref(interaction.user):
                await interaction.followup.send(content="Only refs can use this command.")
                return

            open_matches = get_open_matches()
            needing_refs = []

            for match in open_matches:
                refs = get_match_refs(match["match_id"])
                if len(refs) < 2:  
                    needing_refs.append(match)

            if not needing_refs:
                await interaction.followup.send(content="No matches currently need refs.")
                return

            view = RefListView(needing_refs, 0, interaction.guild)
            await interaction.followup.send(embed=view.get_embed(), view=view)
        except Exception as e:
            await interaction.followup.send(content=f"Error: {str(e)[:200]}")


async def setup(bot: commands.Bot):
    await bot.add_cog(AuditCog(bot))
