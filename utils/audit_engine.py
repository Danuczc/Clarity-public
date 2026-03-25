
import traceback
from dataclasses import dataclass, field
from enum import Enum
from typing import List, Optional

import discord

from utils.db import (
    get_db, return_db, get_config, get_all_teams,
    get_roster, get_vice_captains,
)


class IssueType(Enum):
    DEAD_TEAM = "dead_team"
    ORPHAN_MEMBER_LEFT = "orphan_member_left"
    MISSING_TEAM_ROLE = "missing_team_role"
    MISSING_CAPTAIN_ROLE = "missing_captain_role"
    MISSING_VC_ROLE = "missing_vc_role"
    ORPHANED_TEAM_ROLE = "orphaned_team_role"
    CAPTAIN_WITHOUT_TEAM_ROLE = "captain_without_team_role"
    VC_WITHOUT_TEAM_ROLE = "vc_without_team_role"


@dataclass
class AuditIssue:
    issue_type: IssueType
    team_role_id: int
    team_name: str
    user_id: Optional[int] = None
    role_id: Optional[int] = None
    description: str = ""
    fixable: bool = True


@dataclass
class AuditResult:
    issues: List[AuditIssue] = field(default_factory=list)
    teams_scanned: int = 0


@dataclass
class CleanupStats:
    roles_added: int = 0
    roles_removed: int = 0
    orphan_affiliations_removed: int = 0
    errors: List[str] = field(default_factory=list)


def run_integrity_audit(guild: discord.Guild) -> AuditResult:
    result = AuditResult()
    config = get_config()
    captain_role_id = config.get("captain_role_id")
    vice_captain_role_id = config.get("vice_captain_role_id")

    all_teams = get_all_teams()
    result.teams_scanned = len(all_teams)

    for team in all_teams:
        team_role_id = team["team_role_id"]
        team_discord_role = guild.get_role(team_role_id)
        team_name = team_discord_role.name if team_discord_role else f"ID:{team_role_id}"

        if not team_discord_role:
            result.issues.append(AuditIssue(
                issue_type=IssueType.DEAD_TEAM,
                team_role_id=team_role_id,
                team_name=team_name,
                description=f"Team role deleted from Discord",
                fixable=False,
            ))
            continue

        roster = get_roster(team_role_id)
        captain_id = team["captain_user_id"]
        vice_captain_ids = get_vice_captains(team_role_id)
        roster_user_ids = {m["user_id"] for m in roster}

        all_affiliated_ids = set()
        if captain_id:
            all_affiliated_ids.add(captain_id)
        all_affiliated_ids.update(vice_captain_ids)
        all_affiliated_ids.update(roster_user_ids)

        for member_data in roster:
            user_id = member_data["user_id"]
            member = guild.get_member(user_id)

            if not member:
                result.issues.append(AuditIssue(
                    issue_type=IssueType.ORPHAN_MEMBER_LEFT,
                    team_role_id=team_role_id,
                    team_name=team_name,
                    user_id=user_id,
                    description=f"<@{user_id}> left server but still in roster",
                    fixable=True,
                ))
            elif team_discord_role not in member.roles:
                result.issues.append(AuditIssue(
                    issue_type=IssueType.MISSING_TEAM_ROLE,
                    team_role_id=team_role_id,
                    team_name=team_name,
                    user_id=user_id,
                    role_id=team_role_id,
                    description=f"{member.mention} in roster but missing team role",
                    fixable=True,
                ))

        if captain_id:
            captain_member = guild.get_member(captain_id)

            if not captain_member:
                if captain_id not in roster_user_ids:
                    result.issues.append(AuditIssue(
                        issue_type=IssueType.ORPHAN_MEMBER_LEFT,
                        team_role_id=team_role_id,
                        team_name=team_name,
                        user_id=captain_id,
                        description=f"<@{captain_id}> (captain) left server",
                        fixable=True,
                    ))
            else:
                if captain_role_id:
                    captain_discord_role = guild.get_role(captain_role_id)
                    if captain_discord_role and captain_discord_role not in captain_member.roles:
                        result.issues.append(AuditIssue(
                            issue_type=IssueType.MISSING_CAPTAIN_ROLE,
                            team_role_id=team_role_id,
                            team_name=team_name,
                            user_id=captain_id,
                            role_id=captain_role_id,
                            description=f"{captain_member.mention} (captain) missing Captain role",
                            fixable=True,
                        ))

                if team_discord_role not in captain_member.roles:
                    result.issues.append(AuditIssue(
                        issue_type=IssueType.CAPTAIN_WITHOUT_TEAM_ROLE,
                        team_role_id=team_role_id,
                        team_name=team_name,
                        user_id=captain_id,
                        role_id=team_role_id,
                        description=f"{captain_member.mention} (captain) missing team role",
                        fixable=True,
                    ))

        for vc_id in vice_captain_ids:
            vc_member = guild.get_member(vc_id)

            if not vc_member:
                if vc_id not in roster_user_ids and vc_id != captain_id:
                    result.issues.append(AuditIssue(
                        issue_type=IssueType.ORPHAN_MEMBER_LEFT,
                        team_role_id=team_role_id,
                        team_name=team_name,
                        user_id=vc_id,
                        description=f"<@{vc_id}> (vice captain) left server",
                        fixable=True,
                    ))
            else:
                if vice_captain_role_id:
                    vc_discord_role = guild.get_role(vice_captain_role_id)
                    if vc_discord_role and vc_discord_role not in vc_member.roles:
                        result.issues.append(AuditIssue(
                            issue_type=IssueType.MISSING_VC_ROLE,
                            team_role_id=team_role_id,
                            team_name=team_name,
                            user_id=vc_id,
                            role_id=vice_captain_role_id,
                            description=f"{vc_member.mention} (VC) missing Vice Captain role",
                            fixable=True,
                        ))

                if team_discord_role not in vc_member.roles:
                    result.issues.append(AuditIssue(
                        issue_type=IssueType.VC_WITHOUT_TEAM_ROLE,
                        team_role_id=team_role_id,
                        team_name=team_name,
                        user_id=vc_id,
                        role_id=team_role_id,
                        description=f"{vc_member.mention} (VC) missing team role",
                        fixable=True,
                    ))

        if team_discord_role and team_discord_role.members:
            for member in team_discord_role.members:
                if member.id not in all_affiliated_ids:
                    result.issues.append(AuditIssue(
                        issue_type=IssueType.ORPHANED_TEAM_ROLE,
                        team_role_id=team_role_id,
                        team_name=team_name,
                        user_id=member.id,
                        role_id=team_role_id,
                        description=f"{member.mention} has team role but no DB affiliation",
                        fixable=True,
                    ))

    return result


async def apply_integrity_cleanup(
    guild: discord.Guild,
    issues: List[AuditIssue],
) -> CleanupStats:
    stats = CleanupStats()

    roster_deletes = []
    vc_deletes = []
    captain_clears = []
    affiliation_deletes = []

    for issue in issues:
        if not issue.fixable:
            continue

        try:
            if issue.issue_type == IssueType.ORPHAN_MEMBER_LEFT:
                _queue_orphan_cleanup(
                    issue, roster_deletes, vc_deletes, captain_clears,
                    affiliation_deletes, guild
                )
                stats.orphan_affiliations_removed += 1

            elif issue.issue_type in (
                IssueType.MISSING_TEAM_ROLE,
                IssueType.MISSING_CAPTAIN_ROLE,
                IssueType.MISSING_VC_ROLE,
                IssueType.CAPTAIN_WITHOUT_TEAM_ROLE,
                IssueType.VC_WITHOUT_TEAM_ROLE,
            ):
                member = guild.get_member(issue.user_id)
                role = guild.get_role(issue.role_id)
                if member and role:
                    await member.add_roles(role, reason="Audit cleanup: missing role")
                    stats.roles_added += 1

            elif issue.issue_type == IssueType.ORPHANED_TEAM_ROLE:
                member = guild.get_member(issue.user_id)
                role = guild.get_role(issue.role_id)
                if member and role:
                    await member.remove_roles(role, reason="Audit cleanup: no DB affiliation")
                    stats.roles_removed += 1

        except discord.Forbidden:
            stats.errors.append(f"Permission denied: {issue.description}")
        except discord.HTTPException as e:
            stats.errors.append(f"Discord error for {issue.description}: {e}")
        except Exception as e:
            stats.errors.append(f"Error: {issue.description}: {e}")

    if roster_deletes or vc_deletes or captain_clears or affiliation_deletes:
        try:
            conn = get_db()
            try:
                cursor = conn.cursor()

                for team_role_id, user_id in roster_deletes:
                    cursor.execute(
                        "DELETE FROM roster WHERE team_role_id = %s AND user_id = %s",
                        (team_role_id, user_id)
                    )

                for team_role_id, user_id in vc_deletes:
                    cursor.execute(
                        "DELETE FROM vice_captains WHERE team_role_id = %s AND user_id = %s",
                        (team_role_id, user_id)
                    )

                for team_role_id in captain_clears:
                    cursor.execute(
                        "UPDATE teams SET captain_user_id = NULL WHERE team_role_id = %s AND captain_user_id IS NOT NULL",
                        (team_role_id,)
                    )

                for user_id in affiliation_deletes:
                    cursor.execute(
                        "DELETE FROM team_affiliations WHERE user_id = %s",
                        (user_id,)
                    )

                conn.commit()
                cursor.close()
            except Exception as e:
                conn.rollback()
                stats.errors.append(f"DB transaction error: {e}")
                print(f"[AUDIT-CLEANUP DB ERROR] {e}")
                print(traceback.format_exc())
            finally:
                return_db(conn)
        except Exception as e:
            stats.errors.append(f"DB connection error: {e}")

    return stats


def _queue_orphan_cleanup(
    issue: AuditIssue,
    roster_deletes: list,
    vc_deletes: list,
    captain_clears: list,
    affiliation_deletes: list,
    guild: discord.Guild,
):
    team_role_id = issue.team_role_id
    user_id = issue.user_id

    conn = get_db()
    try:
        cursor = conn.cursor()

        cursor.execute(
            "SELECT 1 FROM roster WHERE team_role_id = %s AND user_id = %s",
            (team_role_id, user_id)
        )
        if cursor.fetchone():
            roster_deletes.append((team_role_id, user_id))

        cursor.execute(
            "SELECT 1 FROM vice_captains WHERE team_role_id = %s AND user_id = %s",
            (team_role_id, user_id)
        )
        if cursor.fetchone():
            vc_deletes.append((team_role_id, user_id))

        cursor.execute(
            "SELECT 1 FROM teams WHERE team_role_id = %s AND captain_user_id = %s",
            (team_role_id, user_id)
        )
        if cursor.fetchone():
            captain_clears.append(team_role_id)

        cursor.execute(
            "SELECT 1 FROM team_affiliations WHERE user_id = %s AND team_role_id = %s",
            (user_id, team_role_id)
        )
        if cursor.fetchone():
            affiliation_deletes.append(user_id)

        cursor.close()
    finally:
        return_db(conn)


def format_audit_report(result: AuditResult) -> str:
    """Format audit result into a Discord-friendly report string."""
    issues = result.issues
    total = len(issues)

    if total == 0:
        return (
            f"**Audit Complete:** No issues found.\n"
            f"Scanned {result.teams_scanned} team(s)."
        )

    dead_teams = [i for i in issues if i.issue_type == IssueType.DEAD_TEAM]
    orphans = [i for i in issues if i.issue_type == IssueType.ORPHAN_MEMBER_LEFT]
    missing_team = [i for i in issues if i.issue_type in (
        IssueType.MISSING_TEAM_ROLE, IssueType.CAPTAIN_WITHOUT_TEAM_ROLE,
        IssueType.VC_WITHOUT_TEAM_ROLE,
    )]
    missing_captain = [i for i in issues if i.issue_type == IssueType.MISSING_CAPTAIN_ROLE]
    missing_vc = [i for i in issues if i.issue_type == IssueType.MISSING_VC_ROLE]
    orphaned_roles = [i for i in issues if i.issue_type == IssueType.ORPHANED_TEAM_ROLE]

    fixable = sum(1 for i in issues if i.fixable)
    report = f"**Role Audit Report**\n"
    report += f"Scanned {result.teams_scanned} team(s) \u2022 Found {total} issue(s) \u2022 {fixable} fixable\n\n"

    def _section(title, items, limit=10):
        if not items:
            return ""
        text = f"**{title} ({len(items)}):**\n"
        for item in items[:limit]:
            text += f"\u2022 **{item.team_name}**: {item.description}\n"
        if len(items) > limit:
            text += f"... and {len(items) - limit} more\n"
        text += "\n"
        return text

    report += _section("Dead Teams", dead_teams, 10)
    report += _section("Orphan Affiliations (Left Server)", orphans, 10)
    report += _section("Missing Team Roles", missing_team, 10)
    report += _section("Missing Captain Roles", missing_captain, 5)
    report += _section("Missing VC Roles", missing_vc, 5)
    report += _section("Orphaned Discord Roles (No DB Affiliation)", orphaned_roles, 10)

    return report
