import os
import traceback
import time
from datetime import datetime, timedelta
from typing import Optional, List, Tuple, Dict, Any
from contextlib import contextmanager

import discord
import psycopg2
from psycopg2 import pool as psycopg2_pool
from psycopg2.extras import RealDictCursor
import pytz

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is not set. This bot requires PostgreSQL.")

DISABLE_CHALLENGE_COOLDOWN = os.getenv("DISABLE_CHALLENGE_COOLDOWN", "").lower() in ("1", "true", "yes")

CET_TZ = pytz.timezone("Europe/Zurich")
UTC_TZ = pytz.UTC


def utc_now() -> datetime:
    """Get current UTC time."""
    return datetime.now(UTC_TZ)


DB_POOL_MIN = int(os.getenv("DB_POOL_MIN", "1"))
DB_POOL_MAX = int(os.getenv("DB_POOL_MAX", "15"))

_pool = None


def init_pool():
    global _pool
    if _pool is None:
        _pool = psycopg2_pool.ThreadedConnectionPool(
            minconn=DB_POOL_MIN,
            maxconn=DB_POOL_MAX,
            dsn=DATABASE_URL,
            cursor_factory=RealDictCursor
        )


def get_db():
    global _pool
    if _pool is None:
        init_pool()
    try:
        return _pool.getconn()
    except psycopg2_pool.PoolError as e:
        print(f"[DB POOL EXHAUSTED] Cannot get connection: {e}")
        raise


def return_db(conn):
    global _pool
    if _pool is not None and conn is not None:
        try:
            _pool.putconn(conn)
        except Exception as e:
            print(f"[DB WARNING] Failed to return connection: {e}")


@contextmanager
def db_connection():
    conn = None
    try:
        conn = get_db()
        yield conn
    except psycopg2_pool.PoolError:
        print("[DB POOL EXHAUSTED] All connections in use!")
        raise
    except Exception:
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
        raise
    finally:
        return_db(conn)


_config_cache = {}  
_CONFIG_TTL = 30  


def get_config_cached() -> dict:
    global _config_cache
    cache_key = "global"
    now = time.time()

    if cache_key in _config_cache:
        cached = _config_cache[cache_key]
        if now - cached["timestamp"] < _CONFIG_TTL:
            return cached["data"]

    config = _get_config_from_db()
    _config_cache[cache_key] = {"data": config, "timestamp": now}
    return config


def invalidate_config_cache():
    global _config_cache
    _config_cache.clear()


def _get_config_from_db() -> dict:
    conn = get_db()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM config WHERE id = 1")
        row = cursor.fetchone()
        cursor.close()
        return dict(row) if row else {}
    finally:
        return_db(conn)


def init_db():
    conn = get_db()
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS config (
                    id INTEGER PRIMARY KEY DEFAULT 1 CHECK (id = 1),
                    leaderboard_channel_id BIGINT,
                    elo_updates_channel_id BIGINT,
                    match_category_id BIGINT,
                    transaction_channel_id BIGINT,
                    referee_channel_id BIGINT,
                    logs_channel_id BIGINT,
                    ref_role_id BIGINT,
                    team_perms_role_id BIGINT,
                    elo_perms_role_id BIGINT,
                    captain_role_id BIGINT,
                    vice_captain_role_id BIGINT,
                    suspended_role_id BIGINT,
                    leaderboard_message_id BIGINT,
                    transactions_open BOOLEAN NOT NULL DEFAULT FALSE
                )
            """)

            cursor.execute("""
                ALTER TABLE config
                ADD COLUMN IF NOT EXISTS head_of_refs_role_id BIGINT
            """)

            cursor.execute("""
                ALTER TABLE config
                ADD COLUMN IF NOT EXISTS logs_channel_id BIGINT
            """)

            cursor.execute("""
                ALTER TABLE config
                ADD COLUMN IF NOT EXISTS cooldown_hours INTEGER DEFAULT 24
            """)

            cursor.execute("""
                INSERT INTO config (id) VALUES (1) ON CONFLICT (id) DO NOTHING
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS teams (
                    team_role_id BIGINT PRIMARY KEY,
                    captain_user_id BIGINT NOT NULL,
                    elo INTEGER NOT NULL DEFAULT 1000,
                    wins INTEGER NOT NULL DEFAULT 0,
                    losses INTEGER NOT NULL DEFAULT 0
                )
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS vice_captains (
                    team_role_id BIGINT NOT NULL REFERENCES teams(team_role_id) ON DELETE CASCADE,
                    user_id BIGINT NOT NULL,
                    PRIMARY KEY (team_role_id, user_id)
                )
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS roster (
                    team_role_id BIGINT NOT NULL REFERENCES teams(team_role_id) ON DELETE CASCADE,
                    user_id BIGINT NOT NULL,
                    position TEXT NOT NULL CHECK (position IN ('Setter', 'Libero', 'Wing Spiker', 'Defensive Specialist')),
                    rank TEXT NOT NULL CHECK (rank IN ('Starter', 'Substitute')),
                    PRIMARY KEY (team_role_id, user_id)
                )
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS matches (
                    match_id SERIAL PRIMARY KEY,
                    channel_id BIGINT,
                    team1_role_id BIGINT NOT NULL REFERENCES teams(team_role_id),
                    team2_role_id BIGINT NOT NULL REFERENCES teams(team_role_id),
                    challenger_team_role_id BIGINT NOT NULL REFERENCES teams(team_role_id),
                    challenged_team_role_id BIGINT NOT NULL REFERENCES teams(team_role_id),
                    team1_elo_locked INTEGER NOT NULL,
                    team2_elo_locked INTEGER NOT NULL,
                    elo_diff_locked INTEGER NOT NULL,
                    dodge_allowed BOOLEAN NOT NULL DEFAULT FALSE,
                    status TEXT NOT NULL DEFAULT 'OPEN' CHECK (status IN ('OPEN', 'SCHEDULED', 'FINISHED', 'CANCELLED')),
                    scheduled_time_utc TIMESTAMPTZ,
                    pending_reschedule_time_utc TIMESTAMPTZ,
                    pending_reschedule_by_team_role_id BIGINT,
                    reminded_captains BOOLEAN NOT NULL DEFAULT FALSE,
                    reminded_refs BOOLEAN NOT NULL DEFAULT FALSE,
                    reminded_captains_15m BOOLEAN NOT NULL DEFAULT FALSE,
                    ref_signup_message_id BIGINT
                )
            """)

            cursor.execute("""
                DO $$
                BEGIN
                    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                                  WHERE table_name='matches' AND column_name='reminded_captains_15m') THEN
                        ALTER TABLE matches ADD COLUMN reminded_captains_15m BOOLEAN NOT NULL DEFAULT FALSE;
                    END IF;
                END $$;
            """)

            cursor.execute("""
                DO $$
                BEGIN
                    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                                  WHERE table_name='matches' AND column_name='created_at_utc') THEN
                        ALTER TABLE matches ADD COLUMN created_at_utc TIMESTAMPTZ DEFAULT NOW();
                        -- Set created_at_utc to NOW() for existing rows that have NULL
                        UPDATE matches SET created_at_utc = NOW() WHERE created_at_utc IS NULL;
                    END IF;
                END $$;
            """)

            cursor.execute("""
                DO $$
                BEGIN
                    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                                  WHERE table_name='matches' AND column_name='pending_schedule_time_utc') THEN
                        ALTER TABLE matches ADD COLUMN pending_schedule_time_utc TIMESTAMPTZ;
                    END IF;
                    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                                  WHERE table_name='matches' AND column_name='pending_schedule_by_team_role_id') THEN
                        ALTER TABLE matches ADD COLUMN pending_schedule_by_team_role_id BIGINT;
                    END IF;
                    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                                  WHERE table_name='matches' AND column_name='pending_schedule_message_id') THEN
                        ALTER TABLE matches ADD COLUMN pending_schedule_message_id BIGINT;
                    END IF;
                    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                                  WHERE table_name='matches' AND column_name='pending_created_at_utc') THEN
                        ALTER TABLE matches ADD COLUMN pending_created_at_utc TIMESTAMPTZ;
                    END IF;
                    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                                  WHERE table_name='matches' AND column_name='schedule_pending') THEN
                        ALTER TABLE matches ADD COLUMN schedule_pending BOOLEAN DEFAULT FALSE;
                    END IF;
                END $$;
            """)

            cursor.execute("""
                DO $$
                BEGIN
                    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                                  WHERE table_name='matches' AND column_name='reminded_refs_15m') THEN
                        ALTER TABLE matches ADD COLUMN reminded_refs_15m BOOLEAN DEFAULT FALSE;
                    END IF;
                END $$;
            """)

            cursor.execute("""
                DO $$
                BEGIN
                    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                                  WHERE table_name='matches' AND column_name='match_info_message_id') THEN
                        ALTER TABLE matches ADD COLUMN match_info_message_id BIGINT;
                    END IF;
                END $$;
            """)

            cursor.execute("""
                DO $$
                BEGIN
                    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                                  WHERE table_name='matches' AND column_name='created_at_utc') THEN
                        ALTER TABLE matches ADD COLUMN created_at_utc TIMESTAMPTZ DEFAULT NOW();
                    END IF;
                    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                                  WHERE table_name='matches' AND column_name='reminded_deadline_1d') THEN
                        ALTER TABLE matches ADD COLUMN reminded_deadline_1d BOOLEAN NOT NULL DEFAULT FALSE;
                    END IF;
                END $$;
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS match_refs (
                    match_id INTEGER NOT NULL REFERENCES matches(match_id) ON DELETE CASCADE,
                    team_side INTEGER NOT NULL CHECK (team_side IN (1, 2)),
                    ref_user_id BIGINT NOT NULL,
                    PRIMARY KEY (match_id, team_side),
                    UNIQUE (match_id, ref_user_id)
                )
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS challenge_cooldowns (
                    challenger_team_role_id BIGINT NOT NULL REFERENCES teams(team_role_id) ON DELETE CASCADE,
                    challenged_team_role_id BIGINT NOT NULL REFERENCES teams(team_role_id) ON DELETE CASCADE,
                    expires_at_utc TIMESTAMPTZ NOT NULL,
                    PRIMARY KEY (challenger_team_role_id, challenged_team_role_id)
                )
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS dodge_cooldowns (
                    challenger_team_role_id BIGINT NOT NULL REFERENCES teams(team_role_id) ON DELETE CASCADE,
                    challenged_team_role_id BIGINT NOT NULL REFERENCES teams(team_role_id) ON DELETE CASCADE,
                    until_utc TIMESTAMPTZ NOT NULL,
                    PRIMARY KEY (challenger_team_role_id, challenged_team_role_id)
                )
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS no_shows (
                    id SERIAL PRIMARY KEY,
                    match_id INTEGER NOT NULL REFERENCES matches(match_id) ON DELETE CASCADE,
                    accused_team_role_id BIGINT NOT NULL REFERENCES teams(team_role_id),
                    reporter_user_id BIGINT NOT NULL,
                    reason TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING', 'CONFIRMED', 'REJECTED', 'RESOLVED')),
                    reviewed_by_user_id BIGINT,
                    resolution TEXT,
                    created_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS forfeit_events (
                    id SERIAL PRIMARY KEY,
                    team_role_id BIGINT NOT NULL REFERENCES teams(team_role_id) ON DELETE CASCADE,
                    created_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS team_affiliations (
                    user_id BIGINT PRIMARY KEY,
                    team_role_id BIGINT NOT NULL REFERENCES teams(team_role_id) ON DELETE CASCADE,
                    affiliation_type TEXT NOT NULL CHECK (affiliation_type IN ('CAPTAIN', 'VICE', 'ROSTER'))
                )
            """)

            cursor.execute("SELECT COUNT(*) as cnt FROM team_affiliations")
            row = cursor.fetchone()
            affiliation_count = row["cnt"] if row else 0

            if affiliation_count == 0:
                conflicts = []

                existing_affiliations = {}  

                # 1. Captains
                cursor.execute("SELECT team_role_id, captain_user_id FROM teams WHERE captain_user_id IS NOT NULL")
                for row in cursor.fetchall():
                    uid = row["captain_user_id"]
                    if uid not in existing_affiliations:
                        existing_affiliations[uid] = []
                    existing_affiliations[uid].append((row["team_role_id"], "CAPTAIN"))

                # 2. Vice Captains
                cursor.execute("SELECT team_role_id, user_id FROM vice_captains")
                for row in cursor.fetchall():
                    uid = row["user_id"]
                    if uid not in existing_affiliations:
                        existing_affiliations[uid] = []
                    existing_affiliations[uid].append((row["team_role_id"], "VICE"))

                # 3. Roster
                cursor.execute("SELECT team_role_id, user_id FROM roster")
                for row in cursor.fetchall():
                    uid = row["user_id"]
                    if uid not in existing_affiliations:
                        existing_affiliations[uid] = []
                    existing_affiliations[uid].append((row["team_role_id"], "ROSTER"))

                for user_id, affiliations in existing_affiliations.items():
                    unique_teams = set(aff[0] for aff in affiliations)
                    if len(unique_teams) > 1:
                        conflicts.append((user_id, affiliations))
                        print(f"[AFFILIATION CONFLICT] User {user_id} is affiliated with multiple teams: {affiliations}")
                    else:
                        team_role_id = affiliations[0][0]
                        if any(a[1] == "CAPTAIN" for a in affiliations):
                            aff_type = "CAPTAIN"
                        elif any(a[1] == "VICE" for a in affiliations):
                            aff_type = "VICE"
                        else:
                            aff_type = "ROSTER"
                        cursor.execute(
                            "INSERT INTO team_affiliations (user_id, team_role_id, affiliation_type) VALUES (%s, %s, %s) ON CONFLICT (user_id) DO NOTHING",
                            (user_id, team_role_id, aff_type)
                        )

                if conflicts:
                    print(f"[CRITICAL] Found {len(conflicts)} users affiliated with multiple teams!")
                    print("[CRITICAL] Manual intervention required to resolve these conflicts.")
                    for user_id, affs in conflicts:
                        print(f"  User {user_id}: {affs}")

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS ref_activity_awards (
                    guild_id BIGINT NOT NULL,
                    match_id BIGINT NOT NULL,
                    ref_user_id BIGINT NOT NULL,
                    awarded_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    UNIQUE (guild_id, match_id, ref_user_id)
                )
            """)

            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_ref_activity_guild_time
                ON ref_activity_awards (guild_id, awarded_at_utc)
            """)

            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_ref_activity_guild_ref
                ON ref_activity_awards (guild_id, ref_user_id)
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS elo_adjustments (
                    id SERIAL PRIMARY KEY,
                    team_role_id BIGINT NOT NULL REFERENCES teams(team_role_id) ON DELETE CASCADE,
                    old_elo INTEGER NOT NULL,
                    new_elo INTEGER NOT NULL,
                    staff_user_id BIGINT NOT NULL,
                    reason TEXT NOT NULL,
                    adjusted_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)

            cursor.execute("""
                DO $$
                BEGIN
                    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                                  WHERE table_name='teams' AND column_name='no_show_count') THEN
                        ALTER TABLE teams ADD COLUMN no_show_count INTEGER NOT NULL DEFAULT 0;
                    END IF;
                    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                                  WHERE table_name='teams' AND column_name='last_no_show_at_utc') THEN
                        ALTER TABLE teams ADD COLUMN last_no_show_at_utc TIMESTAMPTZ;
                    END IF;
                END $$;
            """)

            cursor.execute("""
                DO $$
                BEGIN
                    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                                  WHERE table_name='matches' AND column_name='finished_at_utc') THEN
                        ALTER TABLE matches ADD COLUMN finished_at_utc TIMESTAMPTZ;
                    END IF;
                END $$;
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS leaderboard_cache (
                    team_role_id BIGINT PRIMARY KEY REFERENCES teams(team_role_id) ON DELETE CASCADE,
                    elo INTEGER NOT NULL,
                    wins INTEGER NOT NULL,
                    losses INTEGER NOT NULL,
                    rank INTEGER,
                    updated_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS cooldowns (
                    id SERIAL PRIMARY KEY,
                    challenger_team_role_id BIGINT NOT NULL REFERENCES teams(team_role_id) ON DELETE CASCADE,
                    challenged_team_role_id BIGINT NOT NULL REFERENCES teams(team_role_id) ON DELETE CASCADE,
                    type TEXT NOT NULL CHECK (type IN ('CHALLENGE', 'DODGE')),
                    expires_at_utc TIMESTAMPTZ NOT NULL,
                    created_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    UNIQUE (challenger_team_role_id, challenged_team_role_id, type)
                )
            """)

            cursor.execute("""
                INSERT INTO cooldowns (challenger_team_role_id, challenged_team_role_id, type, expires_at_utc, created_at_utc)
                SELECT challenger_team_role_id, challenged_team_role_id, 'CHALLENGE', expires_at_utc, NOW()
                FROM challenge_cooldowns
                ON CONFLICT DO NOTHING
            """)
            cursor.execute("""
                INSERT INTO cooldowns (challenger_team_role_id, challenged_team_role_id, type, expires_at_utc, created_at_utc)
                SELECT challenger_team_role_id, challenged_team_role_id, 'DODGE', until_utc, NOW()
                FROM dodge_cooldowns
                ON CONFLICT DO NOTHING
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS challenge_rate_limits (
                    team_role_id BIGINT NOT NULL REFERENCES teams(team_role_id) ON DELETE CASCADE,
                    challenge_count INTEGER NOT NULL DEFAULT 1,
                    window_start_utc TIMESTAMPTZ NOT NULL,
                    blocked_until_utc TIMESTAMPTZ,
                    PRIMARY KEY (team_role_id)
                )
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS command_rate_limits (
                    user_id BIGINT PRIMARY KEY,
                    command_count INTEGER NOT NULL DEFAULT 1,
                    window_start_utc TIMESTAMPTZ NOT NULL,
                    blocked_until_utc TIMESTAMPTZ
                )
            """)

            cursor.execute("""
                DO $$
                BEGIN
                    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                                  WHERE table_name='matches' AND column_name='mode') THEN
                        ALTER TABLE matches ADD COLUMN mode TEXT NOT NULL DEFAULT 'ELO' CHECK (mode IN ('ELO', 'LEAGUE'));
                        -- Set all existing matches to ELO mode
                        UPDATE matches SET mode = 'ELO' WHERE mode IS NULL;
                    END IF;
                END $$;
            """)

            cursor.execute("""
                ALTER TABLE config
                ADD COLUMN IF NOT EXISTS league_perms_role_id BIGINT
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS league_state (
                    id INTEGER PRIMARY KEY DEFAULT 1 CHECK (id = 1),
                    season_active BOOLEAN NOT NULL DEFAULT FALSE,
                    season_locked BOOLEAN NOT NULL DEFAULT FALSE,
                    season_name TEXT,
                    current_stage TEXT CHECK (current_stage IN ('GROUPS', 'PLAYOFFS')),
                    current_round INTEGER DEFAULT 0,
                    dashboard_channel_id BIGINT,
                    roster_lock_enabled BOOLEAN NOT NULL DEFAULT FALSE,
                    league_deadline_utc TIMESTAMPTZ,
                    created_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)

            cursor.execute("""
                INSERT INTO league_state (id, season_active, season_locked)
                VALUES (1, FALSE, FALSE)
                ON CONFLICT (id) DO NOTHING
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS league_groups (
                    group_id SERIAL PRIMARY KEY,
                    group_name TEXT NOT NULL,
                    stage_id TEXT NOT NULL,
                    created_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS league_group_teams (
                    group_id INTEGER NOT NULL REFERENCES league_groups(group_id) ON DELETE CASCADE,
                    team_role_id BIGINT NOT NULL REFERENCES teams(team_role_id) ON DELETE CASCADE,
                    joined_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (group_id, team_role_id)
                )
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS league_standings (
                    id SERIAL PRIMARY KEY,
                    group_id INTEGER NOT NULL REFERENCES league_groups(group_id) ON DELETE CASCADE,
                    team_role_id BIGINT NOT NULL REFERENCES teams(team_role_id) ON DELETE CASCADE,
                    sets_won INTEGER NOT NULL DEFAULT 0,
                    sets_lost INTEGER NOT NULL DEFAULT 0,
                    sets_played INTEGER NOT NULL DEFAULT 0,
                    updated_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    UNIQUE (group_id, team_role_id)
                )
            """)

            cursor.execute("""
                DO $$
                BEGIN
                    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                                  WHERE table_name='matches' AND column_name='group_id') THEN
                        ALTER TABLE matches ADD COLUMN group_id INTEGER REFERENCES league_groups(group_id);
                    END IF;
                    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                                  WHERE table_name='matches' AND column_name='league_round') THEN
                        ALTER TABLE matches ADD COLUMN league_round INTEGER;
                    END IF;
                    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                                  WHERE table_name='matches' AND column_name='deadline_utc') THEN
                        ALTER TABLE matches ADD COLUMN deadline_utc TIMESTAMPTZ;
                    END IF;
                    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                                  WHERE table_name='matches' AND column_name='overdue_warned') THEN
                        ALTER TABLE matches ADD COLUMN overdue_warned BOOLEAN DEFAULT FALSE;
                    END IF;
                END $$;
            """)

            cursor.execute("""
                DO $$
                BEGIN
                    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                                  WHERE table_name='matches' AND column_name='bracket') THEN
                        ALTER TABLE matches ADD COLUMN bracket TEXT CHECK (bracket IN ('WINNERS', 'LOSERS'));
                    END IF;
                    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                                  WHERE table_name='matches' AND column_name='series_format') THEN
                        ALTER TABLE matches ADD COLUMN series_format TEXT DEFAULT 'BO3' CHECK (series_format IN ('BO3', 'BO5'));
                    END IF;
                END $$;
            """)

            cursor.execute("""
                DO $$
                BEGIN
                    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                                  WHERE table_name='league_state' AND column_name='current_bracket') THEN
                        ALTER TABLE league_state ADD COLUMN current_bracket TEXT CHECK (current_bracket IN ('WINNERS', 'LOSERS'));
                    END IF;
                END $$;
            """)

        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        return_db(conn)
    print("[DB] Database initialized successfully.")


def award_ref_activity_for_match(guild_id: int, match_id: int) -> int:
    """Award referee activity for a completed match. Returns count of newly awarded refs."""
    conn = get_db()
    cursor = None
    newly_awarded = 0

    try:
        cursor = conn.cursor()

        refs = get_match_refs(match_id)

        if not refs:
            return 0

        ref_user_ids = []
        for ref in refs:
            if isinstance(ref, dict):
                ref_user_id = ref.get("ref_user_id")
            else:
                ref_user_id = ref

            if ref_user_id is not None:
                ref_user_ids.append(ref_user_id)

        if not ref_user_ids:
            return 0

        for ref_user_id in ref_user_ids:
            cursor.execute("""
                INSERT INTO ref_activity_awards (guild_id, match_id, ref_user_id, awarded_at_utc)
                VALUES (%s, %s, %s, NOW())
                ON CONFLICT (guild_id, match_id, ref_user_id) DO NOTHING
            """, (guild_id, match_id, ref_user_id))

            newly_awarded += cursor.rowcount

        conn.commit()

        if newly_awarded > 0:
            print(f"[REF ACTIVITY] Awarded {newly_awarded} ref(s) for match #{match_id}")

        return newly_awarded

    except Exception as e:
        print(f"[REF ACTIVITY ERROR] Failed to award for match #{match_id}: {e}")
        print(traceback.format_exc())
        try:
            conn.rollback()
        except:
            pass
        return 0
    finally:
        if cursor:
            cursor.close()
        return_db(conn)


def get_config() -> dict:
    return get_config_cached()


def update_config(**kwargs):
    conn = get_db()
    try:
        cursor = conn.cursor()
        sets = ", ".join(f"{k} = %s" for k in kwargs.keys())
        cursor.execute(f"UPDATE config SET {sets} WHERE id = 1", list(kwargs.values()))
        conn.commit()
        cursor.close()
    finally:
        return_db(conn)
    # Invalidate cache after update
    invalidate_config_cache()


def get_team(team_role_id: int) -> Optional[dict]:
    conn = get_db()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM teams WHERE team_role_id = %s", (team_role_id,))
        row = cursor.fetchone()
        cursor.close()
        return dict(row) if row else None
    finally:
        return_db(conn)


def get_all_teams() -> List[dict]:
    conn = get_db()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM teams ORDER BY elo DESC")
        rows = cursor.fetchall()
        cursor.close()
        return [dict(row) for row in rows]
    finally:
        return_db(conn)


def create_team(team_role_id: int, captain_user_id: int, starting_elo: int = 1000):
    """Create a new team."""
    conn = get_db()
    try:
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO teams (team_role_id, captain_user_id, elo) VALUES (%s, %s, %s)",
            (team_role_id, captain_user_id, starting_elo)
        )
        conn.commit()
        cursor.close()
    finally:
        return_db(conn)


def update_team(team_role_id: int, **kwargs):
    conn = get_db()
    try:
        cursor = conn.cursor()
        sets = ", ".join(f"{k} = %s" for k in kwargs.keys())
        cursor.execute(f"UPDATE teams SET {sets} WHERE team_role_id = %s", list(kwargs.values()) + [team_role_id])
        conn.commit()
        cursor.close()
    finally:
        return_db(conn)


def delete_team(team_role_id: int):
    conn = get_db()
    try:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM teams WHERE team_role_id = %s", (team_role_id,))
        conn.commit()
        cursor.close()
    finally:
        return_db(conn)


def disband_team_full(team_role_id: int) -> Tuple[bool, str, List[int]]:
    conn = get_db()
    try:
        cursor = conn.cursor()

        cursor.execute("""
            SELECT match_id, channel_id FROM matches
            WHERE team1_role_id = %s OR team2_role_id = %s
              OR challenger_team_role_id = %s OR challenged_team_role_id = %s
        """, (team_role_id, team_role_id, team_role_id, team_role_id))
        match_rows = cursor.fetchall()
        match_ids = [row["match_id"] for row in match_rows]
        channel_ids = [row["channel_id"] for row in match_rows if row["channel_id"]]

        if match_ids:
            placeholders = ",".join(["%s"] * len(match_ids))
            cursor.execute(f"DELETE FROM no_shows WHERE match_id IN ({placeholders})", match_ids)

        if match_ids:
            cursor.execute(f"DELETE FROM match_refs WHERE match_id IN ({placeholders})", match_ids)

        cursor.execute("""
            DELETE FROM matches
            WHERE team1_role_id = %s OR team2_role_id = %s
              OR challenger_team_role_id = %s OR challenged_team_role_id = %s
        """, (team_role_id, team_role_id, team_role_id, team_role_id))

        cursor.execute("""
            DELETE FROM challenge_cooldowns
            WHERE challenger_team_role_id = %s OR challenged_team_role_id = %s
        """, (team_role_id, team_role_id))

        cursor.execute("DELETE FROM vice_captains WHERE team_role_id = %s", (team_role_id,))

        cursor.execute("DELETE FROM roster WHERE team_role_id = %s", (team_role_id,))

        cursor.execute("DELETE FROM team_affiliations WHERE team_role_id = %s", (team_role_id,))

        cursor.execute("DELETE FROM teams WHERE team_role_id = %s", (team_role_id,))

        conn.commit()
        cursor.close()
        return True, "", channel_ids

    except Exception as e:
        conn.rollback()
        return False, str(e), []
    finally:
        return_db(conn)


def get_vice_captains(team_role_id: int) -> List[int]:
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT user_id FROM vice_captains WHERE team_role_id = %s", (team_role_id,))
            rows = cur.fetchall()
            return [row["user_id"] for row in rows]
    finally:
        return_db(conn)


def add_vice_captain(team_role_id: int, user_id: int):
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO vice_captains (team_role_id, user_id) VALUES (%s, %s) ON CONFLICT DO NOTHING", (team_role_id, user_id))
            conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        return_db(conn)


def remove_vice_captain(team_role_id: int, user_id: int):
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM vice_captains WHERE team_role_id = %s AND user_id = %s", (team_role_id, user_id))
            conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        return_db(conn)


def get_roster(team_role_id: int) -> List[dict]:
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM roster WHERE team_role_id = %s", (team_role_id,))
            rows = cur.fetchall()
            return [dict(row) for row in rows]
    finally:
        return_db(conn)


def get_player_team(user_id: int) -> Optional[int]:
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT team_role_id FROM roster WHERE user_id = %s", (user_id,))
            row = cur.fetchone()
            return row["team_role_id"] if row else None
    finally:
        return_db(conn)


def get_user_captain_teams(user_id: int) -> List[int]:
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT team_role_id FROM teams WHERE captain_user_id = %s", (user_id,))
            rows = cur.fetchall()
            return [row["team_role_id"] for row in rows]
    finally:
        return_db(conn)


def get_user_vice_captain_teams(user_id: int) -> List[int]:
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT team_role_id FROM vice_captains WHERE user_id = %s", (user_id,))
            rows = cur.fetchall()
            return [row["team_role_id"] for row in rows]
    finally:
        return_db(conn)


def add_roster_member(team_role_id: int, user_id: int, position: str, rank: str):
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO roster (team_role_id, user_id, position, rank) VALUES (%s, %s, %s, %s)",
                (team_role_id, user_id, position, rank)
            )
            conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        return_db(conn)


def remove_roster_member(team_role_id: int, user_id: int):
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM roster WHERE team_role_id = %s AND user_id = %s", (team_role_id, user_id))
            conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        return_db(conn)


def update_roster_member(team_role_id: int, user_id: int, **kwargs):
    conn = get_db()
    try:
        with conn.cursor() as cur:
            sets = ", ".join(f"{k} = %s" for k in kwargs.keys())
            cur.execute(
                f"UPDATE roster SET {sets} WHERE team_role_id = %s AND user_id = %s",
                list(kwargs.values()) + [team_role_id, user_id]
            )
            conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        return_db(conn)


def get_match(match_id: int) -> Optional[dict]:
    conn = get_db()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM matches WHERE match_id = %s", (match_id,))
        row = cursor.fetchone()
        cursor.close()
        return dict(row) if row else None
    finally:
        return_db(conn)


def get_match_by_channel(channel_id: int) -> Optional[dict]:
    conn = get_db()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM matches WHERE channel_id = %s", (channel_id,))
        row = cursor.fetchone()
        cursor.close()
        return dict(row) if row else None
    finally:
        return_db(conn)


def create_match(**kwargs) -> int:
    conn = get_db()
    try:
        cursor = conn.cursor()
        columns = ", ".join(kwargs.keys())
        placeholders = ", ".join(["%s"] * len(kwargs))
        cursor.execute(
            f"INSERT INTO matches ({columns}) VALUES ({placeholders}) RETURNING match_id",
            list(kwargs.values())
        )
        match_id = cursor.fetchone()["match_id"]
        conn.commit()
        cursor.close()
        return match_id
    finally:
        return_db(conn)


def update_match(match_id: int, **kwargs):
    conn = get_db()
    try:
        cursor = conn.cursor()
        sets = ", ".join(f"{k} = %s" for k in kwargs.keys())
        cursor.execute(f"UPDATE matches SET {sets} WHERE match_id = %s", list(kwargs.values()) + [match_id])
        conn.commit()
        cursor.close()
    finally:
        return_db(conn)


def delete_match(match_id: int):
    conn = get_db()
    try:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM matches WHERE match_id = %s", (match_id,))
        conn.commit()
        cursor.close()
    finally:
        return_db(conn)


def get_open_matches() -> List[dict]:
    conn = get_db()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM matches WHERE status IN ('OPEN', 'SCHEDULED')")
        rows = cursor.fetchall()
        cursor.close()
        return [dict(row) for row in rows]
    finally:
        return_db(conn)


def get_match_refs(match_id: int) -> List[dict]:
    conn = get_db()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM match_refs WHERE match_id = %s", (match_id,))
        rows = cursor.fetchall()
        cursor.close()
        return [dict(row) for row in rows]
    finally:
        return_db(conn)


def add_match_ref(match_id: int, team_side: int, ref_user_id: int):
    conn = get_db()
    try:
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO match_refs (match_id, team_side, ref_user_id) VALUES (%s, %s, %s)",
            (match_id, team_side, ref_user_id)
        )
        conn.commit()
        cursor.close()
    finally:
        return_db(conn)


def remove_match_ref(match_id: int, ref_user_id: int):
    conn = get_db()
    try:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM match_refs WHERE match_id = %s AND ref_user_id = %s", (match_id, ref_user_id))
        conn.commit()
        cursor.close()
    finally:
        return_db(conn)


def get_ref_for_match_side(match_id: int, team_side: int) -> Optional[int]:
    conn = get_db()
    try:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT ref_user_id FROM match_refs WHERE match_id = %s AND team_side = %s",
            (match_id, team_side)
        )
        row = cursor.fetchone()
        cursor.close()
        return row["ref_user_id"] if row else None
    finally:
        return_db(conn)


def is_user_ref_for_match(match_id: int, user_id: int) -> bool:
    conn = get_db()
    try:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT 1 FROM match_refs WHERE match_id = %s AND ref_user_id = %s",
            (match_id, user_id)
        )
        row = cursor.fetchone()
        cursor.close()
        return row is not None
    finally:
        return_db(conn)


def get_cooldown(team1_id: int, team2_id: int) -> Optional[datetime]:
    if DISABLE_CHALLENGE_COOLDOWN:
        return None

    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT expires_at_utc FROM challenge_cooldowns
                   WHERE ((challenger_team_role_id = %s AND challenged_team_role_id = %s)
                      OR (challenger_team_role_id = %s AND challenged_team_role_id = %s))
                   AND expires_at_utc > %s
                   ORDER BY expires_at_utc DESC
                   LIMIT 1""",
                (team1_id, team2_id, team2_id, team1_id, utc_now())
            )
            row = cur.fetchone()
            if row and row["expires_at_utc"]:
                expires = row["expires_at_utc"]
                if isinstance(expires, str):
                    return datetime.fromisoformat(expires)
                return expires
            return None
    finally:
        return_db(conn)


def set_cooldown(team1_id: int, team2_id: int, expires_at: datetime):
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


def set_dodge_cooldown(challenger_team_role_id: int, challenged_team_role_id: int, hours: int = 24):
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


def get_dodge_cooldown_remaining(challenger_team_role_id: int, challenged_team_role_id: int) -> Optional[timedelta]:
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT until_utc FROM dodge_cooldowns
                   WHERE challenger_team_role_id = %s AND challenged_team_role_id = %s
                   AND until_utc > %s""",
                (challenger_team_role_id, challenged_team_role_id, utc_now())
            )
            row = cur.fetchone()
            if row:
                until = row["until_utc"]
                if isinstance(until, str):
                    until = datetime.fromisoformat(until)
                return until - utc_now()
            return None
    finally:
        return_db(conn)


def clear_expired_cooldowns():
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM challenge_cooldowns WHERE expires_at_utc < %s", (utc_now(),))
            cur.execute("DELETE FROM dodge_cooldowns WHERE until_utc < %s", (utc_now(),))
            conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        return_db(conn)


def create_no_show(match_id: int, accused_team_role_id: int, reporter_user_id: int, reason: str) -> int:
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO no_shows (match_id, accused_team_role_id, reporter_user_id, reason, created_at_utc)
                   VALUES (%s, %s, %s, %s, %s) RETURNING id""",
                (match_id, accused_team_role_id, reporter_user_id, reason, utc_now())
            )
            no_show_id = cur.fetchone()["id"]
            conn.commit()
            return no_show_id
    except Exception:
        conn.rollback()
        raise
    finally:
        return_db(conn)


def get_no_show(no_show_id: int) -> Optional[dict]:
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM no_shows WHERE id = %s", (no_show_id,))
            row = cur.fetchone()
            return dict(row) if row else None
    finally:
        return_db(conn)


def get_no_show_by_match(match_id: int) -> Optional[dict]:
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM no_shows WHERE match_id = %s ORDER BY id DESC LIMIT 1",
                (match_id,)
            )
            row = cur.fetchone()
            return dict(row) if row else None
    finally:
        return_db(conn)


def update_no_show(no_show_id: int, **kwargs):
    conn = get_db()
    try:
        with conn.cursor() as cur:
            sets = ", ".join(f"{k} = %s" for k in kwargs.keys())
            cur.execute(f"UPDATE no_shows SET {sets} WHERE id = %s", list(kwargs.values()) + [no_show_id])
            conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        return_db(conn)


def get_user_affiliation(user_id: int) -> Optional[dict]:
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT team_role_id, affiliation_type FROM team_affiliations WHERE user_id = %s",
                (user_id,)
            )
            row = cur.fetchone()
            return dict(row) if row else None
    finally:
        return_db(conn)


def check_affiliation_allowed(user_id: int, target_team_role_id: int, guild: discord.Guild) -> Tuple[bool, str]:
    affiliation = get_user_affiliation(user_id)
    if affiliation is None:
        return True, ""

    if affiliation["team_role_id"] == target_team_role_id:
        return True, ""

    other_team_role = guild.get_role(affiliation["team_role_id"]) if guild else None
    other_team_name = other_team_role.name if other_team_role else f"Team ID {affiliation['team_role_id']}"
    return False, f"That user is already affiliated with **{other_team_name}**. Remove them from that team first."


def add_affiliation(user_id: int, team_role_id: int, affiliation_type: str):
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO team_affiliations (user_id, team_role_id, affiliation_type)
                   VALUES (%s, %s, %s)
                   ON CONFLICT (user_id) DO UPDATE SET team_role_id = EXCLUDED.team_role_id, affiliation_type = EXCLUDED.affiliation_type""",
                (user_id, team_role_id, affiliation_type)
            )
            conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        return_db(conn)


def remove_affiliation(user_id: int):
    all_teams = get_all_teams()
    for team in all_teams:
        if team.get("captain_user_id") == user_id:
            raise ValueError(f"Cannot remove affiliation: user {user_id} is captain of team {team['team_role_id']}. Transfer ownership with /set-captain first.")

    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM team_affiliations WHERE user_id = %s", (user_id,))
            conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        return_db(conn)


def update_affiliation_type(user_id: int, new_type: str):
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE team_affiliations SET affiliation_type = %s WHERE user_id = %s",
                (new_type, user_id)
            )
            conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        return_db(conn)


def clear_team_affiliations(team_role_id: int):
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM team_affiliations WHERE team_role_id = %s", (team_role_id,))
            conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        return_db(conn)


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
        )
        return False

    if not has_league_perms(interaction.user):
        await interaction.followup.send(
            content="You need the League Perms role to use this command.",
            ephemeral=True
        )
        return False

    return True


def get_league_state() -> dict:
    conn = get_db()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM league_state WHERE id = 1")
        state = cursor.fetchone()
        cursor.close()
        return dict(state) if state else {
            "season_active": False,
            "season_locked": False,
            "season_name": None,
            "current_stage": None,
            "current_round": 0,
            "current_bracket": None,
            "dashboard_channel_id": None,
            "roster_lock_enabled": False,
            "league_deadline_utc": None
        }
    finally:
        return_db(conn)


def update_league_state(**kwargs):
    conn = get_db()
    try:
        cursor = conn.cursor()
        set_clauses = []
        values = []
        for key, value in kwargs.items():
            set_clauses.append(f"{key} = %s")
            values.append(value)

        if set_clauses:
            set_clauses.append("updated_at_utc = NOW()")
            query = f"UPDATE league_state SET {', '.join(set_clauses)} WHERE id = 1"
            cursor.execute(query, values)
            conn.commit()
        cursor.close()
    finally:
        return_db(conn)


def validate_match_allowed(mode: str) -> tuple[bool, Optional[str]]:
    
    state = get_league_state()

    if state["season_locked"]:
        return False, "**Season Locked:** No matches can be created while the season is locked."

    if mode == "LEAGUE" and not state["season_active"]:
        return False, "**Off-Season:** League matches cannot be created during off-season."

    return True, None




def create_league_group(group_name: str, stage_id: str) -> int:
    conn = get_db()
    try:
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO league_groups (group_name, stage_id) VALUES (%s, %s) RETURNING group_id",
            (group_name, stage_id)
        )
        group_id = cursor.fetchone()["group_id"]
        conn.commit()
        cursor.close()
        return group_id
    finally:
        return_db(conn)


def add_team_to_group(group_id: int, team_role_id: int):
    conn = get_db()
    try:
        cursor = conn.cursor()

        cursor.execute(
            "INSERT INTO league_group_teams (group_id, team_role_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
            (group_id, team_role_id)
        )

        cursor.execute(
            """INSERT INTO league_standings (group_id, team_role_id, sets_won, sets_lost, sets_played)
               VALUES (%s, %s, 0, 0, 0)
               ON CONFLICT (group_id, team_role_id) DO NOTHING""",
            (group_id, team_role_id)
        )

        conn.commit()
        cursor.close()
    finally:
        return_db(conn)


def get_group_standings(group_id: int) -> List[dict]:
    conn = get_db()
    try:
        cursor = conn.cursor()
        cursor.execute(
            """SELECT team_role_id, sets_won, sets_lost, sets_played,
                      (sets_won - sets_lost) as set_diff
               FROM league_standings
               WHERE group_id = %s
               ORDER BY set_diff DESC, sets_won DESC""",
            (group_id,)
        )
        standings = cursor.fetchall()
        cursor.close()
        return [dict(s) for s in standings]
    finally:
        return_db(conn)


def update_league_standings(group_id: int, team_role_id: int, sets_won: int, sets_lost: int):
    conn = get_db()
    try:
        cursor = conn.cursor()
        cursor.execute(
            """UPDATE league_standings
               SET sets_won = sets_won + %s,
                   sets_lost = sets_lost + %s,
                   sets_played = sets_played + %s,
                   updated_at_utc = NOW()
               WHERE group_id = %s AND team_role_id = %s""",
            (sets_won, sets_lost, sets_won + sets_lost, group_id, team_role_id)
        )
        conn.commit()
        cursor.close()
    finally:
        return_db(conn)


def get_group_teams(group_id: int) -> List[int]:
    conn = get_db()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT team_role_id FROM league_group_teams WHERE group_id = %s", (group_id,))
        teams = cursor.fetchall()
        cursor.close()
        return [t["team_role_id"] for t in teams]
    finally:
        return_db(conn)


def check_duplicate_league_matchup(team1_id: int, team2_id: int, stage_id: str, round_num: int) -> bool:
    conn = get_db()
    try:
        cursor = conn.cursor()
        cursor.execute(
            """SELECT 1 FROM matches m
               JOIN league_groups g ON m.group_id = g.group_id
               WHERE ((m.team1_role_id = %s AND m.team2_role_id = %s)
                   OR (m.team1_role_id = %s AND m.team2_role_id = %s))
                 AND g.stage_id = %s
                 AND m.league_round = %s
                 AND m.mode = 'LEAGUE'
               LIMIT 1""",
            (team1_id, team2_id, team2_id, team1_id, stage_id, round_num)
        )
        exists = cursor.fetchone() is not None
        cursor.close()
        return exists
    finally:
        return_db(conn)


def check_duplicate_playoff_matchup(team1_id: int, team2_id: int, bracket: str, round_num: int) -> bool:
    conn = get_db()
    try:
        cursor = conn.cursor()
        cursor.execute(
            """SELECT 1 FROM matches
               WHERE ((team1_role_id = %s AND team2_role_id = %s)
                   OR (team1_role_id = %s AND team2_role_id = %s))
                 AND bracket = %s
                 AND league_round = %s
                 AND mode = 'LEAGUE'
               LIMIT 1""",
            (team1_id, team2_id, team2_id, team1_id, bracket, round_num)
        )
        exists = cursor.fetchone() is not None
        cursor.close()
        return exists
    finally:
        return_db(conn)


def replace_team_in_group(group_id: int, old_team_id: int, new_team_id: int):
    """Replace a team in a group. Existing results stay, new team starts fresh."""
    conn = get_db()
    try:
        cursor = conn.cursor()

        cursor.execute(
            "DELETE FROM league_group_teams WHERE group_id = %s AND team_role_id = %s",
            (group_id, old_team_id)
        )

        cursor.execute(
            "DELETE FROM league_standings WHERE group_id = %s AND team_role_id = %s",
            (group_id, old_team_id)
        )

        cursor.execute(
            "INSERT INTO league_group_teams (group_id, team_role_id) VALUES (%s, %s)",
            (group_id, new_team_id)
        )

        cursor.execute(
            """INSERT INTO league_standings (group_id, team_role_id, sets_won, sets_lost, sets_played)
               VALUES (%s, %s, 0, 0, 0)""",
            (group_id, new_team_id)
        )

        conn.commit()
        cursor.close()
    finally:
        return_db(conn)
