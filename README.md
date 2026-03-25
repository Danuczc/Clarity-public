# Clarity Bot

A competitive volleyball league bot for Discord. Clarity manages team rosters, ELO ratings, match scheduling, and full league seasons with group stages and playoff brackets.

## Features

- **Team Management** -- Create teams, manage rosters, assign captains and vice-captains, track affiliations
- **ELO Rating System** -- Automatic ELO updates after matches, manual adjustments with audit logging, team stats with trend sparklines
- **Match Recording** -- Best-of-3 and Best-of-5 support, set-by-set score tracking, forfeit and no-show handling
- **Challenge & Scheduling** -- Team-to-team challenges with cooldown enforcement, match scheduling with reminders, reschedule proposals
- **League System** -- Multi-stage seasons (group stage and playoffs), group standings, winners/losers bracket playoffs, deadline management
- **Live Dashboard** -- Real-time league status display, match progress, interactive buttons for creating matches
- **Administration** -- Role-based permissions (League, ELO, Team, Referee), team/user suspension, configuration management
- **Auditing** -- Team/role integrity checks with auto-cleanup, league system validation, referee activity tracking
- **Automated Tasks** -- Match reminders (30 min, 15 min), deadline warnings, overdue league match alerts

## Tech Stack

- **Python 3.10+**
- **discord.py 2.x** -- Slash commands, modals, buttons, select menus
- **PostgreSQL** -- Data storage with connection pooling (psycopg2)
- **pytz** -- Timezone handling (CET/UTC)
- **python-dotenv** -- Environment variable management
- **aiohttp** -- Async HTTP

## Setup

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure environment variables

Create a `.env` file in the project root:

```env
DISCORD_BOT_TOKEN=your-bot-token
DATABASE_URL=postgresql://user:password@host:5432/dbname
```

Optional variables:

| Variable | Description | Default |
|---|---|---|
| `SYNC_GUILD_ID` | Guild ID for instant command sync (dev) | Global sync |
| `SYNC_COMMANDS` | Enable/disable command sync on startup | `true` |
| `ELO_BANNER_URL` | Banner image URL for ELO displays | (none) |
| `DISABLE_CHALLENGE_COOLDOWN` | Skip cooldown checks (testing) | `false` |
| `DB_POOL_MIN` | Minimum DB connection pool size | `1` |
| `DB_POOL_MAX` | Maximum DB connection pool size | `15` |

### 3. Run the bot

```bash
python bot.py
```

### 4. First-time server setup

Use `/setup` in your Discord server to configure roles and channels.

## Project Structure

```
bot.py              -- Entry point, bot initialization
cogs/
  admin.py          -- Server config, leaderboard, suspensions
  audit.py          -- Integrity checks, referee tracking
  cooldowns.py      -- Challenge cooldown management
  elo.py            -- ELO adjustments, team statistics
  league.py         -- League seasons, groups, playoffs
  matches.py        -- Match recording, scheduling, challenges
  teams.py          -- Team CRUD, rosters, captains
tasks/
  lifecycle.py      -- Background reminder and deadline tasks
utils/
  audit_engine.py   -- Audit logic helpers
  db.py             -- PostgreSQL connection pool and queries
  helpers.py        -- Shared embed builders and utilities
  permissions.py    -- Role-based permission checks
views/
  group_picker.py   -- Multi-step group match creation UI
  league_dashboard.py -- Live league status dashboard
  playoff_views.py  -- Playoff bracket match creation modal
  shared_views.py   -- Reusable UI components
```
