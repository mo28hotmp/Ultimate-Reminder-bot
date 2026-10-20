# Recurring Task Bot — Render / webhook version

This is a separate deployment of the same bot, built for Render's **free**
tier instead of Railway's paid Hobby plan. It's a different architecture,
not just a different host — see [Why this version is different](#why-this-version-is-different)
below before deploying it. The Railway version's files are untouched; this
is its own thing.

## What's different from the Railway version

- **Webhook, not polling.** Render's free tier only offers Web Services
  (background workers need a paid plan), so the bot now runs as a real HTTP
  server. Telegram pushes updates to it instead of the bot pulling them.
- **Postgres, not SQLite.** Render's free web services have no persistent
  disk, so task data lives in a free [Neon](https://neon.tech) Postgres
  database instead of a local file.
- **A lightweight scheduler, but only while awake.** Like the Railway
  version, this one also checks for due tasks every 60 seconds on its own
  — but that in-process timer only runs while the process happens to be
  alive; it can't do anything while the service is asleep, and doesn't
  keep it from spinning down on its own. Waking it back up (and keeping
  it awake through quiet periods) is still down to real Telegram traffic
  or an external cron service pinging a `/tick` endpoint on a schedule —
  see [Set up the external pinger(s)](#5-set-up-the-external-pingers)
  below.

## Why this version is different

Render's free web services sleep after ~15 minutes with no incoming HTTP
traffic, and take 30-60 seconds to wake back up on the next request. There's
no way to run a background timer while asleep — so an external service has
to periodically "poke" the bot to keep it awake (or wake it back up) in the
first place. Once it's actually awake, due-checks happen every 60 seconds
on their own, same as the Railway version — the imprecision is specifically
about *how long it can sit fully asleep* before something wakes it, not
about how often it checks once it's up. That means:

- **Notifications during a sleeping stretch are approximate, not precise.**
  Whatever gap your external pinger leaves is roughly the worst-case delay
  for a notification that falls due while the service is asleep. A task
  that becomes due while it's already awake, on the other hand, gets caught
  within about 60 seconds, same as Railway.
- **The first message after a quiet spell may feel slow** (cold start).
- **A second piece of infrastructure (the pinger) is now a dependency.** If
  it stops running, notifications quietly stop until you notice.

If ±15-30 minutes of notification drift isn't acceptable, the Railway
version is the better fit. This version exists for the $0/month case.

## Task types

Every task is one of five types, chosen when you `/add` it:

- 🔔 **Simple reminder** — a single push when it's due, no follow-up until
  you mark it done. This is what the one-line shortcut (`/add Task A 3d`)
  creates.
- 🔕 **No reminder** — never pushes a notification, tracked in /status
  only.
- 📅 **Old-school** — fires once at a fixed local time and date, then
  repeats on a day/week/month cadence. The date is chosen from a tap-through calendar (or typed as `2026-09-01`). The first time you add one, it
  asks for your time zone — a UTC offset (e.g. `+3`, `-5`, `+5:30`) or
  just a city/capital name (e.g. `Madrid`) — so future old-school times
  can be asked/shown in your local time; skip it to just use UTC. Change
  it later anytime with `/timezone`, without needing to add another
  old-school task. It reschedules itself automatically to the next
  occurrence the moment it fires — `/done` has no effect on it (and it's
  excluded from the tappable `/done` list for the same reason). A city name also follows daylight
  saving automatically, so a 9:00 reminder stays at 9:00 on your clock when
  the clocks change; a typed offset like `+3` is fixed and doesn't — see
  [Time zones and daylight saving](#time-zones-and-daylight-saving).
- 📣 **Advanced - Nagging** — sends one early reminder a lead time you choose before it's due ("is due in 2h"), stays quiet until it's actually due, then keeps nagging on its own cadence until you respond. Each nag has Done, quick-snooze buttons (15m/1h/8h), and a
  custom-delay option (type any duration, e.g. "45m").
- ⏰ **One-time reminder** — fires once after a delay you set, then removes
  itself. No recurrence.

Simple reminder, No reminder, and Nagging tasks also let you set a
**first-due date** separate from their regular interval. Wherever a
question has a default ("skip this"), there's a button for it — no need
to type "skip". Given this version's coarser notification timing (see
above), Old-school and Nagging's precision is inherently limited by how
often your external pinger runs — plan cadences accordingly.

## Time zones and daylight saving

Old-school reminders are pinned to a **local clock time** (e.g. 09:00), not
a fixed UTC hour, so they stay at 09:00 on your clock when daylight saving
starts or ends — only their UTC time moves.

- **Send a city** (`/timezone Madrid`) or **share your location** and the bot
  saves your actual time zone (e.g. `Europe/Madrid`). This follows daylight
  saving automatically.
- **Send an offset** (`/timezone +3`) and the bot uses that fixed offset
  forever. It's fine for places without daylight saving, but it won't adjust
  for one — the confirmation reminds you of this.
- **Changing your time zone** keeps each old-school task's local time (09:00
  stays 09:00) in the new zone, on the same local date. Handy when travelling
  or correcting a wrong zone.
- A time that doesn't exist on the night clocks spring forward (e.g. 02:30)
  fires an hour later that one day, then returns to normal.

## 1. Create a Neon database

1. Go to [neon.tech](https://neon.tech) and create a free account (no card
   required) and a new project.
2. Copy the connection string it gives you — it looks like
   `postgresql://user:password@ep-xxxx.region.aws.neon.tech/dbname?sslmode=require`.
   This is your `DATABASE_URL`.

The bot creates its own table and runs migrations automatically on
startup — no manual schema setup needed.

## 2. Create the bot on Telegram

Same as always: message **@BotFather**, send `/newbot`, follow the prompts,
and save the token it gives you.

## 3. Push this code to GitHub

Push the files in this folder (`main.py`, `requirements.txt`, this README)
to their own repo, separate from the Railway version if you're keeping both.

## 4. Deploy on Render

1. On [render.com](https://render.com), **New** → **Web Service**, connect
   the repo.
2. Runtime: Python 3. Build command: `pip install -r requirements.txt`.
   Start command: `python main.py`.
3. Under **Environment**, add just these two for now:

   | Variable | Value |
   |---|---|
   | `TELEGRAM_BOT_TOKEN` | from BotFather |
   | `DATABASE_URL` | your Neon connection string |

4. Under **Settings → Health Check Path**, set it to `/`.
5. Deploy. The service will come up and Render will show you its URL
   (`https://your-app.onrender.com`) — it runs fine at this point, it just
   hasn't told Telegram where to send updates yet (check the logs for a
   `BASE_URL, WEBHOOK_SECRET_TOKEN, and/or TICK_SECRET not set yet` warning,
   which confirms it started correctly).
6. Now add the remaining three variables, using the URL from step 5:

   | Variable | Value |
   |---|---|
   | `BASE_URL` | `https://your-app.onrender.com` (no trailing slash) |
   | `WEBHOOK_SECRET_TOKEN` | any long random string — verifies incoming requests are really from Telegram |
   | `TICK_SECRET` | any long random string — verifies calls to `/tick` are really your pinger |

   Generate random strings for the last two with something like
   `openssl rand -hex 32` in a terminal, or any password generator.
7. Redeploy. Check the logs for `Webhook registered at ...` to confirm it
   worked this time.

   Optionally, also add `ADMIN_USER_ID` = your own numeric Telegram user
   ID, if you want to use `/broadcast` to message every chat that's used
   the bot (e.g. to announce an update). Get your ID from
   [@userinfobot](https://t.me/userinfobot) — message it and it replies
   with your ID. Without this set, `/broadcast` is disabled for everyone. If your message has broken Markdown (say, an unmatched `*`),
   the preview warns you and it's sent as plain text instead of failing.

## 5. Set up the external pinger(s)

These hit `/tick` on a schedule to trigger due-checks while the service
would otherwise be asleep. This needs three layers, not one — here's why,
and how each piece fits together.

**The core problem:** Render's free tier takes 30-60 seconds to cold-start
a fully-asleep service. cron-job.org, the natural free choice for this,
has a **hard 30-second request timeout on its free plan** (not
adjustable without paying) — so a cron-job.org ping arriving while the
service is genuinely asleep gives up before the cold start finishes, and
never actually confirms the wake. It's excellent at *keeping an
already-warm service warm* (which only takes a fast, well-under-30s
response), just not at *waking a cold one*. That split is the reason for
three layers instead of one:

### Layer 1 — cron-job.org: steady-state keep-alive during active hours

1. Create a free account, add a new cron job.
2. URL: `https://your-app.onrender.com/tick?key=YOUR_TICK_SECRET`
3. Schedule: every 15 minutes, restricted to your active hours — e.g.
   "every 15 minutes, hours 9-23 and 0-1 UTC" to skip a 2 AM-9 AM quiet
   window (adjust to your own timezone/preference). As long as pings land
   closer together than Render's ~15-minute idle threshold, the service
   never gets the chance to fully sleep during that window, so every ping
   hits an already-warm app and finishes in well under 30 seconds.

### Layer 2 — Pipedream: the wake-up call, timed just before active hours begin

A ping patient enough to actually sit through a cold start, timed so the
service is already awake by the time cron-job.org's frequent pinging
resumes. Pipedream's free tier is credit-metered (25 credits/day, 1
credit per 30 seconds of execution) — too tight to run every 15 minutes
all day, but pinging once or twice a day costs at most a handful of
credits, nowhere close to the daily cap.

1. Create a free account at [pipedream.com](https://pipedream.com) and
   start a new workflow.
2. **Trigger:** Add Trigger → search for "Schedule" → choose the
   **Custom Interval** (cron) option. Set it to run shortly before your
   quiet window ends — e.g. cron `50 8 * * *` for a window ending at 9 AM
   UTC. To send two pings a few minutes apart as a built-in backup (this
   is what actually catches it if the first one's timing is unlucky),
   use a comma-separated cron minute list instead, e.g. `50,55 8 * * *`
   fires at both 8:50 and 8:55 AM UTC — one workflow, two attempts.
3. **Action step — this is the part that actually matters:** add a step,
   choose **Node.js**, and then **Run Node code**. Then use the code
   below (axios with an explicit timeout) — not the `$.send.http()`
   helper Pipedream sometimes suggests by default, which has a
   **hardcoded, non-adjustable 5-second timeout** that can't survive a
   cold start no matter what else you configure:

   ```javascript
   import axios from "axios";

   export default defineComponent({
     async run({ steps, $ }) {
       const response = await axios({
         method: "GET",
         url: "https://your-app.onrender.com/tick",
         params: { key: "YOUR_TICK_SECRET" },
         timeout: 85000, // 85s — a little under the workflow limit below,
                          // so this fails cleanly instead of getting killed mid-request
       });
       return response.data;
     },
   });
   ```

4. **Workflow settings** (gear icon, or Cmd+, / Ctrl+,) → raise the
   **execution timeout** from the 60-second default to 90 seconds. This
   is a separate setting from the axios timeout above — both need
   raising, since either one alone will still cut the request short.
5. Deploy the workflow, then use **Run Now** once to confirm it actually
   reaches `/tick` and returns `ok` before relying on the schedule.

### Layer 3 — GitHub Actions: a scattered backstop during early active hours

Add this *on top of*, not instead of, the above — a handful of patient
pings scattered across the first few hours of active time, so that even
if Layer 2 somehow didn't land, one of these eventually will. GitHub
Actions' `schedule` trigger doesn't fire at a reliable exact time — runs
can be delayed by minutes to over an hour, and that's a deliberate
platform limitation, not a config mistake. That imprecision doesn't
matter here, since this layer doesn't need to fire at an exact minute —
just *sometime* within a multi-hour window — as long as each individual
run is itself patient enough to outlast a cold start once it does fire.

Add `.github/workflows/wake.yml` to any repo you control:

```yaml
name: Wake up task-bot
on:
  schedule:
    - cron: "*/20 9-11 * * *"  # scattered across the first ~3 hours of active time
jobs:
  ping:
    runs-on: ubuntu-latest
    steps:
      - run: curl -f --max-time 100 "https://your-app.onrender.com/tick?key=${{ secrets.TICK_SECRET }}"
```

Add `TICK_SECRET` as a repository secret with the same value you set on
Render. `--max-time 100` is the key setting — without it, `curl` would
otherwise still be patient by default, but this makes it explicit and
gives comfortable margin over Render's ~60s typical cold start. GitHub
Actions cron schedules are UTC by default; adjust the hour range to
match the first few hours after your own quiet window ends.

### Why not just one of these?

Each layer covers a gap the others can't: cron-job.org can't survive a
genuine cold start (30s cap); Pipedream can, but its daily credit budget
rules it out as the *frequent* pinger; GitHub Actions is free and can be
configured to be patient too, but can't be trusted to fire at a precise
moment. Together — a patient, precisely-timed wake right before active
hours (Pipedream), a patient-but-imprecisely-timed backstop scattered
across the following few hours (GitHub Actions), and cheap frequent
keep-alive once it's confirmed warm (cron-job.org) — each one's weakness
is covered by one of the others' strengths.

## 6. Try it

Message your bot on Telegram — `/start` should respond (possibly slowly,
if it just spun up from being asleep). Add a task, check `/status`. To see
a real proactive notification, `/add` a 📣 Nagging task with very short
values and either wait for your scheduled ping or manually visit
`https://your-app.onrender.com/tick?key=YOUR_TICK_SECRET` in a browser to
trigger a check immediately.

## Upgrading an existing deployment

1. **Back up first** (optional but wise): in Neon, create a branch of your
   database — a one-click snapshot you can fall back on.
2. Push the new `main.py` to GitHub; Render redeploys on its own.
3. The bot upgrades its own database on startup (additive changes plus a
   one-time conversion of existing old-school times). If you had old-school
   tasks, the logs show `Backfilled local reminder time for N old-school
   task(s)`.
4. **Existing users need to re-send their city once** (`/timezone Madrid`) to
   get daylight-saving handling — older versions only saved a plain offset,
   so the bot can't know which city it came from. Until then it keeps using
   their saved offset, as before. A `/broadcast` is a good way to tell them.
5. **Upgrades are one-way.** Don't roll back to an older `main.py` against the
   upgraded database; old-school tasks would fire at the wrong hour. If you
   must, restore the Neon branch from step 1.

## Running locally (optional)

```bash
pip install -r requirements.txt
export TELEGRAM_BOT_TOKEN=your-token-here
export DATABASE_URL=your-neon-connection-string
export BASE_URL=https://your-ngrok-or-public-url
export WEBHOOK_SECRET_TOKEN=some-random-string
export TICK_SECRET=some-random-string
python main.py
```
Telegram needs a real public HTTPS URL to send webhooks to, so local
testing requires something like `ngrok` to tunnel a public URL to your
machine — plain `localhost` won't receive anything.

## Tests

The `tests/` folder holds an automated test suite (time zones and daylight
saving, duplicate notifications, the database upgrade, `/broadcast`, and
more). Run it after any change with `pip install -r requirements-dev.txt`
then `pytest` — it takes a few seconds and never touches your real database.
See [`tests/README.md`](tests/README.md).

## Credits

City/capital name lookups for `/timezone` (e.g. `Madrid`) use the free
[Open-Meteo Geocoding API](https://open-meteo.com/en/docs/geocoding-api),
based on [GeoNames](https://www.geonames.org) data — no API key needed.
Their free tier is non-commercial use only, which is fine for a personal
bot like this one; if you ever turn this into something commercial, check
their [pricing/licensing](https://open-meteo.com/en/pricing) first.
