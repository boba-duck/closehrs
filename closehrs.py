"""
closehrs - Advanced Open Hours Plugin for modmail-dev/Modmail
Automatically manages Modmail availability based on configurable schedules.

Features:
  - Per-day open/close time windows (including overnight spans)
  - Holiday/one-off closed dates
  - Temporary manual override (close until X)
  - Customisable user-facing DM messages (open, closed, holiday, override)
  - Placeholder support: {server}, {open_time}, {close_time}, {day}
  - Configurable embed colour per state
  - Rich status embed via ?openhours status
  - Log channel with state-change embeds
  - "Next open" calculation shown to users and in logs
  - Graceful handling of existing open threads on close
"""

from __future__ import annotations

import asyncio
import datetime
import re
from typing import Optional
from zoneinfo import ZoneInfo

import discord
from discord.ext import commands, tasks

from core import checks
from core.models import PermissionLevel

# ──────────────────────────────────────────────────────────────────────────────
# Constants
# ──────────────────────────────────────────────────────────────────────────────

DAYS = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
DAY_ABBR = {d: d[:3].capitalize() for d in DAYS}

DEFAULT_MESSAGES = {
    "closed_dm": (
        "Hey {mention}! 👋 Our support inbox is currently **closed**.\n\n"
        "We're open {next_open_str}. Please send your message again then "
        "and a staff member will get back to you as soon as possible."
    ),
    "open_dm": (
        "Hey {mention}! 👋 Our support inbox is now **open** — feel free to send us a message."
    ),
    "holiday_dm": (
        "Hey {mention}! 👋 We're **closed today** for a holiday or scheduled day off.\n\n"
        "We'll be back {next_open_str}. Please try again then!"
    ),
    "override_dm": (
        "Hey {mention}! 👋 Our support inbox is temporarily **closed**.\n\n"
        "We expect to reopen {next_open_str}. Please check back then!"
    ),
}

DEFAULT_COLOURS = {
    "open": 0x57F287,       # green
    "closed": 0xED4245,     # red
    "holiday": 0xFEE75C,    # yellow
    "override": 0xEB459E,   # pink
}

DEFAULT_CONFIG = {
    "_id": "openhours",
    "enabled": False,
    "timezone": "Europe/London",
    "schedule": {},          # {weekday: [HH:MM, HH:MM] | "closed"}
    "holidays": {},          # {YYYY-MM-DD: "reason string"}
    "log_channel_id": None,
    "notify_channel_id": None,   # optional channel to ping staff on open/close
    "notify_role_id": None,
    "temp_close_until": None,    # ISO datetime string
    "messages": {},          # overrides for DEFAULT_MESSAGES keys
    "colours": {},           # overrides for DEFAULT_COLOURS keys
    "send_closed_dm": True,  # DM user when they message outside hours
    "close_open_threads": False,  # close existing threads when inbox closes
}

# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

def _parse_time(s: str) -> datetime.time:
    return datetime.datetime.strptime(s.strip(), "%H:%M").time()

def _fmt_time(t: datetime.time) -> str:
    return t.strftime("%H:%M")

def _fmt_dt(dt: datetime.datetime) -> str:
    return discord.utils.format_dt(dt, style="f")

def _time_in_window(current: datetime.time, open_t: datetime.time, close_t: datetime.time) -> bool:
    """Handles both normal and overnight windows."""
    if open_t <= close_t:
        return open_t <= current <= close_t
    return current >= open_t or current <= close_t


def is_owner_or_admin():
    async def predicate(ctx):
        if ctx.guild and ctx.guild.owner_id == ctx.author.id:
            return True
        return await checks.has_permissions(PermissionLevel.ADMINISTRATOR).predicate(ctx)
    return commands.check(predicate)


# ──────────────────────────────────────────────────────────────────────────────
# Cog
# ──────────────────────────────────────────────────────────────────────────────

class OpenHours(commands.Cog):
    """Automatically open and close Modmail based on a weekly schedule."""

    def __init__(self, bot):
        self.bot = bot
        self.db = bot.plugin_db.get_partition(self)
        self.config: dict = {}
        self._last_state: Optional[str] = None   # "open" | "closed" | None
        self._tz: ZoneInfo = ZoneInfo("Europe/London")
        self._check_loop.start()

    # ── Lifecycle ──────────────────────────────────────────────────────────────

    async def cog_load(self):
        raw = await self.db.find_one({"_id": "openhours"})
        self.config = {**DEFAULT_CONFIG, **(raw or {})}
        self._tz = ZoneInfo(self.config.get("timezone", "Europe/London"))
        if raw is None:
            await self._save()

    def cog_unload(self):
        self._check_loop.cancel()

    async def _save(self):
        await self.db.find_one_and_update(
            {"_id": "openhours"},
            {"$set": self.config},
            upsert=True,
        )

    # ── Properties / helpers ──────────────────────────────────────────────────

    def _msg(self, key: str) -> str:
        return self.config.get("messages", {}).get(key) or DEFAULT_MESSAGES.get(key, "")

    def _colour(self, key: str) -> int:
        return self.config.get("colours", {}).get(key) or DEFAULT_COLOURS.get(key, 0x5865F2)

    def _now(self) -> datetime.datetime:
        return datetime.datetime.now(self._tz)

    def _evaluate_state(self) -> tuple[bool, str]:
        """
        Returns (should_be_open, reason).
        reason is one of: "schedule", "holiday", "override", "disabled"
        """
        if not self.config.get("enabled"):
            return False, "disabled"

        now = self._now()
        date_str = now.date().isoformat()

        # 1. Temp override
        temp = self.config.get("temp_close_until")
        if temp:
            expiry = datetime.datetime.fromisoformat(temp).astimezone(self._tz)
            if now < expiry:
                return False, "override"

        # 2. Holiday
        if date_str in (self.config.get("holidays") or {}):
            return False, "holiday"

        # 3. Schedule
        today = DAYS[now.weekday()]
        day_cfg = (self.config.get("schedule") or {}).get(today)
        if day_cfg == "closed" or day_cfg is None:
            return False, "schedule"
        if isinstance(day_cfg, list) and len(day_cfg) == 2:
            try:
                open_t = _parse_time(day_cfg[0])
                close_t = _parse_time(day_cfg[1])
                if _time_in_window(now.time(), open_t, close_t):
                    return True, "schedule"
            except ValueError:
                pass
        return False, "schedule"

    def _next_open_dt(self) -> Optional[datetime.datetime]:
        """Calculate the next datetime the inbox should open (within 7 days)."""
        now = self._now()
        schedule = self.config.get("schedule") or {}
        holidays = self.config.get("holidays") or {}

        for delta in range(0, 8 * 24 * 60, 1):  # minute steps – expensive; use day-level first
            pass  # replaced by day-level below

        # Day-level scan
        for day_offset in range(1, 9):
            candidate_date = now.date() + datetime.timedelta(days=day_offset)
            if candidate_date.isoformat() in holidays:
                continue
            day_name = DAYS[candidate_date.weekday()]
            day_cfg = schedule.get(day_name)
            if not day_cfg or day_cfg == "closed":
                continue
            if isinstance(day_cfg, list) and len(day_cfg) == 2:
                try:
                    open_t = _parse_time(day_cfg[0])
                    candidate_dt = datetime.datetime.combine(candidate_date, open_t, tzinfo=self._tz)
                    temp = self.config.get("temp_close_until")
                    if temp:
                        expiry = datetime.datetime.fromisoformat(temp).astimezone(self._tz)
                        if candidate_dt < expiry:
                            continue
                    return candidate_dt
                except ValueError:
                    continue

        # Same-day check (later today)
        today_cfg = schedule.get(DAYS[now.weekday()])
        if isinstance(today_cfg, list) and len(today_cfg) == 2:
            try:
                open_t = _parse_time(today_cfg[0])
                candidate_dt = datetime.datetime.combine(now.date(), open_t, tzinfo=self._tz)
                if candidate_dt > now:
                    return candidate_dt
            except ValueError:
                pass

        return None

    def _next_open_str(self) -> str:
        dt = self._next_open_dt()
        if dt:
            return _fmt_dt(dt)
        return "soon (check back later)"

    def _format_msg(self, key: str, member: discord.Member | discord.User | None = None) -> str:
        now = self._now()
        today = DAYS[now.weekday()]
        schedule = self.config.get("schedule") or {}
        day_cfg = schedule.get(today)
        close_time = _fmt_time(_parse_time(day_cfg[1])) if isinstance(day_cfg, list) and len(day_cfg) == 2 else "N/A"
        open_time = _fmt_time(_parse_time(day_cfg[0])) if isinstance(day_cfg, list) and len(day_cfg) == 2 else "N/A"

        kwargs = {
            "server": self.bot.modmail_guild.name if self.bot.modmail_guild else "Server",
            "day": today.capitalize(),
            "open_time": open_time,
            "close_time": close_time,
            "next_open_str": self._next_open_str(),
            "mention": member.mention if member else "there",
        }
        try:
            return self._msg(key).format(**kwargs)
        except (KeyError, ValueError):
            return self._msg(key)

    # ── Background task ────────────────────────────────────────────────────────

    @tasks.loop(seconds=30)
    async def _check_loop(self):
        try:
            await self._tick()
        except Exception as exc:
            print(f"[openhours] tick error: {exc}")

    @_check_loop.before_loop
    async def _before_loop(self):
        await self.bot.wait_until_ready()
        # Reload config in case it was modified externally
        raw = await self.db.find_one({"_id": "openhours"})
        if raw:
            self.config = {**DEFAULT_CONFIG, **raw}
            self._tz = ZoneInfo(self.config.get("timezone", "Europe/London"))

    async def _tick(self):
        should_open, reason = self._evaluate_state()
        desired_state = "open" if should_open else "closed"

        if desired_state == self._last_state:
            return  # No change

        # Expire temp_close_until if it's in the past
        temp = self.config.get("temp_close_until")
        if temp:
            expiry = datetime.datetime.fromisoformat(temp).astimezone(self._tz)
            if self._now() >= expiry:
                self.config["temp_close_until"] = None
                await self._save()

        if desired_state == "open":
            success = await self._set_modmail_state(True)
            if success:
                self._last_state = "open"
                await self._log_state_change("open", reason)
                await self._notify_staff("open")
        else:
            success = await self._set_modmail_state(False)
            if success:
                self._last_state = "closed"
                await self._log_state_change("closed", reason)
                await self._notify_staff("closed")
                if self.config.get("close_open_threads"):
                    await self._close_open_threads()

    async def _set_modmail_state(self, enable: bool) -> bool:
        """Toggle Modmail's disable state via the bot config."""
        try:
            if enable:
                self.bot.config["dm_disabled"] = 0   # DMDisabled.NONE
            else:
                self.bot.config["dm_disabled"] = 1   # DMDisabled.NEW_THREADS
            await self.bot.config.update()
            return True
        except Exception as e:
            print(f"[openhours] failed to set modmail state: {e}")
            return False

    async def _close_open_threads(self):
        """Close all currently open Modmail threads with a notice."""
        try:
            for thread in list(self.bot.threads.cache.values()):
                try:
                    await thread.close(closer=self.bot.modmail_guild.me, silent=True, delete_channel=False)
                except Exception:
                    pass
        except Exception as e:
            print(f"[openhours] close_open_threads error: {e}")

    # ── Logging / notifications ────────────────────────────────────────────────

    async def _log_state_change(self, state: str, reason: str):
        log_id = self.config.get("log_channel_id")
        if not log_id:
            return
        chan = self.bot.get_channel(log_id)
        if not chan:
            return

        now = self._now()
        colour = self._colour(state if state in ("open", "closed") else "closed")
        icon = "✅" if state == "open" else "❌"
        next_open = self._next_open_str() if state == "closed" else "—"

        embed = discord.Embed(
            title=f"{icon} Modmail is now {state.upper()}",
            colour=colour,
            timestamp=now,
        )
        embed.add_field(name="Reason", value=reason.capitalize(), inline=True)
        embed.add_field(name="Time (local)", value=now.strftime("%H:%M %Z"), inline=True)
        if state == "closed":
            embed.add_field(name="Next open", value=next_open, inline=False)
        embed.set_footer(text="OpenHours Plugin")
        try:
            await chan.send(embed=embed)
        except discord.Forbidden:
            pass

    async def _notify_staff(self, state: str):
        notify_id = self.config.get("notify_channel_id")
        role_id = self.config.get("notify_role_id")
        if not notify_id:
            return
        chan = self.bot.get_channel(notify_id)
        if not chan:
            return
        role_mention = f"<@&{role_id}>" if role_id else ""
        icon = "✅" if state == "open" else "🔒"
        try:
            await chan.send(f"{role_mention} {icon} Modmail inbox is now **{state.upper()}**.".strip())
        except discord.Forbidden:
            pass

    # ── on_thread_ready — send closed DM if inbox is closed ───────────────────

    @commands.Cog.listener()
    async def on_thread_ready(self, thread, creator, category, initial_message):
        """If a thread opens while inbox should be closed, notify the user."""
        if not self.config.get("send_closed_dm", True):
            return
        if not self.config.get("enabled"):
            return

        should_open, reason = self._evaluate_state()
        if should_open:
            return  # Inbox is open — nothing to do

        msg_key = {
            "holiday": "holiday_dm",
            "override": "override_dm",
        }.get(reason, "closed_dm")

        text = self._format_msg(msg_key, member=creator)

        colour = self._colour({
            "holiday": "holiday",
            "override": "override",
        }.get(reason, "closed"))

        embed = discord.Embed(description=text, colour=colour)
        embed.set_footer(text=self.bot.modmail_guild.name if self.bot.modmail_guild else "")

        try:
            await thread.reply(embed=embed)
        except Exception as e:
            print(f"[openhours] on_thread_ready reply error: {e}")

    # ──────────────────────────────────────────────────────────────────────────
    # Commands
    # ──────────────────────────────────────────────────────────────────────────

    @commands.group(name="openhours", aliases=["oh"], invoke_without_command=True)
    @is_owner_or_admin()
    async def openhours(self, ctx):
        """Manage the OpenHours schedule. Use `?openhours status` to see current state."""
        await ctx.send_help(ctx.command)

    # ── status ────────────────────────────────────────────────────────────────

    @openhours.command(name="status")
    @is_owner_or_admin()
    async def openhours_status(self, ctx):
        """Show a rich embed with the current schedule and state."""
        should_open, reason = self._evaluate_state()
        now = self._now()
        schedule = self.config.get("schedule") or {}
        holidays = self.config.get("holidays") or {}
        tz_str = self.config.get("timezone", "Europe/London")

        colour = self._colour("open" if should_open else "closed")
        state_icon = "✅ OPEN" if should_open else "❌ CLOSED"

        embed = discord.Embed(
            title=f"OpenHours Status — {state_icon}",
            colour=colour,
            timestamp=now,
        )

        # Plugin state
        embed.add_field(
            name="Plugin",
            value="Enabled" if self.config.get("enabled") else "Disabled",
            inline=True,
        )
        embed.add_field(name="Timezone", value=tz_str, inline=True)
        embed.add_field(name="Current reason", value=reason.capitalize(), inline=True)

        # Temp override
        temp = self.config.get("temp_close_until")
        if temp:
            expiry = datetime.datetime.fromisoformat(temp).astimezone(self._tz)
            if now < expiry:
                embed.add_field(
                    name="⏳ Temp close until",
                    value=_fmt_dt(expiry),
                    inline=False,
                )

        # Weekly schedule table
        lines = []
        for day in DAYS:
            cfg = schedule.get(day)
            marker = "▶" if day == DAYS[now.weekday()] else " "
            if cfg == "closed" or cfg is None:
                lines.append(f"`{marker} {DAY_ABBR[day]}` — Closed")
            elif isinstance(cfg, list) and len(cfg) == 2:
                lines.append(f"`{marker} {DAY_ABBR[day]}` — {cfg[0]} – {cfg[1]}")
        embed.add_field(name="📅 Weekly Schedule", value="\n".join(lines) or "No schedule set", inline=False)

        # Upcoming holidays
        today = now.date()
        upcoming = {
            k: v for k, v in sorted(holidays.items())
            if datetime.date.fromisoformat(k) >= today
        }
        if upcoming:
            hlines = [f"`{k}` — {v}" for k, v in list(upcoming.items())[:5]]
            embed.add_field(name="🗓️ Upcoming Holidays", value="\n".join(hlines), inline=False)

        # Next open
        next_dt = self._next_open_dt()
        if not should_open and next_dt:
            embed.add_field(name="⏰ Next open", value=_fmt_dt(next_dt), inline=False)

        embed.set_footer(text="OpenHours Plugin • All times in local timezone")
        await ctx.send(embed=embed)

    # ── toggle ────────────────────────────────────────────────────────────────

    @openhours.command(name="toggle")
    @is_owner_or_admin()
    async def openhours_toggle(self, ctx):
        """Enable or disable the entire OpenHours system."""
        self.config["enabled"] = not self.config.get("enabled", False)
        self._last_state = None
        await self._save()
        state = "**enabled** ✅" if self.config["enabled"] else "**disabled** ❌"
        await ctx.send(f"OpenHours system is now {state}.")

    # ── timezone ──────────────────────────────────────────────────────────────

    @openhours.command(name="timezone")
    @is_owner_or_admin()
    async def openhours_timezone(self, ctx, tz: str):
        """Set the timezone for the schedule (e.g. `Europe/London`, `America/New_York`)."""
        try:
            ZoneInfo(tz)
        except Exception:
            return await ctx.send(f"❌ Unknown timezone `{tz}`. Use IANA format, e.g. `Europe/London`.")
        self.config["timezone"] = tz
        self._tz = ZoneInfo(tz)
        self._last_state = None
        await self._save()
        await ctx.send(f"✅ Timezone set to `{tz}`.")

    # ── set ───────────────────────────────────────────────────────────────────

    @openhours.command(name="set")
    @is_owner_or_admin()
    async def openhours_set(self, ctx, day: str, open_time: str, close_time: str):
        """
        Set open hours for a day.

        Examples:
          ?openhours set monday 09:00 17:30
          ?openhours set friday 09:00 13:00
          ?openhours set saturday 22:00 02:00   (overnight)
        """
        day = day.lower()
        if day not in DAYS:
            return await ctx.send(f"❌ Invalid day. Use: {', '.join(DAYS)}")
        try:
            _parse_time(open_time)
            _parse_time(close_time)
        except ValueError:
            return await ctx.send("❌ Invalid time format. Use HH:MM (24-hour).")
        if "schedule" not in self.config:
            self.config["schedule"] = {}
        self.config["schedule"][day] = [open_time, close_time]
        self._last_state = None
        await self._save()
        await ctx.send(f"✅ `{day.capitalize()}`: open **{open_time}** → **{close_time}**.")

    # ── close (mark a day closed) ─────────────────────────────────────────────

    @openhours.command(name="close")
    @is_owner_or_admin()
    async def openhours_close_day(self, ctx, day: str):
        """Mark a specific weekday as always closed (e.g. `?openhours close sunday`)."""
        day = day.lower()
        if day not in DAYS:
            return await ctx.send(f"❌ Invalid day. Use: {', '.join(DAYS)}")
        if "schedule" not in self.config:
            self.config["schedule"] = {}
        self.config["schedule"][day] = "closed"
        self._last_state = None
        await self._save()
        await ctx.send(f"✅ `{day.capitalize()}` is now marked as **closed**.")

    # ── clear (remove a day's schedule) ───────────────────────────────────────

    @openhours.command(name="clear")
    @is_owner_or_admin()
    async def openhours_clear_day(self, ctx, day: str):
        """Remove the schedule entry for a day (treated as closed)."""
        day = day.lower()
        if day not in DAYS:
            return await ctx.send(f"❌ Invalid day.")
        sched = self.config.get("schedule") or {}
        sched.pop(day, None)
        self.config["schedule"] = sched
        self._last_state = None
        await self._save()
        await ctx.send(f"✅ Schedule cleared for `{day.capitalize()}`.")

    # ── tempclose ──────────────────────────────────────────────────────────────

    @openhours.command(name="tempclose")
    @is_owner_or_admin()
    async def openhours_tempclose(self, ctx, *, duration: str):
        """
        Temporarily close the inbox for a given duration.

        Duration examples: `2h`, `30m`, `1h30m`, `1d`
        """
        seconds = _parse_duration(duration)
        if seconds is None or seconds <= 0:
            return await ctx.send("❌ Invalid duration. Examples: `2h`, `30m`, `1h30m`, `1d`.")
        until = self._now() + datetime.timedelta(seconds=seconds)
        self.config["temp_close_until"] = until.isoformat()
        self._last_state = None
        await self._save()
        await ctx.send(f"🔒 Modmail temporarily closed until {_fmt_dt(until)}.")

    # ── tempopen (cancel temp close) ──────────────────────────────────────────

    @openhours.command(name="tempopen")
    @is_owner_or_admin()
    async def openhours_tempopen(self, ctx):
        """Cancel any active temporary close override."""
        if not self.config.get("temp_close_until"):
            return await ctx.send("ℹ️ No temporary close is active.")
        self.config["temp_close_until"] = None
        self._last_state = None
        await self._save()
        await ctx.send("✅ Temporary close cancelled. Schedule resumes normally.")

    # ── holiday ───────────────────────────────────────────────────────────────

    @openhours.command(name="holiday")
    @is_owner_or_admin()
    async def openhours_holiday(self, ctx, date: str, *, reason: str = "Holiday"):
        """
        Mark a specific date as closed (holiday).

        Date format: YYYY-MM-DD
        Example: `?openhours holiday 2025-12-25 Christmas Day`
        """
        try:
            datetime.date.fromisoformat(date)
        except ValueError:
            return await ctx.send("❌ Invalid date format. Use YYYY-MM-DD.")
        if "holidays" not in self.config:
            self.config["holidays"] = {}
        self.config["holidays"][date] = reason
        self._last_state = None
        await self._save()
        await ctx.send(f"🗓️ `{date}` marked as closed: **{reason}**.")

    @openhours.command(name="removeholiday")
    @is_owner_or_admin()
    async def openhours_remove_holiday(self, ctx, date: str):
        """Remove a holiday entry. Date format: YYYY-MM-DD"""
        holidays = self.config.get("holidays") or {}
        if date not in holidays:
            return await ctx.send(f"❌ No holiday found for `{date}`.")
        del holidays[date]
        self.config["holidays"] = holidays
        self._last_state = None
        await self._save()
        await ctx.send(f"✅ Holiday removed for `{date}`.")

    # ── message config ─────────────────────────────────────────────────────────

    @openhours.command(name="setmessage")
    @is_owner_or_admin()
    async def openhours_setmessage(self, ctx, key: str, *, message: str):
        """
        Customise a user-facing DM message.

        Keys: `closed_dm`, `open_dm`, `holiday_dm`, `override_dm`

        Placeholders: `{mention}`, `{server}`, `{day}`, `{open_time}`, `{close_time}`, `{next_open_str}`

        Example:
          ?openhours setmessage closed_dm Hey {mention}! We're closed right now. Back {next_open_str}.
        """
        valid = list(DEFAULT_MESSAGES.keys())
        if key not in valid:
            return await ctx.send(f"❌ Invalid key. Valid keys: {', '.join(f'`{k}`' for k in valid)}")
        if "messages" not in self.config:
            self.config["messages"] = {}
        self.config["messages"][key] = message
        await self._save()
        await ctx.send(f"✅ Message `{key}` updated.")

    @openhours.command(name="resetmessage")
    @is_owner_or_admin()
    async def openhours_resetmessage(self, ctx, key: str):
        """Reset a message to its default."""
        messages = self.config.get("messages") or {}
        if key in messages:
            del messages[key]
            self.config["messages"] = messages
            await self._save()
        await ctx.send(f"✅ Message `{key}` reset to default.")

    @openhours.command(name="previewmessage")
    @is_owner_or_admin()
    async def openhours_previewmessage(self, ctx, key: str):
        """Preview what a message looks like with placeholders filled."""
        valid = list(DEFAULT_MESSAGES.keys())
        if key not in valid:
            return await ctx.send(f"❌ Invalid key. Valid: {', '.join(f'`{k}`' for k in valid)}")
        text = self._format_msg(key, member=ctx.author)
        _, reason = self._evaluate_state()
        colour_key = {"holiday_dm": "holiday", "override_dm": "override"}.get(key, "closed")
        embed = discord.Embed(description=text, colour=self._colour(colour_key))
        embed.set_footer(text=f"Preview of: {key}")
        await ctx.send(embed=embed)

    # ── colour config ──────────────────────────────────────────────────────────

    @openhours.command(name="setcolour")
    @is_owner_or_admin()
    async def openhours_setcolour(self, ctx, state: str, hex_colour: str):
        """
        Set the embed colour for a state.

        States: `open`, `closed`, `holiday`, `override`
        Colour: hex like `#57F287` or `57F287`

        Example: `?openhours setcolour closed #FF0000`
        """
        valid_states = list(DEFAULT_COLOURS.keys())
        if state not in valid_states:
            return await ctx.send(f"❌ Invalid state. Use: {', '.join(f'`{s}`' for s in valid_states)}")
        hex_colour = hex_colour.lstrip("#")
        try:
            colour_int = int(hex_colour, 16)
        except ValueError:
            return await ctx.send("❌ Invalid hex colour.")
        if "colours" not in self.config:
            self.config["colours"] = {}
        self.config["colours"][state] = colour_int
        await self._save()
        swatch = discord.Colour(colour_int)
        await ctx.send(f"✅ Colour for `{state}` set to `#{hex_colour.upper()}`.")

    # ── log channel ────────────────────────────────────────────────────────────

    @openhours.command(name="logchannel")
    @is_owner_or_admin()
    async def openhours_logchannel(self, ctx, channel: discord.TextChannel):
        """Set the channel where state-change log embeds are posted."""
        self.config["log_channel_id"] = channel.id
        await self._save()
        await ctx.send(f"✅ Log channel set to {channel.mention}.")

    # ── notify channel + role ──────────────────────────────────────────────────

    @openhours.command(name="notifychannel")
    @is_owner_or_admin()
    async def openhours_notifychannel(self, ctx, channel: discord.TextChannel):
        """Set a channel to ping staff when inbox opens/closes."""
        self.config["notify_channel_id"] = channel.id
        await self._save()
        await ctx.send(f"✅ Notify channel set to {channel.mention}.")

    @openhours.command(name="notifyrole")
    @is_owner_or_admin()
    async def openhours_notifyrole(self, ctx, role: discord.Role):
        """Set a role to mention in the notify channel on open/close."""
        self.config["notify_role_id"] = role.id
        await self._save()
        await ctx.send(f"✅ Notify role set to {role.mention}.")

    # ── toggles ───────────────────────────────────────────────────────────────

    @openhours.command(name="togglecloseddm")
    @is_owner_or_admin()
    async def openhours_toggle_closed_dm(self, ctx):
        """Toggle whether users get a DM when they message outside hours."""
        self.config["send_closed_dm"] = not self.config.get("send_closed_dm", True)
        await self._save()
        state = "enabled ✅" if self.config["send_closed_dm"] else "disabled ❌"
        await ctx.send(f"Closed DM notifications {state}.")

    @openhours.command(name="toggleclosethreads")
    @is_owner_or_admin()
    async def openhours_toggle_close_threads(self, ctx):
        """Toggle whether open threads are closed when the inbox closes."""
        self.config["close_open_threads"] = not self.config.get("close_open_threads", False)
        await self._save()
        state = "enabled ✅" if self.config["close_open_threads"] else "disabled ❌"
        await ctx.send(f"Auto-close open threads on inbox close: {state}.")

    # ── nexthours (public) ────────────────────────────────────────────────────

    @commands.command(name="nexthours")
    async def nexthours(self, ctx):
        """Show when the inbox will next be open."""
        if not self.config.get("enabled"):
            return await ctx.send("ℹ️ The OpenHours system is not enabled.")
        should_open, _ = self._evaluate_state()
        if should_open:
            embed = discord.Embed(
                description="✅ The support inbox is **open right now**!",
                colour=self._colour("open"),
            )
        else:
            next_dt = self._next_open_dt()
            desc = f"❌ The inbox is currently closed.\n\n⏰ **Next open:** {_fmt_dt(next_dt)}" if next_dt else "❌ The inbox is currently closed. No upcoming opening found."
            embed = discord.Embed(description=desc, colour=self._colour("closed"))
        await ctx.send(embed=embed)


# ──────────────────────────────────────────────────────────────────────────────
# Duration parser
# ──────────────────────────────────────────────────────────────────────────────

def _parse_duration(s: str) -> Optional[int]:
    """Parse strings like '2h', '30m', '1h30m', '1d' into seconds."""
    pattern = re.compile(r"(?:(\d+)d)?(?:(\d+)h)?(?:(\d+)m)?(?:(\d+)s)?$")
    m = pattern.match(s.strip())
    if not m or not any(m.groups()):
        return None
    days = int(m.group(1) or 0)
    hours = int(m.group(2) or 0)
    minutes = int(m.group(3) or 0)
    secs = int(m.group(4) or 0)
    return days * 86400 + hours * 3600 + minutes * 60 + secs


# ──────────────────────────────────────────────────────────────────────────────
# Setup
# ──────────────────────────────────────────────────────────────────────────────

async def setup(bot):
    await bot.add_cog(OpenHours(bot))
