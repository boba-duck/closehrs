import datetime
import re
import copy
from zoneinfo import ZoneInfo
import discord
from discord.ext import commands, tasks

from core import checks
from core.models import PermissionLevel

LONDON_TZ = ZoneInfo("Europe/London")
DAYS = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]

def is_owner_or_admin():
    async def predicate(ctx):
        if ctx.guild and ctx.guild.owner_id == ctx.author.id:
            return True
        return await checks.has_permissions(PermissionLevel.ADMINISTRATOR).predicate(ctx)
    return commands.check(predicate)

class OpenHours(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.db = self.bot.plugin_db.get_partition(self)
        self.config = None
        self.default_config = {
            "_id": "openhours",
            "enabled": False,
            "schedule": {},
            "log_channel_id": None,
            "temp_close_until": None
        }
        self.last_state = None
        self.check_hours.start()

    async def cog_load(self):
        self.config = await self.db.find_one({"_id": "openhours"})
        if self.config is None:
            self.config = self.default_config
            await self.update_config()

    async def update_config(self):
        await self.db.find_one_and_update(
            {"_id": "openhours"},
            {"$set": self.config},
            upsert=True
        )

    def cog_unload(self):
        self.check_hours.cancel()

    @tasks.loop(seconds=0.2)
    async def check_hours(self):
        if not self.config or not self.config.get("enabled"):
            self.last_state = None
            return

        now = datetime.datetime.now(LONDON_TZ)
        today = DAYS[now.weekday()]
        schedule = self.config.get("schedule", {})
        day_config = schedule.get(today)

        should_enable = False

        # 1. Check Temp Override
        temp_close = self.config.get("temp_close_until")
        if temp_close:
            expiry = datetime.datetime.fromisoformat(temp_close).replace(tzinfo=LONDON_TZ)
            if now < expiry:
                should_enable = False
            else:
                self.config["temp_close_until"] = None
                await self.update_config()

        # 2. Check Schedule
        if not temp_close or now >= datetime.datetime.fromisoformat(self.config.get("temp_close_until", now.isoformat())).replace(tzinfo=LONDON_TZ):
            if isinstance(day_config, list):
                try:
                    open_t = datetime.datetime.strptime(day_config[0], "%H:%M").time()
                    close_t = datetime.datetime.strptime(day_config[1], "%H:%M").time()
                    curr_t = now.time()
                    
                    if open_t <= close_t:
                        should_enable = open_t <= curr_t <= close_t
                    else:
                        should_enable = curr_t >= open_t or curr_t <= close_t
                except:
                    pass

        # 3. State Management
        if should_enable and self.last_state != "enabled":
            if await self.run_command("enable"):
                await self.log_info(f"✅ ModMail is now OPEN for {today.capitalize()}.")
                self.last_state = "enabled"
        
        elif not should_enable and self.last_state != "disabled":
            if await self.run_command("disable new"):
                await self.log_info(f"❌ ModMail is now CLOSED for {today.capitalize()}.")
                self.last_state = "disabled"

    async def run_command(self, full_command: str):
        """Invoke a command by finding a real message to 'borrow' its state."""
        try:
            parts = full_command.split()
            base_name = parts[0]
            cmd = self.bot.get_command(base_name)
            if not cmd: return False

            guild = self.bot.modmail_guild
            if not guild: return False

            # We need a REAL message from the bot's cache to get a valid _state
            log_id = self.config.get("log_channel_id")
            channel = self.bot.get_channel(log_id) if log_id else guild.text_channels[0]
            
            if not channel: return False

            # Create a temporary message to get a real Context object
            temp_msg = await channel.send("🕒 Updating Modmail state...")
            ctx = await self.bot.get_context(temp_msg)
            
            # Elevate permissions
            ctx.author = guild.owner 
            
            # Invoke the command with subcommands (like 'new' in 'disable new')
            if len(parts) > 1:
                sub_cmd = cmd.get_command(parts[1])
                if sub_cmd:
                    await ctx.invoke(sub_cmd)
                else:
                    await ctx.invoke(cmd, parts[1])
            else:
                await ctx.invoke(cmd)

            await temp_msg.delete()
            return True
        except Exception as e:
            print(f"Failed to run {full_command}: {e}")
        return False

    async def log_info(self, message: str):
        chan = self.bot.get_channel(self.config.get("log_channel_id"))
        if chan: await chan.send(f"ℹ️ {message}")

    # ... [Rest of the commands remain the same as previous version] ...

    @commands.group(invoke_without_command=True)
    @is_owner_or_admin()
    async def openhours(self, ctx):
        await ctx.send_help(ctx.command)

    @openhours.command(name="set")
    @is_owner_or_admin()
    async def openhours_set(self, ctx, day: str, open_time: str, close_time: str):
        day = day.lower()
        if day not in DAYS: return await ctx.send("Invalid day.")
        self.config["schedule"][day] = [open_time, close_time]
        self.last_state = None
        await self.update_config()
        await ctx.send(f"Schedule for {day.capitalize()} set: {open_time}-{close_time}")

    @openhours.command(name="close")
    @is_owner_or_admin()
    async def openhours_close(self, ctx, day: str):
        day = day.lower()
        if day not in DAYS: return await ctx.send("Invalid day.")
        self.config["schedule"][day] = "closed"
        self.last_state = None
        await self.update_config()
        await ctx.send(f"{day.capitalize()} set to CLOSED.")

    @openhours.command(name="toggle")
    @is_owner_or_admin()
    async def openhours_toggle(self, ctx):
        self.config["enabled"] = not self.config["enabled"]
        self.last_state = None
        await self.update_config()
        await ctx.send(f"System {'enabled' if self.config['enabled'] else 'disabled'}.")

    @openhours.command(name="logchannel")
    @is_owner_or_admin()
    async def openhours_logchannel(self, ctx, channel: discord.TextChannel):
        self.config["log_channel_id"] = channel.id
        await self.update_config()
        await ctx.send(f"Log channel: {channel.mention}")

async def setup(bot):
    await bot.add_cog(OpenHours(bot))
