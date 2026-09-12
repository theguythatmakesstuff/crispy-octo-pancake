from __future__ import annotations

import json
import os
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import discord
from discord import app_commands
from discord.ext import tasks

STATUS_URL = os.getenv("FIREREC_STATUS_URL", "http://127.0.0.1:2056/")
BOT_TOKEN = os.getenv("DISCORD_BOT_TOKEN", "").strip()
APPLICATION_ID = os.getenv("DISCORD_APPLICATION_ID", "1538335335942197329").strip()
GUILD_ID = os.getenv("DISCORD_GUILD_ID", "1538333700977205362").strip()
DEFAULT_STATUS_CHANNEL_ID = os.getenv("DISCORD_STATUS_CHANNEL_ID", "1538724618594951319").strip()
STATUS_CHANNEL_PREFIX = "2018 Server Status: "


def fetch_status() -> dict[str, Any]:
    request = Request(STATUS_URL, headers={"User-Agent": "FireRecDiscordBot/1.0"})
    try:
        with urlopen(request, timeout=5) as response:
            body = response.read().decode("utf-8")
    except HTTPError as exc:
        return {
            "ok": False,
            "url": STATUS_URL,
            "error": f"HTTP {exc.code}",
        }
    except URLError as exc:
        return {
            "ok": False,
            "url": STATUS_URL,
            "error": str(exc.reason),
        }
    except Exception as exc:
        return {
            "ok": False,
            "url": STATUS_URL,
            "error": str(exc),
        }

    try:
        payload = json.loads(body)
    except json.JSONDecodeError:
        return {
            "ok": False,
            "url": STATUS_URL,
            "error": "Non-JSON response",
            "body": body[:1000],
        }

    return {
        "ok": True,
        "url": STATUS_URL,
        "payload": payload,
    }


class FireRecStatusBot(discord.Client):
    def __init__(self) -> None:
        intents = discord.Intents.default()
        super().__init__(intents=intents, application_id=int(APPLICATION_ID) if APPLICATION_ID else None)
        self.tree = app_commands.CommandTree(self)
        self.status_channel_id: int | None = int(DEFAULT_STATUS_CHANNEL_ID) if DEFAULT_STATUS_CHANNEL_ID else None

    async def setup_hook(self) -> None:
        if GUILD_ID:
            guild = discord.Object(id=int(GUILD_ID))
            self.tree.copy_global_to(guild=guild)
            synced = await self.tree.sync(guild=guild)
            print(f"Synced {len(synced)} guild command(s) to guild {GUILD_ID}", flush=True)
        else:
            synced = await self.tree.sync()
            print(f"Synced {len(synced)} global command(s)", flush=True)
        if self.status_channel_id is not None:
            self.status_channel_updater.start()

    async def close(self) -> None:
        if self.status_channel_updater.is_running():
            self.status_channel_updater.cancel()
        await super().close()

    async def update_status_channel_once(self) -> bool:
        if self.status_channel_id is None:
            return False
        channel = self.get_channel(self.status_channel_id)
        if channel is None:
            try:
                channel = await self.fetch_channel(self.status_channel_id)
            except Exception as exc:
                print(f"Failed to fetch status channel {self.status_channel_id}: {exc}", flush=True)
                return False
        if not isinstance(channel, discord.abc.GuildChannel):
            print(f"Configured status channel {self.status_channel_id} is not a guild channel", flush=True)
            return False

        result = fetch_status()
        suffix = "Online" if result.get("ok") else "Offline"
        desired_name = f"{STATUS_CHANNEL_PREFIX}{suffix}"
        if channel.name != desired_name:
            try:
                await channel.edit(name=desired_name, reason="FireRec status sync")
            except Exception as exc:
                print(f"Failed to rename status channel {channel.id}: {exc}", flush=True)
                return False
        return True

    @tasks.loop(seconds=20)
    async def status_channel_updater(self) -> None:
        await self.update_status_channel_once()

    @status_channel_updater.before_loop
    async def before_status_channel_updater(self) -> None:
        await self.wait_until_ready()


bot = FireRecStatusBot()


@bot.event
async def on_ready() -> None:
    print(f"Discord bot logged in as {bot.user}", flush=True)


@bot.tree.command(name="status", description="Show the FireRec server status endpoint response")
async def status_command(interaction: discord.Interaction) -> None:
    await interaction.response.defer(thinking=False)
    result = fetch_status()
    if not result["ok"]:
        await interaction.followup.send(
            f"Status check failed for {result['url']}: {result['error']}",
            ephemeral=True,
        )
        return

    payload_text = json.dumps(result["payload"], separators=(",", ":"), ensure_ascii=True)
    if len(payload_text) > 1800:
        payload_text = payload_text[:1800] + "..."
    await interaction.followup.send(f"{result['url']}\n```json\n{payload_text}\n```")


@bot.tree.command(name="statuschannel", description="Set the channel that shows 2018 Server Status: Online/Offline")
@app_commands.default_permissions(manage_channels=True)
async def statuschannel_command(interaction: discord.Interaction, channel: discord.abc.GuildChannel) -> None:
    bot.status_channel_id = channel.id
    if not bot.status_channel_updater.is_running():
        bot.status_channel_updater.start()
    updated = await bot.update_status_channel_once()
    if updated:
        await interaction.response.send_message(f"Status channel set to {channel.mention}.", ephemeral=True)
        return
    await interaction.response.send_message(
        f"Status channel set to {channel.mention}, but the first refresh failed.",
        ephemeral=True,
    )


@bot.tree.command(name="statuschannelclear", description="Stop updating the status channel")
@app_commands.default_permissions(manage_channels=True)
async def statuschannelclear_command(interaction: discord.Interaction) -> None:
    bot.status_channel_id = None
    if bot.status_channel_updater.is_running():
        bot.status_channel_updater.cancel()
    await interaction.response.send_message("Status channel updates disabled.", ephemeral=True)


@bot.tree.command(name="statusrefresh", description="Force an immediate status channel refresh")
@app_commands.default_permissions(manage_channels=True)
async def statusrefresh_command(interaction: discord.Interaction) -> None:
    if bot.status_channel_id is None:
        await interaction.response.send_message("No status channel is configured.", ephemeral=True)
        return
    updated = await bot.update_status_channel_once()
    if updated:
        await interaction.response.send_message("Status channel refreshed.", ephemeral=True)
        return
    await interaction.response.send_message("Status channel refresh failed.", ephemeral=True)


def main() -> None:
    if not BOT_TOKEN:
        raise SystemExit("DISCORD_BOT_TOKEN is required")
    if not APPLICATION_ID:
        raise SystemExit("DISCORD_APPLICATION_ID is required")
    try:
        bot.run(BOT_TOKEN)
    except Exception as exc:
        print(f"Discord bot startup failed: {exc}", flush=True)
        raise


if __name__ == "__main__":
    main()
