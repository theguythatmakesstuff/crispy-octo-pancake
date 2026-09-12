# FireRec 2018 Server

Python reimplementation of the local FireRec 2018-style server described in Task.txt.

## Ports

- API and nameserver: `http://localhost:2056`
- Notifications websocket: `ws://localhost:2057`
- Images: `http://localhost:2058`

## Setup

```powershell
.
\.venv\Scripts\python.exe -m pip install -r requirements.txt
\.venv\Scripts\python.exe server.py
```

Do not use plain `python server.py` unless your active `python` command already points at this workspace's `.venv`. The global Python 3.10 install can load incompatible packages and fail before startup.

## Notes

- The server returns the logged JSON payloads for the key endpoints captured in `Task.txt`.
- Unknown API routes currently return a JSON 404 payload so missing endpoints are easy to spot and add.
- The image server generates placeholder PNGs for known image names from the log and returns 404 text for unknown images.

## Discord Bot

There is a standalone Discord bot in `discord_status_bot.py` that reads the server root status endpoint.

Required environment variables:

- `DISCORD_BOT_TOKEN`: your Discord bot token
- `DISCORD_APPLICATION_ID`: your Discord application ID

Optional environment variables:

- `FIREREC_STATUS_URL`: defaults to `http://127.0.0.1:2056/`
- `DISCORD_GUILD_ID`: if set, slash commands sync to one guild faster for testing
- `DISCORD_STATUS_CHANNEL_ID`: if set, the bot renames that channel to `2018 Server Status: Online` or `2018 Server Status: Offline`

When a status channel is configured, the bot refreshes it every 20 seconds.

Run it with:

```powershell
\.venv\Scripts\python.exe discord_status_bot.py
```

Slash commands:

- `/status`: fetches the current FireRec server status endpoint and shows the raw JSON
- `/statuschannel`: sets the channel to rename as `2018 Server Status: Online` or `Offline`
- `/statuschannelclear`: disables status channel updates
- `/statusrefresh`: forces an immediate status channel refresh
