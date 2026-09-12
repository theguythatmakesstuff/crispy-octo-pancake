from __future__ import annotations

import asyncio
import io
import json
import math
import os
import struct
import uuid
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import Any

from fastapi import Body, FastAPI, HTTPException, Request, Response, UploadFile, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse, PlainTextResponse, StreamingResponse
from PIL import Image, ImageDraw
import uvicorn

NAME_SERVER_PORT = int(os.getenv("FIREREC_NAME_SERVER_PORT", os.getenv("PORT", "2059")))
API_PORT = int(os.getenv("FIREREC_API_PORT", os.getenv("PORT", "2056")))
WS_PORT = int(os.getenv("FIREREC_WS_PORT", os.getenv("PORT", "2057")))
IMAGE_PORT = int(os.getenv("FIREREC_IMAGE_PORT", os.getenv("PORT", "2058")))
NOTIF_DASHBOARD_PORT = int(os.getenv("FIREREC_DASHBOARD_PORT", os.getenv("PORT", "8000")))
RECNET_HTML_PORT = int(os.getenv("FIREREC_RECNET_PORT", os.getenv("PORT", "8080")))
HOST = os.getenv("FIREREC_HOST", "0.0.0.0")
RENDER_URL = os.getenv("RENDER_URL", "your-render-url")
PLACEHOLDER_IMAGE_PATH = Path(__file__).with_name("OIP (2).jpe")
NOTIF_DB_DIR = Path(__file__).with_name("db") / "notif"
NOTIF_DASHBOARD_HTML = Path(__file__).with_name("dashboard") / "notif.html"
USER_DB_DIR = Path(__file__).with_name("user_db")
USER_DB_PATH = USER_DB_DIR / "accounts.json"
ROOM_DB_DIR = Path(__file__).with_name("room_db")
ROOM_DB_PATH = ROOM_DB_DIR / "rooms.json"
ROOM_SAVE_DATA_DIR = ROOM_DB_DIR / "save_data"
REL_DB_DIR = Path(__file__).with_name("db") / "relationships"
REL_DB_PATH = REL_DB_DIR / "relationships.json"
RECNET_HTML_DIR = Path(__file__).with_name("recnet")
HUB_ACCESS_TOKEN = "AccessDeezNuts"
HUB_SUPPORTED_TRANSPORTS: list[dict[str, Any]] = [
    {"transport": "WebSockets", "transferFormats": ["Text", "Binary"]}
]
HUB_NEGOTIATE_VERSION = 0
HUB_URL = "http://localhost:2018/"
LATE_WS_PORT = 20161


def log_line(message: str) -> None:
    print(message, flush=True)


def format_payload(payload: Any) -> str:
    if payload is None:
        return ""
    if isinstance(payload, bytes):
        return f"<bytes:{len(payload)}>"
    if isinstance(payload, str):
        return payload
    return json.dumps(payload, separators=(",", ":"))


async def read_request_payload(request: Request) -> str:
    body = await request.body()
    if not body:
        return ""
    try:
        parsed = json.loads(body)
        return format_payload(parsed)
    except json.JSONDecodeError:
        try:
            return body.decode("utf-8")
        except UnicodeDecodeError:
            return f"<bytes:{len(body)}>"


def log_api_response(payload: Any) -> None:
    log_line(f"API Response: {format_payload(payload)}")


def log_websocket_event(path: str, event: str, payload: Any = None) -> None:
    suffix = f" {format_payload(payload)}" if payload not in (None, "") else ""
    log_line(f"[WebSocket.cs] {path} {event}{suffix}")


def hub_handshake(path: str) -> dict[str, Any]:
    return {
        "path": path,
        "connectionId": "firerec-hub",
        "accessToken": HUB_ACCESS_TOKEN,
        "supportedTransports": HUB_SUPPORTED_TRANSPORTS,
        "negotiateVersion": HUB_NEGOTIATE_VERSION,
        "url": HUB_URL,
    }


def signalr_record_separator() -> str:
    return "\x1e"


def signalr_json(payload: dict[str, Any]) -> str:
    return json.dumps(payload, separators=(",", ":")) + signalr_record_separator()


def player_hub_event(event_id: int, message: dict[str, Any]) -> str:
    return signalr_json({
        "type": 1,
        "target": "Player",
        "arguments": [{"Id": event_id, "Msg": message}],
    })


def notification_hub_event(event_id: int, message: dict[str, Any]) -> str:
    return signalr_json({
        "type": 1,
        "target": "Notification",
        "arguments": [json.dumps({"Id": event_id, "Msg": message}, separators=(",", ":"))],
    })


def signalr_completion(invocation_id: str) -> str:
    return signalr_json({"type": 3, "invocationId": invocation_id})


def signalr_handshake_ack() -> str:
    return signalr_record_separator()


def parse_signalr_messages(text: str) -> list[dict[str, Any]]:
    messages: list[dict[str, Any]] = []
    for chunk in text.split(signalr_record_separator()):
        chunk = chunk.strip()
        if not chunk:
            continue
        try:
            parsed = json.loads(chunk)
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, dict):
            messages.append(parsed)
    return messages


def notification_record_path() -> Path:
    return NOTIF_DB_DIR / "notifications.json"


def load_notifications() -> list[dict[str, Any]]:
    path = notification_record_path()
    if not path.exists():
        return []
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []


def save_notifications(records: list[dict[str, Any]]) -> None:
    NOTIF_DB_DIR.mkdir(parents=True, exist_ok=True)
    notification_record_path().write_text(json.dumps(records, indent=2), encoding="utf-8")


def build_notification_payload(title: str, body: str, action: int = 0) -> dict[str, Any]:
    now = datetime.now(timezone.utc).isoformat()
    return {
        "id": uuid.uuid4().hex,
        "createdAt": now,
        "title": title,
        "body": body,
        "action": action,
        "duration": 3.0,
        "priority": 1.0,
        "showToast": True,
        "playSound": True,
    }


def default_account() -> dict[str, Any]:
    return {
        "id": PLAYER["Id"],
        "username": PLAYER["Username"],
        "displayName": PLAYER["DisplayName"],
        "password": "12335678",
        "xp": PLAYER["XP"],
        "level": PLAYER["Level"],
        "platform": 0,
        "platformId": "76561199788433078",
        "profileImageName": PLAYER["ProfileImageName"],
        "juniorProfile": False,
        "developer": True,
        "token": "CuteRebornToken",
    }


def load_accounts() -> list[dict[str, Any]]:
    USER_DB_DIR.mkdir(parents=True, exist_ok=True)
    if not USER_DB_PATH.exists():
        accounts = [default_account()]
        USER_DB_PATH.write_text(json.dumps(accounts, indent=2), encoding="utf-8")
        return accounts
    try:
        accounts = json.loads(USER_DB_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        accounts = [default_account()]
        USER_DB_PATH.write_text(json.dumps(accounts, indent=2), encoding="utf-8")
    if not accounts:
        accounts = [default_account()]
        USER_DB_PATH.write_text(json.dumps(accounts, indent=2), encoding="utf-8")
    return accounts


def save_accounts(accounts: list[dict[str, Any]]) -> None:
    USER_DB_DIR.mkdir(parents=True, exist_ok=True)
    USER_DB_PATH.write_text(json.dumps(accounts, indent=2), encoding="utf-8")


def default_rooms() -> list[dict[str, Any]]:
    return [
        {"room": ROOM, "scene": SCENE, "tags": [{"Tag": "recroomoriginal", "Type": 2}], "hotTags": ["#recroomoriginal"]},
        {"room": REC_CENTER_ROOM, "scene": REC_CENTER_SCENE, "tags": [{"Tag": "community", "Type": 2}], "hotTags": ["#community"]},
        {"room": {"RoomId": 8, "Name": "Paintball", "Description": "", "WarningMask": 0, "CreatorPlayerId": 8703348, "ImageName": "93a53ced93a04f658795a87f4a4aab85", "State": 0, "Accessibility": 1, "SupportsLevelVoting": False, "IsAGRoom": True, "IsDormRoom": False, "CloningAllowed": False, "SupportsScreens": True, "SupportsWalkVR": True, "SupportsTeleportVR": True, "AllowsJuniors": True, "RoomWarningMask": 0, "CustomRoomWarning": None, "DisableMicAutoMute": True}, "scene": None, "tags": [{"Tag": "recroomoriginal", "Type": 2}], "hotTags": ["#recroomoriginal"]},
        {"room": {"RoomId": 7, "Name": "Paddleball", "Description": "", "WarningMask": 0, "CreatorPlayerId": 8703348, "ImageName": "ffdca6ed8bd94631ac15e3e894acb6c6", "State": 0, "Accessibility": 1, "SupportsLevelVoting": False, "IsAGRoom": True, "IsDormRoom": False, "CloningAllowed": False, "SupportsScreens": True, "SupportsWalkVR": True, "SupportsTeleportVR": True, "AllowsJuniors": True, "RoomWarningMask": 0, "CustomRoomWarning": None, "DisableMicAutoMute": True}, "scene": None, "tags": [{"Tag": "recroomoriginal", "Type": 2}], "hotTags": ["#recroomoriginal"]},
        {"room": {"RoomId": 6, "Name": "Dodgeball", "Description": "", "WarningMask": 0, "CreatorPlayerId": 8703348, "ImageName": "6d5c494668784816bbc41d9b870e5003", "State": 0, "Accessibility": 1, "SupportsLevelVoting": False, "IsAGRoom": True, "IsDormRoom": False, "CloningAllowed": False, "SupportsScreens": True, "SupportsWalkVR": True, "SupportsTeleportVR": True, "AllowsJuniors": True, "RoomWarningMask": 0, "CustomRoomWarning": None, "DisableMicAutoMute": True}, "scene": None, "tags": [{"Tag": "recroomoriginal", "Type": 2}], "hotTags": ["#recroomoriginal"]},
        {"room": {"RoomId": 6447, "Name": "TacoLoft", "Description": "", "WarningMask": 0, "CreatorPlayerId": 8703348, "ImageName": "Pc8fGEuX9EGqLlOfnNR3xw", "State": 0, "Accessibility": 1, "SupportsLevelVoting": False, "IsAGRoom": True, "IsDormRoom": False, "CloningAllowed": False, "SupportsScreens": True, "SupportsWalkVR": True, "SupportsTeleportVR": True, "AllowsJuniors": True, "RoomWarningMask": 0, "CustomRoomWarning": None, "DisableMicAutoMute": True}, "scene": None, "tags": [{"Tag": "recroomoriginal", "Type": 2}], "hotTags": ["#recroomoriginal"]},
    ]


def load_rooms() -> list[dict[str, Any]]:
    ROOM_DB_DIR.mkdir(parents=True, exist_ok=True)
    if not ROOM_DB_PATH.exists():
        rooms = default_rooms()
        ROOM_DB_PATH.write_text(json.dumps(rooms, indent=2), encoding="utf-8")
        return rooms
    try:
        rooms = json.loads(ROOM_DB_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        rooms = default_rooms()
        ROOM_DB_PATH.write_text(json.dumps(rooms, indent=2), encoding="utf-8")
    if not rooms:
        rooms = default_rooms()
        ROOM_DB_PATH.write_text(json.dumps(rooms, indent=2), encoding="utf-8")
    return rooms


def save_rooms(rooms: list[dict[str, Any]]) -> None:
    ROOM_DB_DIR.mkdir(parents=True, exist_ok=True)
    ROOM_DB_PATH.write_text(json.dumps(rooms, indent=2), encoding="utf-8")


def room_save_data_path(blob_name: str) -> Path:
    return ROOM_SAVE_DATA_DIR / blob_name


def room_save_metadata_path(room_scene_id: int) -> Path:
    return ROOM_SAVE_DATA_DIR / f"scene_{room_scene_id}.json"


def protobuf_varint(value: int) -> bytes:
    encoded = bytearray()
    while True:
        to_write = value & 0x7F
        value >>= 7
        if value:
            encoded.append(to_write | 0x80)
        else:
            encoded.append(to_write)
            return bytes(encoded)


def protobuf_field_varint(field_number: int, value: int) -> bytes:
    return protobuf_varint((field_number << 3) | 0) + protobuf_varint(value)


def protobuf_field_bytes(field_number: int, value: bytes) -> bytes:
    return protobuf_varint((field_number << 3) | 2) + protobuf_varint(len(value)) + value


def protobuf_field_float(field_number: int, value: float) -> bytes:
    return protobuf_varint((field_number << 3) | 5) + struct.pack("<f", value)


def ensure_seed_room_save_data() -> None:
    rooms = load_rooms()
    changed = False
    for entry in rooms:
        room = entry.get("room") or {}
        scene = entry.get("scene") or {}
        room_id = int(room.get("RoomId", 0) or 0)
        if room_id not in {1, 2}:
            continue
        blob_name = str(scene.get("DataBlobName") or "").strip()
        if blob_name and not room_save_data_path(blob_name).exists():
            scene["DataBlobName"] = ""
            entry["scene"] = scene
            changed = True
    if changed:
        save_rooms(rooms)


def load_relationships() -> list[dict[str, Any]]:
    REL_DB_DIR.mkdir(parents=True, exist_ok=True)
    if not REL_DB_PATH.exists():
        REL_DB_PATH.write_text("[]", encoding="utf-8")
        return []
    try:
        return json.loads(REL_DB_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        REL_DB_PATH.write_text("[]", encoding="utf-8")
        return []


def save_relationships(relationships: list[dict[str, Any]]) -> None:
    REL_DB_DIR.mkdir(parents=True, exist_ok=True)
    REL_DB_PATH.write_text(json.dumps(relationships, indent=2), encoding="utf-8")


def add_friend_request(from_player_id: int, to_player_id: int) -> None:
    relationships = load_relationships()
    for relationship in relationships:
        if int(relationship.get("FromPlayerId", -1)) == from_player_id and int(relationship.get("ToPlayerId", -1)) == to_player_id:
            return
    relationships.append({
        "FromPlayerId": from_player_id,
        "ToPlayerId": to_player_id,
        "Type": 1,
        "Status": 0,
        "Favorited": False,
        "Muted": False,
    })
    save_relationships(relationships)


def find_room_entry(room_id: int | None = None, room_name: str | None = None) -> dict[str, Any] | None:
    for entry in load_rooms():
        room = entry.get("room", {})
        if room_id is not None and int(room.get("RoomId", -1)) == room_id:
            return entry
        if room_name is not None and str(room.get("Name", "")).lower() == room_name.lower():
            return entry
    return None


def get_active_room_entry() -> dict[str, Any]:
    entry = find_room_entry(room_id=1)
    if entry is not None:
        return entry
    return {"room": ROOM, "scene": SCENE}


def get_active_room_scene() -> tuple[dict[str, Any], dict[str, Any]]:
    entry = get_active_room_entry()
    room = dict(entry.get("room") or ROOM)
    scene = dict(entry.get("scene") or SCENE)
    return room, scene


def search_rooms(value: str) -> list[dict[str, Any]]:
    needle = value.strip().lower().lstrip("@")
    results: list[dict[str, Any]] = []
    for entry in load_rooms():
        room = entry.get("room", {})
        room_name = str(room.get("Name", "")).lower()
        creator_name = str(room.get("CreatorName", PLAYER["Username"])).lower()
        if not needle or needle in room_name or needle in creator_name:
            results.append(room)
    return results


def clone_room_entry(source_room_id: int, new_name: str, target_room_id: int | None = None) -> dict[str, Any]:
    rooms = load_rooms()
    source_entry = find_room_entry(room_id=source_room_id)
    if source_entry is None:
        raise HTTPException(status_code=404, detail="source room not found")
    if target_room_id is not None:
        if any(int(entry.get("room", {}).get("RoomId", 0)) == target_room_id for entry in rooms):
            raise HTTPException(status_code=400, detail="target room id already exists")
        next_room_id = target_room_id
    else:
        next_room_id = max(int(entry.get("room", {}).get("RoomId", 0)) for entry in rooms) + 1
    source_room = dict(source_entry.get("room", {}))
    source_scene = dict(source_entry.get("scene") or {})
    cloned_room = dict(source_room)
    cloned_room["RoomId"] = next_room_id
    cloned_room["Name"] = new_name
    cloned_room["IsDormRoom"] = False
    cloned_room["CloningAllowed"] = True
    cloned_room["ImageName"] = source_room.get("ImageName", ROOM["ImageName"])
    cloned_scene = dict(source_scene) if source_scene else None
    if cloned_scene is not None:
        cloned_scene["RoomId"] = next_room_id
        cloned_scene["RoomSceneId"] = 1
    cloned_entry = {
        "room": cloned_room,
        "scene": cloned_scene,
        "tags": list(source_entry.get("tags", [])),
        "hotTags": list(source_entry.get("hotTags", [])),
    }
    rooms.append(cloned_entry)
    save_rooms(rooms)
    return cloned_entry


def account_to_player(account: dict[str, Any]) -> dict[str, Any]:
    player = dict(PLAYER)
    player["Id"] = int(account["id"])
    player["Username"] = str(account["username"])
    player["DisplayName"] = str(account.get("displayName") or account["username"])
    player["XP"] = int(account.get("xp", player["XP"]))
    player["Level"] = int(account.get("level", player["Level"]))
    player["ProfileImageName"] = str(account.get("profileImageName") or player["ProfileImageName"])
    player["JuniorProfile"] = bool(account.get("juniorProfile", False))
    player["Developer"] = bool(account.get("developer", False))
    player["PlatformIds"] = [{
        "Platform": int(account.get("platform", 0)),
        "PlatformId": account.get("platformId", 1),
    }]
    return player


def find_account_by_platform(platform: Any, platform_id: Any) -> dict[str, Any] | None:
    platform_text = str(platform)
    platform_id_text = str(platform_id)
    for account in load_accounts():
        if str(account.get("platform", 0)) == platform_text and str(account.get("platformId", "")) == platform_id_text:
            return account
    return None


def find_account_by_id(player_id: Any) -> dict[str, Any] | None:
    player_id_text = str(player_id)
    for account in load_accounts():
        if str(account.get("id", "")) == player_id_text:
            return account
    return None


def find_account_by_credentials(username: Any, password: Any) -> dict[str, Any] | None:
    username_text = str(username).lower()
    password_text = str(password)
    for account in load_accounts():
        if str(account.get("username", "")).lower() == username_text and str(account.get("password", "")) == password_text:
            return account
    return None


def public_account(account: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": account["id"],
        "username": account["username"],
        "displayName": account.get("displayName") or account["username"],
        "bio": account.get("bio", ""),
        "xp": int(account.get("xp", PLAYER["XP"])),
        "level": int(account.get("level", PLAYER["Level"])),
        "platform": account.get("platform", 0),
        "platformId": account.get("platformId", ""),
        "profileImageName": account.get("profileImageName", PLAYER["ProfileImageName"]),
        "hiddenAvatarItemDescs": list(account.get("hiddenAvatarItemDescs", [])),
        "banned": bool(account.get("banned", False)),
    }


def resolve_players(player_ids: list[Any]) -> list[dict[str, Any]]:
    resolved: list[dict[str, Any]] = []
    for player_id in player_ids:
        account = find_account_by_id(player_id)
        if account is not None:
            resolved.append(account_to_player(account))
    if not resolved:
        resolved.append(account_to_player(load_accounts()[0]))
    return resolved


def presence_payload_for_player(player: dict[str, Any]) -> dict[str, Any]:
    account = find_account_by_id(player["Id"])
    session = session_payload_for_account(account or {"id": player["Id"]})
    return {
        "PlayerId": player["Id"],
        "IsOnline": True,
        "PlayerType": 2,
        "GameSession": session,
    }


def avatar_payload_for_player(player: dict[str, Any]) -> dict[str, Any]:
    payload = dict(AVATAR_V2)
    payload["ProfileImageName"] = player["ProfileImageName"]
    account = find_account_by_id(player["Id"])
    if account is not None:
        for key in ("OutfitSelections", "FaceFeatures", "SkinColor", "HairColor"):
            if key in account:
                payload[key] = account[key]
    return payload


def named_images_payload() -> list[dict[str, Any]]:
    payload = [
        {
            "FriendlyImageName": account.get("username") or account.get("displayName") or "Player",
            "ImageName": account.get("profileImageName") or PLAYER["ProfileImageName"],
            "StartTime": "2021-12-27T21:27:38.188Z",
            "EndTime": "2026-12-27T21:27:38.188Z",
        }
        for account in load_accounts()
    ]
    payload.extend([
        {"FriendlyImageName": "Loft", "ImageName": "cf863d3a013f4332abaac807324cb339.jpg", "StartTime": "2021-12-27T21:27:38.188Z", "EndTime": "2026-12-27T21:27:38.188Z"},
        {"FriendlyImageName": "BackStairs", "ImageName": "b63bcfa3e16042f59e1b2ba40021b9b9.jpg", "StartTime": "2021-12-27T21:27:38.188Z", "EndTime": "2026-12-27T21:27:38.188Z"},
    ])
    return payload


def build_login_response(account: dict[str, Any]) -> dict[str, Any]:
    return {
        "Error": "",
        "Player": account_to_player(account),
        "Token": str(account.get("token") or "CuteRebornToken"),
        "FirstLoginOfTheDay": True,
        "AnalyticsSessionId": 0,
        "CanUseScreenMode": True,
        "CachedPlatformMask": 0,
    }


def create_account(username: str, password: str, platform: int = 0, platform_id: str | None = None) -> dict[str, Any]:
    accounts = load_accounts()
    normalized_username = username.strip()
    if any(str(account.get("username", "")).lower() == normalized_username.lower() for account in accounts):
        raise ValueError("Username already exists")
    next_id = max(int(account.get("id", PLAYER["Id"])) for account in accounts) + 1
    account = {
        "id": next_id,
        "username": normalized_username,
        "displayName": normalized_username,
        "password": password,
        "xp": PLAYER["XP"],
        "level": PLAYER["Level"],
        "platform": platform,
        "platformId": platform_id or f"local-{next_id}",
        "profileImageName": PLAYER["ProfileImageName"],
        "OutfitSelections": f"account-{next_id},,,,{next_id % 3}",
        "FaceFeatures": json.dumps({"ver": 3, "eyeId": f"eye-{next_id}", "mouthId": f"mouth-{next_id}"}, separators=(",", ":")),
        "SkinColor": AVATAR_V2.get("SkinColor", "85343b16-d58a-4091-96d8-083a81fb03ae"),
        "HairColor": AVATAR_V2.get("HairColor", "0e_jaaObREWTf1AorAZ95g"),
        "hiddenAvatarItemDescs": [],
        "juniorProfile": False,
        "developer": False,
        "banned": False,
        "token": f"CuteRebornToken-{next_id}",
    }
    accounts.append(account)
    save_accounts(accounts)
    return account


def set_account_banned(account_id: Any, banned: bool) -> dict[str, Any]:
    accounts = load_accounts()
    account_id_text = str(account_id)
    for account in accounts:
        if str(account.get("id", "")) == account_id_text:
            account["banned"] = banned
            save_accounts(accounts)
            return account
    raise HTTPException(status_code=404, detail="account not found")


def delete_account(account_id: Any) -> None:
    accounts = load_accounts()
    account_id_text = str(account_id)
    filtered = [account for account in accounts if str(account.get("id", "")) != account_id_text]
    if len(filtered) == len(accounts):
        raise HTTPException(status_code=404, detail="account not found")
    if not filtered:
        filtered = [default_account()]
    save_accounts(filtered)


def update_account_avatar(player_id: Any, avatar_data: dict[str, Any]) -> dict[str, Any]:
    accounts = load_accounts()
    player_id_text = str(player_id)
    for account in accounts:
        if str(account.get("id", "")) != player_id_text:
            continue
        for key in ("OutfitSelections", "FaceFeatures", "SkinColor", "HairColor"):
            if key in avatar_data:
                account[key] = avatar_data[key]
        save_accounts(accounts)
        return account
    raise HTTPException(status_code=404, detail="account not found")


def update_account_profile(account_id: Any, updates: dict[str, Any]) -> dict[str, Any]:
    accounts = load_accounts()
    account_id_text = str(account_id)
    for account in accounts:
        if str(account.get("id", "")) != account_id_text:
            continue
        if "level" in updates:
            account["level"] = max(1, int(updates["level"]))
        if "xp" in updates:
            account["xp"] = max(0, int(updates["xp"]))
        if "profileImageName" in updates:
            account["profileImageName"] = str(updates["profileImageName"]).strip() or PLAYER["ProfileImageName"]
        if "bio" in updates:
            account["bio"] = str(updates["bio"])
        save_accounts(accounts)
        return account
    raise HTTPException(status_code=404, detail="account not found")


def set_account_avatar_item_hidden(account_id: Any, avatar_item_desc: str, hidden: bool) -> dict[str, Any]:
    accounts = load_accounts()
    account_id_text = str(account_id)
    for account in accounts:
        if str(account.get("id", "")) != account_id_text:
            continue
        hidden_items = set(str(item) for item in account.get("hiddenAvatarItemDescs", []))
        if hidden:
            hidden_items.add(avatar_item_desc)
        else:
            hidden_items.discard(avatar_item_desc)
        account["hiddenAvatarItemDescs"] = sorted(hidden_items)
        save_accounts(accounts)
        return account
    raise HTTPException(status_code=404, detail="account not found")


notification_clients: set[WebSocket] = set()
hub_clients: set[WebSocket] = set()
ACTIVE_LOGIN_TOKENS: dict[str, int] = {}
ACTIVE_PLAYER_SESSIONS: dict[int, dict[str, Any]] = {}
MAINTENANCE_ENDS_AT: datetime | None = None
MAINTENANCE_DISABLE_ACCOUNT_LOGIN = False
MAINTENANCE_EXPIRY_TASK: asyncio.Task[None] | None = None
# Track active WebSocket connections per account_id for kick-on-ban
ACTIVE_WEBSOCKETS: dict[int, set[WebSocket]] = {}


def bind_login_token(login_lock_token: Any, account: dict[str, Any]) -> None:
    token = str(login_lock_token or "").strip()
    if token:
        ACTIVE_LOGIN_TOKENS[token] = int(account["id"])


def unbind_login_token(login_lock_token: Any) -> None:
    token = str(login_lock_token or "").strip()
    if token:
        ACTIVE_LOGIN_TOKENS.pop(token, None)


def register_websocket_connection(account_id: int, ws: WebSocket) -> None:
    """Register a WebSocket connection for kick-on-ban."""
    ACTIVE_WEBSOCKETS.setdefault(int(account_id), set()).add(ws)


def unregister_websocket_connection(account_id: int, ws: WebSocket) -> None:
    """Unregister a WebSocket connection."""
    if account_id in ACTIVE_WEBSOCKETS:
        ACTIVE_WEBSOCKETS[account_id].discard(ws)
        if not ACTIVE_WEBSOCKETS[account_id]:
            del ACTIVE_WEBSOCKETS[account_id]


def kick_player_by_account_id(account_id: int, reason: str = "") -> None:
    """Close all active WebSocket connections for an account (kick-on-ban)."""
    connections = ACTIVE_WEBSOCKETS.get(int(account_id), set())
    for ws in list(connections):
        try:
            import asyncio
            loop = asyncio.get_event_loop()
            if loop.is_running():
                asyncio.ensure_future(ws.close(code=4000))
            else:
                asyncio.run(ws.close(code=4000))
        except Exception:
            pass
    ACTIVE_WEBSOCKETS.pop(int(account_id), None)
    if reason:
        log_api_response(f"Kicked player {account_id}: {reason}")


def account_from_login_token(login_lock_token: Any) -> dict[str, Any] | None:
    token = str(login_lock_token or "").strip()
    if not token:
        return None
    account_id = ACTIVE_LOGIN_TOKENS.get(token)
    if account_id is None:
        return None
    return find_account_by_id(account_id)


def set_player_session(account_id: int, game_session: dict[str, Any]) -> None:
    ACTIVE_PLAYER_SESSIONS[int(account_id)] = dict(game_session)


def clear_player_session(account_id: int) -> None:
    ACTIVE_PLAYER_SESSIONS.pop(int(account_id), None)


def session_payload_for_account(account: dict[str, Any]) -> dict[str, Any]:
    account_id = int(account.get("id", PLAYER["Id"]))
    existing = ACTIVE_PLAYER_SESSIONS.get(account_id)
    if existing is not None:
        return dict(existing)
    active_room, active_scene = get_active_room_scene()
    room_id = int(active_room.get("RoomId", ROOM["RoomId"]))
    return {
        "GameSessionId": 20182,
        "PhotonRegionId": "us",
        "PhotonRoomId": "FireRec1" if room_id == 2 else f"FireRec{room_id}",
        "Name": active_room.get("Name", ROOM["Name"]),
        "RoomId": room_id,
        "RoomSceneId": int(active_scene.get("RoomSceneId", SCENE["RoomSceneId"])),
        "RoomSceneLocationId": active_scene.get("RoomSceneLocationId", SCENE["RoomSceneLocationId"]),
        "IsSandbox": bool(active_scene.get("IsSandbox", SCENE["IsSandbox"])),
        "DataBlobName": active_scene.get("DataBlobName", ""),
        "Private": bool(active_room.get("IsDormRoom", False)),
        "GameInProgress": False,
        "MaxCapacity": int(active_scene.get("MaxPlayers", SCENE["MaxPlayers"])),
        "IsFull": False,
    }


def resolve_active_account(payload: dict[str, Any] | None = None, request: Request | None = None) -> dict[str, Any]:
    payload = payload or {}
    account = account_from_login_token(payload.get("LoginLockToken"))
    if account is None and request is not None:
        account = account_from_login_token(request.query_params.get("LoginLockToken"))
    if account is None and "PlayerId" in payload:
        account = find_account_by_id(payload.get("PlayerId"))
    if account is None:
        account = load_accounts()[0]
    return account


def maintenance_starts_in_minutes() -> int:
    if MAINTENANCE_ENDS_AT is None:
        return 0
    remaining_seconds = (MAINTENANCE_ENDS_AT - datetime.now(timezone.utc)).total_seconds()
    return max(0, math.ceil(remaining_seconds / 60))


def account_login_is_disabled() -> bool:
    return MAINTENANCE_DISABLE_ACCOUNT_LOGIN and MAINTENANCE_ENDS_AT is not None and maintenance_starts_in_minutes() == 0


def maintenance_is_active() -> bool:
    return MAINTENANCE_ENDS_AT is not None and maintenance_starts_in_minutes() == 0


async def broadcast_notification(payload: dict[str, Any]) -> None:
    if not notification_clients:
        return
    dead_clients: list[WebSocket] = []
    message = json.dumps(payload, separators=(",", ":"))
    for client in notification_clients:
        try:
            await client.send_text(message)
        except Exception:
            dead_clients.append(client)
    for client in dead_clients:
        notification_clients.discard(client)


async def broadcast_hub_notification(event_id: int, message: dict[str, Any]) -> int:
    connections = set(hub_clients)
    if not connections:
        return 0
    dead_connections: list[WebSocket] = []
    payload = notification_hub_event(event_id, message)
    for connection in connections:
        try:
            await connection.send_text(payload)
        except Exception:
            dead_connections.append(connection)
    for connection in dead_connections:
        hub_clients.discard(connection)
        for account_id, sockets in list(ACTIVE_WEBSOCKETS.items()):
            sockets.discard(connection)
            if not sockets:
                del ACTIVE_WEBSOCKETS[account_id]
    return len(connections) - len(dead_connections)


async def disconnect_hub_clients() -> int:
    connections = list(hub_clients)
    for connection in connections:
        try:
            await connection.close(code=1012, reason="Server maintenance")
        except Exception:
            pass
        hub_clients.discard(connection)
    return len(connections)


async def expire_server_maintenance(expected_end_time: datetime, starting_minutes: int) -> None:
    for remaining_minutes in range(starting_minutes - 1, -1, -1):
        notification_time = expected_end_time - timedelta(minutes=remaining_minutes)
        remaining_seconds = max(0, (notification_time - datetime.now(timezone.utc)).total_seconds())
        await asyncio.sleep(remaining_seconds)
        if MAINTENANCE_ENDS_AT != expected_end_time:
            return
        maintenance = {"StartsInMinutes": remaining_minutes}
        CONFIG_V2["ServerMaintenance"] = maintenance
        await broadcast_hub_notification(25, maintenance)
    disconnected_clients = await disconnect_hub_clients()
    log_line(f"Server maintenance started; disconnected {disconnected_clients} hub client(s).")


async def send_blank_ws_json(websocket: WebSocket) -> None:
    await websocket.send_json({})


def iso(value: str) -> str:
    return value


PLAYER = {
    "Id": 8703348,
    "Username": "Coach",
    "DisplayName": "Coach",
    "XP": 48,
    "Level": 99,
    "RegistrationStatus": 2,
    "Developer": True,
    "CanReceiveInvites": False,
    "ProfileImageName": "Coach",
    "JuniorProfile": False,
    "ForceJuniorImages": False,
    "PendingJunior": False,
    "HasBirthday": True,
    "AvoidJuniors": True,
    "PlayerReputation": {
        "Noteriety": 0,
        "CheerGeneral": 1,
        "CheerHelpful": 1,
        "CheerGreatHost": 1,
        "CheerSportsman": 1,
        "CheerCreative": 1,
        "CheerCredit": 77,
        "SubscriberCount": 2,
        "SubscribedCount": 0,
        "SelectedCheer": 0,
    },
    "PlatformIds": [{"Platform": 0, "PlatformId": 1}],
}

ROOM = {
    "RoomId": 1,
    "Name": "DormRoom",
    "Description": "A private room. this is cool",
    "WarningMask": 0,
    "CreatorPlayerId": 8703348,
    "ImageName": "ca673ff19c054158a15ff00f0b844ba7",
    "State": 0,
    "Accessibility": 2,
    "SupportsLevelVoting": False,
    "IsAGRoom": True,
    "IsDormRoom": True,
    "CloningAllowed": False,
    "SupportsScreens": True,
    "SupportsWalkVR": True,
    "SupportsTeleportVR": True,
    "AllowsJuniors": True,
    "RoomWarningMask": 0,
    "CustomRoomWarning": None,
    "DisableMicAutoMute": True,
}

SCENE = {
    "RoomSceneId": 1,
    "RoomId": 1,
    "RoomSceneLocationId": "76d98498-60a1-430c-ab76-b54a29b7a163",
    "Name": "Home",
    "IsSandbox": True,
    "DataBlobName": "",
    "MaxPlayers": 20,
    "CanMatchmakeInto": True,
    "DataModifiedAt": "2026-08-17T02:03:01.6054147Z",
}

REC_CENTER_ROOM = {
    "RoomId": 2,
    "Name": "RecCenter",
    "Description": "A social hub to meet and mingle with friends new and old.",
    "WarningMask": 0,
    "CreatorPlayerId": 8703348,
    "ImageName": "22eefa3219f046fd9e2090814650ede3",
    "State": 0,
    "Accessibility": 1,
    "SupportsLevelVoting": False,
    "IsAGRoom": True,
    "IsDormRoom": False,
    "CloningAllowed": False,
    "SupportsScreens": True,
    "SupportsWalkVR": True,
    "SupportsTeleportVR": True,
    "AllowsJuniors": True,
    "RoomWarningMask": 0,
    "CustomRoomWarning": None,
    "DisableMicAutoMute": True,
}

REC_CENTER_SCENE = {
    "RoomSceneId": 1,
    "RoomId": 2,
    "RoomSceneLocationId": "cbad71af-0831-44d8-b8ef-69edafa841f6",
    "Name": "Home",
    "IsSandbox": False,
    "DataBlobName": "",
    "MaxPlayers": 20,
    "CanMatchmakeInto": True,
    "DataModifiedAt": "2026-08-21T16:50:14.4934643Z",
}

CONFIG_V2 = {
    "MessageOfTheDay": "Welcome to FireRec!",
    "ServerMaintenance": {"StartsInMinutes": 0},
    "CdnBaseUri": f"http://localhost:{IMAGE_PORT}/",
    "ShareBaseUrl": "i love goats",
    "LevelProgressionMaps": [],
    "MatchmakingParams": {"PreferFullRoomsFrequency": 1, "PreferEmptyRoomsFrequency": 0},
    "DailyObjectives": [
        [{"type": 21, "score": 1, "xp": 0}, {"type": 802, "score": 3, "xp": 0}, {"type": 100, "score": 2, "xp": 0}],
        [{"type": 502, "score": 5, "xp": 0}, {"type": 400, "score": 3, "xp": 0}, {"type": 101, "score": 2, "xp": 0}],
        [{"type": 301, "score": 3, "xp": 0}, {"type": 202, "score": 4, "xp": 0}, {"type": 603, "score": 2, "xp": 0}],
        [{"type": 21, "score": 1, "xp": 0}, {"type": 802, "score": 3, "xp": 0}, {"type": 100, "score": 2, "xp": 0}],
        [{"type": 502, "score": 5, "xp": 0}, {"type": 400, "score": 3, "xp": 0}, {"type": 101, "score": 2, "xp": 0}],
        [{"type": 301, "score": 3, "xp": 0}, {"type": 202, "score": 4, "xp": 0}, {"type": 603, "score": 2, "xp": 0}],
        [{"type": 302, "score": 3, "xp": 0}, {"type": 401, "score": 2, "xp": 0}, {"type": 800, "score": 1, "xp": 0}],
    ],
    "ConfigTable": [
        {"Key": "Gift.DropChance", "Value": "0.5"},
        {"Key": "Gift.XP", "Value": "0.5"},
    ],
    "PhotonConfig": {
        "CloudRegion": "us",
        "CrcCheckEnabled": False,
        "EnableServerTracingAfterDisconnect": False,
    },
    "AutoMicMutingConfig": {
        "MicSpamVolumeThreshold": 0,
        "MicVolumeSampleInterval": 0,
        "MicVolumeSampleRollingWindowLength": 0,
        "MicSpamSamplePercentageForWarning": 0,
        "MicSpamSamplePercentageForWarningToEnd": 0,
        "MicSpamSamplePercentageForForceMute": 0,
        "MicSpamSamplePercentageForForceMuteToEnd": 0,
        "MicSpamWarningStateVolumeMultiplier": 0,
    },
}

GAMECONFIGS = [
    {"Key": "Gift.MaxDaily", "Value": "100", "StartTime": None, "EndTime": None},
    {"Key": "Gift.Falloff", "Value": "1", "StartTime": None, "EndTime": None},
    {"Key": "Gift.DropChance", "Value": "100", "StartTime": None, "EndTime": None},
    {"Key": "UseHeartbeatWebSocket", "Value": "0", "StartTime": None, "EndTime": None},
    {"Key": "Screens.ForceVerification", "Value": "1", "StartTime": None, "EndTime": None},
    {"Key": "Screens.ForceVerification", "Value": "1", "StartTime": None, "EndTime": None},
    {"Key": "forceRegistration", "Value": "0", "StartTime": None, "EndTime": None},
    {"Key": "Door.Creative.Query", "Value": "#puzzle", "StartTime": None, "EndTime": None},
    {"Key": "Door.Creative.Title", "Value": "PUZZLE", "StartTime": None, "EndTime": None},
    {"Key": "Door.Featured.Query", "Value": "#featured", "StartTime": None, "EndTime": None},
    {"Key": "Door.Featured.Title", "Value": "Featured", "StartTime": None, "EndTime": None},
    {"Key": "Door.Quests.Query", "Value": "#quest", "StartTime": None, "EndTime": None},
    {"Key": "Door.Quests.Title", "Value": "QUESTS", "StartTime": None, "EndTime": None},
    {"Key": "Door.Shooters.Query", "Value": "#pvp", "StartTime": None, "EndTime": None},
    {"Key": "Door.Shooters.Title", "Value": "PVP", "StartTime": None, "EndTime": None},
    {"Key": "Door.Sports.Query", "Value": "#sport", "StartTime": None, "EndTime": None},
    {"Key": "Door.Sports.Title", "Value": "SPORTS & REC", "StartTime": None, "EndTime": None},
]

SETTINGS = [
    {"Key": "MOD_BLOCKED_TIME", "Value": "0"},
    {"Key": "MOD_BLOCKED_DURATION", "Value": "0"},
    {"Key": "PlayerSessionCount", "Value": "50"},
    {"Key": "ShowRoomCenter", "Value": "0"},
    {"Key": "QualitySettings", "Value": "2"},
    {"Key": "Recroom.OOBE", "Value": "100"},
    {"Key": "VoiceFilter2", "Value": "1"},
    {"Key": "VIGNETTED_TELEPORT_ENABLED", "Value": "0"},
    {"Key": "CONTINUOUS_ROTATION_MODE", "Value": "0"},
    {"Key": "ROTATION_INCREMENT", "Value": "0"},
    {"Key": "ROTATE_IN_PLACE_ENABLED", "Value": "0"},
    {"Key": "OOBE_OBJECTIVES_GRANTED", "Value": "0"},
    {"Key": "TeleportBuffer", "Value": "0"},
    {"Key": "VoiceChat", "Value": "1"},
    {"Key": "PersonalBubble", "Value": "0"},
    {"Key": "IgnoreBuffer", "Value": "0"},
    {"Key": "H.264 plugin", "Value": "1"},
    {"Key": "USER_TRACKING", "Value": "55"},
    {"Key": "google_analytics_clientid_pref_key", "Value": "VWVVWHGC89e1af3c58e903e334dc9b5e587936e1fff92502"},
    {"Key": "TUTORIAL_COMPLETE_MASK", "Value": "57"},
    {"Key": "BACKPACK_FAVORITE_TOOL", "Value": "-1"},
    {"Key": "Recroom.ChallengeMap", "Value": "0"},
    {"Key": "SplitTestAssignedSegments", "Value": "1|{}"},
    {"Key": "FIRST_TIME_IN_FLAGS", "Value": "0"},
    {"Key": "Recroom.AccountCreation.HasStarted", "Value": "True"},
    {"Key": "Recroom.AccountCreation.HasFinished", "Value": "True"},
    {"Key": "Recroom.AccountCreation.HasChosenUsername", "Value": "True"},
    {"Key": "Recroom.AccountCreation.HasCreatedPassword", "Value": "True"},
    {"Key": "MakerPen_SnappingMode", "Value": "0"},
    {"Key": "HasCheckedForPlatformReferrers", "Value": "True"},
    {"Key": "HAS_OPENED_WATCH_MENU_BEFORE", "Value": "True"},
    {"Key": "HAS_SEEN_HOME_SCREEN_CHOICE", "Value": "True"},
    {"Key": "USE_NEW_HOME_SCREEN", "Value": "False"},
]

SETTINGS_MAP = {item["Key"]: item["Value"] for item in SETTINGS}


def current_settings() -> list[dict[str, str]]:
    return [{"Key": item["Key"], "Value": SETTINGS_MAP.get(item["Key"], item["Value"])} for item in SETTINGS]

FEATURED_ROOMS = {
    "Name": "FireRec Featured Rooms",
    "Rooms": [
        {"RoomName": "TacoLoft", "RoomId": 6447, "ImageName": "Pc8fGEuX9EGqLlOfnNR3xw"},
        {"RoomName": "RecCenter", "RoomId": 2, "ImageName": "93a53ced93a04f658795a87f4a4aab85"},
        {"RoomName": "Paintball", "RoomId": 8, "ImageName": "93a53ced93a04f658795a87f4a4aab85"},
        {"RoomName": "Dodgeball", "RoomId": 6, "ImageName": "6d5c494668784816bbc41d9b870e5003"},
        {"RoomName": "Paddleball", "RoomId": 7, "ImageName": "ffdca6ed8bd94631ac15e3e894acb6c6"},
        {"RoomName": "DiscGolfLake", "RoomId": 4, "ImageName": "52cf6c3271894ecd95fb0c9b2d2209a7"},
        {"RoomName": "Lounge", "RoomId": 22, "ImageName": "3e8c2458f1e542ab8aa275e4083ee47a"},
    ],
}

HOT_ROOMS = {
    "#recroomoriginal": [
        {"RoomId": 8, "Name": "Paintball", "ImageName": "93a53ced93a04f658795a87f4a4aab85", "PlayerCount": 12},
        {"RoomId": 7, "Name": "Paddleball", "ImageName": "ffdca6ed8bd94631ac15e3e894acb6c6", "PlayerCount": 6},
        {"RoomId": 6, "Name": "Dodgeball", "ImageName": "6d5c494668784816bbc41d9b870e5003", "PlayerCount": 10},
        {"RoomId": 6447, "Name": "TacoLoft", "ImageName": "Pc8fGEuX9EGqLlOfnNR3xw", "PlayerCount": 4},
    ],
    "#community": [
        {"RoomId": 367247, "Name": "forsophie1", "ImageName": "b59b03d3e2234af8a0ae5b9043440580", "PlayerCount": 3},
        {"RoomId": 155757, "Name": "AntiHamClub", "ImageName": "MgAAM8tYCUq0wwOy9flG2w", "PlayerCount": 2},
        {"RoomId": 9001, "Name": "CommunityHangout", "ImageName": "4kQHuyDAzkeHe-OV4Qrdeg", "PlayerCount": 5},
        {"RoomId": 9002, "Name": "MakerWorld", "ImageName": "JejHE-bjhUmHU47aqVxMpA", "PlayerCount": 7},
        {"RoomId": 9003, "Name": "QuestHub", "ImageName": "yMIjCkEusk6hfv08GqhOVg", "PlayerCount": 8},
        {"RoomId": 9004, "Name": "SandboxLab", "ImageName": "22eefa3219f046fd9e2090814650ede3", "PlayerCount": 1},
    ],
}

AVATAR_V2 = {
    "OutfitSelections": "1fd69ef8-0b74-4962-af5a-67f0bf0358f2,,,,0;d0a9262f-5504-46a7-bb10-7507503db58e,95e4cc30-cb68-473d-a395-feadf5b51512,,0c496f32-0011-4c4d-9778-59446f70c032,1",
    "FaceFeatures": "{\"ver\":3,\"eyeId\":\"AjGMoJhEcEehacRZjUMuDg\",\"eyePos\":{\"x\":-0.009999999776482582,\"y\":-0.03999999910593033},\"eyeScl\":0.05000000074505806,\"mouthId\":\"FrZBRanXEEK29yKJ4jiMjg\",\"mouthPos\":{\"x\":0.0,\"y\":0.07999999821186066},\"mouthScl\":0.05000000074505806,\"hairPrimaryColorId\":\"\",\"hairSecondaryColorId\":\"5ee30295-b05f-4e96-819e-5ac865b2c63d\",\"hairPatternId\":\"\",\"beardColorId\":\"5ee30295-b05f-4e96-819e-5ac865b2c63d\",\"beardSecondaryColorId\":\"5ee30295-b05f-4e96-819e-5ac865b2c63d\",\"faceShapeId\":\"\",\"bodyShapeId\":\"\",\"useHatAnchorParams\":false,\"hideEars\":true,\"hatAnchorParams\":{\"NormalizedPosition\":{\"x\":0.5,\"y\":0.5},\"HemisphereOffsets\":{\"x\":0.0,\"y\":0.0,\"z\":0.0},\"HemisphereRotations\":{\"x\":0.0,\"y\":0.0,\"z\":0.0}}}",
    "SkinColor": "85343b16-d58a-4091-96d8-083a81fb03ae",
    "HairColor": "0e_jaaObREWTf1AorAZ95g",
}

AVATAR_ITEMS = [
    {"AvatarItemDesc": "21caa68e-c3fa-474c-af5e-af1e742b7a60,6564acf1-4d70-4f92-92ac-08e2b76dbb6b,,", "UnlockedLevel": 0, "PlatformMask": -1, "FriendlyName": "Tennis Skirt", "Tooltip": "", "Rarity": 0},
    {"AvatarItemDesc": "21caa68e-c3fa-474c-af5e-af1e742b7a60,cbe29e9f-f2ac-47fb-97e1-8bad16abb89d,,", "UnlockedLevel": 0, "PlatformMask": -1, "FriendlyName": "Tennis Skirt", "Tooltip": "", "Rarity": 0},
    {"AvatarItemDesc": "21caa68e-c3fa-474c-af5e-af1e742b7a60,d66aa400-aa5a-4539-a25d-5f8ce94dc281,,", "UnlockedLevel": 0, "PlatformMask": -1, "FriendlyName": "Tennis Skirt", "Tooltip": "", "Rarity": 0},
]

EQUIPMENT = [
    {"PrefabName": "[MakerPen]", "ModificationGuid": "makerpen-default", "UnlockedLevel": 0, "Favorited": False, "PlatformMask": -1, "FriendlyName": "[MakerPen]", "Tooltip": "", "Rarity": 0},
    {"PrefabName": "[PaintballAssaultRifle]", "ModificationGuid": "357fe573-fee7-467f-93a7-5e61afb024b8", "UnlockedLevel": 0, "Favorited": False, "PlatformMask": -1, "FriendlyName": "[PaintballAssaultRifle]", "Tooltip": "", "Rarity": 0},
    {"PrefabName": "[PaintballAssaultRifle]", "ModificationGuid": "m_qzPvDPF0KPhLd59wzr5A", "UnlockedLevel": 0, "Favorited": False, "PlatformMask": -1, "FriendlyName": "[PaintballAssaultRifle]", "Tooltip": "", "Rarity": 0},
    {"PrefabName": "[ShareCamera]", "ModificationGuid": "e2844f84-ab44-4141-9a0a-bd5da7caa4f6", "UnlockedLevel": 0, "Favorited": False, "PlatformMask": -1, "FriendlyName": "[ShareCamera]", "Tooltip": "", "Rarity": 0},
]

CONSUMABLES = [
    {"Id": 1, "Ids": [1], "ConsumableItemDesc": "7OZ5AE3uuUyqa0P-2W1ptg", "PlatformMask": -1, "CreatedAt": "2022-02-19T05:29:59.909Z", "CreatedAts": ["2022-02-19T05:29:59.909Z"], "Count": 675, "InitialCount": 675, "UnlockedLevel": 0, "IsActive": False, "IsTransferable": True, "Category": 4},
    {"Id": 2, "Ids": [2], "ConsumableItemDesc": "_jnjYGBcyEWY5Ub4OezXcA", "PlatformMask": -1, "CreatedAt": "2022-02-19T05:29:59.909Z", "CreatedAts": ["2022-02-19T05:29:59.909Z"], "Count": 675, "InitialCount": 675, "UnlockedLevel": 0, "IsActive": False, "IsTransferable": True, "Category": 4},
]

IMAGE_NAMES = {
    "Coach",
    "alt/Coach",
    "ca673ff19c054158a15ff00f0b844ba7",
    "cf863d3a013f4332abaac807324cb339.jpg",
    "b63bcfa3e16042f59e1b2ba40021b9b9.jpg",
    "57c0a08d2d074cd0ad499bb74cae197f.png",
    "Pc8fGEuX9EGqLlOfnNR3xw",
    "b59b03d3e2234af8a0ae5b9043440580",
    "MgAAM8tYCUq0wwOy9flG2w",
    "52cf6c3271894ecd95fb0c9b2d2209a7",
    "fc9a1acc47514b64a30d199d5ccdeca9",
    "6d5c494668784816bbc41d9b870e5003",
    "ffdca6ed8bd94631ac15e3e894acb6c6",
    "93a53ced93a04f658795a87f4a4aab85",
    "38e9d0d4eff94556a0b106508249dcf9",
    "51296f28105b48178708e389b6daf057",
    "3ab82779dff94d11920ebf38df249395",
    "45ad53aa002646d0ab3eb509b9f260ef",
    "d0df003353914adfaecdd23f428208b6",
    "51c6f5ac5e6f4777b573e7e43f8a85ea",
    "c5a72193d6904811b2d0195a6deb3125",
    "69fc525056014e39a435c4d2fdf2b887",
    "f9e112bb67fb430d979e5ad6c2c116d4",
    "3e8c2458f1e542ab8aa275e4083ee47a",
    "22eefa3219f046fd9e2090814650ede3",
    "4kQHuyDAzkeHe-OV4Qrdeg",
    "JejHE-bjhUmHU47aqVxMpA",
    "yMIjCkEusk6hfv08GqhOVg",
}

name_server_app = FastAPI()
api_app = FastAPI()
ws_app = FastAPI()
image_app = FastAPI()
dashboard_app = FastAPI()
recnet_app = FastAPI()
app = FastAPI()


@api_app.middleware("http")
async def log_requests(request: Request, call_next):
    path = request.url.path.lstrip("/")
    if request.url.query:
        path = f"{path}?{request.url.query}"
    log_line(f"API Requested: {path}" if path else "API Requested:")
    log_line(f"API Data: {await read_request_payload(request)}")
    response = await call_next(request)
    return response


@name_server_app.get("/")
async def nameserver_root() -> dict[str, str]:
    payload = {
        "API": f"https://{RENDER_URL}.onrender.com",
        "Notifications": f"wss://{RENDER_URL}.onrender.com/hub/v1",
        "Images": f"https://{RENDER_URL}.onrender.com",
    }
    log_api_response(payload)
    return payload


@name_server_app.get("/nameserver")
async def nameserver_alias() -> dict[str, str]:
    return await nameserver_root()


@name_server_app.get("/api")
async def api_root_alias() -> dict[str, str]:
    return await nameserver_root()


@name_server_app.get("/v2")
async def nameserver_v2() -> dict[str, str]:
    return await nameserver_root()


@api_app.get("/")
async def api_root() -> dict[str, str]:
    payload = {"service": "Rec Room 2018 Server", "status": "Ready"}
    log_api_response(payload)
    return payload


@api_app.get("/versioncheck/v3")
async def versioncheck() -> dict[str, Any]:
    if MAINTENANCE_ENDS_AT is not None:
        payload = {"VersionStatus": 1}
        log_api_response(payload)
        return payload
    valid = SETTINGS_MAP.get("ValidVersion", "true")
    payload = {"ValidVersion": valid.lower() == "true"}
    log_api_response(payload)
    return payload


@api_app.get("/api/versioncheck/v3")
async def versioncheck_api_alias() -> dict[str, Any]:
    return await versioncheck()


@api_app.post("/versioncheck/v3/set")
async def set_version_status(payload: dict[str, Any] = Body(default={})) -> dict[str, Any]:
    valid = payload.get("ValidVersion", True)
    SETTINGS_MAP["ValidVersion"] = "true" if valid else "false"
    log_api_response({"ValidVersion": valid})
    return {"ValidVersion": valid}


@api_app.post("/api/versioncheck/v3/set")
async def set_version_status_api_alias(payload: dict[str, Any] = Body(default={})) -> dict[str, Any]:
    return await set_version_status(payload)


@api_app.post("/servermaintenance/v1/set")
async def set_server_maintenance(payload: dict[str, Any] = Body(default={})) -> dict[str, Any]:
    global MAINTENANCE_ENDS_AT, MAINTENANCE_DISABLE_ACCOUNT_LOGIN, MAINTENANCE_EXPIRY_TASK
    try:
        starts_in_minutes = max(0, int(payload.get("StartsInMinutes", 0)))
    except (TypeError, ValueError):
        raise HTTPException(status_code=422, detail="StartsInMinutes must be a whole number")
    if MAINTENANCE_EXPIRY_TASK is not None:
        MAINTENANCE_EXPIRY_TASK.cancel()
        MAINTENANCE_EXPIRY_TASK = None
    maintenance = {"StartsInMinutes": starts_in_minutes}
    CONFIG_V2["ServerMaintenance"] = maintenance
    if starts_in_minutes == 0:
        MAINTENANCE_ENDS_AT = None
        MAINTENANCE_DISABLE_ACCOUNT_LOGIN = False
        delivered_to = 0
    else:
        MAINTENANCE_DISABLE_ACCOUNT_LOGIN = bool(payload.get("DisableAccountLogin", False))
        MAINTENANCE_ENDS_AT = datetime.now(timezone.utc) + timedelta(minutes=starts_in_minutes)
        MAINTENANCE_EXPIRY_TASK = asyncio.create_task(
            expire_server_maintenance(MAINTENANCE_ENDS_AT, starts_in_minutes)
        )
        delivered_to = await broadcast_hub_notification(25, maintenance)
    response = {
        **maintenance,
        "DisableAccountLogin": MAINTENANCE_DISABLE_ACCOUNT_LOGIN,
        "DeliveredTo": delivered_to,
    }
    log_api_response(response)
    return response


@api_app.post("/api/servermaintenance/v1/set")
async def set_server_maintenance_api_alias(payload: dict[str, Any] = Body(default={})) -> dict[str, Any]:
    return await set_server_maintenance(payload)


@api_app.get("/config/v2")
async def config_v2() -> dict[str, Any]:
    CONFIG_V2["ServerMaintenance"] = {"StartsInMinutes": maintenance_starts_in_minutes()}
    log_api_response(CONFIG_V2)
    return CONFIG_V2


@api_app.get("/api/config/v2")
async def config_v2_api_alias() -> dict[str, Any]:
    return await config_v2()


@api_app.get("/api/config/v2/")
async def config_v2_api_alias_slash() -> dict[str, Any]:
    return await config_v2()


@api_app.get("/gameconfigs/v1/all")
async def gameconfigs() -> list[dict[str, Any]]:
    log_api_response(GAMECONFIGS)
    return GAMECONFIGS


@api_app.get("/api/gameconfigs/v1/all")
async def gameconfigs_api_alias() -> list[dict[str, Any]]:
    return await gameconfigs()


@api_app.get("/api/gameconfigs/v1/all/")
async def gameconfigs_api_alias_slash() -> list[dict[str, Any]]:
    return await gameconfigs()


@api_app.api_route("/platformlogin/v1/getcachedlogins", methods=["GET", "POST"])
async def get_cached_logins(request: Request) -> list[dict[str, Any]]:
    if account_login_is_disabled():
        payload: list[dict[str, Any]] = []
        log_api_response(payload)
        return payload
    platform = request.query_params.get("Platform", "0")
    platform_id = request.query_params.get("PlatformId", "")
    payload = []
    for account in load_accounts():
        if platform_id and (
            str(account.get("platform", 0)) != str(platform)
            or str(account.get("platformId", "")) != str(platform_id)
        ):
            continue
        player = account_to_player(account)
        player["JuniorProfile"] = True
        payload.append(player)
    log_api_response(payload)
    return payload


@api_app.api_route("/api/platformlogin/v1/getcachedlogins", methods=["GET", "POST"])
async def get_cached_logins_api_alias(request: Request) -> list[dict[str, Any]]:
    return await get_cached_logins(request)


@api_app.post("/platformlogin/v1/logincached")
async def login_cached(payload: dict[str, Any] = Body(default={})) -> dict[str, Any]:
    if account_login_is_disabled():
        response = {"Error": "Server maintenance in progress"}
        log_api_response(response)
        return response
    account = find_account_by_id(payload.get("PlayerId", ""))
    if account is None:
        account = find_account_by_platform(payload.get("Platform", 0), payload.get("PlatformId", "76561199788433078"))
    if account is None:
        account = load_accounts()[0]
    if account.get("banned", False):
        response = {"Error": "Account is banned"}
        log_api_response(response)
        return response
    bind_login_token(payload.get("LoginLockToken"), account)
    response = build_login_response(account)
    log_api_response(response)
    return response


@api_app.api_route("/api/platformlogin/v1/logincached", methods=["GET", "POST"])
async def login_cached_api_alias(payload: dict[str, Any] = Body(default={})) -> dict[str, Any]:
    return await login_cached(payload)


@api_app.api_route("/api/platformlogin/v1/logincached/", methods=["GET", "POST"])
async def login_cached_api_alias_slash(payload: dict[str, Any] = Body(default={})) -> dict[str, Any]:
    return await login_cached(payload)


@api_app.post("/platformlogin/v1/loginaccount")
async def login_account(payload: dict[str, Any] = Body(default={})) -> dict[str, Any]:
    if account_login_is_disabled():
        response = {"Error": "Server maintenance in progress"}
        log_api_response(response)
        return response
    account = find_account_by_credentials(payload.get("Username", ""), payload.get("Password", ""))
    if account is None:
        response = {"Error": "Invalid username or password"}
        log_api_response(response)
        return response
    if account.get("banned", False):
        response = {"Error": "Account is banned"}
        log_api_response(response)
        return response
    bind_login_token(payload.get("LoginLockToken"), account)
    response = build_login_response(account)
    log_api_response(response)
    return response


@api_app.api_route("/api/platformlogin/v1/loginaccount", methods=["GET", "POST"])
async def login_account_api_alias(payload: dict[str, Any] = Body(default={})) -> dict[str, Any]:
    return await login_account(payload)


@api_app.api_route("/api/platformlogin/v1/loginaccount/", methods=["GET", "POST"])
async def login_account_api_alias_slash(payload: dict[str, Any] = Body(default={})) -> dict[str, Any]:
    return await login_account(payload)


@api_app.get("/PlayerReporting/v1/moderationBlockDetails")
async def moderation_block_details() -> dict[str, Any]:
    payload = {"ReportCategory": 0, "Duration": 0, "GameSessionId": 0, "Message": ""}
    log_api_response(payload)
    return payload


@api_app.get("/api/PlayerReporting/v1/moderationBlockDetails")
async def moderation_block_details_api_alias() -> dict[str, Any]:
    return await moderation_block_details()


@api_app.get("/config/v1/amplitude")
async def amplitude() -> dict[str, str]:
    payload = {"AmplitudeKey": "RebornKey"}
    log_api_response(payload)
    return payload


@api_app.get("/api/config/v1/amplitude")
async def amplitude_api_alias() -> dict[str, str]:
    return await amplitude()


@api_app.post("/presence/v1/setplayertype")
async def set_player_type() -> Response:
    log_api_response("")
    return Response(status_code=200)


@api_app.api_route("/api/presence/v1/setplayertype", methods=["GET", "POST"])
async def set_player_type_api_alias() -> Response:
    return await set_player_type()


@api_app.api_route("/api/presence/v1/setplayertype/", methods=["GET", "POST"])
async def set_player_type_api_alias_slash() -> Response:
    return await set_player_type()


@api_app.get("/messages/v2/get")
async def messages_get() -> list[Any]:
    payload: list[Any] = []
    log_api_response(payload)
    return payload


@api_app.get("/api/messages/v2/get")
async def messages_get_api_alias() -> list[Any]:
    return await messages_get()


@api_app.post("/sanitize/v1/isPure")
async def sanitize_is_pure(payload: dict[str, Any] = Body(default={})) -> dict[str, Any]:
    response = {"IsPure": True, "Value": str(payload.get("Value", ""))}
    log_api_response(response)
    return response


@api_app.post("/api/sanitize/v1/isPure")
async def sanitize_is_pure_api_alias(payload: dict[str, Any] = Body(default={})) -> dict[str, Any]:
    return await sanitize_is_pure(payload)


@api_app.post("//api/sanitize/v1/isPure")
async def sanitize_is_pure_api_double_slash(payload: dict[str, Any] = Body(default={})) -> dict[str, Any]:
    return await sanitize_is_pure(payload)


@api_app.api_route("//api/chat/v2/myChats", methods=["GET", "POST"])
async def my_chats() -> list[Any]:
    payload: list[Any] = []
    log_api_response(payload)
    return payload


@api_app.api_route("/api/chat/v2/myChats", methods=["GET", "POST"])
async def my_chats_single_slash() -> list[Any]:
    return await my_chats()


@api_app.api_route("/chat/v2/myChats", methods=["GET", "POST"])
async def my_chats_short() -> list[Any]:
    return await my_chats()


@api_app.get("/relationships/v2/get")
async def relationships_get(request: Request) -> list[Any]:
    account = resolve_active_account(request=request)
    payload = [
        relationship
        for relationship in load_relationships()
        if int(relationship.get("FromPlayerId", -1)) == int(account["id"])
        or int(relationship.get("ToPlayerId", -1)) == int(account["id"])
    ]
    log_api_response(payload)
    return payload


@api_app.get("/api/relationships/v2/get")
async def relationships_get_api_alias(request: Request) -> list[Any]:
    return await relationships_get(request)


@api_app.get("/relationships/v2/sendfriendrequest")
async def relationships_send_friend_request(request: Request, id: int | None = None) -> Response:
    account = resolve_active_account(request=request)
    if id is not None:
        add_friend_request(int(account["id"]), int(id))
    log_api_response("")
    return Response(status_code=200)


@api_app.get("/api/relationships/v2/sendfriendrequest")
async def relationships_send_friend_request_api_alias(request: Request, id: int | None = None) -> Response:
    return await relationships_send_friend_request(request, id)


@api_app.get("/playersubscriptions/v1/my")
async def player_subscriptions() -> list[Any]:
    payload: list[Any] = []
    log_api_response(payload)
    return payload


@api_app.get("/api/playersubscriptions/v1/my")
async def player_subscriptions_api_alias() -> list[Any]:
    return await player_subscriptions()


@api_app.post("/playersubscriptions/v1/subscribe/{user_id}")
async def player_subscriptions_subscribe(user_id: int) -> Response:
    log_api_response("")
    return Response(status_code=200)


@api_app.post("/api/playersubscriptions/v1/subscribe/{user_id}")
async def player_subscriptions_subscribe_api_alias(user_id: int) -> Response:
    return await player_subscriptions_subscribe(user_id)


@api_app.get("/images/v2/named")
async def images_named() -> list[dict[str, Any]]:
    payload = named_images_payload()
    log_api_response(payload)
    return payload


@api_app.get("/api/images/v2/named")
async def images_named_api_alias() -> list[dict[str, Any]]:
    return await images_named()


@api_app.get("/avatar/v2")
async def avatar_v2(request: Request, playerId: int | None = None) -> dict[str, Any]:
    account = find_account_by_id(playerId) if playerId is not None else resolve_active_account(request=request)
    payload = avatar_payload_for_player(account_to_player(account))
    log_api_response(payload)
    return payload


@api_app.get("/api/avatar/v2")
async def avatar_v2_api_alias(request: Request, playerId: int | None = None) -> dict[str, Any]:
    return await avatar_v2(request, playerId)


@api_app.post("/avatar/v2/set")
async def avatar_v2_set(payload: dict[str, Any] = Body(default={})) -> Response:
    account = resolve_active_account(payload)
    update_account_avatar(account["id"], payload)
    log_api_response("")
    return Response(status_code=200)


@api_app.post("/api/avatar/v2/set")
async def avatar_v2_set_api_alias(payload: dict[str, Any] = Body(default={})) -> Response:
    return await avatar_v2_set(payload)


@api_app.get("/settings/v2/")
async def settings_v2() -> list[dict[str, str]]:
    payload = current_settings()
    log_api_response(payload)
    return payload


@api_app.get("/api/settings/v2/")
async def settings_v2_api_alias() -> list[dict[str, str]]:
    return await settings_v2()


@api_app.post("/settings/v2/set")
async def settings_set(payload: dict[str, str] = Body(default={})) -> Response:
    key = payload.get("Key")
    value = payload.get("Value")
    if key and value is not None:
        SETTINGS_MAP[key] = value
        found = False
        for item in SETTINGS:
            if item["Key"] == key:
                item["Value"] = value
                found = True
                break
        if not found:
            SETTINGS.append({"Key": key, "Value": value})
    log_api_response("")
    return Response(status_code=200)


@api_app.api_route("/api/settings/v2/set", methods=["GET", "POST"])
async def settings_set_api_alias(payload: dict[str, str] = Body(default={})) -> Response:
    return await settings_set(payload)


@api_app.get("/avatar/v3/items")
async def avatar_items(request: Request) -> list[dict[str, Any]]:
    account = resolve_active_account(request=request)
    hidden_items = set(str(item) for item in account.get("hiddenAvatarItemDescs", []))
    payload = [item for item in AVATAR_ITEMS if str(item.get("AvatarItemDesc", "")) not in hidden_items]
    log_api_response(payload)
    return payload


@api_app.get("/api/avatar/v3/items")
async def avatar_items_api_alias(request: Request) -> list[dict[str, Any]]:
    return await avatar_items(request)


@api_app.get("/equipment/v1/getUnlocked")
async def equipment_unlocked() -> list[dict[str, Any]]:
    log_api_response(EQUIPMENT)
    return EQUIPMENT


@api_app.get("/api/equipment/v1/getUnlocked")
async def equipment_unlocked_api_alias() -> list[dict[str, Any]]:
    return await equipment_unlocked()


@api_app.get("/avatar/v3/saved")
async def avatar_saved() -> list[Any]:
    payload: list[Any] = []
    log_api_response(payload)
    return payload


@api_app.get("/api/avatar/v3/saved")
async def avatar_saved_api_alias() -> list[Any]:
    return await avatar_saved()


@api_app.get("/consumables/v1/getUnlocked")
async def consumables_unlocked() -> list[dict[str, Any]]:
    log_api_response(CONSUMABLES)
    return CONSUMABLES


@api_app.get("/api/consumables/v1/getUnlocked")
async def consumables_unlocked_api_alias() -> list[dict[str, Any]]:
    return await consumables_unlocked()


@api_app.get("/avatar/v2/gifts")
async def avatar_gifts() -> list[Any]:
    payload: list[Any] = []
    log_api_response(payload)
    return payload


@api_app.get("/api/avatar/v2/gifts")
async def avatar_gifts_api_alias() -> list[Any]:
    return await avatar_gifts()


@api_app.get("/storefronts/v3/giftdropstore/{store_id}")
async def giftdropstore(store_id: int) -> Response:
    log_api_response("")
    return Response(status_code=200)


@api_app.get("/api/storefronts/v3/giftdropstore/{store_id}")
async def giftdropstore_api_alias(store_id: int) -> Response:
    return await giftdropstore(store_id)


@api_app.get("/objectives/v1/myprogress")
async def objectives_progress() -> dict[str, Any]:
    payload = {
        "Objectives": [
            {"Index": 2, "Group": 0, "Progress": 0, "VisualProgress": 0, "IsCompleted": False, "IsRewarded": False, "HasClaimedReward": False},
            {"Index": 1, "Group": 0, "Progress": 0, "VisualProgress": 0, "IsCompleted": False, "IsRewarded": False, "HasClaimedReward": False},
            {"Index": 0, "Group": 0, "Progress": 0, "VisualProgress": 0, "IsCompleted": False, "IsRewarded": False, "HasClaimedReward": False},
        ],
        "ObjectiveGroups": [{"Group": 0, "IsCompleted": False, "ClearedAt": "2021-04-18T01:59:14.864Z"}],
    }
    log_api_response(payload)
    return payload


@api_app.get("/api/objectives/v1/myprogress")
async def objectives_progress_api_alias() -> dict[str, Any]:
    return await objectives_progress()


@api_app.get("/rooms/v2/myrooms")
async def my_rooms() -> list[dict[str, Any]]:
    payload = [entry.get("room", {}) for entry in load_rooms() if entry.get("room", {}).get("IsDormRoom")]
    log_api_response(payload)
    return payload


@api_app.get("/api/rooms/v2/myrooms")
async def my_rooms_api_alias() -> list[dict[str, Any]]:
    return await my_rooms()


@api_app.get("/rooms/v2/mySubscriptions")
async def my_room_subscriptions(skip: int = 0, take: int = 40) -> list[Any]:
    payload: list[Any] = []
    log_api_response(payload)
    return payload


@api_app.get("/api/rooms/v2/mySubscriptions")
async def my_room_subscriptions_api_alias(skip: int = 0, take: int = 40) -> list[Any]:
    return await my_room_subscriptions(skip, take)


@api_app.get("/rooms/v2/mybookmarkedrooms")
async def my_bookmarked_rooms() -> list[Any]:
    payload: list[Any] = []
    log_api_response(payload)
    return payload


@api_app.get("/api/rooms/v2/mybookmarkedrooms")
async def my_bookmarked_rooms_api_alias() -> list[Any]:
    return await my_bookmarked_rooms()


@api_app.get("/rooms/v2/search")
async def rooms_search(value: str = "") -> list[dict[str, Any]]:
    payload = search_rooms(value)
    log_api_response(payload)
    return payload


@api_app.get("/api/rooms/v2/search")
async def rooms_search_api_alias(value: str = "") -> list[dict[str, Any]]:
    return await rooms_search(value)


@api_app.post("/rooms/v1/clone")
async def rooms_clone(payload: dict[str, Any] = Body(default={})) -> dict[str, Any]:
    source_room_id = int(payload.get("RoomId", 1))
    new_name = str(payload.get("Name") or f"Room{source_room_id}Clone").strip()
    target_room_id = payload.get("TargetRoomId")
    cloned_entry = clone_room_entry(source_room_id, new_name, int(target_room_id) if target_room_id is not None else None)
    response = dict(cloned_entry.get("room", {}))
    log_api_response(response)
    return response


@api_app.post("/api/rooms/v1/clone")
async def rooms_clone_api_alias(payload: dict[str, Any] = Body(default={})) -> dict[str, Any]:
    return await rooms_clone(payload)


@api_app.get("/playerevents/v1/all")
async def player_events() -> dict[str, list[Any]]:
    payload = {"Created": [], "Responses": []}
    log_api_response(payload)
    return payload


@api_app.get("/api/playerevents/v1/all")
async def player_events_api_alias() -> dict[str, list[Any]]:
    return await player_events()


@api_app.api_route("/playerevents/{path:path}", methods=["GET", "POST", "PUT", "DELETE"])
async def playerevents_catch_all(path: str) -> Response:
    log_api_response(f"playerevents catch-all: {path}")
    return Response(status_code=200)


@api_app.api_route("/api/playerevents/{path:path}", methods=["GET", "POST", "PUT", "DELETE"])
async def playerevents_catch_all_api_alias(path: str) -> Response:
    return await playerevents_catch_all(path)


@api_app.get("/checklist/v1/current")
async def checklist_current() -> list[dict[str, int]]:
    payload = [
        {"Order": 0, "Objective": 400, "Count": 3, "CreditAmount": 200},
        {"Order": 1, "Objective": 1003, "Count": 40, "CreditAmount": 200},
        {"Order": 2, "Objective": 603, "Count": 50, "CreditAmount": 500},
        {"Order": 3, "Objective": 802, "Count": 10, "CreditAmount": 100},
        {"Order": 4, "Objective": 38, "Count": 1, "CreditAmount": 500},
        {"Order": 5, "Objective": 502, "Count": 300, "CreditAmount": 150},
        {"Order": 6, "Objective": 35, "Count": 20, "CreditAmount": 100},
    ]
    log_api_response(payload)
    return payload


@api_app.get("/api/checklist/v1/current")
async def checklist_current_api_alias() -> list[dict[str, int]]:
    return await checklist_current()


@api_app.get("/challenge/v1/getCurrent")
async def challenge_current() -> dict[str, Any]:
    message = {
        "ChallengeMapId": 0,
        "StartAt": "2021-12-27T21:27:38.188Z",
        "EndAt": "2025-12-27T21:27:38.188Z",
        "ServerTime": "2023-12-27T21:27:38.188Z",
        "Challenges": [],
        "Gifts": [{"GiftDropId": 1, "AvatarItemDesc": "", "Xp": 2, "Level": 0, "EquipmentPrefabName": "[WaterBottle]"}],
        "ChallengeThemeString": "Version: 2018",
    }
    payload = {"Success": True, "Message": json.dumps(message, separators=(",", ":"))}
    log_api_response(payload)
    return payload


@api_app.get("/api/challenge/v1/getCurrent")
async def challenge_current_api_alias() -> dict[str, Any]:
    return await challenge_current()


@api_app.post("/gamesessions/v3/joinroom")
async def join_room(payload: dict[str, Any] = Body(default={})) -> dict[str, Any]:
    if maintenance_is_active():
        response = {"Result": 1, "Error": "Server maintenance in progress"}
        log_api_response(response)
        return response
    room_name = payload.get("RoomName", "DormRoom")
    entry = find_room_entry(room_name=room_name) or find_room_entry(room_id=1)
    room = entry.get("room", ROOM)
    scene = entry.get("scene")
    # UGC rooms have null scenes - they download scene data separately
    # Use SCENE as fallback for UGC rooms
    if scene is None:
        scene = SCENE
    game_session = {
        "GameSessionId": 20182,
        "PhotonRegionId": "us",
        "PhotonRoomId": "FireRec1" if room.get("RoomId") == 2 else "FireRec2",
        "Name": room.get("Name", "DormRoom"),
        "RoomId": room.get("RoomId", 1),
        "RoomSceneId": scene.get("RoomSceneId", 1),
        "RoomSceneLocationId": scene.get("RoomSceneLocationId", SCENE["RoomSceneLocationId"]),
        "IsSandbox": scene.get("IsSandbox", True),
        "DataBlobName": scene.get("DataBlobName", ""),
        "Private": bool(room.get("IsDormRoom", False)),
        "GameInProgress": False,
        "MaxCapacity": scene.get("MaxPlayers", 20),
        "IsFull": False,
    }
    active_account = resolve_active_account(payload)
    set_player_session(int(active_account["id"]), game_session)
    payload = {
        "Result": 0,
        "GameSession": game_session,
        "RoomDetails": {
            "Room": room,
            "Scenes": [scene] if scene else [],
            "CoOwners": [],
            "InvitedCoOwners": [],
            "Moderators": [],
            "InvitedModerators": [],
            "Hosts": [],
            "InvitedHosts": [],
            "CheerCount": 1,
            "FavoriteCount": 1,
            "VisitCount": 1,
            "Tags": [{"Tag": "recroomoriginal", "Type": 2}],
        },
    }
    log_api_response(payload)
    return payload


@api_app.api_route("/api/gamesessions/v3/joinroom", methods=["GET", "POST"])
async def join_room_api_alias(payload: dict[str, Any] = Body(default={})) -> dict[str, Any]:
    return await join_room(payload)


@api_app.get("/storefronts/v1/balanceAddType/{balance_type}/{amount}")
async def balance_add_type(balance_type: int, amount: int) -> Response:
    log_api_response("")
    return Response(status_code=200)


@api_app.get("/api/storefronts/v1/balanceAddType/{balance_type}/{amount}")
async def balance_add_type_api_alias(balance_type: int, amount: int) -> Response:
    return await balance_add_type(balance_type, amount)


@api_app.get("/rooms/v1/featuredRoomGroup")
async def featured_room_group() -> dict[str, Any]:
    log_api_response(FEATURED_ROOMS)
    return FEATURED_ROOMS


@api_app.get("/api/rooms/v1/featuredRoomGroup")
async def featured_room_group_api_alias() -> dict[str, Any]:
    return await featured_room_group()


@api_app.post("/gamesessions/v2/reportjoinresult")
async def report_join_result() -> Response:
    log_api_response("")
    return Response(status_code=200)


@api_app.post("/api/gamesessions/v2/reportjoinresult")
async def report_join_result_api_alias() -> Response:
    return await report_join_result()


@api_app.post("/gamesessions/v3/joininstance")
async def join_instance(payload: dict[str, Any] = Body(default={})) -> dict[str, Any]:
    active_account = resolve_active_account(payload)
    game_session = session_payload_for_account(active_account)
    game_session["GameSessionId"] = payload.get("GameSessionId", game_session.get("GameSessionId", 20182))
    entry = find_room_entry(room_id=int(game_session.get("RoomId", ROOM["RoomId"]))) or {"room": ROOM, "scene": SCENE}
    payload = {
        "Result": 0,
        "GameSession": game_session,
        "RoomDetails": {
            "Room": entry.get("room", ROOM),
            "Scenes": [entry.get("scene") or SCENE],
            "CoOwners": [],
            "InvitedCoOwners": [],
            "Moderators": [],
            "InvitedModerators": [],
            "Hosts": [],
            "InvitedHosts": [],
            "CheerCount": 1,
            "FavoriteCount": 1,
            "VisitCount": 1,
            "Tags": [{"Tag": "recroomoriginal", "Type": 2}],
        },
    }
    log_api_response(payload)
    return payload


@api_app.post("/api/gamesessions/v3/joininstance")
async def join_instance_api_alias(payload: dict[str, Any] = Body(default={})) -> dict[str, Any]:
    return await join_instance(payload)


@api_app.post("/gamesessions/v3/joinevent")
async def join_event(payload: dict[str, Any] = Body(default={})) -> dict[str, Any]:
    active_account = resolve_active_account(payload)
    game_session = session_payload_for_account(active_account)
    entry = find_room_entry(room_id=int(game_session.get("RoomId", ROOM["RoomId"]))) or {"room": ROOM, "scene": SCENE}
    payload = {
        "Result": 0,
        "GameSession": game_session,
        "RoomDetails": {
            "Room": entry.get("room", ROOM),
            "Scenes": [entry.get("scene") or SCENE],
            "CoOwners": [],
            "InvitedCoOwners": [],
            "Moderators": [],
            "InvitedModerators": [],
            "Hosts": [],
            "InvitedHosts": [],
            "CheerCount": 1,
            "FavoriteCount": 1,
            "VisitCount": 1,
            "Tags": [{"Tag": "recroomoriginal", "Type": 2}],
        },
    }
    log_api_response(payload)
    return payload


@api_app.post("/api/gamesessions/v3/joinevent")
async def join_event_api_alias(payload: dict[str, Any] = Body(default={})) -> dict[str, Any]:
    return await join_event(payload)


@api_app.post("/gamesessions/v3/joinplayer")
async def join_player(payload: dict[str, Any] = Body(default={})) -> dict[str, Any]:
    target_account = find_account_by_id(payload.get("PlayerId")) or resolve_active_account(payload)
    game_session = session_payload_for_account(target_account)
    entry = find_room_entry(room_id=int(game_session.get("RoomId", ROOM["RoomId"]))) or {"room": ROOM, "scene": SCENE}
    payload = {
        "Result": 0,
        "GameSession": game_session,
        "RoomDetails": {
            "Room": entry.get("room", ROOM),
            "Scenes": [entry.get("scene") or SCENE],
            "CoOwners": [],
            "InvitedCoOwners": [],
            "Moderators": [],
            "InvitedModerators": [],
            "Hosts": [],
            "InvitedHosts": [],
            "CheerCount": 1,
            "FavoriteCount": 1,
            "VisitCount": 1,
            "Tags": [{"Tag": "recroomoriginal", "Type": 2}],
        },
    }
    log_api_response(payload)
    return payload


@api_app.post("/api/gamesessions/v3/joinplayer")
async def join_player_api_alias(payload: dict[str, Any] = Body(default={})) -> dict[str, Any]:
    return await join_player(payload)


@api_app.post("/gamesessions/v2/setinprogress")
async def set_in_progress(payload: dict[str, Any] = Body(default={})) -> Response:
    log_api_response("")
    return Response(status_code=200)


@api_app.post("/api/gamesessions/v2/setinprogress")
async def set_in_progress_api_alias(payload: dict[str, Any] = Body(default={})) -> Response:
    return await set_in_progress(payload)


@api_app.get("/storefronts/v3/balance/{balance_id}")
async def balance(balance_id: int) -> Response:
    log_api_response("")
    return Response(status_code=200)


@api_app.get("/api/storefronts/v3/balance/{balance_id}")
async def balance_api_alias(balance_id: int) -> Response:
    return await balance(balance_id)


@api_app.get("/images/v1/slideshow")
async def images_slideshow() -> dict[str, Any]:
    payload = {
        "Images": [
            {"SavedImageId": 1, "ImageName": "1c603274267b4f45858f7506ab48b4fa", "Username": "Origami", "RoomName": "forsophie"},
            {"SavedImageId": 2, "ImageName": "dRoGHY4JvU-t1L51gbdhqQ", "Username": "Sublime", "RoomName": "TheOfficePvP"},
            {"SavedImageId": 3, "ImageName": "v9_qfp0bH0a3KL0xGz4Zsg", "Username": "Jbluna", "RoomName": "USSCoach"},
            {"SavedImageId": 4, "ImageName": "390f59bb8cfe4254bd7c83f5c7f01550", "Username": "Coach", "RoomName": "Lounge"},
        ],
        "ValidTill": "2025-06-09T19:06:44.927Z",
    }
    log_api_response(payload)
    return payload


@api_app.get("/api/images/v1/slideshow")
async def images_slideshow_api_alias() -> dict[str, Any]:
    return await images_slideshow()


@api_app.post("/objectives/v1/cleargroup")
async def objectives_clear_group() -> Response:
    log_api_response("")
    return Response(status_code=200)


@api_app.post("/api/objectives/v1/cleargroup")
async def objectives_clear_group_api_alias() -> Response:
    return await objectives_clear_group()


@api_app.post("/platformlogin/v1/logout")
async def platform_logout(request: Request) -> Response:
    unbind_login_token(request.query_params.get("LoginLockToken"))
    log_api_response("")
    return Response(status_code=200)


@api_app.api_route("/api/platformlogin/v1/logout", methods=["GET", "POST"])
async def platform_logout_api_alias(request: Request) -> Response:
    return await platform_logout(request)


@api_app.api_route("/api/platformlogin/v1/logout/", methods=["GET", "POST"])
async def platform_logout_api_alias_slash(request: Request) -> Response:
    return await platform_logout(request)


@api_app.post("/players/v1/objectives")
async def player_objectives() -> list[Any]:
    payload: list[Any] = []
    log_api_response(payload)
    return payload


@api_app.post("/players/v1/bio")
async def player_bio(payload: dict[str, Any] = Body(default={})) -> Response:
    account = resolve_active_account(payload)
    update_account_profile(account["id"], {"bio": payload.get("Bio", "")})
    log_api_response("")
    return Response(status_code=200)


@api_app.post("/api/players/v1/bio")
async def player_bio_api_alias(payload: dict[str, Any] = Body(default={})) -> Response:
    return await player_bio(payload)


@api_app.get("/players/v2/search")
async def players_search(name: str = "") -> list[dict[str, Any]]:
    needle = name.strip().lower()
    payload = []
    for account in load_accounts():
        username = str(account.get("username", ""))
        display_name = str(account.get("displayName") or username)
        if needle and needle not in username.lower() and needle not in display_name.lower():
            continue
        payload.append(account_to_player(account))
    log_api_response(payload)
    return payload


@api_app.get("/api/players/v2/search")
async def players_search_api_alias(name: str = "") -> list[dict[str, Any]]:
    return await players_search(name)


@api_app.post("/api/players/v1/objectives")
async def player_objectives_api_alias() -> list[Any]:
    return await player_objectives()


@api_app.api_route("/players/v1/list", methods=["GET", "POST"])
async def players_list_v1(payload: list[int] = Body(default=[])) -> list[dict[str, Any]]:
    payload = resolve_players(payload)
    log_api_response(payload)
    return payload


@api_app.api_route("/api/players/v1/list", methods=["GET", "POST"])
async def players_list_v1_api_alias(payload: list[int] = Body(default=[])) -> list[dict[str, Any]]:
    return await players_list_v1(payload)


@api_app.api_route("/players/v2/listByPlatformId", methods=["GET", "POST"])
async def players_list_by_platform_id(request: Request, payload: dict[str, Any] = Body(default={}), platformId: str | None = None) -> list[dict[str, Any]]:
    platform = int(payload.get("Platform", 0)) if isinstance(payload, dict) else 0
    platform_ids = payload.get("PlatformIds", []) if isinstance(payload, dict) else []
    if platformId:
        platform_ids = [platformId]
    resolved_players: list[dict[str, Any]] = []
    for candidate in platform_ids:
        account = find_account_by_platform(platform, candidate)
        if account is not None:
            resolved_players.append(account_to_player(account))
    payload = resolved_players
    log_api_response(payload)
    return payload


@api_app.api_route("/api/players/v2/listByPlatformId", methods=["GET", "POST"])
async def players_list_by_platform_id_api_alias(request: Request, payload: dict[str, Any] = Body(default={}), platformId: str | None = None) -> list[dict[str, Any]]:
    return await players_list_by_platform_id(request, payload, platformId)


@api_app.api_route("/presence/v2/list", methods=["GET", "POST"])
async def presence_list_v2(payload: list[int] = Body(default=[])) -> list[dict[str, Any]]:
    payload = [presence_payload_for_player(player) for player in resolve_players(payload)]
    log_api_response(payload)
    return payload


@api_app.api_route("/api/presence/v2/list", methods=["GET", "POST"])
async def presence_list_v2_api_alias(payload: list[int] = Body(default=[])) -> list[dict[str, Any]]:
    return await presence_list_v2(payload)


@api_app.api_route("/versioncheck/v1", methods=["GET", "POST"])
async def versioncheck_v1() -> dict[str, bool]:
    payload = {"ValidVersion": True}
    log_api_response(payload)
    return payload


@api_app.api_route("/api/versioncheck/v1", methods=["GET", "POST"])
async def versioncheck_v1_api_alias() -> dict[str, bool]:
    return await versioncheck_v1()


@api_app.api_route("/activities/charades/v1/words", methods=["GET", "POST"])
async def charades_words() -> list[str]:
    payload = [
        "apple",
        "astronaut",
        "backpack",
        "banana",
        "castle",
        "dragon",
        "guitar",
        "pirate",
        "robot",
        "telescope",
    ]
    log_api_response(payload)
    return payload


@api_app.api_route("/api/activities/charades/v1/words", methods=["GET", "POST"])
async def charades_words_api_alias() -> list[str]:
    return await charades_words()


@api_app.post("/storefronts/v1/objectives")
async def storefront_objectives() -> Response:
    log_api_response("")
    return Response(status_code=200)


@api_app.post("/api/storefronts/v1/objectives")
async def storefront_objectives_api_alias() -> Response:
    return await storefront_objectives()


@api_app.post("/presence/v3/heartbeat")
async def heartbeat(payload: dict[str, Any] = Body(default={})) -> dict[str, Any]:
    player = account_to_player(resolve_active_account(payload))
    payload = {
        "Id": 4,
        "Msg": {
            "Error": "",
            "PlayerId": player["Id"],
            "IsOnline": True,
            "PlayerType": 2,
            "StatusVisibility": 0,
            "GameSession": presence_payload_for_player(player)["GameSession"],
        },
    }
    log_api_response(payload)
    return payload


@api_app.post("/api/presence/v3/heartbeat")
async def heartbeat_api_alias(payload: dict[str, Any] = Body(default={})) -> dict[str, Any]:
    return await heartbeat(payload)


@api_app.post("/PlayerCheer/v1/create")
async def player_cheer_create() -> Response:
    log_api_response("")
    return Response(status_code=200)


@api_app.post("/api/PlayerCheer/v1/create")
async def player_cheer_create_api_alias() -> Response:
    return await player_cheer_create()


@api_app.api_route("/rooms/v1/filters", methods=["GET", "POST"])
async def room_filters() -> list[Any]:
    payload: list[Any] = []
    log_api_response(payload)
    return payload


@api_app.api_route("/api/rooms/v1/filters", methods=["GET", "POST"])
async def room_filters_api_alias() -> list[Any]:
    return await room_filters()


@api_app.get("/rooms/v1/hot")
async def hot_rooms(tags: str = "") -> list[dict[str, Any]]:
    normalized_tags = tags.strip()
    if normalized_tags:
        payload = [
            {
                "RoomId": entry.get("room", {}).get("RoomId"),
                "Name": entry.get("room", {}).get("Name"),
                "ImageName": entry.get("room", {}).get("ImageName"),
                "PlayerCount": 1 if entry.get("room", {}).get("IsDormRoom") else 4,
            }
            for entry in load_rooms()
            if normalized_tags in entry.get("hotTags", [])
        ]
    else:
        payload = HOT_ROOMS.get(normalized_tags, [])
    log_api_response(payload)
    return payload


@api_app.get("/api/rooms/v1/hot")
async def hot_rooms_api_alias(tags: str = "") -> list[dict[str, Any]]:
    return await hot_rooms(tags)


@api_app.post("/consumables/v1/consume")
async def consumables_consume() -> Response:
    log_api_response("")
    return Response(status_code=200)


@api_app.post("/api/consumables/v1/consume")
async def consumables_consume_api_alias() -> Response:
    return await consumables_consume()


@api_app.get("/rooms/v2/baserooms")
async def rooms_base_rooms() -> list[dict[str, Any]]:
    payload = [entry.get("room", {}) for entry in load_rooms()]
    log_api_response(payload)
    return payload


@api_app.get("/api/rooms/v2/baserooms")
async def rooms_base_rooms_api_alias() -> list[dict[str, Any]]:
    return await rooms_base_rooms()


@api_app.get("/rooms/v2/personaldetails/{room_id}")
async def room_personal_details(room_id: int) -> dict[str, Any]:
    payload = {
        "RoomId": room_id,
        "IsFavorite": False,
        "IsSubscribed": False,
        "NotificationPreference": 0,
        "Role": 0,
    }
    log_api_response(payload)
    return payload


@api_app.get("/api/rooms/v2/personaldetails/{room_id}")
async def room_personal_details_api_alias(room_id: int) -> dict[str, Any]:
    return await room_personal_details(room_id)


@api_app.post("/rooms/v2/modify")
async def room_modify(request: Request, payload: dict[str, Any] = Body(default={})) -> Response:
    room_id_raw = payload.get("RoomId")
    try:
        room_id = int(room_id_raw)
    except (TypeError, ValueError):
        raise HTTPException(status_code=422, detail="RoomId is required")

    active_account = resolve_active_account(payload, request)
    active_username = str(active_account.get("username", "")).strip().lower()
    if room_id == 2 and active_username != "coach":
        raise HTTPException(status_code=403, detail="Only Coach can edit RecCenter")

    accessibility = payload.get("Accessibility")
    accessibility_map = {"Private": 0, "Public": 1, "Unlisted": 2}
    if isinstance(accessibility, str):
        accessibility = accessibility_map.get(accessibility, accessibility)
    if accessibility is not None:
        try:
            accessibility = int(accessibility)
        except (TypeError, ValueError):
            raise HTTPException(status_code=422, detail="Accessibility must be Private, Public, Unlisted, or an integer")

    updates: dict[str, Any] = {}
    field_map = {
        "Name": str,
        "Description": str,
        "ShouldAllowCloning": bool,
        "SupportsScreens": bool,
        "SupportsWalkVR": bool,
        "SupportsTeleportVR": bool,
    }
    for field_name, field_type in field_map.items():
        if field_name not in payload:
            continue
        value = payload.get(field_name)
        if field_type is bool:
            updates[field_name] = bool(value)
        elif value is not None:
            updates[field_name] = field_type(value)
    if accessibility is not None:
        updates["Accessibility"] = accessibility

    rooms = load_rooms()
    for entry in rooms:
        room = entry.get("room", {})
        if room.get("RoomId") == room_id:
            if "Name" in updates:
                room["Name"] = updates["Name"]
            if "Description" in updates:
                room["Description"] = updates["Description"]
            if "Accessibility" in updates:
                room["Accessibility"] = updates["Accessibility"]
            if "ShouldAllowCloning" in updates:
                room["CloningAllowed"] = updates["ShouldAllowCloning"]
            if "SupportsScreens" in updates:
                room["SupportsScreens"] = updates["SupportsScreens"]
            if "SupportsWalkVR" in updates:
                room["SupportsWalkVR"] = updates["SupportsWalkVR"]
            if "SupportsTeleportVR" in updates:
                room["SupportsTeleportVR"] = updates["SupportsTeleportVR"]
            save_rooms(rooms)
            log_api_response(f"Room {room_id} modified: {room}")
            return Response(status_code=200)
    
    log_api_response(f"Room {room_id} not found")
    raise HTTPException(status_code=404, detail="Room not found")


@api_app.post("/api/rooms/v2/modify")
async def room_modify_api_alias(request: Request, payload: dict[str, Any] = Body(default={})) -> Response:
    return await room_modify(payload, request)


@api_app.post("/rooms/v2/saveData/{room_scene_id}")
async def room_save_data(room_scene_id: int, request: Request) -> dict[str, Any]:
    form = await request.form()
    upload = form.get("data")
    if not isinstance(upload, UploadFile):
        raise HTTPException(status_code=422, detail="data upload is required")

    img_list_raw = str(form.get("imgList") or "")
    data_blob_list_raw = str(form.get("dataBlobList") or "")
    try:
        room_image_list = list(json.loads(img_list_raw).get("roomImageList", [])) if img_list_raw else []
        data_blob_list = list(json.loads(data_blob_list_raw).get("dataBlobList", [])) if data_blob_list_raw else []
    except (AttributeError, TypeError, json.JSONDecodeError):
        raise HTTPException(status_code=422, detail="imgList and dataBlobList must be valid JSON objects")

    payload_bytes = await upload.read()
    if not payload_bytes:
        raise HTTPException(status_code=422, detail="data upload cannot be empty")

    rooms = load_rooms()
    target_entry = None
    for entry in rooms:
        scene = entry.get("scene") or {}
        if int(scene.get("RoomSceneId", -1)) == room_scene_id:
            target_entry = entry
            break
    if target_entry is None:
        raise HTTPException(status_code=404, detail="Room scene not found")

    ROOM_SAVE_DATA_DIR.mkdir(parents=True, exist_ok=True)
    blob_name = f"scene_{room_scene_id}_{uuid.uuid4().hex}.roomdata"
    room_save_data_path(blob_name).write_bytes(payload_bytes)
    metadata = {
        "RoomSceneId": room_scene_id,
        "BlobName": blob_name,
        "SavedAt": datetime.now(timezone.utc).isoformat(),
        "RoomImageList": room_image_list,
        "DataBlobList": data_blob_list,
        "Size": len(payload_bytes),
    }
    room_save_metadata_path(room_scene_id).write_text(json.dumps(metadata, indent=2), encoding="utf-8")

    scene = target_entry.get("scene")
    if scene is None:
        scene = {"RoomSceneId": room_scene_id}
        target_entry["scene"] = scene
    scene["DataBlobName"] = blob_name
    scene["DataModifiedAt"] = metadata["SavedAt"]
    save_rooms(rooms)

    response = {"Success": True, "DataBlobName": blob_name}
    log_api_response(response)
    return response


@api_app.post("/api/rooms/v2/saveData/{room_scene_id}")
async def room_save_data_api_alias(room_scene_id: int, request: Request) -> dict[str, Any]:
    return await room_save_data(room_scene_id, request)


@api_app.post("/playerevents/v2")
async def create_player_event(payload: dict[str, Any] = Body(default={})) -> dict[str, Any]:
    room_id = payload.get("RoomId")
    name = payload.get("Name")
    description = payload.get("Description")
    start_time = payload.get("StartTime")
    end_time = payload.get("EndTime")
    accessibility = payload.get("Accessibility")
    
    # Map accessibility string to enum value
    accessibility_map = {"Private": 0, "Public": 1, "Unlisted": 2}
    accessibility_int = accessibility_map.get(accessibility, 1) if isinstance(accessibility, str) else accessibility
    
    # Create event response (simplified)
    event_id = 1001  # Simulated event ID
    payload = {
        "Result": 0,  # Success
        "Event": {
            "EventId": event_id,
            "RoomId": room_id,
            "Name": name,
            "Description": description,
            "StartTime": start_time,
            "EndTime": end_time,
            "Accessibility": accessibility_int,
            "CreatorPlayerId": 8703348,
            "AttendeeCount": 0,
            "IsPrivate": accessibility_int == 0,
        }
    }
    log_api_response(payload)
    return payload


@api_app.post("/api/playerevents/v2")
async def create_player_event_api_alias(payload: dict[str, Any] = Body(default={})) -> dict[str, Any]:
    return await create_player_event(payload)


RECNET_HTML_DIR.mkdir(parents=True, exist_ok=True)


@recnet_app.get("/")
async def recnet_home() -> FileResponse:
    return FileResponse(RECNET_HTML_DIR / "recnet home.html")


@recnet_app.get("/room/browse")
async def room_browse() -> FileResponse:
    return FileResponse(RECNET_HTML_DIR / "recnet room browse (point to our own DB).html")


@recnet_app.get("/recnet/{path:path}")
async def serve_recnet_html(path: str) -> FileResponse:
    if not path:
        path = "recnet home.html"
    file_path = RECNET_HTML_DIR / path
    if file_path.exists():
        return FileResponse(file_path)
    raise HTTPException(status_code=404, detail="File not found")


@api_app.get("/rooms/v4/details/{room_id}")
async def room_details_v4(room_id: int) -> dict[str, Any]:
    entry = find_room_entry(room_id=room_id)
    if entry is None:
        raise HTTPException(status_code=404, detail="Room not found")
    room = entry.get("room", ROOM)
    scene = entry.get("scene")
    payload = {
        "RoomId": room_id,
        "Name": room.get("Name", "Unknown"),
        "Description": room.get("Description", ""),
        "CreatorPlayerId": room.get("CreatorPlayerId", 0),
        "ImageName": room.get("ImageName", ""),
        "State": room.get("State", 0),
        "Accessibility": room.get("Accessibility", 1),
        "SupportsLevelVoting": room.get("SupportsLevelVoting", False),
        "IsAGRoom": room.get("IsAGRoom", True),
        "IsDormRoom": room.get("IsDormRoom", False),
        "CloningAllowed": room.get("CloningAllowed", False),
        "SupportsScreens": room.get("SupportsScreens", True),
        "SupportsWalkVR": room.get("SupportsWalkVR", True),
        "SupportsTeleportVR": room.get("SupportsTeleportVR", True),
        "AllowsJuniors": room.get("AllowsJuniors", True),
        "RoomWarningMask": room.get("RoomWarningMask", 0),
        "CustomRoomWarning": room.get("CustomRoomWarning"),
        "DisableMicAutoMute": room.get("DisableMicAutoMute", True),
        "Scenes": [scene] if scene else [],
        "Tags": entry.get("tags", []),
        "HotTags": entry.get("hotTags", []),
        "IsFavorite": False,
        "IsSubscribed": False,
        "NotificationPreference": 0,
        "Role": 0,
    }
    log_api_response(payload)
    return payload


@api_app.get("/api/rooms/v4/details/{room_id}")
async def room_details_v4_api_alias(room_id: int) -> dict[str, Any]:
    return await room_details_v4(room_id)


@api_app.get("/rooms/v2/instancedetails/{room_id}")
async def room_instance_details(room_id: int) -> dict[str, Any]:
    payload = {
        "RoomId": room_id,
        "GameSessionId": 20182,
        "PlayerCount": 1,
        "MaxCapacity": 20,
        "IsPrivate": room_id == 1,
        "IsFull": False,
    }
    log_api_response(payload)
    return payload


@api_app.get("/api/rooms/v2/instancedetails/{room_id}")
async def room_instance_details_api_alias(room_id: int) -> dict[str, Any]:
    return await room_instance_details(room_id)

@api_app.api_route("/presence/v1", methods=["GET", "POST"])
async def presence_root() -> list[Any]:
    payload: list[Any] = []
    log_api_response(payload)
    return payload


@api_app.api_route("/notifications/v2", methods=["GET", "POST"])
async def notifications_root() -> list[Any]:
    payload = load_notifications()
    log_api_response(payload)
    return payload


@api_app.api_route("/api/notifications/v2", methods=["GET", "POST"])
async def notifications_root_api_alias() -> list[Any]:
    return await notifications_root()


@api_app.post("/notifications/v2/send")
async def notifications_send(payload: dict[str, Any] = Body(default={})) -> dict[str, Any]:
    record = build_notification_payload(
        str(payload.get("title", "New notification!")),
        str(payload.get("body", "Check your watch...")),
        int(payload.get("action", 0)),
    )
    records = load_notifications()
    records.append(record)
    save_notifications(records)
    await broadcast_notification(record)
    log_api_response(record)
    return record


@api_app.post("/api/notifications/v2/send")
async def notifications_send_api_alias(payload: dict[str, Any] = Body(default={})) -> dict[str, Any]:
    return await notifications_send(payload)


@api_app.api_route("/matchmaking/v1", methods=["GET", "POST"])
async def matchmaking_root() -> list[Any]:
    payload: list[Any] = []
    log_api_response(payload)
    return payload


@api_app.api_route("/rooms/v1/search", methods=["GET", "POST"])
async def room_search() -> list[Any]:
    payload: list[Any] = []
    log_api_response(payload)
    return payload


@api_app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH"])
async def fallback_api(path: str, request: Request) -> JSONResponse:
    log_line(f"Unhandled API route: {request.method} /{path}?{request.url.query}")
    payload = {"path": path, "handled": False}
    log_api_response(payload)
    return JSONResponse(payload, status_code=404)


@ws_app.websocket("/")
async def websocket_root(websocket: WebSocket):
    await websocket.accept()
    log_line("[WebSocket.cs] hub requested.")
    log_websocket_event("/", "connected")
    await send_blank_ws_json(websocket)
    try:
        while True:
            message = await websocket.receive()
            if message.get("type") == "websocket.disconnect":
                log_websocket_event("/", "disconnected", {"code": message.get("code")})
                break
            if "text" in message:
                log_websocket_event("/", "received-text", message["text"])
                await send_blank_ws_json(websocket)
            elif "bytes" in message:
                log_websocket_event("/", "received-bytes", f"<bytes:{len(message['bytes'])}>")
                await send_blank_ws_json(websocket)
    except WebSocketDisconnect:
        log_websocket_event("/", "disconnected")
        return


@ws_app.websocket("/api/notification/v2")
async def websocket_notifications(websocket: WebSocket):
    await websocket.accept()
    log_line("LateWebSocket.cs Notif Requested.")
    log_websocket_event("/api/notification/v2", "connected")
    notification_clients.add(websocket)
    for record in load_notifications():
        await websocket.send_text(json.dumps(record, separators=(",", ":")))
    try:
        while True:
            message = await websocket.receive()
            if message.get("type") == "websocket.disconnect":
                log_websocket_event("/api/notification/v2", "disconnected", {"code": message.get("code")})
                notification_clients.discard(websocket)
                break
            if "text" in message:
                log_websocket_event("/api/notification/v2", "received-text", message["text"])
                await send_blank_ws_json(websocket)
            elif "bytes" in message:
                log_websocket_event("/api/notification/v2", "received-bytes", f"<bytes:{len(message['bytes'])}>")
                await send_blank_ws_json(websocket)
    except WebSocketDisconnect:
        notification_clients.discard(websocket)
        log_websocket_event("/api/notification/v2", "disconnected")
        return


@dashboard_app.get("/")
async def dashboard_index() -> Response:
    return Response(NOTIF_DASHBOARD_HTML.read_text(encoding="utf-8"), media_type="text/html")


@dashboard_app.get("/api/notifications")
async def dashboard_notifications() -> list[dict[str, Any]]:
    return load_notifications()


@dashboard_app.post("/api/notifications")
async def dashboard_send_notification(payload: dict[str, Any] = Body(default={})) -> dict[str, Any]:
    return await notifications_send(payload)


@dashboard_app.get("/api/versioncheck/v3")
async def dashboard_versioncheck() -> dict[str, Any]:
    return await versioncheck()


@dashboard_app.post("/api/versioncheck/v3/set")
async def dashboard_set_version_status(payload: dict[str, Any] = Body(default={})) -> dict[str, Any]:
    return await set_version_status(payload)


@dashboard_app.get("/api/servermaintenance/v1")
async def dashboard_server_maintenance() -> dict[str, Any]:
    return {
        "StartsInMinutes": maintenance_starts_in_minutes(),
        "DisableAccountLogin": MAINTENANCE_DISABLE_ACCOUNT_LOGIN,
        "AccountLoginDisabled": account_login_is_disabled(),
    }


@dashboard_app.post("/api/servermaintenance/v1/set")
async def dashboard_set_server_maintenance(payload: dict[str, Any] = Body(default={})) -> dict[str, Any]:
    return await set_server_maintenance(payload)


@dashboard_app.get("/api/accounts")
async def dashboard_accounts() -> list[dict[str, Any]]:
    return [public_account(account) for account in load_accounts()]


@dashboard_app.post("/api/accounts")
async def dashboard_create_account(payload: dict[str, Any] = Body(default={})) -> dict[str, Any]:
    username = str(payload.get("username", "")).strip()
    password = str(payload.get("password", "")).strip()
    if not username or not password:
        raise HTTPException(status_code=400, detail="username and password are required")
    account = create_account(
        username=username,
        password=password,
        platform=int(payload.get("platform", 0)),
        platform_id=str(payload.get("platformId") or "").strip() or None,
    )
    return public_account(account)


@dashboard_app.patch("/api/accounts/{account_id}")
async def dashboard_update_account(account_id: int, payload: dict[str, Any] = Body(default={})) -> dict[str, Any]:
    updates: dict[str, Any] = {}
    for key in ("level", "xp", "profileImageName"):
        if key in payload:
            updates[key] = payload[key]
    return public_account(update_account_profile(account_id, updates))


@dashboard_app.post("/api/accounts/{account_id}/avatar-items")
async def dashboard_toggle_avatar_item(account_id: int, payload: dict[str, Any] = Body(default={})) -> dict[str, Any]:
    avatar_item_desc = str(payload.get("avatarItemDesc", "")).strip()
    if not avatar_item_desc:
        raise HTTPException(status_code=400, detail="avatarItemDesc is required")
    hidden = bool(payload.get("hidden", True))
    return public_account(set_account_avatar_item_hidden(account_id, avatar_item_desc, hidden))


@dashboard_app.post("/api/accounts/{account_id}/ban")
async def dashboard_ban_account(account_id: int) -> dict[str, Any]:
    return public_account(set_account_banned(account_id, True))


@dashboard_app.post("/api/accounts/{account_id}/unban")
async def dashboard_unban_account(account_id: int) -> dict[str, Any]:
    return public_account(set_account_banned(account_id, False))


@dashboard_app.delete("/api/accounts/{account_id}")
async def dashboard_delete_account(account_id: int) -> dict[str, Any]:
    delete_account(account_id)
    return {"deleted": True, "id": account_id}


@api_app.post("/api/PlayersBanned/v2/ban")
async def players_banned_ban(payload: dict[str, Any] = Body(default={})) -> dict[str, Any]:
    player_id = payload.get("PlayerId")
    reason = payload.get("Reason", "10")
    ban_type = payload.get("BanType", "0")
    display_reason = payload.get("DisplayReason", "You are banned for Code of conduct violation. For any questions contact support@againstgrav.com")
    banned_until = payload.get("BannedUntil", "")
    
    # Find and ban the account
    account = find_account_by_id(player_id)
    if account is not None:
        set_account_banned(player_id, True)
        # Kick the player by closing all active WebSocket connections
        account_id = int(account["id"])
        kick_player_by_account_id(account_id, display_reason)
    
    log_api_response(f"Player {player_id} banned: {display_reason}")
    return {"Result": 0}


@api_app.post("/api/PlayersBanned/v2/unban")
async def players_banned_unban(payload: dict[str, Any] = Body(default={})) -> dict[str, Any]:
    player_id = payload.get("PlayerId")
    
    # Find and unban the account
    account = find_account_by_id(player_id)
    if account is not None:
        set_account_banned(player_id, False)
    
    log_api_response(f"Player {player_id} unbanned")
    return {"Result": 0}


@ws_app.websocket("/api/presence/v3/heartbeatwebsocket")
async def websocket_presence_heartbeat(websocket: WebSocket):
    await websocket.accept()
    log_websocket_event(
        "/api/presence/v3/heartbeatwebsocket",
        "connected",
        {"authorization": websocket.headers.get("authorization", "")},
    )
    try:
        while True:
            message = await websocket.receive()
            if message.get("type") == "websocket.disconnect":
                log_websocket_event(
                    "/api/presence/v3/heartbeatwebsocket",
                    "disconnected",
                    {"code": message.get("code")},
                )
                break
            if "text" in message:
                log_websocket_event(
                    "/api/presence/v3/heartbeatwebsocket",
                    "received-text",
                    message["text"],
                )
                await send_blank_ws_json(websocket)
            elif "bytes" in message:
                log_websocket_event(
                    "/api/presence/v3/heartbeatwebsocket",
                    "received-bytes",
                    f"<bytes:{len(message['bytes'])}>",
                )
                await send_blank_ws_json(websocket)
    except WebSocketDisconnect:
        log_websocket_event("/api/presence/v3/heartbeatwebsocket", "disconnected")
        return


@api_app.api_route("/hub/v1/negotiate", methods=["GET", "POST"])
async def hub_v1_negotiate(request: Request) -> dict[str, Any]:
    payload = await read_request_payload(request)
    if payload:
        log_line(f"HUB NEGOTIATE /hub/v1/negotiate {payload}")
    response = hub_handshake("/hub/v1")
    log_api_response(response)
    return response


@api_app.api_route("/api/hub/v1/negotiate", methods=["GET", "POST"])
async def hub_v1_negotiate_api_alias(request: Request) -> dict[str, Any]:
    return await hub_v1_negotiate(request)


@ws_app.websocket("/hub/v1")
async def websocket_hub_v1(websocket: WebSocket):
    if maintenance_is_active():
        log_websocket_event("/hub/v1", "rejected", {"reason": "Server maintenance"})
        await websocket.close(code=1012, reason="Server maintenance")
        return
    await websocket.accept()
    hub_clients.add(websocket)
    log_line("LateWebSocket.cs Hub Requested.")
    log_websocket_event("/hub/v1", "connected")
    account_id = None
    try:
        while True:
            message = await websocket.receive()
            if message.get("type") == "websocket.disconnect":
                log_websocket_event("/hub/v1", "disconnected", {"code": message.get("code")})
                break
            text = None
            if "text" in message:
                text = message["text"]
                log_websocket_event("/hub/v1", "received-text", text)
            elif "bytes" in message:
                raw_bytes = message["bytes"]
                log_websocket_event("/hub/v1", "received-bytes", f"<bytes:{len(raw_bytes)}>")
                try:
                    text = raw_bytes.decode("utf-8")
                except UnicodeDecodeError:
                    await send_blank_ws_json(websocket)
                    continue

            if text is not None:
                parsed_messages = parse_signalr_messages(text)
                handled = False
                for parsed in parsed_messages:
                    message_type = parsed.get("type")
                    # Extract LoginLockToken from arguments to track account
                    arguments = parsed.get("arguments") or []
                    if arguments and isinstance(arguments[0], dict):
                        login_token = arguments[0].get("LoginLockToken")
                        if login_token:
                            acc = account_from_login_token(login_token)
                            if acc:
                                account_id = int(acc["id"])
                                register_websocket_connection(account_id, websocket)
                    if parsed.get("protocol") == "json" and parsed.get("version") == 1:
                        await websocket.send_text(signalr_handshake_ack())
                        handled = True
                        continue
                    if message_type == 1 and parsed.get("target") == "SubscribeToPlayers":
                        invocation_id = str(parsed.get("invocationId", uuid.uuid4().hex))
                        arguments = parsed.get("arguments") or []
                        subscription = arguments[0] if arguments and isinstance(arguments[0], dict) else {}
                        player_ids = subscription.get("PlayerIds") or []
                        session_account = find_account_by_id(account_id) if account_id is not None else load_accounts()[0]
                        session_payload = session_payload_for_account(session_account)
                        active_room = find_room_entry(room_id=int(session_payload.get("RoomId", ROOM["RoomId"])))
                        active_room_data = (active_room or {}).get("room", ROOM)
                        log_websocket_event(
                            "/hub/v1",
                            "subscribe-to-players",
                            {"PlayerIds": player_ids},
                        )
                        await websocket.send_text(signalr_completion(invocation_id))
                        await websocket.send_text(player_hub_event(11, PLAYER))
                        await websocket.send_text(
                            player_hub_event(
                                12,
                                {
                                    "PlayerId": PLAYER["Id"],
                                    "IsOnline": True,
                                    "PlayerType": 2,
                                    "StatusVisibility": 0,
                                },
                            )
                        )
                        await websocket.send_text(
                            player_hub_event(
                                13,
                                session_payload,
                            )
                        )
                        await websocket.send_text(player_hub_event(15, active_room_data))
                        await websocket.send_text(
                            player_hub_event(
                                4,
                                {
                                    "PlayerId": PLAYER["Id"],
                                    "IsOnline": True,
                                    "PlayerType": 2,
                                    "GameSession": session_payload,
                                },
                            )
                        )
                        await websocket.send_text(player_hub_event(21, {}))
                        await websocket.send_text(
                            player_hub_event(
                                60,
                                {
                                    "CurrencyType": 3,
                                    "Balance": 0,
                                },
                            )
                        )
                        await websocket.send_text(
                            player_hub_event(
                                90,
                                {
                                    "Id": 1,
                                    "RoomId": 1,
                                    "SenderPlayerId": PLAYER["Id"],
                                    "SentTime": datetime.now(timezone.utc).isoformat(),
                                    "Content": json.dumps(
                                        {
                                            "Version": 1,
                                            "Type": 0,
                                            "Data": "",
                                        },
                                        separators=(",", ":"),
                                    ),
                                },
                            )
                        )
                        handled = True
                if not handled:
                    await send_blank_ws_json(websocket)
    except WebSocketDisconnect:
        log_websocket_event("/hub/v1", "disconnected")
    finally:
        hub_clients.discard(websocket)
        if account_id is not None:
            unregister_websocket_connection(account_id, websocket)


@ws_app.websocket("/{path:path}")
async def websocket_any(websocket: WebSocket, path: str):
    await websocket.accept()
    log_line("[WebSocket.cs] hub requested.")
    log_websocket_event(f"/{path}", "connected")
    await send_blank_ws_json(websocket)
    try:
        while True:
            message = await websocket.receive()
            if message.get("type") == "websocket.disconnect":
                log_websocket_event(f"/{path}", "disconnected", {"code": message.get("code")})
                break
            if "text" in message:
                log_websocket_event(f"/{path}", "received-text", message["text"])
                await send_blank_ws_json(websocket)
            elif "bytes" in message:
                log_websocket_event(f"/{path}", "received-bytes", f"<bytes:{len(message['bytes'])}>")
                await send_blank_ws_json(websocket)
    except WebSocketDisconnect:
        log_websocket_event(f"/{path}", "disconnected")
        return


def build_image(label: str) -> bytes:
    if PLACEHOLDER_IMAGE_PATH.exists():
        return PLACEHOLDER_IMAGE_PATH.read_bytes()
    image = Image.new("RGB", (512, 512), color=(34, 52, 74))
    draw = ImageDraw.Draw(image)
    draw.rectangle((24, 24, 488, 488), outline=(255, 255, 255), width=4)
    draw.text((40, 240), label[:24], fill=(255, 255, 255))
    buffer = io.BytesIO()
    image.save(buffer, format="PNG")
    return buffer.getvalue()


@image_app.get("/{image_path:path}")
async def get_image(image_path: str) -> Response:
    normalized = image_path.lstrip("/")
    log_line(f"Image Requested: /{normalized}")
    if normalized.startswith("room/"):
        blob_name = normalized.split("/", 1)[1].strip()
        if blob_name:
            blob_path = room_save_data_path(blob_name)
            if blob_path.exists() and blob_path.is_file():
                return Response(content=blob_path.read_bytes(), media_type="application/octet-stream")
    if normalized in IMAGE_NAMES:
        media_type = "image/jpeg" if PLACEHOLDER_IMAGE_PATH.exists() else "image/png"
        return StreamingResponse(io.BytesIO(build_image(normalized)), media_type=media_type)
    log_line("[ImageServer.cs] Image not found on img.rec.net.")
    return PlainTextResponse("Image not found on img.rec.net.", status_code=404)


async def serve(app: FastAPI, port: int) -> None:
    config = uvicorn.Config(app, host=HOST, port=port, log_level="warning")
    server = uvicorn.Server(config)
    await server.serve()


app.mount("/ns", name_server_app)
app.mount("/dashboard", dashboard_app)
app.mount("/recnet", recnet_app)
app.mount("/img", image_app)
app.mount("/ws", ws_app)
app.mount("/", api_app)


async def main() -> None:
    log_line("[NameServer.cs] has started.")
    log_line("[APIServer.cs] has started.")
    log_line("[NameServer.cs] is listening.")
    log_line("[APIServer.cs] is listening.")
    log_line("[DiscordPresence.cs] has started.")
    log_line("Please start up the build you want now.")
    log_line("[DiscordPresence.cs] successfully updated activity!")
    log_line(f'NameServer Response: {{"API":"https://{RENDER_URL}.onrender.com","Notifications":"wss://{RENDER_URL}.onrender.com/hub/v1","Images":"https://{RENDER_URL}.onrender.com"}}')
    log_line("[ImageServer.cs] has started.")
    log_line("[WebSocket.cs] has started and is listening.")
    log_line("[ImageServer.cs] is listening.")
    log_line(f"[NotifDashboard.cs] is listening on https://{RENDER_URL}.onrender.com/dashboard/")
    log_line(f"[RecNet.cs] is listening on https://{RENDER_URL}.onrender.com/recnet/")
    await serve(app, API_PORT)


ensure_seed_room_save_data()


if __name__ == "__main__":
    asyncio.run(main())
