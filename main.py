#!/usr/bin/env python3
"""TikTok LIVE -> Facebook & Instagram RTMPS restreamer.

Selects H.264 HD stream ('h264_hd' or 'hd' depending on CDN manifest) and avoids
HEVC. Video is copied without re-encoding; audio is encoded to AAC; an optional
FLV filler stage paces output; FFmpeg tee muxer pushes to Facebook and Instagram
simultaneously.
"""

from __future__ import annotations

import asyncio
import gc
import os
import queue
import re
import resource
import shlex
import signal
import struct
import subprocess
import sys
import threading
import time
from typing import BinaryIO, List, Optional

import streamlink

try:
    from TikTokLive import TikTokLiveClient
    from TikTokLive.events import ConnectEvent, LiveEndEvent
    TIKTOKLIVE_IMPORT_ERROR = None
except Exception as exc:
    TikTokLiveClient = None
    TIKTOKLIVE_IMPORT_ERROR = exc


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

def env_value(name: str, default, cast=str):
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return cast(raw)
    except (TypeError, ValueError):
        return default


TIKTOK_URL = env_value(
    "TIKTOK_URL",
    "https://www.tiktok.com/@d.shakertawfiqalaroury/live",
)

FACEBOOK_RTMP = env_value(
    "FACEBOOK_RTMP",
    "rtmps://live-api-s.facebook.com:443/rtmp/FB-122145065265180204-0-Ab6FqbEw50G14AO3ATPTeo9h",
).strip()

# حط مفتاح بث إنستجرام بعد /rtmp/ هنا
INSTAGRAM_RTMP = env_value(
    "INSTAGRAM_RTMP",
    "rtmps://edgetee-upload-hbe1-2.xx.fbcdn.net:443/rtmp/",
).strip()

_match = re.search(r"tiktok\.com/@([^/?#]+)", TIKTOK_URL)
TIKTOK_USERNAME = env_value(
    "TIKTOK_USERNAME",
    _match.group(1) if _match else "",
)

PREFERRED_QUALITIES = ("h264_hd", "hd", "h264_sd", "sd")

COPY_VIDEO = env_value("COPY_VIDEO", 1, int)  # 1 = copy; 0 = H.264 re-encode
FILLER_KBPS = env_value("FILLER_KBPS", 3850, int)  # 0 disables filler stage
AUDIO_BITRATE = env_value("AUDIO_BITRATE", "128k")

BUF_MB = env_value("BUF_MB", 48, int)
PREFILL_PCT = env_value("PREFILL_PCT", 2, int)

SLATE_SEC = env_value("SLATE_SEC", 0, int)
SLATE_CODEC = "h264"
SLATE_SIZE = env_value("SLATE_SIZE", "1080x1920")
SLATE_FPS = env_value("SLATE_FPS", 15, int)
SLATE_MIN_OFFLINE = env_value("SLATE_MIN_OFFLINE", 120, int)
SLATE_DIR = env_value("SLATE_DIR", os.path.dirname(os.path.abspath(__file__)))
SLATE_PATH = os.path.join(
    SLATE_DIR,
    f"slate_{SLATE_CODEC}_{SLATE_SIZE}_{SLATE_FPS}.flv",
)

VIDEO_BITRATE = env_value("VIDEO_BITRATE", "4000k")
FPS = env_value("FPS", 30, int)
X264_PRESET = env_value("X264_PRESET", "veryfast")
GOP = max(1, FPS * 2)

CHECK_INTERVAL = env_value("CHECK_INTERVAL", 2.0, float)
LIVE_END_COOLDOWN = env_value("LIVE_END_COOLDOWN", 15, int)
RESTART_DELAY = env_value("RESTART_DELAY", 1, int)
CRASH_DELAY = env_value("CRASH_DELAY", 5, int)

RSS_LIMIT_MB = env_value("RSS_LIMIT_MB", 350, int)
SESSION_TTL_SEC = env_value("SESSION_TTL_SEC", 600, int)
MEMORY_CHECK_INTERVAL = env_value("MEMORY_CHECK_INTERVAL", 30, int)
MEMORY_CLEANUP_COOLDOWN = env_value("MEMORY_CLEANUP_COOLDOWN", 120, int)

FILLER_QUEUE_MAX_TAGS = env_value("FILLER_QUEUE_MAX_TAGS", 256, int)
MIN_RESERVE_SEC = 3.0


def log(message: str) -> None:
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {message}", flush=True)


def get_rss_mb() -> float:
    try:
        with open("/proc/self/statm", "r", encoding="utf-8") as handle:
            fields = handle.read().split()
        resident_pages = int(fields[1])
        page_size = os.sysconf("SC_PAGE_SIZE")
        return resident_pages * page_size / (1024 * 1024)
    except Exception:
        return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0


def get_active_targets() -> List[tuple[str, str]]:
    targets = []
    if (
        FACEBOOK_RTMP
        and not FACEBOOK_RTMP.endswith("/rtmp/")
        and "PUT_YOUR" not in FACEBOOK_RTMP
    ):
        targets.append(("Facebook", FACEBOOK_RTMP))
    if (
        INSTAGRAM_RTMP
        and not INSTAGRAM_RTMP.endswith("/rtmp/")
        and "PUT_YOUR" not in INSTAGRAM_RTMP
    ):
        targets.append(("Instagram", INSTAGRAM_RTMP))
    return targets


def select_stream_quality(streams) -> Optional[str]:
    """Pick an H.264 video stream and never select HEVC/H.265 variants."""
    if not streams:
        return None

    for candidate in PREFERRED_QUALITIES:
        if candidate in streams:
            return candidate

    for name in sorted(streams.keys()):
        if name.startswith("h264_") and not name.endswith("_ao"):
            return name

    # Only fall back to 'best' if no H.265 variants exist in the manifest.
    has_hevc = any("h265" in k or "hevc" in k for k in streams.keys())
    if not has_hevc and "best" in streams:
        return "best"

    return None


# ---------------------------------------------------------------------------
# FLV parsing and filler
# ---------------------------------------------------------------------------

def iter_flv_tags(handle: BinaryIO):
    header = handle.read(13)
    if len(header) < 13 or header[:3] != b"FLV":
        return

    while True:
        tag_header = handle.read(11)
        if len(tag_header) < 11:
            return

        data_size = int.from_bytes(tag_header[1:4], "big")
        timestamp = (
            int.from_bytes(tag_header[4:7], "big")
            | (tag_header[7] << 24)
        )
        payload = handle.read(data_size)
        previous_tag_size = handle.read(4)
        if len(payload) != data_size or len(previous_tag_size) != 4:
            return

        yield tag_header[0], timestamp, payload


def run_filler(kbps: int, slate_path: Optional[str] = None, slate_min: float = 0) -> None:
    if kbps <= 0:
        raise ValueError("FILLER_KBPS must be greater than zero")

    output = sys.stdout.buffer
    bytes_per_ms = kbps * 1000 / 8 / 1000.0
    tags: queue.Queue = queue.Queue(maxsize=max(32, FILLER_QUEUE_MAX_TAGS))
    reader_done = threading.Event()
    source_info = {"base": None, "newest": 0}

    def reader() -> None:
        try:
            for tag in iter_flv_tags(sys.stdin.buffer):
                tag_type, timestamp, _payload = tag
                if tag_type in (8, 9):
                    if source_info["base"] is None:
                        source_info["base"] = timestamp
                    source_info["newest"] = max(source_info["newest"], timestamp)
                tags.put(tag)
        except (BrokenPipeError, OSError):
            pass
        except Exception as exc:
            log(f"[FILLER] Reader error: {exc}")
        finally:
            reader_done.set()
            tags.put(None)

    def reserve_seconds() -> float:
        base = source_info["base"]
        if base is None:
            return 0.0
        return max(0.0, (source_info["newest"] - base) / 1000.0)

    threading.Thread(target=reader, name="flv-reader", daemon=True).start()

    state = {
        "nal_len": 4,
        "last_video_ts": None,
        "audio_bytes": 0,
        "carry_bytes": 0.0,
        "last_output_ts": 0,
    }

    output.write(b"FLV\x01\x05\x00\x00\x00\x09\x00\x00\x00\x00")

    def make_filler_nal(kind: str, filler_bytes: int) -> bytes:
        filler_bytes = max(0, filler_bytes)
        if kind == "h264":
            return (
                struct.pack(">I", filler_bytes + 2)
                + b"\x0c"
                + (b"\xff" * filler_bytes)
                + b"\x80"
            )
        if kind == "hevc":
            return (
                struct.pack(">I", filler_bytes + 3)
                + b"\x4c\x01"
                + (b"\xff" * filler_bytes)
                + b"\x80"
            )
        return b""

    def classify_video(payload: bytes):
        if not payload:
            return None, False, False, False

        first = payload[0]
        if first & 0x80:
            packet_type = first & 0x0F
            fourcc = payload[1:5]
            codec = "hevc" if fourcc == b"hvc1" else "h264" if fourcc == b"avc1" else None
            is_keyframe = ((first >> 4) & 0x07) == 1
            return codec, packet_type == 0, packet_type in (1, 3), is_keyframe

        codec_id = first & 0x0F
        if codec_id in (7, 12) and len(payload) >= 2:
            codec = "h264" if codec_id == 7 else "hevc"
            packet_type = payload[1]
            is_keyframe = (first >> 4) == 1
            return codec, packet_type == 0, packet_type == 1, is_keyframe

        return None, False, False, False

    def emit(tag_type: int, timestamp: int, payload: bytes) -> None:
        data = payload
        data_size = len(data)

        if tag_type == 8:
            state["audio_bytes"] += data_size + 15
        elif tag_type == 9 and data_size > 5:
            codec, is_sequence, is_frame, _is_keyframe = classify_video(data)

            if codec and is_sequence:
                if codec == "hevc" and data_size > 26:
                    state["nal_len"] = (data[26] & 0x03) + 1
                elif codec == "h264" and data_size > 9:
                    state["nal_len"] = (data[9] & 0x03) + 1

            elif codec and is_frame and state["nal_len"] == 4:
                last_ts = state["last_video_ts"]
                if last_ts is not None and timestamp >= last_ts:
                    state["carry_bytes"] += (timestamp - last_ts) * bytes_per_ms
                state["last_video_ts"] = timestamp

                needed = (
                    int(state["carry_bytes"])
                    - (data_size + 15)
                    - state["audio_bytes"]
                )
                state["audio_bytes"] = 0

                if needed > 8:
                    extra = make_filler_nal(codec, min(needed - 7, 200000))
                    if extra:
                        data += extra
                        data_size = len(data)

                state["carry_bytes"] -= data_size + 15
                if state["carry_bytes"] < 0:
                    state["carry_bytes"] = 0.0

        timestamp &= 0xFFFFFFFF
        if data_size > 0xFFFFFF:
            raise ValueError(f"FLV tag too large: {data_size} bytes")

        output.write(
            bytes([tag_type])
            + data_size.to_bytes(3, "big")
            + (timestamp & 0xFFFFFF).to_bytes(3, "big")
            + bytes([timestamp >> 24])
            + b"\x00\x00\x00"
        )
        output.write(data)
        output.write(struct.pack(">I", 11 + data_size))

        if tag_type == 9:
            output.flush()
        state["last_output_ts"] = max(state["last_output_ts"], timestamp)

    if slate_path and slate_min > 0 and os.path.isfile(slate_path):
        try:
            with open(slate_path, "rb") as handle:
                slate_tags = [tag for tag in iter_flv_tags(handle) if tag[0] in (8, 9)]
        except Exception as exc:
            log(f"[SLATE] Could not read slate: {exc}")
            slate_tags = []

        start = time.monotonic()
        index = 0
        while slate_tags:
            elapsed = time.monotonic() - start
            source_ready = reserve_seconds() >= MIN_RESERVE_SEC
            if elapsed >= slate_min and source_ready:
                break
            if index >= len(slate_tags):
                break

            tag_type, timestamp, payload = slate_tags[index]
            if timestamp / 1000.0 <= elapsed:
                emit(tag_type, timestamp, payload)
                index += 1
            else:
                time.sleep(0.005)

            if reader_done.is_set() and tags.empty():
                break

    timestamp_offset = None
    first_source_ts = None
    wall_clock_start = None
    seen_keyframe = False

    while True:
        tag = tags.get()
        if tag is None:
            break

        tag_type, timestamp, payload = tag
        if tag_type not in (8, 9) or len(payload) < 2:
            continue

        if tag_type == 9:
            _codec, _is_sequence, is_frame, is_keyframe = classify_video(payload)
            if is_frame and not seen_keyframe:
                if not is_keyframe:
                    continue
                seen_keyframe = True

        if timestamp_offset is None:
            prior_ts = state["last_output_ts"]
            timestamp_offset = prior_ts + (40 if prior_ts else 0) - timestamp
            first_source_ts = timestamp
            wall_clock_start = time.monotonic()

        assert first_source_ts is not None and wall_clock_start is not None
        target_elapsed = (timestamp - first_source_ts) / 1000.0
        actual_elapsed = time.monotonic() - wall_clock_start
        wait_seconds = target_elapsed - actual_elapsed
        if wait_seconds > 0 and state["last_output_ts"]:
            time.sleep(min(wait_seconds, 0.5))

        emit(tag_type, max(0, timestamp + timestamp_offset), payload)

    output.flush()


# ---------------------------------------------------------------------------
# FFmpeg / Streamlink pipeline (Single or Dual RTMPS via tee muxer)
# ---------------------------------------------------------------------------

def build_rtmp_output_args(targets: List[tuple[str, str]]) -> List[str]:
    if len(targets) == 1:
        return [
            "-flvflags", "no_duration_filesize",
            "-f", "flv", targets[0][1],
        ]

    tee_slaves = "|".join(
        f"[f=flv:flvflags=no_duration_filesize:onfail=ignore]{url}"
        for _name, url in targets
    )
    return ["-f", "tee", tee_slaves]


def build_pipeline(quality: str, use_slate: bool = False) -> str:
    targets = get_active_targets()
    if not targets:
        raise RuntimeError("No active RTMPS targets configured.")

    if COPY_VIDEO:
        video_options = ["-c:v", "copy"]
    else:
        video_options = [
            "-c:v", "libx264",
            "-preset", X264_PRESET,
            "-profile:v", "high",
            "-pix_fmt", "yuv420p",
            "-b:v", VIDEO_BITRATE,
            "-minrate", VIDEO_BITRATE,
            "-maxrate", VIDEO_BITRATE,
            "-bufsize", VIDEO_BITRATE,
            "-x264-params", "nal-hrd=cbr:force-cfr=1",
            "-r", str(FPS),
            "-fps_mode", "cfr",
            "-g", str(GOP),
            "-keyint_min", str(GOP),
            "-sc_threshold", "0",
        ]

    ffmpeg_options = [
        "ffmpeg", "-hide_banner", "-nostdin", "-loglevel", "warning",
        "-stats", "-stats_period", "15",
        "-fflags", "+genpts+discardcorrupt",
        "-i", "pipe:0",
        "-map", "0:v:0", "-map", "0:a:0?",
        *video_options,
        "-c:a", "aac", "-b:a", AUDIO_BITRATE,
        "-ar", "44100", "-ac", "2",
        "-af", "aresample=async=1000:min_hard_comp=0.100000:first_pts=0",
        "-max_muxing_queue_size", "2048",
    ]

    use_filler = bool(COPY_VIDEO and FILLER_KBPS > 0)
    if use_filler:
        ffmpeg_options += ["-flvflags", "no_duration_filesize", "-f", "flv", "pipe:1"]
    else:
        ffmpeg_options += build_rtmp_output_args(targets)

    source_command = [
        "streamlink", "--no-config", "--stdout", "--loglevel", "warning",
        "--stream-timeout", "60",
        "--http-timeout", "30",
        "--retry-streams", "2",
        "--retry-max", "5",
        TIKTOK_URL,
        quality,
    ]
    buffer_command = [
        "mbuffer", "-q", "-m", f"{BUF_MB}M", "-P", str(PREFILL_PCT),
    ]

    commands = [source_command, buffer_command, ffmpeg_options]

    if use_filler:
        filler_command = [
            sys.executable,
            os.path.abspath(__file__),
            "--filler",
            str(FILLER_KBPS),
        ]
        if use_slate and SLATE_SEC > 0 and os.path.isfile(SLATE_PATH):
            filler_command += ["--slate", SLATE_PATH, str(SLATE_SEC)]
        commands.append(filler_command)

        final_ffmpeg = [
            "ffmpeg", "-hide_banner", "-nostdin", "-loglevel", "warning",
            "-f", "flv", "-i", "pipe:0",
            "-map", "0:v:0", "-map", "0:a:0?",
            "-c", "copy",
            *build_rtmp_output_args(targets),
        ]
        commands.append(final_ffmpeg)

    pipeline = " | ".join(shlex.join(command) for command in commands)
    return "set -o pipefail; " + pipeline


def ensure_slate() -> None:
    if SLATE_SEC <= 0 or os.path.exists(SLATE_PATH):
        return

    log(f"[SLATE] Building H.264 slate {SLATE_SIZE} at {SLATE_FPS} FPS.")
    temporary_path = SLATE_PATH + ".tmp"
    video_encoder = [
        "-c:v", "libx264", "-preset", "ultrafast",
        "-g", str(SLATE_FPS), "-sc_threshold", "0",
    ]
    command = [
        "ffmpeg", "-hide_banner", "-loglevel", "error", "-y",
        "-f", "lavfi", "-i", f"color=c=black:s={SLATE_SIZE}:r={SLATE_FPS}",
        "-f", "lavfi", "-i", "anullsrc=r=44100:cl=stereo",
        "-t", str(max(SLATE_SEC + 12, 20)),
        *video_encoder,
        "-pix_fmt", "yuv420p",
        "-c:a", "aac", "-b:a", "128k", "-ar", "44100", "-ac", "2",
        "-f", "flv", temporary_path,
    ]

    try:
        subprocess.run(command, check=True)
        os.replace(temporary_path, SLATE_PATH)
        log("[SLATE] Slate ready.")
    except Exception as exc:
        try:
            if os.path.exists(temporary_path):
                os.remove(temporary_path)
        except OSError:
            pass
        log(f"[SLATE] Failed to build slate: {exc}")


# ---------------------------------------------------------------------------
# Streamlink session and live-end monitoring
# ---------------------------------------------------------------------------

def make_session():
    session = streamlink.Streamlink()
    try:
        session.set_option("stream-timeout", 60)
        session.set_option("http-timeout", 30)
    except Exception as exc:
        log(f"[WARN] Could not set Streamlink session options: {exc}")
    return session


class LiveEndMonitor(threading.Thread):
    def __init__(self, owner: "Restreamer"):
        super().__init__(daemon=True, name="tiktok-live-end-monitor")
        self.owner = owner

    def run(self) -> None:
        asyncio.run(self._main())

    async def _main(self) -> None:
        while not self.owner.stop_event.is_set():
            try:
                client = TikTokLiveClient(unique_id=f"@{TIKTOK_USERNAME}")

                @client.on(ConnectEvent)
                async def on_connect(event):
                    log("[WS] Connected to TikTok LIVE event monitor.")

                @client.on(LiveEndEvent)
                async def on_live_end(event):
                    log("[WS] LiveEndEvent received; verifying source status (3 negatives required).")
                    offline_confirmations = 0

                    for attempt in range(1, 6):
                        if self.owner.stop_event.is_set():
                            return

                        await asyncio.sleep(3)
                        streams = await asyncio.to_thread(self.owner.fetch_source_streams)

                        if streams is None:
                            log(f"[WS] Verification {attempt}/5: network/API error; not counted.")
                            continue

                        if streams:
                            log(f"[WS] False alarm ignored at check {attempt}/5; TikTok is still live.")
                            return

                        offline_confirmations += 1
                        log(f"[WS] Offline confirmation {offline_confirmations}/3.")
                        if offline_confirmations >= 3:
                            log("[WS] Three successful offline checks; stopping the relay.")
                            self.owner.on_live_ended()
                            return

                    log("[WS] LiveEndEvent ignored: source could not be confirmed offline.")

                if await client.is_live():
                    await client.connect()
            except Exception as exc:
                if not self.owner.stop_event.is_set():
                    log(f"[WS] Monitor reconnecting after error: {exc}")
            self.owner.stop_event.wait(5)


# ---------------------------------------------------------------------------
# Main restreamer
# ---------------------------------------------------------------------------

class Restreamer:
    def __init__(self):
        self.stop_event = threading.Event()
        self.live_ended_event = threading.Event()
        self.session_lock = threading.Lock()
        self.session = make_session()
        self.session_created_at = time.monotonic()
        self.proc: Optional[subprocess.Popen] = None
        self.selected_quality: str = "h264_hd"
        self.last_live_end: Optional[float] = None
        self.last_memory_check = 0.0
        self.last_memory_cleanup = 0.0
        self.last_wait_log = 0.0
        self.last_status_error_log = 0.0

    def _rebuild_session_unlocked(self) -> None:
        old_session = self.session
        try:
            old_session.http.close()
        except Exception:
            pass
        self.session = make_session()
        self.session_created_at = time.monotonic()
        gc.collect()

    def fetch_source_streams(self):
        with self.session_lock:
            now = time.monotonic()
            if now - self.session_created_at >= SESSION_TTL_SEC:
                self._rebuild_session_unlocked()
            try:
                return self.session.streams(TIKTOK_URL)
            except Exception as exc:
                wall_now = time.time()
                if wall_now - self.last_status_error_log >= 60:
                    log(f"[CHECK] TikTok status request failed (temporary/unknown): {exc}")
                    self.last_status_error_log = wall_now
                return None

    def maintain_memory(self, is_idle: bool = False) -> None:
        now = time.monotonic()
        if now - self.last_memory_check < max(5, MEMORY_CHECK_INTERVAL):
            return
        self.last_memory_check = now

        with self.session_lock:
            if now - self.session_created_at >= SESSION_TTL_SEC:
                log("[SESSION] Rotating Streamlink API session.")
                self._rebuild_session_unlocked()

            rss_mb = get_rss_mb()
            if rss_mb <= RSS_LIMIT_MB:
                return

            if now - self.last_memory_cleanup < max(30, MEMORY_CLEANUP_COOLDOWN):
                return

            self.last_memory_cleanup = now
            log(
                f"[MEM] Python RSS is {rss_mb:.1f} MiB (limit {RSS_LIMIT_MB} MiB); "
                "rebuilding the API session and running garbage collection."
            )
            self._rebuild_session_unlocked()
            rss_after = get_rss_mb()
            if rss_after > RSS_LIMIT_MB:
                mode = "idle" if is_idle else "live"
                log(
                    f"[MEM] RSS remains {rss_after:.1f} MiB after cleanup ({mode}). "
                    "No live pipeline will be killed by memory maintenance."
                )

    def stop_pipeline(self) -> None:
        process = self.proc
        if process is None:
            return

        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        except Exception as exc:
            log(f"[PROC] SIGTERM error: {exc}")

        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            except Exception as exc:
                log(f"[PROC] SIGKILL error: {exc}")
            try:
                process.wait(timeout=2)
            except subprocess.TimeoutExpired:
                log("[PROC] Process group did not exit after SIGKILL.")

    def on_live_ended(self) -> None:
        self.live_ended_event.set()
        self.stop_pipeline()

    def wait_for_source(self, streams) -> bool:
        if streams is None:
            now = time.time()
            if now - self.last_wait_log >= 60:
                log("[CHECK] TikTok API/network check failed; keeping the relay process alive and retrying.")
                self.last_wait_log = now
            return False

        if not streams:
            if self.last_wait_log == 0 or time.time() - self.last_wait_log >= 60:
                log("TikTok is currently offline; checking again soon.")
                self.last_wait_log = time.time()
            return False

        chosen = select_stream_quality(streams)
        if not chosen:
            if self.last_wait_log == 0 or time.time() - self.last_wait_log >= 60:
                available = ", ".join(sorted(streams.keys()))
                log(
                    "TikTok is live, but no compatible H.264 video stream is available. "
                    f"Available streams: {available}"
                )
                self.last_wait_log = time.time()
            return False

        self.selected_quality = chosen
        self.last_wait_log = 0.0
        return True

    def run(self) -> None:
        targets = get_active_targets()
        if not targets:
            log("ERROR: Set FACEBOOK_RTMP and/or INSTAGRAM_RTMP before starting.")
            raise SystemExit(1)

        if BUF_MB <= 0 or not 0 <= PREFILL_PCT <= 100:
            log("ERROR: BUF_MB must be positive and PREFILL_PCT must be between 0 and 100.")
            raise SystemExit(1)

        target_names = " + ".join(name for name, _url in targets)
        mode = (
            f"video COPY + filler {FILLER_KBPS}k"
            if COPY_VIDEO and FILLER_KBPS > 0
            else "video COPY" if COPY_VIDEO
            else f"H.264 CBR re-encode {VIDEO_BITRATE}"
        )
        log(
            f"Starting relay -> [{target_names}] | {mode} | audio AAC {AUDIO_BITRATE} | "
            f"auto H.264 quality | Python RSS limit {RSS_LIMIT_MB} MiB"
        )
        log(f"TikTok source: {TIKTOK_URL}")

        if TikTokLiveClient and TIKTOK_USERNAME:
            LiveEndMonitor(self).start()
        else:
            log(f"[WS] TikTokLive monitor unavailable: {TIKTOKLIVE_IMPORT_ERROR}")

        slate_thread = None
        if COPY_VIDEO and FILLER_KBPS > 0 and SLATE_SEC > 0:
            slate_thread = threading.Thread(target=ensure_slate, name="slate-builder", daemon=True)
            slate_thread.start()

        while not self.stop_event.is_set():
            self.maintain_memory(is_idle=True)
            if self.stop_event.is_set():
                break

            streams = self.fetch_source_streams()
            if not self.wait_for_source(streams):
                self.stop_event.wait(CHECK_INTERVAL)
                continue

            offline_for = None if self.last_live_end is None else time.time() - self.last_live_end
            use_slate = SLATE_SEC > 0 and (
                offline_for is None or offline_for >= SLATE_MIN_OFFLINE
            )

            if use_slate:
                if slate_thread is not None and slate_thread.is_alive():
                    log("Waiting for the optional slate file to finish building...")
                    slate_thread.join(timeout=90)
                log(f"TikTok LIVE detected (quality={self.selected_quality}); starting [{target_names}] with slate.")
            elif offline_for is not None and offline_for < SLATE_MIN_OFFLINE:
                log(
                    f"TikTok LIVE returned after {int(offline_for)}s (quality={self.selected_quality}); "
                    "starting without a slate."
                )
            else:
                log(
                    f"TikTok LIVE detected (quality={self.selected_quality}); "
                    f"starting Streamlink -> mbuffer -> FFmpeg -> [{target_names}]."
                )

            self.live_ended_event.clear()
            started_at = time.time()

            try:
                self.proc = subprocess.Popen(
                    build_pipeline(self.selected_quality, use_slate),
                    shell=True,
                    executable="/bin/bash",
                    preexec_fn=os.setsid,
                )
            except Exception as exc:
                self.proc = None
                log(f"[PROC] Could not start pipeline: {exc}")
                self.stop_event.wait(CRASH_DELAY)
                continue

            while not self.stop_event.is_set() and not self.live_ended_event.is_set():
                if self.proc.poll() is not None:
                    log(f"Pipeline exited with code {self.proc.returncode}; checking source and restarting if live.")
                    break
                self.maintain_memory(is_idle=False)
                self.stop_event.wait(1)

            self.stop_pipeline()
            self.proc = None

            if self.stop_event.is_set():
                break

            if self.live_ended_event.is_set():
                self.last_live_end = time.time()
                self.stop_event.wait(LIVE_END_COOLDOWN)
            else:
                elapsed = time.time() - started_at
                self.last_live_end = time.time()
                if elapsed < 10:
                    log("Pipeline stopped shortly after startup; waiting before retry.")
                    self.stop_event.wait(CRASH_DELAY)
                else:
                    self.stop_event.wait(RESTART_DELAY)

        log("Relay stopped cleanly.")


def main() -> None:
    if len(sys.argv) >= 3 and sys.argv[1] == "--filler":
        try:
            if len(sys.argv) >= 6 and sys.argv[3] == "--slate":
                run_filler(int(sys.argv[2]), sys.argv[4], float(sys.argv[5]))
            else:
                run_filler(int(sys.argv[2]))
        except BrokenPipeError:
            pass
        return

    restreamer = Restreamer()

    def handle_signal(_signum, _frame):
        restreamer.stop_event.set()
        restreamer.stop_pipeline()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)
    restreamer.run()


if __name__ == "__main__":
    main()
