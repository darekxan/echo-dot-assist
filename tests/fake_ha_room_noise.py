#!/usr/bin/env python3
"""Optional background-noise telemetry through the host daemon's native ESPHome API.

The sensor reports relative dBFS before microphone gain, starts unknown, holds its last value during the Echo's
playback, returns to unknown when stale, and recovers from fresh room audio. The test uses the real 30-second
stale timeout.
"""
import asyncio
import io
import math
import os
import struct
import tempfile
import threading
import traceback
import wave
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from aioesphomeapi import MediaPlayerEntityState, MediaPlayerInfo, MediaPlayerState, SensorInfo, SensorState
from fake_ha_volumes import Echo, check, until

OBJECT_ID = "background_noise_level"


def noise(path, db):
    # The inode stays the same: the file backend holds an open FILE* and rewinds it after every second.
    a = round(32768 * 10 ** (db / 20))
    data = struct.pack("<80h", *([a] * 40 + [-a] * 40)) * 200
    with open(path, "r+b" if os.path.exists(path) else "wb") as f:
        f.write(data)
        f.flush()


class LongPlayback(BaseHTTPRequestHandler):
    def do_GET(self):
        b = io.BytesIO()
        with wave.open(b, "wb") as w:
            w.setnchannels(1)
            w.setsampwidth(2)
            w.setframerate(16000)
            w.writeframes(struct.pack("<80h", *([1000] * 40 + [-1000] * 40)) * 8000)  # 40 s
        data = b.getvalue()
        self.send_response(200)
        self.send_header("Content-Type", "audio/wav")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        try:
            self.wfile.write(data)
        except OSError:
            pass

    def log_message(self, *args):
        pass


def latest(echo, key):
    return next((s for s in reversed(echo.states) if isinstance(s, SensorState) and s.key == key), None)


def known(echo, key, db):
    value = latest(echo, key)
    return value is not None and not value.missing_state and math.isfinite(value.state) and abs(value.state - db) < 1


def player_state(echo, key):
    return next((s.state for s in reversed(echo.states) if isinstance(s, MediaPlayerEntityState) and s.key == key), None)


async def main():
    check.failed = False
    capture = tempfile.TemporaryDirectory()
    cap = os.path.join(capture.name, "room.raw")
    noise(cap, -55)
    echo = Echo(23, "even_volume=off\n", cap=cap)
    c = None
    httpd = ThreadingHTTPServer(("127.0.0.1", 0), LongPlayback)
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    try:
        c = await echo.connect()
        check(not any(e.object_id == OBJECT_ID for e in echo.ents), "the feature is off by default and no background-noise entity is listed")
        b = echo.page()
        b.set(room_noise_sensor="on")
        # Feature changes close native API connections so HA relists entities; the direct fixture reconnects itself.
        await c.disconnect()
        c = await echo.connect()
        sensor = next((e for e in echo.ents if isinstance(e, SensorInfo) and e.object_id == OBJECT_ID), None)
        check(sensor is not None, "enabling the feature lists Background noise level after reconnect")
        if sensor is None:
            raise AssertionError("missing background-noise sensor")
        check(sensor.name == "Background noise level" and sensor.unit_of_measurement == "dBFS"
              and int(sensor.state_class) == 1 and not sensor.device_class,
              "sensor metadata is relative dBFS with measurement state class and no sound-pressure device class")
        await until(lambda: latest(echo, sensor.key) is not None, 1)
        initial = latest(echo, sensor.key)
        check(initial is not None and initial.missing_state,
              f"a fresh subscription starts unknown until the room has been measured ({initial})")
        await until(lambda: known(echo, sensor.key, -55), 8)
        value = latest(echo, sensor.key)
        check(known(echo, sensor.key, -55), f"fresh quiet-room PCM reports about -55 dBFS ({value})")

        noise(cap, -35)
        await until(lambda: known(echo, sensor.key, -35), 8)
        value = latest(echo, sensor.key)
        check(known(echo, sensor.key, -35), f"a sustained louder room produces a new numeric state ({value})")
        if not known(echo, sensor.key, -35):
            raise AssertionError("no valid room estimate before playback exclusion test")

        mp = next(e for e in echo.ents if isinstance(e, MediaPlayerInfo))
        c.media_player_command(mp.key, media_url=f"http://127.0.0.1:{httpd.server_port}/long.wav")
        await until(lambda: player_state(echo, mp.key) == MediaPlayerState.PLAYING)
        check(player_state(echo, mp.key) == MediaPlayerState.PLAYING, "the Echo starts its own long playback")
        noise(cap, -10)                                # loud capture during playback must never refresh the estimate
        await asyncio.sleep(4)
        value = latest(echo, sensor.key)
        check(known(echo, sensor.key, -35), f"during self-playback the sensor holds the previous room estimate ({value})")
        await until(lambda: latest(echo, sensor.key) is not None and latest(echo, sensor.key).missing_state, 33)
        value = latest(echo, sensor.key)
        check(value is not None and value.missing_state and player_state(echo, mp.key) == MediaPlayerState.PLAYING,
              f"after 30 s without eligible room audio, the sensor publishes an unknown state while playback continues ({value})")

        c.media_player_command(mp.key, command=2)
        await until(lambda: player_state(echo, mp.key) == MediaPlayerState.IDLE)
        await until(lambda: known(echo, sensor.key, -10), 8)
        value = latest(echo, sensor.key)
        check(known(echo, sensor.key, -10), f"after playback ends, a fresh room window restores a numeric state ({value})")
        check(echo.prop("MainVolume") == 40 and echo.prop("TTSVolume") == 40,
              "measuring a loud room leaves the speaker levels unchanged")

        b.set(room_noise_sensor="off")
        await c.disconnect()
        c = await echo.connect()
        check(not any(e.object_id == OBJECT_ID for e in echo.ents) and echo.config().get("room_noise_sensor") == "off",
              "disabling the sensor saves the setting, relists entities and removes its entity")
    except Exception:
        print("FAIL " + traceback.format_exc().rstrip().replace("\n", "\n     "))
        check.failed = True
    finally:
        if c:
            await c.disconnect()
        echo.stop()
        httpd.shutdown()
        httpd.server_close()
        capture.cleanup()
    print("FAIL" if check.failed else "PASS")
    raise SystemExit(1 if check.failed else 0)


if __name__ == "__main__":
    asyncio.run(main())
