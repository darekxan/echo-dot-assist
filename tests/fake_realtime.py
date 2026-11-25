#!/usr/bin/env python3
"""Exercise the native Realtime route and its playback worker using a fake TLS provider."""
import asyncio
import base64
from datetime import datetime, timedelta, timezone
import hashlib
import json
from math import cos, pi, sin, sqrt
from pathlib import Path
import ssl
import struct
import sys
import tempfile

from aioesphomeapi import VoiceAssistantEventType

from esphome_common import home_assistant, run, sync, tone, until
from fake_web_realtime import KEY, TOKEN, candidate, read, save

PORTS = (17155, 17156, 17157, 17158, 17159)
TALK_OVER_PORTS = (17160, 17161, 17162, 17163, 17164)
VOIP_PORTS = (17165, 17166, 17167, 17168, 17169)
RECOVERY_PORTS = (17170, 17171, 17172, 17173, 17174)
RECOVERY_ERROR_PORTS = (17175, 17176, 17177, 17178, 17179)
INDEPENDENT_ROUTE_PORTS = (
    (17321, 17322, 17323, 17324, 17325),
    (17326, 17327, 17328, 17329, 17330),
    (17331, 17332, 17333, 17334, 17335),
)
CAPTURE_RATE = 16000
RATE = 24000
TIMEOUT = 5
VAD_RMS_FLOOR = 550
LEGACY_MODELS = ("gpt-realtime", "gpt-realtime-mini")
FILLER_TOOL_COUNT = 39
LARGE_TOOL_RESULT = {"entity_id": "sensor.kitchen", "state": "ready",
                     "attributes": {"details": "a" * (20 * 1024)}}


class Session:
    def __init__(self, reader, writer, model):
        self.reader = reader
        self.writer = writer
        self.model = model
        self.mic_frames = 0

    async def send(self, event_type, **fields):
        payload = json.dumps({"type": event_type, **fields}).encode()
        if len(payload) < 126:
            header = bytes((0x81, len(payload)))
        elif len(payload) <= 65535:
            header = struct.pack("!BBH", 0x81, 126, len(payload))
        else:
            header = struct.pack("!BBQ", 0x81, 127, len(payload))
        self.writer.write(header + payload)
        await self.writer.drain()

    async def send_audio(self, response_id, item_id, output_index, pcm, done=False):
        item = dict(response_id=response_id, item_id=item_id, output_index=output_index, content_index=0)
        await self.send("response.output_audio.delta", delta=base64.b64encode(pcm).decode(), **item)
        if done:
            await self.send("response.output_audio.done", **item)

    async def receive(self):
        try:
            head, length = await self.reader.readexactly(2)
        except asyncio.IncompleteReadError:
            return None
        assert length & 0x80, "client WebSocket frame must be masked"
        length &= 0x7f
        if length == 126:
            length = struct.unpack("!H", await self.reader.readexactly(2))[0]
        elif length == 127:
            length = struct.unpack("!Q", await self.reader.readexactly(8))[0]
        mask = await self.reader.readexactly(4)
        payload = await self.reader.readexactly(length)
        if head == 0x88:
            return None
        assert head == 0x81, f"unexpected frame opcode {head:#x}"
        event = json.loads(bytes(b ^ mask[i % 4] for i, b in enumerate(payload)))
        if event["type"] == "input_audio_buffer.append":
            self.mic_frames += len(mic_samples(event))
        return event

    async def expect(self, event_type, condition=lambda event: True):
        async def match():
            while True:
                event = await self.receive()
                assert event is not None, f"session ended before {event_type}"
                if event["type"] == event_type and condition(event):
                    return event
        try:
            return await asyncio.wait_for(match(), TIMEOUT)
        except asyncio.TimeoutError as error:
            raise AssertionError(f"no {event_type} received") from error

    async def committed_speech(self, item_id, already_started=False):
        if not already_started:
            await self.send("input_audio_buffer.speech_started")
        await self.send("input_audio_buffer.speech_stopped")
        await self.send("input_audio_buffer.committed", item_id=item_id)
        await self.expect("response.create")

    async def closed(self):
        async def wait():
            while await self.receive() is not None:
                pass
        await asyncio.wait_for(wait(), TIMEOUT)


class Provider:
    def __init__(self):
        self.sessions = asyncio.Queue()
        self.tools = []
        filter_schema = {"type": "object", "properties": {
            f"filter_{i}": {"type": "string", "description": "Filter option for the entity selection. " * 3}
            for i in range(22)
        }}
        self.catalogue = [
            {"name": f"SearchEntities_{i}", "description": "Search a set of entities.", "inputSchema": filter_schema}
            for i in range(FILLER_TOOL_COUNT)
        ]
        # An otherwise usable tool came after the former 80 KiB aggregate limit.
        assert 80 * 1024 < len(json.dumps(self.catalogue).encode()) < 256 * 1024
        self.catalogue.append({"name": "HassGetState", "description": "Read a device.",
                               "inputSchema": {"type": "object", "properties": {
                                   "entity_id": {"type": "string"}}, "required": ["entity_id"]}})

    async def handle(self, reader, writer):
        try:
            request = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), TIMEOUT)
            first, *headers = request.decode().strip().split("\r\n")
            headers = {k.lower(): v.strip() for k, v in (line.split(":", 1) for line in headers)}
            method, path, _ = first.split()
            if method == "GET":
                assert path.startswith("/v1/realtime?model=")
                model = path.removeprefix("/v1/realtime?model=")
                assert model in ("mock-realtime", *LEGACY_MODELS)
                assert headers["authorization"] == "Bearer " + KEY.decode()
                key = headers["sec-websocket-key"] + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
                accept = base64.b64encode(hashlib.sha1(key.encode()).digest())
                writer.write(b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
                             b"Sec-WebSocket-Accept: " + accept + b"\r\n\r\n")
                await writer.drain()
                session = Session(reader, writer, model)
                await session.send("session.created")
                self.sessions.put_nowait(session)
                return

            assert method == "POST" and path == "/api/mcp/assist"
            body = await reader.readexactly(int(headers["content-length"]))
            response = self.mcp(headers, json.loads(body))
            writer.write(response)
            await writer.drain()
        except BaseException:
            writer.close()
            raise
        writer.close()

    def mcp(self, headers, request):
        token = headers["authorization"]
        assert token == "Bearer " + TOKEN.decode(), "MCP authorization mismatch"
        method = request["method"]
        self.tools.append(method)
        static_headers = b"Connection: close\r\n"
        if method == "initialize":
            result = {"protocolVersion": "2025-11-25", "capabilities": {"tools": {}}}
            static_headers += b"Mcp-Session-Id: test-session\r\n"
        else:
            assert headers["mcp-session-id"] == "test-session"
            assert headers["mcp-protocol-version"] == "2025-11-25"
            if method == "notifications/initialized":
                assert "id" not in request
                return b"HTTP/1.1 202 Accepted\r\nContent-Length: 0\r\n" + static_headers + b"\r\n"
            if method == "tools/list":
                result = {"tools": self.catalogue}
            else:
                entity_id = request["params"]["arguments"]["entity_id"]
                assert method == "tools/call" and request["params"] == {
                    "name": "HassGetState", "arguments": {"entity_id": entity_id}}
                if entity_id == "light.kitchen":
                    result = {"content": [{"type": "text", "text": "Kitchen light is on"}]}
                else:
                    assert entity_id == "sensor.kitchen"
                    result = {"structuredContent": LARGE_TOOL_RESULT,
                              "content": [{"type": "text", "text": json.dumps(LARGE_TOOL_RESULT)}]}
        data = json.dumps({"jsonrpc": "2.0", "id": request["id"], "result": result}).encode()
        media = b"application/json"
        if method == "tools/list":
            data = b": fake Home Assistant\n\ndata: " + data + b"\n\n"
            media = b"text/event-stream"
        return (b"HTTP/1.1 200 OK\r\nContent-Type: " + media + b"\r\n" + static_headers +
                f"Content-Length: {len(data)}\r\n\r\n".encode() + data)


def mic_samples(event):
    pcm = base64.b64decode(event["audio"], validate=True)
    return struct.unpack(f"<{len(pcm) // 2}h", pcm)


def rms(samples):
    return sqrt(sum(sample * sample for sample in samples) / len(samples))


def speech_clip(name, start, duration, level, rate=CAPTURE_RATE):
    """Independent prerecorded speech for the room and loudspeaker, with a calibrated RMS."""
    data = (Path(__file__).resolve().parent.parent / "testdata" / name).read_bytes()
    count = int(duration * CAPTURE_RATE)
    clip = struct.unpack_from(f"<{count}h", data, int(start * CAPTURE_RATE) * 2)
    scale = level / rms(clip)
    output_count = int(duration * rate)
    return struct.pack(f"<{output_count}h", *(round(clip[i * CAPTURE_RATE // rate] * scale)
                                               for i in range(output_count)))


def replace_mic(path, pcm):
    # Keep the inode the fake capture backend opened, and its length/loop unchanged.
    assert path.stat().st_size == len(pcm)
    with path.open("r+b") as capture:
        capture.write(pcm)


async def vad_onset(session, frames, description):
    """A minimal provider VAD that starts a turn only after two voiced 20 ms frames."""
    voiced = 0
    for _ in range(frames):
        event = await asyncio.wait_for(session.receive(), TIMEOUT)
        assert event and event["type"] == "input_audio_buffer.append", (description, event)
        voiced = voiced + 1 if rms(mic_samples(event)) >= VAD_RMS_FLOOR else 0
        if voiced == 2:
            await session.send("input_audio_buffer.speech_started")
            return True
    return False


async def wait_for_quiet_pcm(session, floor=VAD_RMS_FLOOR):
    # Fake capture may still hold a block from before the in-place source change.
    quiet = 0
    for _ in range(60):
        event = await asyncio.wait_for(session.receive(), TIMEOUT)
        assert event and event["type"] == "input_audio_buffer.append", event
        quiet = quiet + 1 if rms(mic_samples(event)) < floor else 0
        if quiet == 10:
            return
    raise AssertionError("provider microphone did not settle after speech")


async def expect_echo_discarded(session, item_id):
    async def read_until_discarded():
        while True:
            event = await session.receive()
            assert event and event["type"] not in ("response.cancel", "response.create"), (
                "playback echo produced an interruption", event)
            if event["type"] == "conversation.item.delete":
                assert event["item_id"] == item_id
                return
    await asyncio.wait_for(read_until_discarded(), TIMEOUT)


async def check_talk_over(echo):
    capture = Path(echo.env["HASSMIC_CAP"])
    quiet = capture.read_bytes()
    first = speech_clip("alexa_espeak.raw", 5.0, 1.0, 1000)
    near = speech_clip("alexa_espeak.raw", 5.0, 1.0, 200)
    # The fake room's -25 dB direct echo of the 3500-RMS speaker is about 200 RMS,
    # plus two reflections. The overlapping near speech has comparable power.
    far = speech_clip("echo_espeak_en-us.raw", 4.8, 2.0, 3500, RATE) * 3
    ha = await home_assistant(echo)
    provider = Provider()
    tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    tls.load_cert_chain(echo.srv_pem)
    server = await asyncio.start_server(provider.handle, "127.0.0.1", 0, ssl=tls)
    port = server.sockets[0].getsockname()[1]
    try:
        async with server:
            settings = await asyncio.to_thread(echo.page.set, wake_sound="off", volume=100)
            assert settings["applied"] == 2, settings
            profile = candidate(read(echo.page), backend=["openai", "assist"], model="mock-realtime",
                                endpoint=f"wss://127.0.0.1:{port}/v1/realtime", barge_in=True,
                                sensitivity=10, api_key_action="replace")
            status, _, body = await asyncio.to_thread(save, echo.page, profile, KEY)
            assert status == 200, body
            echo.wake()
            session = await asyncio.wait_for(provider.sessions.get(), TIMEOUT)
            try:
                await session.expect("session.update")
                await session.send("session.updated")
                assert not await vad_onset(session, 10, "before the first request")
                replace_mic(capture, first)
                assert await vad_onset(session, 60, "first near speech"), "provider received no first request"
                replace_mic(capture, quiet)
                await wait_for_quiet_pcm(session)
                await session.committed_speech("talk_over_first", already_started=True)

                play = Path(echo.play)
                offset = play.stat().st_size
                await session.send_audio("talk_over_reply", "talk_over_audio", 0, far)
                assert await until(lambda: play.stat().st_size > offset + 4096), "reply never reached the speaker"
                # If fake VAD misclassifies echo, the local check must still veto it.
                # Otherwise force the same provider event once to exercise that check.
                if not await vad_onset(session, 45, "echo-only playback"):
                    await session.send("input_audio_buffer.speech_started")
                await session.send("input_audio_buffer.speech_stopped")
                await session.send("input_audio_buffer.committed", item_id="talk_over_false_onset")
                await expect_echo_discarded(session, "talk_over_false_onset")
                assert play.stat().st_size > offset + 4096

                replace_mic(capture, near)
                assert await vad_onset(session, 65, "near speech over playback"), "provider received no talk-over speech"
                await session.expect("response.cancel")
                await session.send("response.done", response={
                    "id": "talk_over_reply", "status": "cancelled", "output": [{
                        "type": "message", "id": "talk_over_audio", "role": "assistant",
                        "content": [{"type": "output_audio"}]}]})
                cut = await session.expect("conversation.item.truncate")
                assert cut["item_id"] == "talk_over_audio" and 0 < cut["audio_end_ms"] < 6000, cut
                assert 0 < play.stat().st_size - offset < len(far), "talk-over drained all queued reply audio"

                replace_mic(capture, quiet)
                await wait_for_quiet_pcm(session)
                await session.committed_speech("talk_over_follow_up", already_started=True)
                offset = play.stat().st_size
                follow_up = tone(RATE, 0.2, 510)
                await session.send_audio("talk_over_follow_up_reply", "talk_over_follow_up_audio", 0, follow_up, done=True)
                await session.send("response.done", response={"status": "completed", "output": []})
                assert await until(lambda: play.stat().st_size == offset + len(follow_up)), "follow-up audio did not play"
                assert not ha.va.started.is_set(), "talk-over started an Assist pipeline"
            finally:
                session.writer.close()
    finally:
        await ha.cli.disconnect()
    print("ok   playback echo rejected, similarly loud near speech interrupted, follow-up reply played")


def has_tone(samples, frequency):
    angular_step = 2 * pi * frequency / RATE
    real = sum(sample * cos(i * angular_step) for i, sample in enumerate(samples))
    imaginary = sum(sample * sin(i * angular_step) for i, sample in enumerate(samples))
    power = sum(sample * sample for sample in samples)
    return real * real + imaginary * imaginary > 0.35 * len(samples) * power


def capture_tone(frequency, peak=25):
    step = 2 * pi * frequency / CAPTURE_RATE
    return struct.pack(f"<{CAPTURE_RATE}h", *(round(peak * sin(i * step)) for i in range(CAPTURE_RATE)))


async def check_voip(echo):
    capture = Path(echo.env["HASSMIC_CAP"])
    hfp = Path(echo.env["HASSMIC_FAKE_HFP"])
    play = Path(echo.play)
    voip = Path(echo.env["HASSMIC_FAKE_VOIP_PLAY"])
    silence = bytes(CAPTURE_RATE * 2)
    provider = Provider()
    tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    tls.load_cert_chain(echo.srv_pem)
    server = await asyncio.start_server(provider.handle, "127.0.0.1", 0, ssl=tls)
    port = server.sockets[0].getsockname()[1]

    def hfp_frame(event, frequency=700):
        samples = mic_samples(event)
        # At sensitivity 10, the ordinary gate rejects PCM below roughly 890 RMS.
        return 2 < rms(samples) < 700 and has_tone(samples, frequency)

    ha = await home_assistant(echo)
    try:
        async with server:
            settings = await asyncio.to_thread(echo.page.set, wake_sound="off", volume=100)
            assert settings["applied"] == 2, settings
            profile = candidate(read(echo.page), backend=["openai", "assist"], model="mock-realtime",
                                endpoint=f"wss://127.0.0.1:{port}/v1/realtime", barge_in=True,
                                sensitivity=10, voip_audio=True, api_key_action="replace")
            status, _, body = await asyncio.to_thread(save, echo.page, profile, KEY)
            assert status == 200, body
            echo.wake()
            session = await asyncio.wait_for(provider.sessions.get(), TIMEOUT)
            try:
                await session.expect("session.update")
                await session.send("session.updated")
                await session.expect("input_audio_buffer.append", lambda event: has_tone(mic_samples(event), 400))
                assert not voip.exists(), "VoIP playback opened before the first reply"
                await session.send("input_audio_buffer.speech_started")
                replace_mic(capture, silence)
                await wait_for_quiet_pcm(session, floor=2)
                await session.committed_speech("voip_first", already_started=True)

                ordinary_size = play.stat().st_size
                far = tone(RATE, 4.0, 610)
                await session.send_audio("voip_answer", "voip_speech", 0, far)
                assert await until(lambda: voip.exists() and voip.stat().st_size > 4096)
                assert voip.read_bytes()[:4096] == far[:4096]
                assert play.stat().st_size == ordinary_size, "Realtime PCM went to the ordinary playback sink"

                replace_mic(hfp, capture_tone(700))
                await session.expect("input_audio_buffer.append", hfp_frame)
                await session.expect("input_audio_buffer.append", hfp_frame)
                await session.send("input_audio_buffer.speech_started")
                await session.expect("response.cancel")
                await session.send("response.done", response={
                    "id": "voip_answer", "status": "cancelled", "output": [{
                        "type": "message", "id": "voip_speech", "role": "assistant",
                        "content": [{"type": "output_audio"}]}]})
                cut = await session.expect("conversation.item.truncate")
                assert cut["item_id"] == "voip_speech" and 0 < cut["audio_end_ms"] < 4000, cut
                cut_size = voip.stat().st_size
                assert 0 < cut_size < len(far), "the VoIP interruption did not discard reply audio"

                replace_mic(hfp, capture_tone(800))
                await session.expect("input_audio_buffer.append", lambda event: hfp_frame(event, 800))
                replace_mic(hfp, silence)
                await wait_for_quiet_pcm(session, floor=2)
                await session.committed_speech("voip_next", already_started=True)
                follow_up = tone(RATE, 0.2, 510)
                await session.send_audio("voip_next_answer", "voip_next_speech", 0, follow_up, done=True)
                await session.send("response.done", response={"status": "completed", "output": []})
                assert await until(lambda: voip.stat().st_size >= cut_size + len(follow_up))
                assert voip.read_bytes()[cut_size:] == follow_up, "follow-up did not play cleanly on VoIP"
                assert play.stat().st_size == ordinary_size and not ha.va.started.is_set()

                await session.committed_speech("voip_end")
                await session.send("response.done", response={"status": "completed", "output": [{
                    "type": "function_call", "name": "end_conversation", "call_id": "end_voip", "arguments": "{}"}]})
                await session.closed()
            finally:
                session.writer.close()
    finally:
        await ha.cli.disconnect()
    print("ok   Realtime switched from ASR to VoIP/HFP, quiet speech interrupted playback, and the follow-up played")


async def check_independent_routing(echo, playback, microphone):
    hfp = Path(echo.env["HASSMIC_FAKE_HFP"])
    hfp_available = hfp.exists()
    tts = Path(echo.play)
    voip = Path(echo.env["HASSMIC_FAKE_VOIP_PLAY"])
    provider = Provider()
    tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    tls.load_cert_chain(echo.srv_pem)
    server = await asyncio.start_server(provider.handle, "127.0.0.1", 0, ssl=tls)
    port = server.sockets[0].getsockname()[1]
    async with server:
        await asyncio.to_thread(echo.page.set, wake_sound="off", volume=100)
        saved = read(echo.page)
        assert saved["voip_audio_supported"] and saved["hfp_audio_supported"]
        profile = candidate(saved, backend=["openai", "assist"], model="mock-realtime",
                            endpoint=f"wss://127.0.0.1:{port}/v1/realtime", barge_in=True, sensitivity=100,
                            playback_stream=playback, microphone_mode=microphone, api_key_action="replace")
        status, _, body = await asyncio.to_thread(save, echo.page, profile, KEY)
        assert status == 200, body
        echo.wake()
        session = await asyncio.wait_for(provider.sessions.get(), TIMEOUT)
        try:
            await session.expect("session.update")
            await session.send("session.updated")
            await session.expect("input_audio_buffer.append", lambda event: has_tone(mic_samples(event), 400))
            await session.committed_speech("independent_request")
            tts_offset = tts.stat().st_size
            reply = tone(RATE, 1.6, 610)
            await session.send_audio("independent_answer", "independent_audio", 0, reply, done=True)
            if playback == "tts":
                assert await until(lambda: tts.stat().st_size > tts_offset + 4096)
                assert tts.read_bytes()[tts_offset:tts_offset + 4096] == reply[:4096]
                assert not voip.exists(), "TTS opened VoIP playback"
            else:
                assert await until(lambda: voip.exists() and voip.stat().st_size > 4096)
                assert voip.read_bytes()[:4096] == reply[:4096]
                assert tts.stat().st_size == tts_offset
            # With both inputs open, use distinct tones to identify which one reaches the provider.
            if hfp_available:
                replace_mic(hfp, capture_tone(700, peak=400))
            selected, other = (700, 400) if microphone == "hfp" and hfp_available else (400, 700)
            await session.expect("input_audio_buffer.append", lambda event: has_tone(mic_samples(event), selected))
            await only_mic(session, 8, lambda samples: not has_tone(samples, other))
        finally:
            session.writer.close()
    expected_mic = microphone if hfp_available else "ASR fallback"
    print(f"ok   Realtime {playback} playback used the selected sink with only {expected_mic} sent to the provider")


async def expect_control(session, wanted):
    async def receive_control():
        while True:
            event = await session.receive()
            assert event, f"session ended before {wanted}"
            if event["type"] != "input_audio_buffer.append":
                return event
    event = await asyncio.wait_for(receive_control(), TIMEOUT)
    assert event["type"] == wanted, (wanted, event["type"])
    return event


async def expect_waiting_for_ack(task):
    finished, _ = await asyncio.wait((task,), timeout=0.08)
    if finished:
        raise AssertionError(f"control event before provider acknowledgement: {task.result()['type']}")


async def only_mic(session, count, check=lambda samples: True):
    for _ in range(count):
        event = await asyncio.wait_for(session.receive(), TIMEOUT)
        assert event and event["type"] == "input_audio_buffer.append", event and event["type"]
        assert check(mic_samples(event)), "unexpected microphone source"


async def silent_mic_for(session, seconds):
    clock = asyncio.get_running_loop().time
    deadline = clock() + seconds
    while clock() < deadline:
        await only_mic(session, 1, lambda pcm: not any(pcm))


def check_recovered_audio(item):
    assert item["type"] == "message" and item["role"] == "user", (item["type"], item["role"])
    audio, = item["content"]
    assert audio["type"] == "input_audio", audio["type"]
    samples = mic_samples(audio)
    step = RATE // 50
    markers = {frequency: [] for frequency in (700, 900, 1100, 1300, 1500)}
    for start in range(0, len(samples) - step + 1, step):
        frame = samples[start:start + step]
        for frequency in markers:
            if has_tone(frame, frequency):
                markers[frequency].append(start)
    assert len(markers[900]) >= 2 and len(markers[1100]) >= 2, markers
    assert max(markers[900]) < RATE <= min(markers[1100]), markers
    assert all(start >= RATE for start in markers[700]), "HFP audio leaked into the ASR lead-in"
    assert not markers[1300], "ASR audio leaked into the HFP tail"
    assert not markers[1500], "audio sent after the provider's speech end leaked into the replacement"


async def check_voip_recovery(echo, reject_replacement=False):
    capture = Path(echo.env["HASSMIC_CAP"])
    hfp = Path(echo.env["HASSMIC_FAKE_HFP"])
    speaker = Path(echo.env["HASSMIC_FAKE_VOIP_PLAY"])
    silence = bytes(CAPTURE_RATE * 2)
    provider = Provider()
    tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    tls.load_cert_chain(echo.srv_pem)
    server = await asyncio.start_server(provider.handle, "127.0.0.1", 0, ssl=tls)
    port = server.sockets[0].getsockname()[1]
    async with server:
        settings = await asyncio.to_thread(echo.page.set, wake_sound="off", volume=100)
        assert settings["applied"] == 2, settings
        profile = candidate(read(echo.page), backend=["openai", "assist"], model="mock-realtime",
                            endpoint=f"wss://127.0.0.1:{port}/v1/realtime", barge_in=True,
                            sensitivity=10, voip_audio=True, voip_asr_recovery=True, api_key_action="replace")
        status, _, body = await asyncio.to_thread(save, echo.page, profile, KEY)
        assert status == 200, body
        assert read(echo.page)["voip_asr_recovery"] is True
        echo.wake()
        session = await asyncio.wait_for(provider.sessions.get(), TIMEOUT)
        pending = None
        try:
            await session.expect("session.update")
            await session.send("session.updated")
            await session.expect("input_audio_buffer.append", lambda event: has_tone(mic_samples(event), 400))
            await session.send("input_audio_buffer.speech_started", item_id="asr_first")
            replace_mic(capture, silence)
            await wait_for_quiet_pcm(session, floor=2)
            await session.committed_speech("asr_first", already_started=True)

            await session.send_audio("asr_answer", "asr_answer_audio", 0, tone(RATE, 4, 610))
            assert await until(lambda: speaker.exists() and speaker.stat().st_size > 4096)
            replace_mic(hfp, capture_tone(650))
            await session.expect("input_audio_buffer.append", lambda event: has_tone(mic_samples(event), 650))
            replace_mic(hfp, silence)
            await wait_for_quiet_pcm(session, floor=2)
            await silent_mic_for(session, 0.35)

            # The prefix is captured by micAsr while only silent HFP reaches the provider.
            replace_mic(capture, capture_tone(900, peak=500))
            await silent_mic_for(session, 0.32)
            replace_mic(capture, silence)
            # Wait longer than the fake ASR file's read-ahead and capture block.
            await silent_mic_for(session, 0.24)
            replace_mic(hfp, capture_tone(700))
            for _ in range(2):
                await session.expect("input_audio_buffer.append", lambda event: has_tone(mic_samples(event), 700))
            original = "asr_interrupt_original"
            audio_start_ms = max(0, session.mic_frames * 1000 // RATE - 350)
            await session.send("input_audio_buffer.speech_started", item_id=original, audio_start_ms=audio_start_ms)
            await expect_control(session, "response.cancel")
            # The live HFP stream continues after the cut. A distinct ASR tone
            # verifies that the provider receives neither ASR nor a mix here.
            replace_mic(capture, capture_tone(1300, peak=500))
            replace_mic(hfp, capture_tone(1100))
            await session.send("response.done", response={"id": "asr_answer", "status": "cancelled", "output": [{
                "type": "message", "id": "asr_answer_audio", "role": "assistant",
                "content": [{"type": "output_audio"}]}]})
            truncate = await expect_control(session, "conversation.item.truncate")
            assert truncate["item_id"] == "asr_answer_audio" and 0 < truncate["audio_end_ms"] < 4000, truncate
            await session.expect("input_audio_buffer.append", lambda event: has_tone(mic_samples(event), 1100))
            await only_mic(session, 4, lambda pcm: has_tone(pcm, 1100) and not has_tone(pcm, 1300))
            replace_mic(hfp, silence)
            await wait_for_quiet_pcm(session, floor=2)
            replace_mic(capture, silence)
            if reject_replacement:
                await session.send("input_audio_buffer.speech_stopped", item_id=original)
            else:
                audio_end_ms = session.mic_frames * 1000 // RATE
                replace_mic(hfp, capture_tone(1500))
                await session.expect("input_audio_buffer.append", lambda event: has_tone(mic_samples(event), 1500))
                replace_mic(hfp, silence)
                await session.send("input_audio_buffer.speech_stopped", item_id=original, audio_end_ms=audio_end_ms)
            await session.send("input_audio_buffer.committed", item_id=original)
            pending = asyncio.create_task(expect_control(session, "conversation.item.create"))
            await expect_waiting_for_ack(pending)
            original_item = {"id": original, "type": "message", "role": "user", "status": "completed"}
            await session.send("conversation.item.added", item=original_item, previous_item_id="asr_answer_audio")
            await session.send("conversation.item.done", item=original_item, previous_item_id="asr_answer_audio")

            submitted = await pending
            assert submitted["previous_item_id"] == original, submitted["previous_item_id"]
            repaired = submitted["item"]
            assert repaired["id"].startswith("item_asr_"), repaired["id"]
            check_recovered_audio(repaired)
            if reject_replacement:
                await session.send("error", error={"type": "invalid_request_error", "code": "invalid_value",
                    "message": "test rejection of replacement item", "event_id": submitted["event_id"]})
                pending = asyncio.create_task(expect_control(session, "response.create"))
            else:
                pending = asyncio.create_task(expect_control(session, "conversation.item.delete"))
                await session.send("conversation.item.added", item={"id": "unrelated_audio", "status": "completed"},
                                   previous_item_id=original)
                await expect_waiting_for_ack(pending)
                await session.send("conversation.item.added", item=repaired | {"status": "in_progress"},
                                   previous_item_id=original)
                await expect_waiting_for_ack(pending)
                await session.send("conversation.item.done", item=repaired | {"status": "completed"},
                                   previous_item_id=original)
                deletion = await pending
                assert deletion["item_id"] == original, deletion
                pending = asyncio.create_task(expect_control(session, "response.create"))
                await expect_waiting_for_ack(pending)
                await session.send("conversation.item.deleted", item_id=original)
            await pending
            await only_mic(session, 4)

            replace_mic(hfp, capture_tone(800))
            offset = speaker.stat().st_size
            follow_up = tone(RATE, 0.8, 510)
            await session.send_audio("asr_follow_up", "asr_follow_up_audio", 0, follow_up, done=True)
            await session.expect("input_audio_buffer.append", lambda event: has_tone(mic_samples(event), 800))
            await session.send("response.done", response={"status": "completed", "output": []})
            assert await until(lambda: speaker.stat().st_size >= offset + len(follow_up))
            assert speaker.read_bytes()[offset:] == follow_up
        finally:
            if pending is not None and not pending.done():
                pending.cancel()
                await asyncio.gather(pending, return_exceptions=True)
            session.writer.close()
    if reject_replacement:
        print("ok   VoIP recovery kept the original item after a correlated provider rejection")
    else:
        print("ok   VoIP recovery joined the 1000 ms ASR lead-in to the uninterrupted HFP tail and trimmed at speech end")


async def main(echo):
    ha = await home_assistant(echo)
    echo.wake()
    await asyncio.wait_for(ha.va.started.wait(), TIMEOUT)
    assert await until(lambda: len(ha.va.mic) > 25600), "Assist stopped receiving microphone audio"
    ha.cli.send_voice_assistant_event(VoiceAssistantEventType.VOICE_ASSISTANT_RUN_END, None)
    await sync(ha.cli)
    ha.va.started.clear()

    provider = Provider()
    tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    tls.load_cert_chain(echo.srv_pem)
    server = await asyncio.start_server(provider.handle, "127.0.0.1", 0, ssl=tls)
    port = server.sockets[0].getsockname()[1]
    async with server:
        profile = candidate(read(echo.page), backend=["openai", "assist"], model="mock-realtime", sensitivity=100,
                            endpoint=f"wss://127.0.0.1:{port}/v1/realtime",
                            ha_endpoint=f"https://127.0.0.1:{port}/api/mcp/assist",
                            voice="cedar", language="pl-PL", reasoning_effort="low",
                            max_output_tokens=2048, noise_reduction="far_field",
                            vad_threshold=46, vad_prefix_padding_ms=350, vad_silence_duration_ms=650,
                            api_key_action="replace", ha_token_action="replace")
        status, _, body = await asyncio.to_thread(save, echo.page, profile, KEY, TOKEN)
        assert status == 200, body
        echo.wake()
        session = await asyncio.wait_for(provider.sessions.get(), TIMEOUT)
        try:
            await check_session(session, echo, ha)
            assert provider.tools == ["initialize", "notifications/initialized", "tools/list", "tools/call", "tools/call"]
        finally:
            session.writer.close()

        for model in LEGACY_MODELS:
            ha.cli.switch_command(ha.by["mute"].key, False)
            await sync(ha.cli)
            await check_semantic(provider, echo, ha, model)
    await ha.cli.disconnect()
    print("ok   Realtime settings, PCM, MCP, barge-in, effective mute and conversation ending")


async def check_semantic(provider, echo, ha, model):
    profile = candidate(read(echo.page), model=model, turn_detection="semantic_vad", semantic_eagerness="low",
                        ha_endpoint="", ha_token_action="clear",
                        max_output_tokens=0, noise_reduction="off")
    status, _, body = await asyncio.to_thread(save, echo.page, profile)
    assert status == 200, body
    assert json.loads(body)["reasoning_effort"] == "low"
    echo.wake()
    session = await asyncio.wait_for(provider.sessions.get(), TIMEOUT)
    try:
        assert session.model == model
        update = (await session.expect("session.update"))["session"]
        assert update["audio"]["input"]["turn_detection"] == {
            "type": "semantic_vad", "eagerness": "low", "create_response": False,
            "interrupt_response": False,
        }
        assert update["audio"]["input"]["noise_reduction"] is None
        assert "transcription" not in update["audio"]["input"]
        assert "max_output_tokens" not in update
        assert "reasoning" not in update, f"retained reasoning preference was sent to {model}"
        assert [tool["name"] for tool in update["tools"]] == ["end_conversation", "get_local_time"]
        assert "get_local_time before answering" in update["instructions"]
        await session.send("session.updated")
        await session.expect("input_audio_buffer.append")
        await session.committed_speech("semantic_request")
        await session.send("response.done", response={"status": "completed", "output": [{
            "type": "function_call", "name": "get_local_time", "call_id": "clock_test", "arguments": "{}"}]})
        clock = (await session.expect("conversation.item.create"))["item"]
        assert clock["call_id"] == "clock_test"
        date_time = datetime.fromisoformat(json.loads(clock["output"])["local_time"])
        assert date_time.utcoffset() == timedelta(hours=2, minutes=30)
        assert abs((date_time - datetime.now(timezone.utc)).total_seconds()) < TIMEOUT
        await session.expect("response.create")
        ha.cli.switch_command(ha.by["mute"].key, True)
        await session.closed()
    finally:
        session.writer.close()


async def check_session(session, echo, ha):
    assert session.model == "mock-realtime"
    update = (await session.expect("session.update"))["session"]
    pcm_format = {"type": "audio/pcm", "rate": RATE}
    assert update["audio"]["input"]["format"] == update["audio"]["output"]["format"] == pcm_format
    assert update["audio"]["output"]["voice"] == "cedar"
    assert update["reasoning"] == {"effort": "low"} and update["max_output_tokens"] == 2048
    assert update["audio"]["input"]["noise_reduction"] == {"type": "far_field"}
    assert "transcription" not in update["audio"]["input"]
    assert update["audio"]["input"]["turn_detection"] == {
        "type": "server_vad", "threshold": 0.46, "prefix_padding_ms": 350, "silence_duration_ms": 650,
        "create_response": False, "interrupt_response": False,
    }
    assert "pl-PL" in update["instructions"]
    tools = [tool["name"] for tool in update["tools"]]
    assert len(tools) == FILLER_TOOL_COUNT + 3 and tools[:2] == ["end_conversation", "get_local_time"]
    assert tools[-1] == "HassGetState", "the late MCP tool did not reach the provider"
    await session.send("session.updated")

    def voiced(event):
        return max(mic_samples(event)) > 500

    def capture_tone(event):
        mic = mic_samples(event)
        rises = [i for i in range(1, len(mic)) if mic[i - 1] <= 0 < mic[i]]
        periods = [b - a for a, b in zip(rises, rises[1:])]
        return len(periods) >= 4 and any(abs(p - 60) <= 1 for p in periods)

    # The fake microphone also hears the wake chime. Wait for the capture fixture's 400 Hz tone.
    mic = mic_samples(await session.expect("input_audio_buffer.append", capture_tone))
    assert 479 <= len(mic) <= 480, "20 ms at 24 kHz should contain 480 samples"
    assert not ha.va.started.is_set(), "native wake also started an Assist pipeline"
    await session.send("input_audio_buffer.committed", item_id="discarded_request")
    discard = await session.expect("conversation.item.delete")
    assert discard["item_id"] == "discarded_request"
    await session.committed_speech("first_request")

    play = Path(echo.play)
    preamble_offset = play.stat().st_size
    first_preamble, second_preamble = tone(RATE, 0.4, 600), tone(RATE, 0.25, 900)
    await session.send_audio("tool_response", "first_preamble", 0, first_preamble, done=True)
    await session.send_audio("tool_response", "second_preamble", 1, second_preamble, done=True)
    await session.send("response.done", response={"id": "tool_response", "status": "completed", "output": [{
        "type": "message", "id": "first_preamble", "role": "assistant", "phase": "commentary",
        "content": [{"type": "output_audio"}]}, {
        "type": "message", "id": "second_preamble", "role": "assistant", "phase": "commentary",
        "content": [{"type": "output_audio"}]}, {
        "type": "function_call", "name": "HassGetState", "call_id": "light_test",
        "arguments": '{"entity_id":"light.kitchen"}'}, {
        "type": "function_call", "name": "HassGetState", "call_id": "sensor_test",
        "arguments": '{"entity_id":"sensor.kitchen"}'}]})
    tool = (await session.expect("conversation.item.create"))["item"]
    assert tool["type"] == "function_call_output" and tool["call_id"] == "light_test"
    assert json.loads(tool["output"])["content"][0]["text"] == "Kitchen light is on"
    tool = (await session.expect("conversation.item.create"))["item"]
    assert tool["type"] == "function_call_output" and tool["call_id"] == "sensor_test"
    assert json.loads(tool["output"]) == LARGE_TOOL_RESULT, "expected only the oversized result's structured content"
    await session.expect("response.create")
    reply_offset = play.stat().st_size
    assert reply_offset - preamble_offset == len(first_preamble) + len(second_preamble), "part of the tool response audio was lost"

    reply = tone(RATE, 2, 750)
    await session.send_audio("answer_response", "first_answer", 0, reply, done=True)
    await session.send_audio("answer_response", "unheard_answer", 1, tone(RATE, 0.3, 950))
    assert await until(lambda: play.stat().st_size - reply_offset > 4096), "no Realtime PCM reached the speaker"
    await session.expect("input_audio_buffer.append", voiced)
    await session.send("input_audio_buffer.speech_started")
    await session.expect("response.cancel")
    await session.send("response.done", response={"id": "answer_response", "status": "cancelled", "output": [{
        "type": "message", "id": "first_answer", "role": "assistant", "status": "completed",
        "content": [{"type": "output_audio"}]}, {
        "type": "message", "id": "unheard_answer", "role": "assistant", "status": "incomplete",
        "content": [{"type": "output_audio"}]}]})
    cut = await session.expect("conversation.item.truncate")
    assert cut["item_id"] == "first_answer" and cut["content_index"] == 0
    assert 0 < cut["audio_end_ms"] < 2000, cut
    unheard = await session.expect("conversation.item.truncate")
    assert unheard["item_id"] == "unheard_answer" and unheard["content_index"] == 0
    assert unheard["audio_end_ms"] == 0, unheard
    assert 0 < play.stat().st_size - reply_offset < len(reply), "barge-in drained the queued answer"

    await session.expect("input_audio_buffer.append", voiced)
    await session.committed_speech("second_request", already_started=True)
    await session.send("response.done", response={"status": "completed", "output": [{
        "type": "function_call", "name": "end_conversation", "call_id": "end_test", "arguments": "{}"}]})
    await session.closed()
    assert not ha.va.started.is_set(), "Realtime turn leaked into Assist"


def with_failure_log(test):
    async def wrapped(echo):
        try:
            await test(echo)
        except BaseException:
            print(echo.log()[-8000:], file=sys.stderr)
            raise
    return wrapped


if __name__ == "__main__":
    with tempfile.TemporaryDirectory() as folder:
        capture = Path(folder) / "mic.raw"
        capture.write_bytes(tone(CAPTURE_RATE, 0.2, 400))
        env = {"HASSMIC_CAP": str(capture), "HASSMIC_CLOCK_SYNCED": "1", "TZ": "FIX-2:30"}
        try:
            run(with_failure_log(main), PORTS, env, log=True)
        except SystemExit as result:
            if result.code:
                raise
        capture.write_bytes(bytes(CAPTURE_RATE * 2))
        try:
            run(with_failure_log(check_talk_over), TALK_OVER_PORTS, env | {"HASSMIC_FAKE_ECHO": "64:-25"}, log=True)
        except SystemExit as result:
            if result.code:
                raise
        hfp = Path(folder) / "hfp.raw"
        hfp.write_bytes(bytes(CAPTURE_RATE * 2))
        capture.write_bytes(tone(CAPTURE_RATE, 1.0, 400))
        env |= {"HASSMIC_FAKE_VOIP": "1", "HASSMIC_FAKE_HFP": str(hfp)}
        try:
            run(with_failure_log(check_voip), VOIP_PORTS,
                env | {"HASSMIC_FAKE_VOIP_PLAY": str(Path(folder) / "voip.raw")}, log=True)
        except SystemExit as result:
            if result.code:
                raise
        for ports, reject in ((RECOVERY_PORTS, False), (RECOVERY_ERROR_PORTS, True)):
            hfp.write_bytes(bytes(CAPTURE_RATE * 2))
            capture.write_bytes(tone(CAPTURE_RATE, 1.0, 400))
            try:
                run(with_failure_log(lambda echo: check_voip_recovery(echo, reject)), ports,
                    env | {"HASSMIC_FAKE_VOIP_PLAY": str(Path(folder) / f"recovery-voip-{ports[0]}.raw")}, log=True)
            except SystemExit as result:
                if result.code:
                    raise
        routes = (("tts", "hfp", hfp), ("voip", "asr", hfp), ("voip", "hfp", hfp.with_suffix(".missing")))
        for ports, (playback, microphone, hfp_path) in zip(INDEPENDENT_ROUTE_PORTS, routes):
            hfp.write_bytes(bytes(CAPTURE_RATE * 2))
            capture.write_bytes(tone(CAPTURE_RATE, 1.0, 400))
            try:
                run(with_failure_log(lambda echo: check_independent_routing(echo, playback, microphone)), ports,
                    env | {"HASSMIC_FAKE_HFP": str(hfp_path),
                           "HASSMIC_FAKE_VOIP_PLAY": str(Path(folder) / f"route-voip-{ports[0]}.raw")}, log=True)
            except SystemExit as result:
                if result.code:
                    raise
