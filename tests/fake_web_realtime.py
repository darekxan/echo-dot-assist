#!/usr/bin/env python3
"""Signed Realtime settings and saved audio routes."""
import json
import os
from pathlib import Path
import stat
import subprocess
import tempfile

from webclient import Browser, req, wait_port

ROOT = Path(__file__).resolve().parent.parent
PORT, WEB = 17146, 17147
PATH = "/api/realtime"
KEY = b"sk-realtime-test-1"
TOKEN = b"ey-realtime-test-2"
INSTRUCTIONS = "Pisz po polsku.\nMów krótko."
OPENAI_DEFAULTS = {
    "max_output_tokens": 0,
    "reasoning_effort": "",
    "noise_reduction": "",
    "voip_audio": False,
    "voip_asr_recovery": False,
    "turn_detection": "server_vad",
    "semantic_eagerness": "auto",
    "vad_threshold": 50,
    "vad_prefix_padding_ms": 300,
    "vad_silence_duration_ms": 500,
}
OPENAI_TUNED = {
    "max_output_tokens": 1024,
    "reasoning_effort": "low",
    "noise_reduction": "far_field",
    "voip_audio": True,
    "voip_asr_recovery": True,
    "turn_detection": "semantic_vad",
    "semantic_eagerness": "low",
    "vad_threshold": 55,
    "vad_prefix_padding_ms": 350,
    "vad_silence_duration_ms": 700,
}
ROUTING_DEFAULTS = {"playback_stream": "tts", "microphone_mode": "asr"}
ROUTING_LEGACY = {"playback_stream": "voip", "microphone_mode": "hfp_asr_recovery"}


def candidate(saved, **values):
    profile = {k: saved[k] for k in ("revision", "backend", "endpoint", "model", "voice", "language",
                                    "idle_seconds", "barge_in", "sensitivity", "ha_endpoint")}
    profile.update(instructions_hex=saved["instructions"].encode().hex(),
                   api_key_action="keep", ha_token_action="keep")
    return profile | values


def save(browser, profile, key=b"", token=b""):
    def body(counter):
        request = profile.copy()
        if key or token:
            clear = key + b"\0" + token + b"\0"
            request["secret_box"] = browser.seal(counter, clear.ljust((len(clear) + 63) // 64 * 64, b"\0")).hex()
        return json.dumps(request).encode()
    return browser.call("POST", PATH, body)


def read(browser):
    status, _, body = browser.call("GET", PATH)
    assert status == 200
    return json.loads(body)


def check_routing(browser, path, saved):
    # A playback-only edit must leave the selected microphone mode intact.
    status, _, body = save(browser, candidate(saved, playback_stream="tts"))
    assert status == 200, body
    saved = json.loads(body)
    assert saved["playback_stream"] == "tts" and saved["microphone_mode"] == "hfp_asr_recovery"

    # Old pages echo the legacy booleans on save. An unchanged pair keeps the independent selection.
    status, _, body = save(browser, candidate(saved, voip_audio=False, voip_asr_recovery=True))
    assert status == 200, body
    saved = json.loads(body)
    assert saved["playback_stream"] == "tts" and saved["microphone_mode"] == "hfp_asr_recovery"

    for playback, microphone in (("tts", "hfp"), ("voip", "asr")):
        status, _, body = save(browser, candidate(saved, playback_stream=playback, microphone_mode=microphone))
        assert status == 200, body
        saved = json.loads(body)
        for profile in (saved, read(browser), json.loads(path.read_bytes())):
            assert profile["playback_stream"] == playback and profile["microphone_mode"] == microphone
        assert saved["voip_audio"] == (playback == "voip") and saved["voip_asr_recovery"] is False

    # While disabled, old pages could keep a latent recovery preference.
    status, _, body = save(browser, candidate(saved, voip_audio=False, voip_asr_recovery=True))
    assert status == 200, body
    saved = json.loads(body)
    assert {k: saved[k] for k in ROUTING_DEFAULTS} == ROUTING_DEFAULTS
    assert saved["voip_asr_recovery"] is True
    status, _, body = save(browser, candidate(saved, voip_audio=True))
    assert status == 200, body
    saved = json.loads(body)
    assert {k: saved[k] for k in ROUTING_LEGACY} == ROUTING_LEGACY


def start(directory, log):
    env = dict(os.environ, HASSMIC_STATE=str(directory), HASSMIC_MDNS_FILE=str(directory / "none"),
               HASSMIC_CAP=str(ROOT / "testdata/alexa_espeak.raw"), HASSMIC_PLAY=str(directory / "play.raw"))
    proc = subprocess.Popen([str(ROOT / "build/hassmic-host"), "-p", str(PORT), "-W", str(WEB),
                             "-n", "Echo Realtime Settings", "-L", "-z", "0", "-o", "0",
                             "-a", "0", "-i", "0", "-B"], env=env, stdout=log, stderr=log)
    try:
        wait_port(PORT, WEB)
        return proc
    except BaseException:
        terminate(proc)
        raise


def terminate(proc):
    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()


def main():
    with tempfile.TemporaryDirectory() as name:
        directory = Path(name) / "state"
        directory.mkdir()
        with (directory / "log").open("w+") as log:
            proc = start(directory, log)
            try:
                assert req(WEB, "GET", PATH)[0] == 401
                assert req(WEB, "POST", PATH, b"{}")[0] == 401
                browser = Browser(WEB)
                assert browser.login_with_button(proc)
                default = read(browser)
                assert default["backend"] == ["assist", "assist"]
                assert isinstance(default["voip_audio_supported"], bool)
                assert isinstance(default["hfp_audio_supported"], bool)
                assert not default["api_key_set"] and not default["ha_token_set"]
                assert {k: default[k] for k in OPENAI_DEFAULTS} == OPENAI_DEFAULTS
                assert {k: default[k] for k in ROUTING_DEFAULTS} == ROUTING_DEFAULTS
                for kind in ("openai", "home-assistant"):
                    assert json.loads(browser.call("POST", f"{PATH}/test/{kind}")[2])["status"] == "not_configured"

                edited = candidate(default, backend=["assist", "openai"], language="pl-PL",
                                   model="gpt-realtime-2.1-mini", voice="cedar",
                                   instructions_hex=INSTRUCTIONS.encode().hex(), idle_seconds=1,
                                   ha_endpoint="https://ha.example/api/mcp/assist",
                                   api_key_action="replace", ha_token_action="replace", **OPENAI_TUNED)
                status, _, body = save(browser, edited, KEY, TOKEN)
                assert status == 200, body
                saved = json.loads(body)
                assert saved["backend"] == ["assist", "openai"] and saved["idle_seconds"] == 1
                assert saved["instructions"] == INSTRUCTIONS
                assert saved["api_key_set"] and saved["ha_token_set"]
                assert {k: saved[k] for k in OPENAI_TUNED} == OPENAI_TUNED
                assert {k: saved[k] for k in ROUTING_LEGACY} == ROUTING_LEGACY
                assert KEY not in body and TOKEN not in body
                assert KEY not in browser.call("GET", "/api/export")[2]
                assert TOKEN not in browser.call("GET", "/api/state")[2]
                path = directory / "realtime.json"
                assert stat.S_IMODE(path.stat().st_mode) == 0o600
                before = path.read_bytes()
                assert KEY in before and TOKEN in before

                assert save(browser, edited, KEY, TOKEN)[0] == 409
                assert save(browser, candidate(saved, endpoint="wss://relay.example/v1/realtime"))[0] == 400
                assert save(browser, candidate(saved, ha_endpoint="https://other.example/api/mcp/assist"))[0] == 400
                assert save(browser, candidate(saved, endpoint="wss://user@relay.example/v1/realtime",
                                               api_key_action="replace"), KEY)[0] == 400
                assert save(browser, candidate(saved, api_key_action="replace"), KEY + b"\r\nHeader: bad")[0] == 400
                for field, invalid in {
                    "idle_seconds": 0,
                    "max_output_tokens": 4097,
                    "reasoning_effort": "extra",
                    "noise_reduction": "medium",
                    "voip_audio": "true",
                    "voip_asr_recovery": "true",
                    "playback_stream": "music",
                    "microphone_mode": "micRaw",
                    "turn_detection": "automatic",
                    "semantic_eagerness": "",
                    "vad_threshold": 101,
                    "vad_prefix_padding_ms": 2001,
                    "vad_silence_duration_ms": 99,
                }.items():
                    assert save(browser, candidate(saved, **{field: invalid}))[0] == 400, field
                assert path.read_bytes() == before
                assert read(browser) == saved
                # Pre-upgrade settings pages only send the old fields.
                status, _, body = save(browser, candidate(saved, sensitivity=80))
                assert status == 200, body
                from_old_client = json.loads(body)
                assert from_old_client["sensitivity"] == 80
                assert {k: from_old_client[k] for k in OPENAI_TUNED} == OPENAI_TUNED
                check_routing(browser, path, from_old_client)
            finally:
                terminate(proc)

            # A saved profile from the previous release contains only the legacy route fields.
            previous = json.loads(path.read_bytes())
            for field in ROUTING_DEFAULTS:
                del previous[field]
            path.write_text(json.dumps(previous))
            proc = start(directory, log)
            try:
                restarted = read(browser)
                assert restarted["instructions"] == INSTRUCTIONS and restarted["backend"] == ["assist", "openai"]
                assert {k: restarted[k] for k in OPENAI_TUNED} == OPENAI_TUNED
                assert {k: restarted[k] for k in ROUTING_LEGACY} == ROUTING_LEGACY
            finally:
                terminate(proc)

            legacy = json.loads(path.read_bytes())
            for field in OPENAI_DEFAULTS:
                del legacy[field]
            path.write_text(json.dumps(legacy))
            proc = start(directory, log)
            try:
                browser = Browser(WEB)
                assert browser.login_with_button(proc)
                migrated = read(browser)
                assert migrated["backend"] == ["assist", "openai"]
                assert migrated["api_key_set"] and migrated["ha_token_set"]
                assert migrated["language"] == "pl-PL" and migrated["instructions"] == INSTRUCTIONS
                assert {k: migrated[k] for k in OPENAI_DEFAULTS} == OPENAI_DEFAULTS
                assert {k: migrated[k] for k in ROUTING_DEFAULTS} == ROUTING_DEFAULTS
                status, _, body = save(browser, candidate(migrated, api_key_action="clear",
                                                         ha_token_action="clear", ha_endpoint=""))
                assert status == 200, body
                cleared = json.loads(body)
                assert cleared["backend"] == ["assist", "assist"]
                assert not cleared["api_key_set"] and not cleared["ha_token_set"]
                assert KEY not in path.read_bytes() and TOKEN not in path.read_bytes()
                assert KEY not in (directory / "log").read_bytes() and TOKEN not in (directory / "log").read_bytes()
            finally:
                terminate(proc)
    print("ok   signed Realtime settings and saved audio routes")


if __name__ == "__main__":
    main()
