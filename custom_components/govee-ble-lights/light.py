from __future__ import annotations

import array
import asyncio
import logging
import re

from enum import IntEnum
import bleak_retry_connector

from bleak import BleakClient
from homeassistant.components import bluetooth
from homeassistant.components.light import (ATTR_BRIGHTNESS, ATTR_RGB_COLOR, ATTR_EFFECT, ColorMode, LightEntity,
                                            LightEntityFeature, ATTR_COLOR_TEMP_KELVIN)

from homeassistant.core import HomeAssistant, callback
from homeassistant.config_entries import ConfigEntry
from homeassistant.helpers.storage import Store

from .const import DOMAIN
from pathlib import Path
import json
from .govee_utils import prepareMultiplePacketsData
import base64
from . import Hub

_LOGGER = logging.getLogger(__name__)

UUID_CONTROL_CHARACTERISTIC = '00010203-0405-0607-0809-0a0b0c0d2b11'
EFFECT_PARSE = re.compile("\[(\d+)/(\d+)/(\d+)/(\d+)]")
SEGMENTED_MODELS = ['H6053', 'H6072', 'H6102', 'H6199', 'H617A', 'H617C']
PERCENT_MODELS = ['H617A']

# Models whose BLE advertisement manufacturer_data encodes live on/off state.
# The integration writes to the bulb over GATT without response, so a dropped
# write is silent — HA's optimistic state can diverge from reality. Reading
# the bulb's own broadcast reconciles it within one advertisement interval.
ADVERT_STATE_MODELS = {"H617A", "H617C"}
# H617A/C broadcast `ec 00 0a 01 <state>` under mfr id 0x0288 or 0x0388,
# where <state> is 0x01 on / 0x00 off. Match on the prefix, not the mfr id.
_ADVERT_GOVEE_EC_PREFIX = b"\xec\x00\x0a\x01"

# Verify-and-retry budget for POWER writes on advert-capable models. Each
# attempt writes a POWER packet and then waits up to _CONFIRM_TIMEOUT_S
# for an advertisement whose decoded state matches. A typical H617A
# broadcasts every 3-5s, so 10s per attempt gives two advertising windows.
_POWER_WRITE_ATTEMPTS = 3
_CONFIRM_TIMEOUT_S = 10.0

# Retry budget for fire-and-forget writes (brightness / rgb / effect). The
# bulb doesn't broadcast these values, so we can't verify they landed —
# but we can retry on connect/write exceptions so a transient slot
# exhaustion or mid-write disconnect doesn't silently drop the command.
_FIRE_AND_FORGET_ATTEMPTS = 3

# Hard outer bound on connect / disconnect. bleak_retry_connector has its
# own internal retries without a total budget; observed under concurrent
# stress a single _connectBluetooth() took 129s and a client.disconnect()
# took 9s. Wrapping these in asyncio.wait_for prevents one wedged proxy
# from cascading into service-call timeouts several minutes long.
_CONNECT_TIMEOUT_S = 15.0
_DISCONNECT_TIMEOUT_S = 3.0

# Durable-retry backoff schedule for background pending-state worker.
# When a service-call-level write fails (3 attempts exhausted), the user's
# intent is parked on self._pending_state and a background task retries
# at these delays until either an advert confirms the target OR a newer
# user intent supersedes. Covers the case where a cooperating integration
# (e.g. adaptive_lighting) keeps flipping the bulb back between our
# retries — we just keep trying until we win a window.
_PENDING_RETRY_BACKOFF_S = (60, 120, 300, 600, 1800)


def _decode_advert_state(service_info) -> bool | None:
    """Return True/False if any manufacturer_data entry matches the Govee
    on/off pattern; None if no entry is recognizable."""
    for mfr_bytes in (service_info.manufacturer_data or {}).values():
        if len(mfr_bytes) == 5 and mfr_bytes[:4] == _ADVERT_GOVEE_EC_PREFIX:
            return bool(mfr_bytes[4])
    return None

class LedCommand(IntEnum):
    """ A control command packet's type. """
    POWER = 0x01
    BRIGHTNESS = 0x04
    COLOR = 0x05


class LedMode(IntEnum):
    """
    The mode in which a color change happens in.
    
    Currently only manual is supported.
    """
    MANUAL = 0x02
    MICROPHONE = 0x06
    SCENES = 0x05
    SEGMENTS = 0x15


async def async_setup_entry(hass: HomeAssistant, config_entry: ConfigEntry, async_add_entities):
    if config_entry.entry_id in hass.data[DOMAIN]:
        hub: Hub = hass.data[DOMAIN][config_entry.entry_id]
    else:
        return

    if hub.devices is not None:
        devices = hub.devices
        for device in devices:
            if device['type'] == 'devices.types.light':
                _LOGGER.info("Adding device: %s", device)
                async_add_entities([GoveeAPILight(hub, device)])
    elif hub.address is not None:
        ble_device = bluetooth.async_ble_device_from_address(hass, hub.address.upper(), False)
        async_add_entities([GoveeBluetoothLight(hub, ble_device, config_entry)])


class GoveeAPILight(LightEntity, dict):
    _attr_color_mode = ColorMode.RGB

    def __init__(self, hub: Hub, device: dict) -> None:
        """Initialize an API light."""
        super().__init__()

        self.hub = hub

        self._state = None
        self._brightness = None

        self.device_data = device
        self.sku = self.device_data["sku"]
        self.device = self.device_data["device"]

        self._attr_name = device["deviceName"]

        color_modes: set[ColorMode] = set()

        for cap in device["capabilities"]:
            if cap['instance'] == 'powerSwitch':
                color_modes.add(ColorMode.ONOFF)
            if cap['instance'] == 'brightness':
                color_modes.add(ColorMode.BRIGHTNESS)
            if cap['instance'] == 'colorTemperatureK':
                color_modes.add(ColorMode.COLOR_TEMP)
            if cap['instance'] == 'colorRgb':
                color_modes.add(ColorMode.RGB)
            if cap['instance'] == 'lightScene':
                self._attr_supported_features = LightEntityFeature(
                    LightEntityFeature.EFFECT
                )

        if ColorMode.ONOFF in color_modes:
            self._attr_supported_color_modes = {ColorMode.ONOFF}
        if ColorMode.BRIGHTNESS in color_modes:
            self._attr_supported_color_modes = {ColorMode.BRIGHTNESS}
        if ColorMode.COLOR_TEMP in color_modes:
            self._attr_supported_color_modes = {ColorMode.COLOR_TEMP}
        if ColorMode.RGB in color_modes:
            self._attr_supported_color_modes = {ColorMode.RGB}

        self._state = None
        self._brightness = None

    async def async_update(self):
        """Retrieve latest state."""
        _LOGGER.info("Updating device: %s", self.device_data)

        if LightEntityFeature.EFFECT in self.supported_features_compat:
            if self._attr_effect_list is None or len(self._attr_effect_list) == 0:
                _LOGGER.info("Updating device effects: %s", self.device_data)

                store = Store(self.hass, 1, f"{DOMAIN}/effect_list_{self.sku}.json")
                scenes = await self.hub.api.list_scenes(self.sku, self.device)

                await store.async_save(scenes)

                self._attr_effect_list = [scene['name'] for scene in scenes]

    @property
    def name(self) -> str:
        return self._attr_name

    @property
    def unique_id(self) -> str:
        return self.device

    @property
    def brightness(self):
        return self._brightness

    @property
    def is_on(self) -> bool | None:
        return self._state

    async def async_turn_on(self, **kwargs) -> None:
        self._state = True

        if ATTR_BRIGHTNESS in kwargs:
            brightness = kwargs.get(ATTR_BRIGHTNESS, 255)
            self._brightness = brightness
            await self.hub.api.set_brightness(self.sku, self.device, (brightness / 255) * 100)

        if ATTR_RGB_COLOR in kwargs:
            red, green, blue = kwargs.get(ATTR_RGB_COLOR)
            await self.hub.api.set_color_rgb(self.sku, self.device, red, green, blue)

        if ATTR_COLOR_TEMP_KELVIN in kwargs:
            kelvin = kwargs.get(ATTR_COLOR_TEMP_KELVIN)
            await self.hub.api.set_color_temp(self.sku, self.device, kelvin)

        if ATTR_EFFECT in kwargs:
            effect_name = kwargs.get(ATTR_EFFECT)
            store = Store(self.hass, 1, f"{DOMAIN}/effect_list_{self.sku}.json")
            scenes = (
                scene for scene in await store.async_load()
                if scene['name'] == effect_name
            )
            scene = next(scenes)
            _LOGGER.info("Set scene: %s", scene)
            await self.hub.api.set_scene(self.sku, self.device, scene['value'])

        await self.hub.api.toggle_power(self.sku, self.device, 1)

    async def async_turn_off(self, **kwargs) -> None:
        self._state = False
        await self.hub.api.toggle_power(self.sku, self.device, 0)


class GoveeBluetoothLight(LightEntity):
    _attr_color_mode = ColorMode.RGB
    _attr_supported_color_modes = {ColorMode.RGB}
    _attr_supported_features = LightEntityFeature(
        LightEntityFeature.EFFECT | LightEntityFeature.FLASH | LightEntityFeature.TRANSITION)

    def __init__(self, hub: Hub, ble_device, config_entry: ConfigEntry) -> None:
        """Initialize an bluetooth light."""
        self._mac = hub.address
        self._model = config_entry.data["model"]
        self._is_segmented = self._model in SEGMENTED_MODELS
        self._use_percent = self._model in PERCENT_MODELS
        self._advert_state_supported = self._model in ADVERT_STATE_MODELS
        self._unsub_advert = None
        self._ble_device = ble_device
        self._state = None
        self._brightness = None
        self._rgb_color = None
        # Verify-and-retry coordination. _expected_state is the power state we
        # are currently commanding; _state_confirmed fires when an advert
        # arrives whose decoded state matches _expected_state.
        self._expected_state: bool | None = None
        self._state_confirmed: asyncio.Event | None = None
        # Per-entity lock serializing turn_on / turn_off. HA's service
        # dispatcher does NOT guarantee serialization at the entity level —
        # three parallel light.turn_on on the same entity (e.g. an
        # adaptive_lighting intercept firing alongside a bare call) run
        # concurrently and race on _expected_state / _state_confirmed AND
        # spawn concurrent GATT sessions on the same proxy. The lock
        # collapses duplicates: first acquirer runs the write loop, later
        # acquirers see _state already at target and early-out.
        self._entity_lock: asyncio.Lock = asyncio.Lock()
        # Durable pending-state intent. When a service-call-level turn_on/
        # turn_off exhausts its 3-attempt retries (e.g. because another
        # integration like adaptive_lighting keeps re-flipping the bulb
        # between our writes), the desired power state is parked here and
        # a background task retries with exponential backoff until an
        # advert confirms the target OR a newer user intent supersedes.
        # Cleared on advert-match (in _apply_advert_state) or on success.
        self._pending_state: bool | None = None
        self._pending_task: asyncio.Task | None = None

    def _canonical_mac(self) -> str:
        raw = (self._mac or "").replace(":", "").upper()
        return ":".join(raw[i:i + 2] for i in range(0, 12, 2))

    async def async_added_to_hass(self) -> None:
        await super().async_added_to_hass()
        if not self._advert_state_supported:
            return

        address = self._canonical_mac()
        # Seed from HA's cached last service info so state is correct
        # immediately after restart, not after the next advertisement.
        last = bluetooth.async_last_service_info(self.hass, address, connectable=False)
        if last is not None and self._apply_advert_state(last):
            self.async_write_ha_state()

        self._unsub_advert = bluetooth.async_register_callback(
            self.hass,
            self._async_handle_advertisement,
            bluetooth.BluetoothCallbackMatcher(address=address),
            bluetooth.BluetoothScanningMode.PASSIVE,
        )

    async def async_will_remove_from_hass(self) -> None:
        if self._unsub_advert is not None:
            self._unsub_advert()
            self._unsub_advert = None
        if self._pending_task is not None:
            self._pending_task.cancel()
            self._pending_task = None
        self._pending_state = None
        await super().async_will_remove_from_hass()

    def _ensure_pending_worker(self) -> None:
        """Start the background pending-state retry task if not running."""
        if self._pending_task is not None and not self._pending_task.done():
            return
        self._pending_task = self.hass.async_create_task(
            self._pending_retry_worker(),
            name=f"govee-ble-lights pending retry {self._canonical_mac()}",
        )

    async def _pending_retry_worker(self) -> None:
        """Background worker: keep trying to reach self._pending_state
        until an advert confirms it or a newer intent supersedes.

        Exponential backoff per _PENDING_RETRY_BACKOFF_S. Wakes up and
        retries under the entity lock, so it yields to in-flight user
        service calls. Each retry is a single POWER batch — if other
        packets (brightness/rgb) were also pending, they're lost; the
        durability contract is only for POWER state.
        """
        mac = self._canonical_mac()
        attempt = 0
        while self._pending_state is not None:
            idx = min(attempt, len(_PENDING_RETRY_BACKOFF_S) - 1)
            delay = _PENDING_RETRY_BACKOFF_S[idx]
            _LOGGER.info(
                "govee-ble-lights: %s pending %s — retry #%d in %ds",
                mac, self._pending_state, attempt + 1, delay,
            )
            try:
                await asyncio.sleep(delay)
            except asyncio.CancelledError:
                return
            target = self._pending_state
            if target is None:
                return
            if self._state == target:
                self._pending_state = None
                return
            async with self._entity_lock:
                target = self._pending_state
                if target is None:
                    return
                if self._state == target:
                    self._pending_state = None
                    return
                batch = [
                    self._prepareSinglePacketData(
                        LedCommand.POWER, [0x1 if target else 0x0],
                    )
                ]
                try:
                    await self._batch_write_and_confirm(
                        batch, want_on=target, need_confirm=True,
                    )
                    self._pending_state = None
                    _LOGGER.info(
                        "govee-ble-lights: %s pending %s succeeded on retry #%d",
                        mac, target, attempt + 1,
                    )
                    return
                except Exception as exc:
                    _LOGGER.warning(
                        "govee-ble-lights: %s pending %s retry #%d failed: %s",
                        mac, target, attempt + 1, exc,
                    )
                    attempt += 1

    @callback
    def _async_handle_advertisement(self, service_info, change) -> None:
        if self._apply_advert_state(service_info):
            self.async_write_ha_state()

    def _apply_advert_state(self, service_info) -> bool:
        """Update self._state from a BLE advert. Returns True on change.

        While a POWER write is pending confirmation, an advert carrying the
        pre-command (stale) state is ignored so the UI doesn't flap. A
        matching advert signals _state_confirmed so the write loop can
        return immediately.
        """
        new_state = _decode_advert_state(service_info)
        if new_state is None:
            return False
        if (
            self._expected_state is not None
            and new_state != self._expected_state
        ):
            # Stale pre-command advert; don't revert HA state mid-write.
            return False
        if (
            self._expected_state is not None
            and self._state_confirmed is not None
            and new_state == self._expected_state
        ):
            self._state_confirmed.set()
        # Durable pending intent: if this advert matches what the user
        # asked for (even if we couldn't confirm it during the live
        # service call), consider the target achieved and the background
        # retry worker can stop.
        if self._pending_state is not None and new_state == self._pending_state:
            self._pending_state = None
        if new_state == self._state:
            return False
        _LOGGER.debug(
            "govee-ble-lights: %s advert state %s -> %s via %s rssi=%s",
            self._canonical_mac(),
            self._state,
            new_state,
            getattr(service_info, "source", "?"),
            getattr(service_info, "rssi", "?"),
        )
        self._state = new_state
        return True

    async def _write_once(self, command: bytes, label: str) -> None:
        """Connect + write_gatt_char(response=False) + disconnect, retrying
        on exception up to _FIRE_AND_FORGET_ATTEMPTS times.

        Used for brightness / rgb / effect writes where the bulb's
        advertisement doesn't carry the commanded value, so verification
        isn't possible. Still defends against transient connect failures
        (slot exhaustion, mid-session disconnect) which ARE observable.
        Raises ConnectionError if every attempt fails to connect/write.
        """
        last_exc: Exception | None = None
        for attempt in range(1, _FIRE_AND_FORGET_ATTEMPTS + 1):
            client = None
            try:
                client = await asyncio.wait_for(
                    self._connectBluetooth(), timeout=_CONNECT_TIMEOUT_S,
                )
                await client.write_gatt_char(
                    UUID_CONTROL_CHARACTERISTIC, command, False
                )
                return
            except asyncio.TimeoutError:
                last_exc = asyncio.TimeoutError(
                    f"connect exceeded {_CONNECT_TIMEOUT_S}s"
                )
                _LOGGER.warning(
                    "govee-ble-lights: %s %s connect attempt %d/%d timed out after %.1fs",
                    self._canonical_mac(), label, attempt,
                    _FIRE_AND_FORGET_ATTEMPTS, _CONNECT_TIMEOUT_S,
                )
                if attempt < _FIRE_AND_FORGET_ATTEMPTS:
                    await asyncio.sleep(1.0)
            except Exception as exc:
                last_exc = exc
                _LOGGER.warning(
                    "govee-ble-lights: %s %s write attempt %d/%d failed: %s",
                    self._canonical_mac(), label, attempt,
                    _FIRE_AND_FORGET_ATTEMPTS, exc,
                )
                if attempt < _FIRE_AND_FORGET_ATTEMPTS:
                    await asyncio.sleep(1.0)
            finally:
                if client is not None:
                    try:
                        await asyncio.wait_for(
                            client.disconnect(), timeout=_DISCONNECT_TIMEOUT_S,
                        )
                    except (asyncio.TimeoutError, Exception):
                        pass
        raise ConnectionError(
            f"Govee {self._canonical_mac()} {label} write failed after "
            f"{_FIRE_AND_FORGET_ATTEMPTS} attempts: {last_exc}"
        )

    async def _batch_write_and_confirm(
        self,
        packets: list[bytes],
        want_on: bool,
        need_confirm: bool,
    ) -> None:
        """Write a list of GATT packets in a single connect-write-disconnect
        session, then (optionally) wait for advert confirmation of POWER.

        Models the proven timniklas/hass-govee_light_ble pattern: one GATT
        session per logical operation, all packets transmitted in sequence
        before closing the session. Reduces proxy-slot churn dramatically
        versus the prior one-session-per-packet approach: a turn_on with
        brightness+rgb dropped from 3 sessions to 1.

        On retry, re-transmit the entire packet list (idempotent writes).
        """
        if not packets:
            return
        mac = self._canonical_mac()
        if not self._advert_state_supported:
            # No advert-based verification available for this model. Run
            # one session with retry-on-exception; no confirm wait.
            last_exc: Exception | None = None
            for attempt in range(1, _POWER_WRITE_ATTEMPTS + 1):
                client = None
                try:
                    client = await asyncio.wait_for(
                        self._connectBluetooth(), timeout=_CONNECT_TIMEOUT_S,
                    )
                    for packet in packets:
                        await client.write_gatt_char(
                            UUID_CONTROL_CHARACTERISTIC, packet, False,
                        )
                    return
                except asyncio.TimeoutError:
                    last_exc = asyncio.TimeoutError(
                        f"connect exceeded {_CONNECT_TIMEOUT_S}s"
                    )
                except Exception as exc:
                    last_exc = exc
                finally:
                    if client is not None:
                        try:
                            await asyncio.wait_for(
                                client.disconnect(), timeout=_DISCONNECT_TIMEOUT_S,
                            )
                        except (asyncio.TimeoutError, Exception):
                            pass
                if attempt < _POWER_WRITE_ATTEMPTS:
                    await asyncio.sleep(1.0)
            raise ConnectionError(
                f"Govee {mac} batch write failed after "
                f"{_POWER_WRITE_ATTEMPTS} attempts: {last_exc}"
            )

        if need_confirm:
            self._expected_state = want_on
            self._state_confirmed = asyncio.Event()
        try:
            last_exc = None
            for attempt in range(1, _POWER_WRITE_ATTEMPTS + 1):
                if need_confirm and self._state_confirmed.is_set():
                    return
                client = None
                try:
                    client = await asyncio.wait_for(
                        self._connectBluetooth(), timeout=_CONNECT_TIMEOUT_S,
                    )
                    for packet in packets:
                        await client.write_gatt_char(
                            UUID_CONTROL_CHARACTERISTIC, packet, False,
                        )
                except asyncio.TimeoutError:
                    last_exc = asyncio.TimeoutError(
                        f"connect exceeded {_CONNECT_TIMEOUT_S}s"
                    )
                    _LOGGER.warning(
                        "govee-ble-lights: %s batch connect attempt %d/%d timed out after %.1fs",
                        mac, attempt, _POWER_WRITE_ATTEMPTS, _CONNECT_TIMEOUT_S,
                    )
                    if attempt < _POWER_WRITE_ATTEMPTS:
                        await asyncio.sleep(1.0)
                    continue
                except Exception as exc:
                    last_exc = exc
                    _LOGGER.warning(
                        "govee-ble-lights: %s batch write attempt %d/%d failed: %s",
                        mac, attempt, _POWER_WRITE_ATTEMPTS, exc,
                    )
                    if attempt < _POWER_WRITE_ATTEMPTS:
                        await asyncio.sleep(1.0)
                    continue
                finally:
                    if client is not None:
                        try:
                            await asyncio.wait_for(
                                client.disconnect(), timeout=_DISCONNECT_TIMEOUT_S,
                            )
                        except (asyncio.TimeoutError, Exception):
                            pass

                # Writes sent. If POWER didn't change, nothing to verify.
                if not need_confirm:
                    return
                try:
                    await asyncio.wait_for(
                        self._state_confirmed.wait(),
                        timeout=_CONFIRM_TIMEOUT_S,
                    )
                    return
                except asyncio.TimeoutError:
                    _LOGGER.warning(
                        "govee-ble-lights: %s batch power=%s not confirmed after attempt %d/%d (%.1fs)",
                        mac, want_on, attempt, _POWER_WRITE_ATTEMPTS, _CONFIRM_TIMEOUT_S,
                    )
                    continue
            if need_confirm:
                raise ConnectionError(
                    f"Govee {mac} did not confirm power={want_on} "
                    f"after {_POWER_WRITE_ATTEMPTS} attempts"
                    + (f"; last write error: {last_exc}" if last_exc else "")
                )
            raise ConnectionError(
                f"Govee {mac} batch write failed after "
                f"{_POWER_WRITE_ATTEMPTS} attempts: {last_exc}"
            )
        finally:
            if need_confirm:
                self._expected_state = None
                self._state_confirmed = None

    async def _write_power_and_confirm(self, want_on: bool) -> None:
        """Write POWER and block until an advert confirms state==want_on.

        Retries the write up to _POWER_WRITE_ATTEMPTS times. Raises
        ConnectionError if still unconfirmed, or bubbles the last bleak
        exception if every attempt also failed to connect/write.

        Falls back to single fire-and-forget write for models that don't
        broadcast state in their manufacturer_data, since we have no way
        to confirm there.
        """
        payload = [0x1 if want_on else 0x0]
        if not self._advert_state_supported:
            # No advert encoding for this model — no way to verify. Send
            # via _write_once so we still get retry + disconnect hygiene.
            await self._write_once(
                self._prepareSinglePacketData(LedCommand.POWER, payload),
                f"power={want_on} (fallback, no advert)",
            )
            return

        self._expected_state = want_on
        self._state_confirmed = asyncio.Event()
        try:
            # If the most recent advert already shows the desired state,
            # no write is needed — the bulb is already there.
            if self._state == want_on:
                return

            last_exc: Exception | None = None
            cmd = self._prepareSinglePacketData(LedCommand.POWER, payload)
            for attempt in range(1, _POWER_WRITE_ATTEMPTS + 1):
                if self._state_confirmed.is_set():
                    return
                client = None
                try:
                    client = await asyncio.wait_for(
                        self._connectBluetooth(), timeout=_CONNECT_TIMEOUT_S,
                    )
                    await client.write_gatt_char(
                        UUID_CONTROL_CHARACTERISTIC, cmd, False
                    )
                except asyncio.TimeoutError:
                    last_exc = asyncio.TimeoutError(
                        f"connect exceeded {_CONNECT_TIMEOUT_S}s"
                    )
                    _LOGGER.warning(
                        "govee-ble-lights: %s power=%s connect attempt %d/%d timed out after %.1fs",
                        self._canonical_mac(),
                        want_on,
                        attempt,
                        _POWER_WRITE_ATTEMPTS,
                        _CONNECT_TIMEOUT_S,
                    )
                    if attempt < _POWER_WRITE_ATTEMPTS:
                        await asyncio.sleep(1.0)
                    continue
                except Exception as exc:
                    last_exc = exc
                    _LOGGER.warning(
                        "govee-ble-lights: %s power=%s write attempt %d/%d failed: %s",
                        self._canonical_mac(),
                        want_on,
                        attempt,
                        _POWER_WRITE_ATTEMPTS,
                        exc,
                    )
                    if attempt < _POWER_WRITE_ATTEMPTS:
                        await asyncio.sleep(1.0)
                    continue
                finally:
                    # Drop the GATT connection after the write so the bulb can
                    # resume broadcasting advertisements — Govee H617A stays
                    # silent while a GATT client is actively connected, which
                    # starves the advert-based confirmation loop.
                    if client is not None:
                        try:
                            await asyncio.wait_for(
                                client.disconnect(), timeout=_DISCONNECT_TIMEOUT_S,
                            )
                        except (asyncio.TimeoutError, Exception):
                            pass
                try:
                    await asyncio.wait_for(
                        self._state_confirmed.wait(),
                        timeout=_CONFIRM_TIMEOUT_S,
                    )
                    return
                except asyncio.TimeoutError:
                    _LOGGER.warning(
                        "govee-ble-lights: %s power=%s not confirmed after attempt %d/%d (%.1fs)",
                        self._canonical_mac(),
                        want_on,
                        attempt,
                        _POWER_WRITE_ATTEMPTS,
                        _CONFIRM_TIMEOUT_S,
                    )
                    continue

            raise ConnectionError(
                f"Govee {self._canonical_mac()} did not confirm power={want_on} "
                f"after {_POWER_WRITE_ATTEMPTS} attempts"
                + (f"; last write error: {last_exc}" if last_exc else "")
            )
        finally:
            self._expected_state = None
            self._state_confirmed = None

    @property
    def effect_list(self) -> list[str] | None:
        effect_list = []
        json_data = json.loads(Path(Path(__file__).parent / "jsons" / (self._model + ".json")).read_text())
        for categoryIdx, category in enumerate(json_data['data']['categories']):
            for sceneIdx, scene in enumerate(category['scenes']):
                for leffectIdx, lightEffect in enumerate(scene['lightEffects']):
                    for seffectIxd, specialEffect in enumerate(lightEffect['specialEffect']):
                        # if 'supportSku' not in specialEffect or self._model in specialEffect['supportSku']:
                        # Workaround cause we need to store some metadata in effect (effect names not unique)
                        indexes = str(categoryIdx) + "/" + str(sceneIdx) + "/" + str(leffectIdx) + "/" + str(
                            seffectIxd)
                        effect_list.append(
                            category['categoryName'] + " - " + scene['sceneName'] + ' - ' + lightEffect[
                                'scenceName'] + " [" + indexes + "]")

        return effect_list

    @property
    def name(self) -> str:
        """Return the name of the switch."""
        return "GOVEE Light"

    @property
    def unique_id(self) -> str:
        """Return a unique, Home Assistant friendly identifier for this entity."""
        return self._mac.replace(":", "")

    @property
    def brightness(self):
        return self._brightness

    @property
    def rgb_color(self) -> tuple[int, int, int] | None:
        return self._rgb_color

    @property
    def is_on(self) -> bool | None:
        """Return true if light is on."""
        return self._state

    async def async_turn_on(self, **kwargs) -> None:
        async with self._entity_lock:
            await self._async_turn_on_inner(**kwargs)

    async def _async_turn_on_inner(self, **kwargs) -> None:
        # Intentionally NOT setting self._state optimistically here: the
        # verify-and-retry loop below uses self._state as the last known
        # bulb state (maintained by _apply_advert_state). An optimistic
        # set would make the early-out short-circuit even when the bulb
        # is actually in the opposite state.

        # Non-POWER commands. POWER is handled below with verify-and-retry;
        # the rest are still fire-and-forget because the bulb's advertised
        # state only reflects on/off, not brightness / rgb / effect.
        #
        # Dedup each command against the last tracked value: skip the write
        # if the bulb is already at the requested value. This is the same
        # idempotence pattern POWER uses (early-out when `self._state ==
        # want_on`). Without it, a single user/script action frequently
        # fans out via HA group expansion + adaptive_lighting intercept
        # into 5-11 identical turn_on calls on the same BLE entity within
        # 100ms, each queued behind the entity_lock. Even with brief GATT
        # sessions, N×redundant writes × M bulbs × 3 slots on the nearest
        # proxy saturates that proxy's scan duty cycle and starves Bermuda
        # into scanner-staleness (observed 2026-04-23: tv_backglow received
        # 9 turn_on calls with 3× duplicate brightness + 2× duplicate
        # color_temp in 2 seconds, triggering 45s of connect timeouts).
        other_commands: list[bytes] = []

        if ATTR_BRIGHTNESS in kwargs:
            brightness = kwargs.get(ATTR_BRIGHTNESS, 255)
            if brightness == self._brightness:
                _LOGGER.debug(
                    "govee-ble-lights: %s skip brightness (already %d)",
                    self._canonical_mac(), brightness,
                )
            else:
                if self._use_percent:
                    brightnessPercent = int(brightness * 100 / 255)
                    other_commands.append(self._prepareSinglePacketData(LedCommand.BRIGHTNESS, [brightnessPercent]))
                else:
                    other_commands.append(self._prepareSinglePacketData(LedCommand.BRIGHTNESS, [brightness]))
                self._brightness = brightness

        if ATTR_RGB_COLOR in kwargs:
            red, green, blue = kwargs.get(ATTR_RGB_COLOR)
            if (red, green, blue) == self._rgb_color:
                _LOGGER.debug(
                    "govee-ble-lights: %s skip rgb (already %s)",
                    self._canonical_mac(), (red, green, blue),
                )
            else:
                if self._is_segmented:
                    other_commands.append(self._prepareSinglePacketData(LedCommand.COLOR,
                                                                  [LedMode.SEGMENTS, 0x01, red, green, blue, 0x00, 0x00, 0x00,
                                                                   0x00, 0x00, 0xFF, 0x7F]))
                else:
                    other_commands.append(self._prepareSinglePacketData(LedCommand.COLOR, [LedMode.MANUAL, red, green, blue]))

                self._rgb_color = (red, green, blue)
        if ATTR_EFFECT in kwargs:
            effect = kwargs.get(ATTR_EFFECT)
            if len(effect) > 0:
                search = EFFECT_PARSE.search(effect)

                # Parse effect indexes
                categoryIndex = int(search.group(1))
                sceneIndex = int(search.group(2))
                lightEffectIndex = int(search.group(3))
                specialEffectIndex = int(search.group(4))

                json_data = json.loads(Path(Path(__file__).parent / "jsons" / (self._model + ".json")).read_text())
                category = json_data['data']['categories'][categoryIndex]
                scene = category['scenes'][sceneIndex]
                lightEffect = scene['lightEffects'][lightEffectIndex]
                specialEffect = lightEffect['specialEffect'][specialEffectIndex]

                # Prepare packets to send big payload in separated chunks
                for command in prepareMultiplePacketsData(0xa3,
                                                          array.array('B', [0x02]),
                                                          array.array('B',
                                                                      base64.b64decode(specialEffect['scenceParam'])
                                                                      )):
                    other_commands.append(command)

        # Assemble one batch: POWER first (if state needs to change), then
        # any non-POWER packets. Everything transmitted in one GATT session
        # — one connect, N writes, one disconnect — matching the
        # timniklas/hass-govee_light_ble proven pattern. If POWER state
        # needs to change, wait for advert confirmation post-disconnect.
        need_power = self._state != True
        batch: list[bytes] = []
        if need_power:
            batch.append(self._prepareSinglePacketData(LedCommand.POWER, [0x1]))
        batch.extend(other_commands)
        if not batch:
            # Bulb already on, all non-POWER changes deduped — nothing to do.
            return
        # Durable intent: park user's desired power state. If the live
        # _batch_write_and_confirm fails (e.g. adaptive_lighting keeps
        # re-firing turn_on between our retries, winning the advert
        # confirm race), the background worker retries until the bulb's
        # own advert shows state=True or a newer intent supersedes.
        # Only for POWER; non-POWER (brightness/rgb) remains best-effort.
        if need_power and self._advert_state_supported:
            self._pending_state = True
        try:
            await self._batch_write_and_confirm(
                batch, want_on=True, need_confirm=need_power,
            )
            if not need_power:
                self._pending_state = None
        except Exception:
            if self._pending_state is True:
                self._ensure_pending_worker()
            raise

    async def async_turn_off(self, **kwargs) -> None:
        async with self._entity_lock:
            if self._advert_state_supported:
                self._pending_state = False
            try:
                await self._write_power_and_confirm(False)
            except Exception:
                if self._pending_state is False:
                    self._ensure_pending_worker()
                raise

    async def _connectBluetooth(self) -> BleakClient:
        # PATCH: refresh BleakDevice from bluetooth registry each call (handles stale cache after proxy reboots)
        fresh = bluetooth.async_ble_device_from_address(self.hass, self.unique_id.upper(), True)
        if fresh is not None:
            self._ble_device = fresh
        last_exc = None
        for i in range(3):
            try:
                client = await bleak_retry_connector.establish_connection(BleakClient, self._ble_device, self.unique_id)
                return client
            except Exception as e:
                last_exc = e
                _LOGGER.warning("govee-ble-lights connect attempt %d/3 failed for %s: %s", i+1, self.unique_id, e)
                continue
        # All 3 attempts failed — raise so caller sees error instead of NoneType crash
        raise ConnectionError(f"Failed to establish BLE connection to {self.unique_id} after 3 attempts: {last_exc}")

    def _prepareSinglePacketData(self, cmd, payload):
        if not isinstance(cmd, int):
            raise ValueError('Invalid command')
        if not isinstance(payload, bytes) and not (
                isinstance(payload, list) and all(isinstance(x, int) for x in payload)):
            raise ValueError('Invalid payload')
        if len(payload) > 17:
            raise ValueError('Payload too long')

        cmd = cmd & 0xFF
        payload = bytes(payload)

        frame = bytes([0x33, cmd]) + bytes(payload)
        # pad frame data to 19 bytes (plus checksum)
        frame += bytes([0] * (19 - len(frame)))

        # The checksum is calculated by XORing all data bytes
        checksum = 0
        for b in frame:
            checksum ^= b

        frame += bytes([checksum & 0xFF])
        return frame
