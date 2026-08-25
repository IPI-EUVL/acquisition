#!/usr/bin/python3

import time
import signal
import threading
import queue
import numpy as np
import rp

# ============================================================
# User settings
# ============================================================

CHANNEL = rp.RP_CH_1

ADC_RATE = 125_000_000.0
SAMPLE_RATE = ADC_RATE
DT = 1.0 / SAMPLE_RATE

FULL_BUFFER_SAMPLES = 16_384
TRIGGER_INDEX = FULL_BUFFER_SAMPLES // 2

# Capture window: t - 1 us to t + 9 us
WINDOW_US = 10.0
START_OFFSET_US = -1.0

WINDOW_SAMPLES = int(round(WINDOW_US * 1e-6 * SAMPLE_RATE))
START_OFFSET_SAMPLES = int(round(START_OFFSET_US * 1e-6 * SAMPLE_RATE))

START_INDEX = TRIGGER_INDEX + START_OFFSET_SAMPLES
END_INDEX = START_INDEX + WINDOW_SAMPLES

TRIGGER_SOURCE = rp.RP_TRIG_SRC_EXT_PE

# Bounded queue prevents accidental multi-second latency buildup.
WAVEFORM_QUEUE_SIZE = 100
waveform_queue = queue.Queue(maxsize=WAVEFORM_QUEUE_SIZE)

stop_event = threading.Event()

# First-three-LED marquee:
# 000, 00X, 0XX, XXX, XX0, X00
LED_PATTERNS = [
    0b000,
    0b001,
    0b011,
    0b111,
    0b110,
    0b100,
]


# ============================================================
# Helpers
# ============================================================

def check(name, result):
    """
    Red Pitaya Python API calls usually return either:
      - integer status
      - tuple/list where first element is status
    """
    code = result[0] if isinstance(result, (tuple, list)) else result
    if code != rp.RP_OK:
        raise RuntimeError(f"{name} failed with code {code}")


def set_pulse_led_marquee(shot_index):
    """
    Updates first three user LEDs once per captured pulse.
    """
    pattern = LED_PATTERNS[shot_index % len(LED_PATTERNS)]
    check("rp_LEDSetState", rp.rp_LEDSetState(pattern))


def wait_for_trigger():
    while not stop_event.is_set():
        result = rp.rp_AcqGetTriggerState()
        state = result[1]

        if state == rp.RP_TRIG_STATE_TRIGGERED:
            return True

    return False


def wait_for_buffer_fill():
    while not stop_event.is_set():
        result = rp.rp_AcqGetBufferFillState()
        filled = result[1]

        if filled:
            return True

    return False


# ============================================================
# Producer thread: capture waveforms
# ============================================================

def acquisition_producer():
    shot = 0
    dropped = 0

    # Allocate once and reuse.
    volt_buffer = rp.fBuffer(FULL_BUFFER_SAMPLES)

    while not stop_event.is_set():
        # Reset and configure acquisition.
        check("rp_AcqReset", rp.rp_AcqReset())
        check("rp_AcqSetDecimation", rp.rp_AcqSetDecimation(rp.RP_DEC_1))
        check("rp_AcqSetTriggerDelay", rp.rp_AcqSetTriggerDelay(0))

        # External trigger debounce in microseconds.
        # 1 us is useful for short clean logic pulses.
        if hasattr(rp, "rp_AcqSetExtTriggerDebouncerUs"):
            check(
                "rp_AcqSetExtTriggerDebouncerUs",
                rp.rp_AcqSetExtTriggerDebouncerUs(1.0),
            )

        check("rp_AcqStart", rp.rp_AcqStart())

        # Let the pre-trigger circular buffer fill.
        # Full 16k buffer at 125 MS/s is about 131 us.
        time.sleep(0.001)

        # Arm external rising-edge trigger.
        check("rp_AcqSetTriggerSrc", rp.rp_AcqSetTriggerSrc(TRIGGER_SOURCE))

        if not wait_for_trigger():
            break

        if not wait_for_buffer_fill():
            break

        # Read full 16k calibrated voltage buffer.
        check(
            "rp_AcqGetDataV",
            rp.rp_AcqGetDataV(CHANNEL, 0, FULL_BUFFER_SAMPLES, volt_buffer),
        )

        # Extract t - 1 us to t + 9 us window.
        # copy() is important because volt_buffer is reused next shot.
        window = np.array(
            [volt_buffer[i] for i in range(START_INDEX, END_INDEX)],
            dtype=np.float32,
        ).copy()

        item = {
            "shot": shot,
            "t_capture_monotonic": time.monotonic(),
            "samples_v": window,
            "dropped_before_this": dropped,
        }

        try:
            waveform_queue.put_nowait(item)
        except queue.Full:
            dropped += 1

        # LED update indicates actual captured pulses.
        set_pulse_led_marquee(shot)

        shot += 1


# ============================================================
# Consumer thread: process waveforms
# ============================================================

def consumer():
    while not stop_event.is_set() or not waveform_queue.empty():
        try:
            item = waveform_queue.get(timeout=0.1)
        except queue.Empty:
            continue

        samples = item["samples_v"].astype(np.float64, copy=False)

        mean_v = float(np.mean(samples))
        min_v = float(np.min(samples))
        max_v = float(np.max(samples))

        # No baseline compensation.
        # Integrates the exact selected window:
        # t - 1 us to t + 9 us.
        area_v_s = float(np.sum(samples) * DT)
        area_v_us = area_v_s * 1e6

        print(
            f"shot={item['shot']:06d} "
            f"mean={mean_v:+.6e} V "
            f"min={min_v:+.6e} V "
            f"max={max_v:+.6e} V "
            f"area={area_v_s:+.6e} V*s "
            f"({area_v_us:+.6e} V*us) "
            f"q={waveform_queue.qsize():3d} "
            f"dropped={item['dropped_before_this']}"
        )

        waveform_queue.task_done()


# ============================================================
# Main
# ============================================================

def handle_sigint(signum, frame):
    stop_event.set()


def main():
    if START_INDEX < 0 or END_INDEX > FULL_BUFFER_SAMPLES:
        raise ValueError(
            f"Window [{START_INDEX}:{END_INDEX}] is outside "
            f"{FULL_BUFFER_SAMPLES}-sample buffer."
        )

    print("Initializing Red Pitaya...")
    check("rp_Init", rp.rp_Init())

    print()
    print("Capture configuration")
    print("---------------------")
    print(f"sample_rate_Hz      = {SAMPLE_RATE:.0f}")
    print(f"dt_ns               = {DT * 1e9:.3f}")
    print(f"full_buffer_samples = {FULL_BUFFER_SAMPLES}")
    print(f"trigger_index       = {TRIGGER_INDEX}")
    print(f"window_us           = {WINDOW_US:.3f}")
    print(f"window_samples      = {WINDOW_SAMPLES}")
    print(f"start_offset_us     = {START_OFFSET_US:.3f}")
    print(f"start_index         = {START_INDEX}")
    print(f"end_index           = {END_INDEX}")
    print()

    signal.signal(signal.SIGINT, handle_sigint)

    prod = threading.Thread(target=acquisition_producer, name="acq-producer")
    cons = threading.Thread(target=consumer, name="consumer")

    try:
        # Clear LEDs on startup.
        check("rp_LEDSetState", rp.rp_LEDSetState(0x00))

        print("Waiting for external triggers... Ctrl+C to stop.")

        cons.start()
        prod.start()

        while prod.is_alive():
            prod.join(timeout=0.2)

    finally:
        stop_event.set()

        prod.join(timeout=1.0)
        cons.join(timeout=1.0)

        try:
            check("rp_LEDSetState", rp.rp_LEDSetState(0x00))
        except Exception:
            pass

        rp.rp_Release()
        print("Released Red Pitaya resources.")


if __name__ == "__main__":
    main()