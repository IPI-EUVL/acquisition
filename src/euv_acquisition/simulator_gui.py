from __future__ import annotations

import threading

from euv_acquisition.simulator_controls import SimulatorFaultControls


class SimulatorControlWindow:
    REFRESH_MS = 100

    def __init__(self, controls: SimulatorFaultControls, stop_event: threading.Event) -> None:
        import tkinter as tk
        from tkinter import ttk

        self._controls = controls
        self._stop_event = stop_event
        self._root = tk.Tk()
        self._root.title("EUV Acquisition Simulator")
        self._root.resizable(False, False)
        self._root.protocol("WM_DELETE_WINDOW", self._on_close)

        frame = ttk.Frame(self._root, padding=12)
        frame.grid(sticky="nsew")
        self._laser_enabled = tk.BooleanVar(value=True)
        self._chopper_enabled = tk.BooleanVar(value=True)
        self._pll_locked = tk.BooleanVar(value=True)
        self._status_text = tk.StringVar()

        ttk.Label(frame, text="Simulated laser timing", font=("TkDefaultFont", 11, "bold")).grid(
            row=0, column=0, columnspan=2, sticky="w"
        )
        ttk.Checkbutton(
            frame,
            text="Laser enabled (no triggers when off)",
            variable=self._laser_enabled,
            command=self._set_laser_enabled,
        ).grid(row=1, column=0, columnspan=2, sticky="w", pady=(8, 0))
        ttk.Checkbutton(
            frame,
            text="Chopper enabled (no triggers when off)",
            variable=self._chopper_enabled,
            command=self._set_chopper_enabled,
        ).grid(row=2, column=0, columnspan=2, sticky="w")
        ttk.Checkbutton(
            frame,
            text="PLL locked (flat captures when unlocked)",
            variable=self._pll_locked,
            command=self._set_pll_locked,
        ).grid(row=3, column=0, columnspan=2, sticky="w")
        ttk.Separator(frame, orient="horizontal").grid(row=4, column=0, columnspan=2, sticky="ew", pady=8)
        ttk.Label(frame, textvariable=self._status_text, justify="left").grid(
            row=5, column=0, columnspan=2, sticky="w"
        )
        ttk.Button(frame, text="Restore nominal", command=self._restore_nominal).grid(
            row=6, column=0, sticky="w", pady=(10, 0)
        )
        ttk.Button(frame, text="Stop simulator", command=self._on_close).grid(
            row=6, column=1, sticky="e", pady=(10, 0)
        )
        self._refresh()

    def run(self) -> None:
        self._root.mainloop()

    def _set_laser_enabled(self) -> None:
        self._controls.set_laser_enabled(self._laser_enabled.get())

    def _set_chopper_enabled(self) -> None:
        self._controls.set_chopper_enabled(self._chopper_enabled.get())

    def _set_pll_locked(self) -> None:
        self._controls.set_pll_locked(self._pll_locked.get())

    def _restore_nominal(self) -> None:
        self._controls.restore_nominal()
        self._laser_enabled.set(True)
        self._chopper_enabled.set(True)
        self._pll_locked.set(True)

    def _refresh(self) -> None:
        if self._stop_event.is_set():
            self._root.destroy()
            return
        status = self._controls.status()
        trigger_state = "enabled" if status.effective_triggers_enabled else "disabled"
        beam_state = "transmitting" if status.effective_euv_transmitting else "blocked"
        rate = "unavailable" if status.trigger_rate_hz is None else f"{status.trigger_rate_hz:.3f} Hz"
        upstream = "transmitting" if status.upstream_euv_transmitting else "blocked"
        self._status_text.set(
            f"Effective triggers: {trigger_state}\n"
            f"Effective EUV: {beam_state}\n"
            f"Trigger rate: {rate}\n"
            f"DDS timing beam: {upstream}"
        )
        self._root.after(self.REFRESH_MS, self._refresh)

    def _on_close(self) -> None:
        self._stop_event.set()
        self._root.destroy()