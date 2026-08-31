# IPI EUV Acquisition

`ipi-euv-acquisition` is the standalone pulse-resolved digitizer service used by chamber control.

## Local simulator

Install the package with its development dependencies, then run:

```powershell
euv-acquisition-sim --spool C:\temp\euv-acquisition-spool
```

The service listens on `127.0.0.1:11760` for control and pulse reports, and `127.0.0.1:11761` for HDF5 artifact transfer. Override either port with `--control-port` or `--artifact-port`.

The simulator opens a small control window by default. Use it to turn the simulated laser or chopper off, or clear `PLL locked` to block EUV while preserving the trigger cadence. Closing the window stops the simulator; use `--no-control-gui` for unattended runs.

The simulator follows the chamber's versioned laser timing state by default and fails closed before the first timing update. It uses `ECS_HOST` or `127.0.0.1` for DDS, and defaults to the chamber Laser Sync Controller UUID. `--dds-host` and `--laser-subsystem-uuid` override those values. Use `--standalone-timing` only for isolated simulator tests that should continuously model a nominal transmitting laser. Manual controls remain fault-injection overrides in either mode.

When the simulator runs headless on the Red Pitaya, chamber control can operate its laser, chopper, and PLL fault gates through the acquisition controller's DDS interface. The simulator advertises this capability in acquisition status; the hardware service does not, so simulator-control commands fail closed against the physical ADC source.

Capture sessions are tagged as `experiment` or `diagnostic`. Experiment artifacts retain the existing acknowledge-then-release lifecycle. Diagnostic clients may purge each verified, acknowledged snapshot immediately and may discard only a stopped diagnostic session; these commands reject experiment sessions so test cleanup cannot remove exposure data.

The Red Pitaya adapter is isolated in `euv_acquisition.sources.red_pitaya`; it is tested with a fake API locally and must be validated against the STEMlab hardware before authoritative use.

## Capture pipeline and telemetry

Capture uses one hardware producer, one ordered analysis worker, one persistence worker, and one FIFO control writer. Socket and HDF5 latency therefore do not sit on the hardware capture path. A queue overflow or worker failure stops new intake, drains work already accepted where possible, retains the spool, and records one terminal reason.

The service `status` response includes bounded `pipeline_metrics`. Chamber control polls it once per second and forwards it in `acquisition_status`; Capture Diagnostics shows:

- requested and effective capture mode, including any fallback reason;
- average accepted pulse rate and cumulative accepted count;
- current/high-water/capacity values for capture, persistence, and control queues;
- p95 hardware-read, capture-wait, analysis, snapshot-write, and trigger-to-report timings;
- the terminal pipeline fault, if any.

Timing samples retain only the latest 512 observations per stage. Counters and queue high-water marks cover the current or most recently completed session.

## Red Pitaya service

The hardware server is exposed as `euv-acquisition-red-pitaya`. Its default capture window is 10 microseconds with 1 microsecond of pre-trigger data at 125 MS/s. Starting the server does not open the ADC; chamber control opens it with `start_capture` and maintains the watchdog heartbeat for the duration of an exposure.

The production unit is `deploy/euv-acquisition.service`. It listens on ports 11760 and 11761, stores recoverable HDF5 snapshots under `/var/lib/euv-acquisition/spool`, and sends structured events to the central ECS logger at `10.11.13.1:11751`. Every message is also written to journald, and acquisition continues if central logging is unavailable. The unit explicitly pins `legacy-single-shot`, capture/persistence/control queue capacities of 32/8/512, a 10-second pipeline drain deadline, and a 30-second systemd stop deadline.

From the Windows workspace, deploy a new immutable release without interrupting a running service:

```powershell
.\scripts\deploy_red_pitaya.ps1
```

The script builds the application wheel, verifies and uploads the checked-in ARMv7 h5py/HDF5 runtime from `vendor/armv7`, validates the release on the board, atomically updates `/opt/euv-acquisition/current`, and enables the systemd unit. Its remote preflight verifies checksums, the systemd unit, CPython 3.10, NumPy/HDF5 versions, HDF5 round-trip storage, production argument wiring, and the legacy Red Pitaya API symbols. It does not call `rp_Init`, open the ADC, or restart the service.

Deployment deliberately leaves the running service untouched by default. Confirm that no exposure or diagnostic capture is active before using the restart option. For the initial installation, or after that idle check, deploy and activate the release with:

```powershell
.\scripts\deploy_red_pitaya.ps1 -RestartService -ConfirmInstrumentIdle
```

Each release remains under `/opt/euv-acquisition/releases/<version>-<wheel-hash>`. To roll back, repoint `/opt/euv-acquisition/current` to a prior release and restart the unit.

```bash
systemctl status euv-acquisition --no-pager
journalctl -u euv-acquisition -f
systemctl restart euv-acquisition
systemctl stop euv-acquisition
```

After activation, verify the pinned mode and bounded queues in Capture Diagnostics or through the service `status` command. The initial acceptance gate is a legacy-mode diagnostic that completes without a terminal fault and leaves every queue below capacity.

## Hardware capture modes

`legacy-single-shot` reads the established 16,384-sample acquisition ring and extracts the requested window. It remains the production mode until faster backends pass physical validation.

`single-shot` retains trigger-by-trigger rearming but uses `rp_AcqGetDataVNP` to read only the requested window. `axi-continuous` uses the reserved AXI ring, starts acquisition once, and unlocks each completed trigger. `auto` tries AXI, then narrow single-shot, then legacy; the selected mode and fallback reason are visible in pipeline status.

Test a faster backend only while the instrument is idle. Create `/etc/systemd/system/euv-acquisition.service.d/capture-mode.conf` with one explicit mode, then reload and restart the unit:

```ini
[Service]
Environment="EUV_CAPTURE_MODE=single-shot"
```

```bash
systemctl daemon-reload
systemctl restart euv-acquisition
systemctl show euv-acquisition -p Environment --no-pager
```

Validate `single-shot` before AXI. For each candidate mode, run a diagnostic at the nominal 96 Hz cadence and require sequence continuity, accepted rate consistent with the trigger rate, zero queue overflows, bounded queue high-water marks, no terminal fault, and successful flush/stop artifact transfer. Exercise controller disconnect and service shutdown once to confirm accepted pulses remain in the spool. Do not use `axi-continuous` for an exposure until these gates pass on the physical STEMlab.

To return to the production default, remove only the capture-mode drop-in, reload systemd, and restart while idle:

```bash
rm -f /etc/systemd/system/euv-acquisition.service.d/capture-mode.conf
systemctl daemon-reload
systemctl restart euv-acquisition
```

Chamber control must use the board's instrument-LAN address:

```text
EUV_ACQUISITION_HOST=10.11.13.50
EUV_ACQUISITION_CONTROL_PORT=11760
EUV_ACQUISITION_ARTIFACT_PORT=11761
```

For WAN-only administration, run the systemd commands through `ssh euvl-red-pitaya`. The existing SSH relay exposes only SSH; the capture ports are not exposed to IllinoisNet.
