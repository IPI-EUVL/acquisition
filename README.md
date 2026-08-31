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

The Red Pitaya adapter is isolated in `euv_acquisition.sources.red_pitaya`; it is tested with a fake API locally and must be validated against the STEMlab hardware before authoritative use.

## Red Pitaya service

The hardware server is exposed as `euv-acquisition-red-pitaya`. Its default capture window is 10 microseconds with 1 microsecond of pre-trigger data at 125 MS/s. Starting the server does not open the ADC; chamber control opens it with `start_capture` and maintains the watchdog heartbeat for the duration of an exposure.

The production unit is `deploy/euv-acquisition.service`. It listens on ports 11760 and 11761, stores recoverable HDF5 snapshots under `/var/lib/euv-acquisition/spool`, and sends structured events to the central ECS logger at `10.11.13.1:11751`. Every message is also written to journald, and acquisition continues if central logging is unavailable.

From the Windows workspace, deploy a new immutable release without interrupting a running service:

```powershell
.\scripts\deploy_red_pitaya.ps1
```

The script builds the application wheel, verifies and uploads the checked-in ARMv7 h5py/HDF5 runtime from `vendor/armv7`, validates the release on the board, atomically updates `/opt/euv-acquisition/current`, and enables the systemd unit. It deliberately does not restart an active service by default. For the initial installation, or when no exposure is active, deploy and start the release with:

```powershell
.\scripts\deploy_red_pitaya.ps1 -RestartService
```

Each release remains under `/opt/euv-acquisition/releases/<version>-<wheel-hash>`. To roll back, repoint `/opt/euv-acquisition/current` to a prior release and restart the unit.

```bash
systemctl status euv-acquisition --no-pager
journalctl -u euv-acquisition -f
systemctl restart euv-acquisition
systemctl stop euv-acquisition
```

Chamber control must use the board's instrument-LAN address:

```text
EUV_ACQUISITION_HOST=10.11.13.50
EUV_ACQUISITION_CONTROL_PORT=11760
EUV_ACQUISITION_ARTIFACT_PORT=11761
```

For WAN-only administration, run the systemd commands through `ssh euvl-red-pitaya`. The existing SSH relay exposes only SSH; the capture ports are not exposed to IllinoisNet.
