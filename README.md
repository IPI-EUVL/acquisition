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
