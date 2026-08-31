# ARMv7 runtime

This directory contains the non-reproducible-on-board portion of the Red Pitaya deployment runtime.

- Target: Ubuntu 22.04 ARMhf, ARMv7 EABI5, CPython 3.10.12.
- h5py: 3.11.0 built from the official PyPI source distribution.
- NumPy build ABI: 2.2.5, matching the Red Pitaya installation.
- HDF5: Jammy 1.10.7 runtime libraries loaded only through the release-local `LD_LIBRARY_PATH`.
- Build environment: Ubuntu 22.04 ARMhf under QEMU, Cython 3.3.0, setuptools 84.0.0.

Board validation passed with the vendor `/opt/redpitaya/lib/python/_rp_py.so` and a gzip-compressed HDF5 dataset round trip. Deployment does not modify the board's system or `/usr/local` Python packages.

`SHA256SUMS` records the validated artifacts consumed by `scripts/deploy_red_pitaya.ps1`.