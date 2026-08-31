[CmdletBinding()]
param(
    [string]$Target = "euvl-red-pitaya",
    [string]$Python = "python",
    [switch]$RestartService,
    [switch]$ConfirmInstrumentIdle
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$projectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$workspaceRoot = Split-Path $projectRoot -Parent
$ecsSource = Join-Path $workspaceRoot "ecs"
$mtEventsSource = Join-Path $workspaceRoot "mt_events"
$segmentBytesSource = Join-Path $workspaceRoot "segment_bytes"
$artifactRoot = Join-Path $projectRoot "vendor\armv7"
$serviceUnit = Join-Path $projectRoot "deploy\euv-acquisition.service"
$temporaryRoot = Join-Path ([IO.Path]::GetTempPath()) ("euv-acquisition-deploy-" + [guid]::NewGuid().ToString("N"))
$wheelRoot = Join-Path $temporaryRoot "wheel"
$bundleRoot = Join-Path $temporaryRoot "bundle"
$bundleArchive = Join-Path $temporaryRoot "bundle.tar.gz"
$remoteToken = [guid]::NewGuid().ToString("N").Substring(0, 12)
$remoteArchive = "/tmp/euv-acquisition-$remoteToken.tar.gz"
$remoteStage = "/tmp/euv-acquisition-$remoteToken"
$remoteArchiveUploaded = $false

function Assert-LastExitCode {
    param([string]$Operation)

    if ($LASTEXITCODE -ne 0) {
        throw "$Operation failed with exit code $LASTEXITCODE."
    }
}

function Get-SingleFile {
    param(
        [string]$Path,
        [string]$Filter,
        [string]$Description
    )

    $matches = @(Get-ChildItem -Path $Path -Filter $Filter -File)
    if ($matches.Count -ne 1) {
        throw "Expected one $Description matching '$Filter' in '$Path'; found $($matches.Count)."
    }
    return $matches[0]
}

function Get-LowerSha256 {
    param([string]$Path)

    return (Get-FileHash -Path $Path -Algorithm SHA256).Hash.ToLowerInvariant()
}

function Write-Utf8LfText {
    param(
        [string]$Path,
        [string]$Text
    )

    $normalized = $Text.Replace("`r`n", "`n").Replace("`r", "`n")
    [IO.File]::WriteAllText($Path, $normalized, [Text.UTF8Encoding]::new($false))
}

function Write-Utf8LfLines {
    param(
        [string]$Path,
        [string[]]$Lines
    )

    Write-Utf8LfText $Path (($Lines -join "`n") + "`n")
}

if ($Target -notmatch '^[A-Za-z0-9_.@-]+$') {
    throw "Target must be an SSH host or alias containing only letters, digits, '.', '_', '@', or '-'."
}
if ($RestartService -and -not $ConfirmInstrumentIdle) {
    throw "RestartService requires ConfirmInstrumentIdle after verifying that no exposure or diagnostic capture is active."
}
if (-not (Test-Path $serviceUnit -PathType Leaf)) {
    throw "Systemd unit does not exist: $serviceUnit"
}
if (-not (Test-Path $artifactRoot -PathType Container)) {
    throw "ARMv7 artifact directory does not exist: $artifactRoot"
}
foreach ($sourcePath in @($ecsSource, $mtEventsSource, $segmentBytesSource)) {
    if (-not (Test-Path (Join-Path $sourcePath "pyproject.toml") -PathType Leaf)) {
        throw "Required workspace dependency source does not exist: $sourcePath"
    }
}

$h5pyWheel = Get-SingleFile $artifactRoot "h5py-3.11.0-cp310-cp310-linux_armv7l.whl" "h5py wheel"
$runtimeRoot = Join-Path $artifactRoot "hdf5-runtime"
$runtimeNames = @(
    "libaec.so.0",
    "libhdf5_serial.so.103",
    "libhdf5_serial_hl.so.100",
    "libsz.so.2"
)
foreach ($runtimeName in $runtimeNames) {
    $runtimePath = Join-Path $runtimeRoot $runtimeName
    if (-not (Test-Path $runtimePath -PathType Leaf)) {
        throw "Required HDF5 runtime library does not exist: $runtimePath"
    }
}

New-Item -ItemType Directory -Path $wheelRoot, $bundleRoot | Out-Null
try {
    Write-Output "Building the application wheel..."
    & $Python -m pip wheel --no-cache-dir --no-deps --wheel-dir $wheelRoot $projectRoot $ecsSource $mtEventsSource $segmentBytesSource
    Assert-LastExitCode "Release wheel build"
    $applicationWheel = Get-SingleFile $wheelRoot "ipi_euv_acquisition-*.whl" "application wheel"
    $ecsWheel = Get-SingleFile $wheelRoot "ipi_ecs-*.whl" "ECS wheel"
    $mtEventsWheel = Get-SingleFile $wheelRoot "mt_events-*.whl" "mt-events wheel"
    $segmentBytesWheel = Get-SingleFile $wheelRoot "segment_bytes-*.whl" "segment-bytes wheel"
    if ($applicationWheel.Name -notmatch '^ipi_euv_acquisition-(?<Version>.+)-py3-none-any\.whl$') {
        throw "Unexpected application wheel name: $($applicationWheel.Name)"
    }

    $applicationVersion = $Matches.Version
    $applicationHash = Get-LowerSha256 -Path $applicationWheel.FullName
    $releaseId = "$applicationVersion-$($applicationHash.Substring(0, 12))"
    if ($releaseId -notmatch '^[A-Za-z0-9._+-]+$') {
        throw "Generated release ID contains unsupported characters: $releaseId"
    }

    Copy-Item $applicationWheel.FullName $bundleRoot
    Copy-Item $ecsWheel.FullName $bundleRoot
    Copy-Item $mtEventsWheel.FullName $bundleRoot
    Copy-Item $segmentBytesWheel.FullName $bundleRoot
    Copy-Item $h5pyWheel.FullName $bundleRoot
    Write-Utf8LfText (Join-Path $bundleRoot "euv-acquisition.service") ([IO.File]::ReadAllText($serviceUnit))
    $bundleRuntimeRoot = Join-Path $bundleRoot "lib"
    New-Item -ItemType Directory -Path $bundleRuntimeRoot | Out-Null
    foreach ($runtimeName in $runtimeNames) {
        Copy-Item (Join-Path $runtimeRoot $runtimeName) $bundleRuntimeRoot
    }

    $releaseMetadata = @(
        "release_id=$releaseId",
        "application_version=$applicationVersion",
        "application_sha256=$applicationHash",
        "ipi_ecs_sha256=$(Get-LowerSha256 -Path $ecsWheel.FullName)",
        "mt_events_sha256=$(Get-LowerSha256 -Path $mtEventsWheel.FullName)",
        "segment_bytes_sha256=$(Get-LowerSha256 -Path $segmentBytesWheel.FullName)",
        "h5py_sha256=$(Get-LowerSha256 -Path $h5pyWheel.FullName)"
    )
    $releasePath = Join-Path $bundleRoot "RELEASE"
    Write-Utf8LfLines -Path $releasePath -Lines $releaseMetadata

    $checksumLines = @(
        Get-ChildItem $bundleRoot -Recurse -File |
            Sort-Object FullName |
            ForEach-Object {
                $filePath = $_.FullName
                $relativePath = $filePath.Substring($bundleRoot.Length + 1).Replace('\', '/')
                "$(Get-LowerSha256 -Path $filePath)  $relativePath"
            }
    )
    $checksumPath = Join-Path $bundleRoot "SHA256SUMS"
    Write-Utf8LfLines -Path $checksumPath -Lines $checksumLines

    foreach ($linuxTextPath in @($releasePath, $checksumPath, (Join-Path $bundleRoot "euv-acquisition.service"))) {
        if ([Array]::IndexOf([IO.File]::ReadAllBytes($linuxTextPath), [byte]13) -ge 0) {
            throw "Linux deployment text contains a carriage return: $linuxTextPath"
        }
    }
    foreach ($checksumLine in $checksumLines) {
        $expectedHash, $relativePath = $checksumLine -split '  ', 2
        $localPath = Join-Path $bundleRoot $relativePath
        $actualHash = Get-LowerSha256 -Path $localPath
        if ($actualHash -ne $expectedHash) {
            throw "Local bundle checksum mismatch for $relativePath."
        }
    }
    $bundleManifestHash = Get-LowerSha256 -Path $checksumPath

    Write-Output "Packaging release $releaseId..."
    & tar.exe -czf $bundleArchive -C $bundleRoot .
    Assert-LastExitCode "Deployment bundle creation"

    Write-Output "Checking SSH access to $Target..."
    & ssh.exe -o BatchMode=yes -o ConnectionAttempts=1 -o ConnectTimeout=10 $Target true
    Assert-LastExitCode "SSH preflight"

    Write-Output "Uploading release $releaseId..."
    & scp.exe $bundleArchive "${Target}:$remoteArchive"
    Assert-LastExitCode "Deployment bundle upload"
    $remoteArchiveUploaded = $true

    $activation = if ($RestartService) { "restart" } else { "defer" }
    $remoteScript = @'
set -euo pipefail

archive=$1
stage=$2
release_id=$3
bundle_manifest_sha256=$4
activation=$5
application_root=/opt/euv-acquisition
releases_root=$application_root/releases
release_path=$releases_root/$release_id
current_link=$application_root/current
unit_name=euv-acquisition.service
temporary_release=$releases_root/.$release_id.tmp.$$

cleanup() {
    rm -rf -- "$stage" "$temporary_release"
    rm -f -- "$archive"
}
trap cleanup EXIT

install -d -m 0700 "$stage"
tar -xzf "$archive" -C "$stage"
cd "$stage"
printf '%s  %s\n' "$bundle_manifest_sha256" SHA256SUMS | sha256sum --check
sha256sum --check SHA256SUMS
grep -Fqx 'Environment="EUV_CAPTURE_MODE=legacy-single-shot"' euv-acquisition.service
grep -Fq -- '--capture-queue-capacity 32 --persistence-queue-capacity 8 --control-queue-capacity 512 --pipeline-drain-timeout-seconds 10' euv-acquisition.service
systemd-analyze verify "$stage/euv-acquisition.service"

install -d -m 0755 "$releases_root"
if [ -e "$release_path" ]; then
    if [ ! -f "$release_path/.bundle-sha256" ] || [ "$(cat "$release_path/.bundle-sha256")" != "$bundle_manifest_sha256" ]; then
        echo "Release path already exists with different contents: $release_path" >&2
        exit 1
    fi
    echo "Reusing verified release $release_id."
else
    install -d -m 0755 "$temporary_release/python" "$temporary_release/lib"
    application_wheel=$(find "$stage" -maxdepth 1 -type f -name 'ipi_euv_acquisition-*.whl' -print -quit)
    ecs_wheel=$(find "$stage" -maxdepth 1 -type f -name 'ipi_ecs-*.whl' -print -quit)
    mt_events_wheel=$(find "$stage" -maxdepth 1 -type f -name 'mt_events-*.whl' -print -quit)
    segment_bytes_wheel=$(find "$stage" -maxdepth 1 -type f -name 'segment_bytes-*.whl' -print -quit)
    h5py_wheel=$(find "$stage" -maxdepth 1 -type f -name 'h5py-3.11.0-cp310-cp310-linux_armv7l.whl' -print -quit)
    test -n "$application_wheel"
    test -n "$ecs_wheel"
    test -n "$mt_events_wheel"
    test -n "$segment_bytes_wheel"
    test -n "$h5py_wheel"
    /usr/bin/python3 -m pip install --no-index --no-deps --target "$temporary_release/python" "$application_wheel" "$ecs_wheel" "$mt_events_wheel" "$segment_bytes_wheel" "$h5py_wheel"
    install -m 0644 "$stage"/lib/*.so.* "$temporary_release/lib/"

    RELEASE_PYTHON_ROOT="$temporary_release/python" \
    PYTHONPATH="$temporary_release/python:/opt/redpitaya/lib/python" \
    LD_LIBRARY_PATH="$temporary_release/lib" \
    /usr/bin/python3 - <<'PY'
import os
import pathlib
import sys
import tempfile

import h5py
import numpy as np
import rp
import _rp_py
import segment_bytes
from euv_acquisition import red_pitaya_service
from euv_acquisition.simulator_controls import SimulatorFaultControls
from ipi_ecs.core.tcp import TCPClientSocket
from ipi_ecs.dds.client import DDSClient
from ipi_ecs.logging.client import LogClient

release_python_root = pathlib.Path(os.environ["RELEASE_PYTHON_ROOT"]).resolve()
assert sys.version_info[:2] == (3, 10), sys.version
assert np.__version__ == "2.2.5", np.__version__
assert h5py.__version__ == "3.11.0", h5py.__version__
assert h5py.version.hdf5_version == "1.10.7", h5py.version.hdf5_version
assert pathlib.Path(rp.__file__).resolve().is_relative_to("/opt/redpitaya/lib/python")
assert pathlib.Path(_rp_py.__file__).resolve().is_relative_to("/opt/redpitaya/lib/python")
assert pathlib.Path(segment_bytes.__file__).resolve().is_relative_to(release_python_root)
required_legacy_symbols = {
    "RP_CH_1",
    "RP_DEC_1",
    "RP_OK",
    "RP_TRIG_SRC_EXT_PE",
    "RP_TRIG_STATE_TRIGGERED",
    "fBuffer",
    "rp_AcqGetBufferFillState",
    "rp_AcqGetDataV",
    "rp_AcqGetTriggerState",
    "rp_AcqGetWritePointerAtTrig",
    "rp_AcqReset",
    "rp_AcqSetDecimation",
    "rp_AcqSetTriggerDelay",
    "rp_AcqSetTriggerSrc",
    "rp_AcqStart",
    "rp_Init",
    "rp_Release",
}
missing_legacy_symbols = sorted(name for name in required_legacy_symbols if not hasattr(rp, name))
assert not missing_legacy_symbols, missing_legacy_symbols
with tempfile.TemporaryDirectory(prefix="euv-acquisition-config-") as spool:
    args = red_pitaya_service._parse_args([
        "--spool", spool,
        "--capture-mode", "legacy-single-shot",
        "--capture-queue-capacity", "32",
        "--persistence-queue-capacity", "8",
        "--control-queue-capacity", "512",
        "--pipeline-drain-timeout-seconds", "10",
    ])
    server = red_pitaya_service._build_server(args)
    assert server.engine.source.state == "stopped"
    assert server.engine.source.requested_capture_mode == "legacy-single-shot"
    assert server.config.capture_queue_capacity == 32
    assert server.config.persistence_queue_capacity == 8
    assert server.config.control_queue_capacity == 512
    assert server.config.pipeline_drain_timeout_seconds == 10.0
assert SimulatorFaultControls().status_value()["pll_locked"] is True
assert TCPClientSocket is not None
assert DDSClient is not None
assert LogClient is not None

expected = np.arange(12, dtype=np.float32).reshape(3, 4)
with tempfile.TemporaryDirectory(prefix="euv-acquisition-deploy-") as directory:
    path = pathlib.Path(directory) / "preflight.h5"
    with h5py.File(path, "w") as output:
        output.create_dataset("samples", data=expected, compression="gzip")
    with h5py.File(path, "r") as source:
        np.testing.assert_array_equal(source["samples"][:], expected)
print("release_preflight_ok")
PY

    printf '%s\n' "$bundle_manifest_sha256" > "$temporary_release/.bundle-sha256"
    cp "$stage/RELEASE" "$temporary_release/RELEASE"
    find "$temporary_release" -type f -exec chmod a-w {} +
    find "$temporary_release" -type d -exec chmod 0555 {} +
    mv "$temporary_release" "$release_path"
fi

previous_release=
if [ -L "$current_link" ]; then
    previous_release=$(readlink -f "$current_link")
fi
temporary_link=$application_root/.current.$$.tmp
ln -s "$release_path" "$temporary_link"
mv -Tf "$temporary_link" "$current_link"
install -m 0644 "$stage/euv-acquisition.service" "/etc/systemd/system/$unit_name"
systemctl daemon-reload
systemctl enable "$unit_name" >/dev/null

if [ "$activation" = restart ]; then
    if ! systemctl restart "$unit_name" || ! systemctl is-active --quiet "$unit_name"; then
        echo "The new release failed to start; restoring the previous release." >&2
        if [ -n "$previous_release" ] && [ -d "$previous_release" ]; then
            rollback_link=$application_root/.current.rollback.$$.tmp
            ln -s "$previous_release" "$rollback_link"
            mv -Tf "$rollback_link" "$current_link"
            systemctl restart "$unit_name" || true
        else
            rm -f "$current_link"
            systemctl stop "$unit_name" || true
        fi
        exit 1
    fi
fi

echo "DEPLOYED_RELEASE=$release_id"
echo "PREVIOUS_RELEASE=${previous_release:-none}"
echo "CURRENT_RELEASE=$(readlink -f "$current_link")"
echo "SERVICE_STATE=$(systemctl is-active "$unit_name" 2>/dev/null || true)"
if [ "$activation" = defer ]; then
    echo "Activation was deferred; restart the service after the current exposure."
fi
'@
    $encodedScript = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes(($remoteScript -replace "`r", "")))
    & ssh.exe $Target "printf '%s' '$encodedScript' | base64 -d | bash -s -- '$remoteArchive' '$remoteStage' '$releaseId' '$bundleManifestHash' '$activation'"
    Assert-LastExitCode "Remote release installation"
    $remoteArchiveUploaded = $false

    Write-Output "Release $releaseId deployed successfully."
    if (-not $RestartService) {
        Write-Output "The running service was not restarted. Activate later with: ssh $Target sudo systemctl restart euv-acquisition"
    }
}
finally {
    if ($remoteArchiveUploaded) {
        & ssh.exe -o BatchMode=yes -o ConnectTimeout=5 $Target "rm -f -- '$remoteArchive'; rm -rf -- '$remoteStage'" 2>$null
    }
    Remove-Item $temporaryRoot -Recurse -Force -ErrorAction SilentlyContinue
}