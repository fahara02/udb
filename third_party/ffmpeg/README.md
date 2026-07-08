# ffmpeg runtime contract

UDB AssetService transcode steps use an explicit ffmpeg executable, not an
implicit host `PATH` lookup. The runtime search order is:

1. `UDB_FFMPEG_BIN` pointing directly at an executable.
2. `UDB_FFMPEG_ROOT/bin/<platform>/ffmpeg(.exe)`.
3. `third_party/ffmpeg/bin/<platform>/ffmpeg(.exe)` under the process working
   directory, executable-adjacent roots, then the source checkout.

`<platform>` is one of `linux`, `macos`, or `windows`.

GitHub does not accept the reviewed Windows ffmpeg executable in normal Git
history because it is larger than 100 MB. Release CI and the release Docker
image therefore install ffmpeg from the OS package manager and set
`UDB_FFMPEG_BIN` to the resolved executable. The `third_party/ffmpeg` layout is
kept as an optional local/offline cache contract for deployments that want to
ship reviewed binaries beside the broker.

Use the verifier to install a reviewed local/offline binary into the optional
cache path:

```bash
python scripts/check-vendored-ffmpeg.py --install-from /path/to/ffmpeg --write-manifest
```

Then store the platform binary in your release artifact store and keep the
generated `vendored-ffmpeg.json` manifest with that artifact. The verifier runs
`ffmpeg -version`, records size and SHA-256, and fixes executable mode on Unix.

Before cutting an offline bundle, verify the manifest still matches the staged
binaries:

```bash
python scripts/check-vendored-ffmpeg.py --selftest
python scripts/check-vendored-ffmpeg.py --verify-manifest --all-platforms
```

The selftest covers the manifest verifier against fixture success and checksum
drift. The cross-platform mode hash-checks every platform entry and executes
`ffmpeg -version` only for the host platform, so Linux CI can still detect stale
macOS/Windows hashes without trying to execute foreign binaries.

The release Docker image installs ffmpeg from Debian and sets:

```text
UDB_FFMPEG_BIN=/usr/bin/ffmpeg
```

so the broker fails closed if the package is missing instead of silently using a
different `PATH` executable.

After installing or selecting a reviewed binary, run the transcode smoke:

```bash
python scripts/ffmpeg_transcode_smoke.py --ffmpeg-bin /path/to/ffmpeg --artifact-dir ffmpeg-transcode-smoke
```

That smoke generates a tiny deterministic MP4, transcodes it through the same
`libx264`/`aac`/`+faststart` command shape used by AssetService, and decodes the
result back through ffmpeg. It is the binary/container codec proof. The full
served-path proof still needs a live AssetService `TRANSCODE` pipeline run with
storage attached.
