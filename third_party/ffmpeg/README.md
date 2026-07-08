# Vendored ffmpeg

UDB AssetService transcode steps use a repository-owned ffmpeg binary, not a
host `PATH` lookup. The runtime search order is:

1. `UDB_FFMPEG_BIN` pointing directly at an executable.
2. `UDB_FFMPEG_ROOT/bin/<platform>/ffmpeg(.exe)`.
3. `third_party/ffmpeg/bin/<platform>/ffmpeg(.exe)` under the process working
   directory, executable-adjacent roots, then the source checkout.

`<platform>` is one of `linux`, `macos`, or `windows`.

Use the verifier to install a reviewed binary into the expected path:

```bash
python scripts/check-vendored-ffmpeg.py --install-from /path/to/ffmpeg --write-manifest
```

Then commit the platform binary and the generated `vendored-ffmpeg.json`
manifest. The verifier runs `ffmpeg -version`, records size and SHA-256, and
fixes executable mode on Unix.

Before cutting a release, verify the committed manifest still matches the
committed binaries:

```bash
python scripts/check-vendored-ffmpeg.py --selftest
python scripts/check-vendored-ffmpeg.py --verify-manifest --all-platforms
```

The selftest covers the manifest verifier against fixture success and checksum
drift. The cross-platform mode hash-checks every platform entry and executes
`ffmpeg -version` only for the host platform, so Linux CI can still detect stale
macOS/Windows hashes without trying to execute foreign binaries.

The release Docker image sets:

```text
UDB_FFMPEG_ROOT=/app/third_party/ffmpeg
```

so the `COPY third_party ./third_party` release step makes the same vendored
binary visible inside the container.

After installing the reviewed binary, run the transcode smoke:

```bash
python scripts/ffmpeg_transcode_smoke.py --artifact-dir ffmpeg-transcode-smoke
```

That smoke generates a tiny deterministic MP4, transcodes it through the same
`libx264`/`aac`/`+faststart` command shape used by AssetService, and decodes the
result back through ffmpeg. It is the binary/container codec proof. The full
served-path proof still needs a live AssetService `TRANSCODE` pipeline run with
storage attached.
