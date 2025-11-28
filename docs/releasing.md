# Releasing Monad (Avra fork)

## Prereqs
- Tag name must match `project(Monad VERSION …)` in `CMakeLists.txt` (enforced by CI).
- `APPLE_*` signing secrets available for macOS notarization (optional; CI skips signing when empty).
- `GOOGLE_ARTIFACT_REGISTRY_CREDENTIAL` service account JSON with Artifact Registry publish permission.

## Normal release (remote)
1. Update version in `CMakeLists.txt`.
2. Tag: `git tag vX.Y.Z && git push origin vX.Y.Z`.
3. GitHub Actions workflow `Build Artifacts & Release` runs:
   - Builds sdist + wheels for CPython 3.12/3.13/3.14 (Linux x86_64 + aarch64, macOS universal).
   - Builds CLI/lib tarballs for Linux (x86_64/aarch64) and macOS universal.
   - Publishes wheels/sdist to Google Artifact Registry and attaches all artifacts to the GitHub Release.

## Dry-run / local check with `act`
- Run only builds and skip publish: `act workflow_dispatch -W .github/workflows/build-release.yml -j linux-wheels --input publish_full=false` (repeat for other jobs you want to test).
- To test full graph locally, provide secrets via `--secret-file` or env; otherwise publishing will be skipped when `publish_full=false`.

## Inputs
- `publish_full` (workflow_dispatch): set to `false` to build artifacts without running publish jobs (useful with `act`).

## Notes
- CCache keys are content-hashed to reuse caches across tags.
- macOS signing/notarization is best-effort; build continues if `APPLE_IDENTITY` is blank.
