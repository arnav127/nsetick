# Releasing

For maintainers. Releases are built and published by GitHub Actions; nothing is built by hand.

## Cutting a release

1. Update `version` in the root `Cargo.toml` (the Python package takes its version from it).
2. Add a section for that version at the top of `CHANGELOG.md`, headed `## X.Y.Z`. It becomes
   the release notes.
3. Commit, then tag and push:

   ```bash
   git tag vX.Y.Z
   git push origin main vX.Y.Z
   ```

The **Release** workflow then:

- checks the tag matches `Cargo.toml` and stops if it does not;
- builds the `nsetick` binary for Linux x86_64 and aarch64 (static, musl), macOS arm64 and
  x86_64, and Windows x86_64, and smoke-tests each that can run on its build machine;
- builds Python wheels (abi3, Python 3.9+) for the same platforms, Linux as manylinux2014, plus
  a source distribution, and smoke-tests them;
- creates the GitHub release with every archive, wheel and a `SHA256SUMS` file.

To check a release without publishing, run the workflow manually from the **Actions** tab
(*Release → Run workflow*). It builds everything and uploads the artifacts to the run, but
creates no release.

## Publishing to PyPI (optional)

With PyPI enabled, users can simply `pip install nsetick`. The name is currently free. One-time
setup:

1. Create an account on <https://pypi.org>.
2. Under *Your account → Publishing → Add a new pending publisher*, enter: PyPI project name
   `nsetick`, owner `arnav127`, repository `nsetick`, workflow `release.yml`, environment `pypi`.
3. In the GitHub repository, *Settings → Environments → New environment* named `pypi`.
4. *Settings → Secrets and variables → Actions → Variables*: add `PYPI_PUBLISH` = `true`.

The next tagged release then publishes to PyPI through trusted publishing; no API token is
stored anywhere.

## The wiki

The wiki's source is `docs/wiki/` in the repository, so documentation changes are reviewed with
the code. The **Wiki** workflow mirrors it to the GitHub wiki on every push to `main` that
touches `docs/wiki/`.

GitHub creates a wiki's repository only when its first page is saved, so once, before the first
sync: open the **Wiki** tab, create any page, save it. Then run *Actions → Wiki → Run
workflow*. From then on it is automatic. Edit pages in `docs/wiki/`, not in the web editor; the
next sync overwrites web edits.

## Continuous integration

The **CI** workflow runs on every push and pull request: formatting (`cargo fmt --check`), lints
(`cargo clippy`, reported), Rust tests and Python tests on Linux, macOS and Windows, and the
layout-spec validator.
