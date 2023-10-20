# How to release

1. Make sure `CHANGELOG.md` is up-to-date with all changes since last release.
2. Update versions in all three sub-package's `__init__.py` files.
3. Update `appVersion` in [Chart.yaml](helm/Chart.yaml)
4. Create a new release on `master` branch with https://github.com/jupyter-incubator/sparkmagic/releases/new — Tag format is "0.15.0", no "v" prefix.
