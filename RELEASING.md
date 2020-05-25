# How to release

1. Make sure `CHANGELOG.md` is up-to-date with all changes since last release.
2. Update versions in all three sub-package's `__init__.py` files.
3. Merge `master` into `release`.
4. Create a new release on `release` branch with https://github.com/jupyter-incubator/sparkmagic/releases/new — make sure you do it on `release` branch, the default is `master`. Tag format is "0.15.0", no "v" prefix.

[Possibly we should drop `release` branch concept, not sure it adds anything, and just do releases on `master`, but that's how it was when I got here. —Itamar]

