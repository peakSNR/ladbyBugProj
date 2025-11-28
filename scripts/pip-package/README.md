# Build an sdist locally
```
chmod +x package_tar.py
./package_tar.py
pip install monad-<version>.tar.gz
```

Note: installing from source requires the full toolchain for building the project, including CMake (>=3.11), Python 3, and a compiler compatible with C++20. The package works for both Linux and macOS.
