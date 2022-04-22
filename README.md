# SimpleZNSDevice
SimpleZNSDevice is a simpler wrapper around SPDK that allows developing for ZNS devices without (much) effort.
It only uses a subset of what is currently possible with SPDK, but it greatly simplifies the logic and comes with some utility functions. It is mainly developed to be used in an alteration of RocksDB, [ZNSLSM](https://github.com/Krien/znslsm). However, since the logic can also be used for other ZNS projects, it got its own repository. The project contains an interface that can be used with C and C++, see [./incude](./include) and some simple debugging and utility tools, see [./tools](tools).

# DISCLAIMER
This code is in early stages of development and prone to change. Use at own risk, for more "risk-involved" information read the rest of this section.
SPDK needs to bind the device. Therefore ensure that your device is actually binded to SPDK. This can be done by navigating to the `scripts` directory in the SPDK directory and calling `setup.sh`. When a device is binded to SPDK, it CANNOT be used anymore by your own filesystem anymore, untill it is released. Do NOT use on your main disk or disks that you intent to use for important data. There are also some helper scripts in [./scripts](scripts), such as spdk_bind.sh and spdk_unbind.sh. spdk_bind binds all devices to SPDK and spdk_unbind unbinds all devices to SPDK. Further on, always run as root with e.g. `sudo`, the permissions are needed :/. Lastly, the code is only tested on GNU/Linux, specifically Ubuntu 20.04 LTS. It will NOT run properly on non-Unix systems such as Windows (WSL included). FreeBSD, OpenBDS, MacOs etc. are all not tested and will probably not work as well. We make NO guarantees on security or safety, run at own risk for now.

# How to use the SimpleZNSDevice
SimpleZNSDevice is an interface that we can use to interface with the ZNS device through SPDK. It comes with an interface that allows to write to zones without worrying about SPDK internals. There are functions for reading lbas, writing lbas, erasing zones, getting write heads of zones and getting general device information. Functions for now are synchronous, but asynchronous functions may be added in the future.
It can at the moment be used as a:
* Shared library called `znsdevicelib`. For examples on how to use this lib, look in tools.

# How to build
There are multiple ways to built this project.
In principal only CMake and SPDK and its dependencies (DPDK, etc...) are needed.

## Setup dependencies
 The project already has SPDK as a submodule. SPDK itself also has submodules, so be sure to install those as well. For example by calling `git submodule update --init --recursive`. This should automatically use the version of SPDK that is tested with this project. This dependency then needs to be built as well, see [SPDK: Getting started](https://spdk.io/doc/getting_started.html). We do not use any additional configuration options. It is also possible to use a custom SPDK installation. To do this,  please set the SPDK dir path in an environment variable `SPDK_DIR`, otherwise the dir is not found, since there is no standard SPDK location.


## Example of building the project with CMake
After dependencies are installed, it should compile (tested on Ubuntu 20.04 LTS), but no guarantees are made. The next line is a minimal setup with CMake and make:
```bash
rm -f CMakeCache.txt
cmake .
make <TARGET>
```
if the output does not change, try cleaning the build first with:
```bash
make clean
```

<details>
  <summary>Faster builds</summary>
  
Builds can be slow, especially on old laptops or behind a VM. Therefore, we use some tricks ourself to speed up compilation, which might aid in your development as well. To further speed up compilation, we advise to use alternative build tools such as `Ninja` instead of plain old `Makefiles` and to use faster linkers such as [`mold`](https://github.com/rui314/mold) (other linkers such as GNU Gold and LDD might work as well at an increased speed, but not tested). For example:
```bash
# Remove old build artifacts first if changing build tools.
rm -f CMakeCache.txt
# Make cmake use another build tool with -G.
cmake -j$(nproc) -GNinja .
# cleaning (to force rebuilding).
mold -run ninja build.ninja clean
# Actual build, generally only this command needs to be run incrementally during development.
mold -run ninja build.ninja <TARGET>
```
</details>

<details>
    <summary> What to do about linking errors? </summary>

It is possible that even after the code is compiled without errors, the applications that use this project do not run and complain about missing SPDK libraries. This is at least true for our setup on Qemu.
This has to do with the "LD_LIBRARY_PATH" not always properly transferring when using sudo. Mold as a linker automatically solved this issue, otherwise the following might work (at your own risk):
```bash
export LD_LIBRARY_PATH="LD_LIBRARY_PATH:/<installation_directory_of_spdk>/spdk/dpdk/build/lib"
alias sudo='sudo PATH="$PATH" HOME="$HOME" LD_LIBRARY_PATH="$LD_LIBRARY_PATH"'
```
</details>

# Code guidelines
## Where to put code
Please create all headers with `.h` and put them in `./include`. Similarly, please create all implementation code with `.cpp` and put them in `./src`.
## Formatting
If submitting code, please format the code. This prevents spurious commit changes. We use `clang-format` with the config in `.clang-format`, based on `LLVM`. It is possible to automatically format with make or ninja (depending on what build tool is used). This requires clang-format to be either set in `/home/$USER/bin/clang-format` or requires setting the environmental variable `CLANG_FORMAT_PATH` directly (e.g. `export CLANG_FORMAT_PATH=...`). Then simply run:
```bash
<make_command(make,ninja,...)> format
```
## Tests
If submitting code, please ensure that the tests still pass. Either run all tests manually by building and running all tests, such as `<make_command(make,ninja,...)> device_initial_test && sudo ./bin/device_initial_test`. Or use the standard builtin testing functionalities of CMake by running:
```bash
<make_command(make,ninja,...)> test
```
## Documentation
Documentation is generated with Doxygen. This can be done with:
```bash
<make_command(make,ninja,...)> docs
```
