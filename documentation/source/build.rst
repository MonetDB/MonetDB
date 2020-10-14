****************************
Building MonetDB from source
****************************

Summary
=======

For cmake, you should always build the code in a separate directory, say
"build". This directory should be outside of the source code tree. The
results of the build are stored in this directory. The location on the
filesystem is not important, as long as you have permissions to write in
that location.

Assuming the MonetDB source code is checked out in directory
"/path/to/monetdb/source", and if you have all the required packages
(see below) to build MonetDB, these are the set of commands to build and
install it from source. Install is one of the predefined targets
[``install``, ``test``, ``mtest``]. When you test MonetDB, you will
likely not want to install it in the default location, the standard GNU
directory structure. So you may want to set the install prefix variable
when generating the build system, using ``-DCMAKE_INSTALL_PREFIX``::

  mkdir build
  cd build
  cmake -DCMAKE_INSTALL_PREFIX=/path/to/install/monetdb /path/to/monetdb/source
  cmake --build .
  cmake --build . --target install


Prerequisites
=============

PATH settings: None
Role of clients?? How to install

## Testing

For testing, you likely don't want to install in the default location,
so you need to add the installation prefix parameter to the cmake
command. But you do not need any configuration to run mtest (on
Linux). Just run the command::

  cmake --build . --target mtest

Configuration options
=====================

The way options interact with building of the MonetDB source has
fundamentally changed from the way this was done using the autotools
build system. Now almost all options are on by default. And these options
mostly control library detection. In the old system, it was possible to
build a subset of the code base. For example, you could choose not to
build the sql part. Now the every part of the code is build, as long as
the dependent libraries are detected. And by default, the system would
try to detect all dependent libraries. If your system does not have a
required library, that section of the code will not be build. Only if
you want to prevent the build of a certain section, you could use the
option to prevent that a dependency is detected.

Evidently there are several options to control as illustrated in
``$SOURCE/cmake/monetdb-options.cmake``

The important once to choose from are ``-DCMAKE_BUILD_TYPE``, which
takes the value Release, Debug, RelWithDebInfo and MinSizeRel. The
first creates the binary ready for shipping, including all compiler
optimizations that come with it. The Debug mode is necessary if you
plan to debug the binary and needs access to the symbol tables. This
build type also typically leads to a slower execution time, because
also all kinds of assertions are being checked. The RelWithDebInfo
combines Release and Debug with both compiler optimizations and symbol
tables for debugging. Finally MinSizeRel is a Release build optimized
for binary size instead of speed.

Other relevant properties are also ``-DASSERT=ON`` and ``-DSTRICT=ON``,
used in combination with a Debug build, e.g.::

  CONFIGURE_OPTIONS="-DCMAKE_BUILD_TYPE=Debug -DASSERT=ON -DSTRICT=ON"
  mkdir build
  cd build
  cmake $CONFIGURE_OPTIONS -DCMAKE_INSTALL_PREFIX=/path/to/install/monetdb /path/to/monetdb/source
  cmake --build .
  cmake --build . --target install

Explain the role of cmake --build . --target mtest
In particular how to call it from anywhere in the tree

Platform specifics
==================

The packages required to build MonetDB from source depends mostly on the
operating system environment.  They are specified in the corresponding
README files,

README-Debian .... which version

README-Fedora .... Which version


Windows
=======

Run as Administrator::

  @"%SystemRoot%\System32\WindowsPowerShell\v1.0\powershell.exe" -NoProfile -InputFormat None -ExecutionPolicy Bypass -Command "iex ((New-Object System.Net.WebClient).DownloadString('https://chocolatey.org/install.ps1'))" && SET "PATH=%PATH%;%ALLUSERSPROFILE%\chocolatey\bin"
  choco feature enable -n allowGlobalConfirmation
  choco install ActivePerl ant ruby python3 hg git winflexbison
  cinst VisualStudio2017community --package-parameters "--add Microsoft.VisualStudio.Workload.NativeDesktop --add microsoft.visualstudio.component.vc.cmake.project --add microsoft.visualstudio.component.vc.ATLMFC"
  refreshenv

  cd \
  git clone https://github.com/microsoft/vcpkg
  cd vcpkg
  bootstrap-vcpkg.bat -disableMetrics
  vcpkg integrate install
  # needed for 64 bits (with the available python being 64 bit this is needed)
  set VCPKG_DEFAULT_TRIPLET=x64-windows
  vcpkg install libiconv openssl bzip2 geos libxml2 pcre pcre2 zlib getopt

To compile MonetDB (as normal user)::

  hg clone http://dev.monetdb.org/hg/MonetDB/

  "c:\Program Files (x86)\Microsoft Visual Studio\2017\Community\common7\tools\vsdevcmd.bat"
  "c:\Program Files (x86)\Microsoft Visual Studio\2017\Community\VC\Auxiliary\Build\vcvars64.bat"

  cd MonetDB
  mkdir build
  cd build
  cmake -G "Visual Studio 15 2017" -DCMAKE_TOOLCHAIN_FILE=/vcpkg/scripts/buildsystems/vcpkg.cmake -DCMAKE_INSTALL_PREFIX=%HOME%\install -A x64 ..
  cmake --build . --target ALL_BUILD --config Release
  cmake --build . --target INSTALL --config Release
  set PATH=%HOME%\install\bin;%HOME%\install\lib;%HOME%\install\lib\monetdb5;\vcpkg\installed\x64-windows\bin;\vcpkg\installed\x64-windows\debug\bin;%PATH%
  cmake --build . --target RUN_TESTS
  cmake --build . --target mtest

MacOS
=====

Install homebrew (this will also install the xcode tools)

Using homebrew install at least current ::

  mercurial
  cmake
  pkg-config
  pcre
  openssl
  bison

optional::

  readline
  ant
  geos
  gsl
  cfitscio

To compile MonetDB (as normal user)::

  hg clone http://dev.monetdb.org/hg/MonetDB/

  cd MonetDB
  mkdir build
  cd build
  PKG_CONFIG_PATH=/usr/local/opt/readline/lib/pkgconfig/ cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=$HOME/install -DOPENSSL_ROOT_DIR=/usr/local/opt/openssl ..
  cmake --build .
  cmake --build . --target install
  cmake --build . --target test
  cmake --build . --target mtest

How to start
============
