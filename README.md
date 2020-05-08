# MonetDB

## Summary

For cmake, you should always build the code in a separete directory. For testing, you will likely don't want to install in the default location, so you need to add a parameter to the cmake command. Assuming the monetdb source code is checked out in "~/hg/MonetDB". And if you have all the required packages to build MonetDB, these are the set of commands to build and install it from source.

```
mkdir build
cd build
cmake -DCMAKE_INSTALL_PREFIX=/tmp/monetdb ~/hg/MonetDB/
cmake --build .
cmake --build . --target install
```

## Debian

## Fedora

## Windows

## MacOS

## How to start
