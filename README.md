# Building MonetDB from source

## Summary

For cmake, you should always build the code in a separate directory, say ${SOURCE}. 
The results of the build are stored in a location designated by ${PREFIX}, a full path
to the location you want the binaries to be stored. 
Make sure you have these environment variables set and you have write permissions to the ${PREFIX} location

Assuming the monetdb source code is checked out in  directory ${SOURCE}.
And if you have all the required packages(See below) to build MonetDB, these are the set of commands 
to build and *install* it from source. Install is one of the predefined commands [install, test, mtest]

```
mkdir build
cd build
cmake -DCMAKE_INSTALL_PREFIX=$PREFIX ${SOURCE}
cmake --build .
cmake --build . --target install
```

## Testing
For testing, you likely don't want to install in the default location, so you need to add a parameter to the cmake command.

The MonetDB Mtest.py program is installed in $PREFIX/lib/python3.7/site-packages/.
You have to set or extend the environment variable $PYTHON3PATH to include this location for Mtest.

##Configuration options
Evidently there are several options to control as illustrated in $SOURCE/cmake/monetdb-options.cmake

The important once to choose from are -DCMAKE\_BUILD\_TYPE, which takes the value Release or Debug.
The former creates the binary ready for shipping, including all compiler optimizations that come with it.
The Debug mode is necessary if you plan to debug the binary and needs access to the symbol tables.
This build type also typically leads to a slower execution time, because also all kinds of assertions
are being checked.

The relevant properties are also -DASSERT=ON and DSTRICT=ON

## Platform specifics
The packages required to built MonetDB from source depends mostly on the operating system environment. 
They are specified in the corresponding README files,

README-Debian .... which version

README-Fedora .... Which version


## Windows

## MacOS

## How to start
