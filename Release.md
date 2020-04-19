# MonetDB Release

## Procedure
There are several differences between a normal build and a release build:
- The name of the release
- Th monetdb version

## Implementation
There are 3 different sets of versions:
- The version description
- The monetdb version number
- The monetb libraries version numbers

The version description is "unreleased", unless there is an actual release. Than is contains the name, for example "Nov2019-SP3". The monetdb version number is the version of the entire application, previously managed with vertoo. It contains three parts, a major, minor and release number. The release number is even during development and incremented to even for the actual release version.

## Building a release
When doing a release build, the only extra thing to do is to add the "-DRELEASE_VERSION=ON" parameter to the cmake command. This will make sure that the build will use the required version string and numbers. After building a successful release the final step is to tag the current version of the code in the release branch. Then you can start the next release by incrementing the "release" number of the monetdb version by 2. Or if necessary, create a new release branch.
