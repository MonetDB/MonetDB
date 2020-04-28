#[[
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
#]]

cmake_minimum_required(VERSION 3.12)

set(CMAKE_MODULE_PATH
  "${CMAKE_SOURCE_DIR}/cmake"
  "${CMAKE_SOURCE_DIR}/cmake/Modules"
  CACHE
  INTERNAL
  "Location of custom CMake modules.")


include(CMakeDependentOption REQUIRED)

include(monetdb-options)
include(monetdb-versions)
project(MonetDB VERSION "${MONETDB_VERSION}" LANGUAGES C)
set(C_STANDARD_REQUIRED ON)
set(CMAKE_C_STANDARD 99)

if(${CMAKE_VERSION} VERSION_GREATER_EQUAL "3.13.0")
  cmake_policy(SET CMP0076 OLD)
endif()

set(CMAKE_FIND_FRAMEWORK LAST) # We give precedence to UNIX include directories over OS X Frameworks directories
set(CMAKE_FIND_APPBUNDLE LAST)

include(CheckCSourceCompiles REQUIRED)
include(CheckIncludeFile REQUIRED)
include(CMakePushCheckState REQUIRED)
include(CheckStructHasMember REQUIRED)

include(CheckSymbolExists REQUIRED)
include(CheckFunctionExists REQUIRED)
include(CheckTypeSize REQUIRED)

include(TestBigEndian REQUIRED)

include(monetdb-functions)
include(monetdb-findpackages)
monetdb_hg_revision()

check_symbol_exists("opendir" "dirent.h" HAVE_DIRENT_H)
find_path(HAVE_DISPATCH_DISPATCH_H "dispatch/dispatch.h")
find_path(HAVE_DLFCN_H "dlfcn.h")
find_path(HAVE_FCNTL_H "fcntl.h")
find_path(HAVE_ICONV_H "iconv.h")
find_path(HAVE_IO_H "io.h")
find_path(HAVE_KVM_H "kvm.h")
find_path(HAVE_LANGINFO_H "langinfo.h")
find_path(HAVE_LIBGEN_H "libgen.h")
find_path(HAVE_LIBINTL_H "libintl.h")
find_path(HAVE_MACH_MACH_INIT_H "mach/mach_init.h")
find_path(HAVE_MACH_TASK_H "mach/task.h")
find_path(HAVE_MACH_O_DYLD_H "mach-o/dyld.h")
check_symbol_exists("gethostbyname" "netdb.h" HAVE_NETDB_H)
find_path(HAVE_NETINET_IN_H "netinet/in.h")
find_path(HAVE_POLL_H "poll.h")
find_path(HAVE_PROCFS_H "procfs.h")
find_path(HAVE_PWD_H "pwd.h")
find_path(HAVE_STRINGS_H "strings.h")
find_path(HAVE_STROPTS_H "stropts.h")
find_path(HAVE_SYS_FILE_H "sys/file.h")
find_path(HAVE_SYS_IOCTL_H "sys/ioctl.h")
find_path(HAVE_SYS_IOCTL_H "sys/sysctl.h")
find_path(HAVE_SYS_MMAN_H "sys/mman.h")
find_path(HAVE_SYS_PARAM_H "sys/param.h")
find_path(HAVE_SYS_RESOURCE_H "sys/resource.h")
check_symbol_exists("setsockopt" "sys/socket.h" HAVE_SYS_SOCKET_H)
check_symbol_exists("gettimeofday" "sys/time.h" HAVE_SYS_TIME_H)
find_path(HAVE_SYS_TIMES_H "sys/times.h")
find_path(HAVE_SYS_UIO_H "sys/uio.h")
find_path(HAVE_SYS_UN_H "sys/un.h")
find_path(HAVE_SYS_UTIME_H "sys/utime.h")
find_path(HAVE_SYS_WAIT_H "sys/wait.h")
find_path(HAVE_TERMIOS_H "sys/termios.h")
find_path(HAVE_UNISTD_H "unistd.h")
find_path(HAVE_UUID_UUID_H "uuid/uuid.h")
find_path(HAVE_WINSOCK_H "winsock2.h")

find_path(HAVE_SYS_TYPES_H "sys/types.h")
find_path(HAVE_SEMAPHORE_H "semaphore.h")
find_path(HAVE_GETOPT_H "getopt.h")
if(HAVE_GETOPT_H)
	set(HAVE_GETOPT 1)
endif()

check_include_file("stdatomic.h" HAVE_STDATOMIC_H)

# Linux specific, in the future, it might be ported to other platforms
check_symbol_exists("S_ISREG" "sys/stat.h" HAVE_SYS_STAT_H)
check_symbol_exists("getaddrinfo" "netdb.h" HAVE_GETADDRINFO)
#check_symbol_exists("WSADATA" "winsock2.h" HAVE_WINSOCK_H)
check_symbol_exists("fdatasync" "unistd.h" HAVE_FDATASYNC)

check_symbol_exists("accept4" "sys/types.h;sys/socket.h" HAVE_ACCEPT4) # Some libc versions on Linux distributions don't have it
check_symbol_exists("asctime_r" "time.h" HAVE_ASCTIME_R)
check_symbol_exists("clock_gettime" "time.h" HAVE_CLOCK_GETTIME)
check_symbol_exists("ctime_r" "time.h" HAVE_CTIME_R)
check_symbol_exists("dispatch_semaphore_create" "dispatch/dispatch.h" HAVE_DISPATCH_SEMAPHORE_CREATE)
check_symbol_exists("fallocate" "fcntl.h" HAVE_FALLOCATE) # Linux specific, in the future, it might be ported to other platforms
check_function_exists("fcntl" HAVE_FCNTL)
check_symbol_exists("fork" "unistd.h" HAVE_FORK)
check_symbol_exists("fsync" "unistd.h" HAVE_FSYNC)
check_symbol_exists("ftime" "sys/timeb.h" HAVE_FTIME)
check_function_exists("getexecname" HAVE_GETEXECNAME)
check_function_exists("getlogin" HAVE_GETLOGIN)
check_symbol_exists("getopt_long" "getopt.h" HAVE_GETOPT_LONG)
check_function_exists("getrlimit" HAVE_GETRLIMIT)
check_function_exists("gettimeofday" HAVE_GETTIMEOFDAY)
check_function_exists("getuid" HAVE_GETUID)
check_symbol_exists("gmtime_r" "time.h" HAVE_GMTIME_R)
check_symbol_exists("localtime_r" "time.h" HAVE_LOCALTIME_R)
check_symbol_exists("strerror_r" "string.h" HAVE_STRERROR_R)
check_function_exists("lockf" HAVE_LOCKF)
check_symbol_exists("madvise" "sys/mman.h" HAVE_MADVISE)
check_symbol_exists("mremap" "sys/mman.h" HAVE_MREMAP)
check_function_exists("nanosleep" HAVE_NANOSLEEP)
check_function_exists("nl_langinfo" HAVE_NL_LANGINFO)
check_function_exists("_NSGetExecutablePath" HAVE__NSGETEXECUTABLEPATH)
check_symbol_exists("pipe2" "fcntl.h;unistd.h" HAVE_PIPE2) # Some libc versions on Linux distributions don't have it
check_function_exists("poll" HAVE_POLL) 
check_symbol_exists("popen" "stdio.h" HAVE_POPEN)
check_symbol_exists("posix_fadvise" "fcntl.h" HAVE_POSIX_FADVISE) 
check_symbol_exists("posix_fallocate" "fcntl.h" HAVE_POSIX_FALLOCATE) # Some POSIX systems don't have it (e.g. Macos)
check_symbol_exists("posix_madvise" "sys/mman.h" HAVE_POSIX_MADVISE)
check_function_exists("putenv" HAVE_PUTENV) 
check_function_exists("setsid" HAVE_SETSID) 
check_function_exists("shutdown" HAVE_SHUTDOWN) 
check_function_exists("sigaction" HAVE_SIGACTION) 
check_function_exists("stpcpy" HAVE_STPCPY) 
check_function_exists("strcasestr" HAVE_STRCASESTR) 
check_symbol_exists("strncasecmp" "strings.h" HAVE_STRNCASECMP)
check_function_exists("strptime" HAVE_STRPTIME) 
check_function_exists("strsignal" HAVE_STRSIGNAL) 
check_symbol_exists("sysconf" "unistd.h" HAVE_SYSCONF)
check_function_exists("task_info" HAVE_TASK_INFO) 
check_function_exists("times" HAVE_TIMES) 
check_function_exists("uname" HAVE_UNAME) 

check_symbol_exists("semtimedop" "sys/types.h;sys/ipc.h;sys/sem.h" HAVE_SEMTIMEDOP) # Some libc versions on Linux distributions don't have it

# Likely unused, because it contained typo's
if(HAVE_PTHREAD_H)
  check_function_exists("pthread_kill" HAVE_PTHREAD_KILL)
  check_function_exists("pthread_sigmask" HAVE_PTHREAD_SIGMASK)
endif()

# On C99, but we have to calculate the size
check_type_size(size_t SIZEOF_SIZE_T LANGUAGE C)
set(SIZEOF_VOID_P ${CMAKE_SIZEOF_VOID_P})
if(HAVE_NETDB_H)
  set(CMAKE_EXTRA_INCLUDE_FILES "netdb.h" "unistd.h")
endif()
if(HAVE_WINSOCK_H)
  set(CMAKE_EXTRA_INCLUDE_FILES "winsock.h")
endif()
check_type_size(socklen_t HAVE_SOCKLEN_T LANGUAGE C)

set(CMAKE_REQUIRED_INCLUDES "/usr/include")

# Set variables to define C macro's
# These are related to the detected packages
set(HAVE_ICONV ${Iconv_FOUND})
set(HAVE_PTHREAD_H ${CMAKE_USE_PTHREADS_INIT})
set(HAVE_LIBPCRE ${PCRE_FOUND})
set(HAVE_OPENSSL ${OPENSSL_FOUND})
set(HAVE_COMMONCRYPTO ${COMMONCRYPTO_FOUND})
set(HAVE_LIBBZ2 ${BZIP2_FOUND})
set(HAVE_CURL ${CURL_FOUND})
set(HAVE_LIBLZMA ${LIBLZMA_FOUND})
set(HAVE_LIBXML ${LibXml2_FOUND})
set(HAVE_LIBZ ${ZLIB_FOUND})
set(HAVE_LZ4 ${LZ4_FOUND})
set(HAVE_PROJ ${PROJ_FOUND})
set(HAVE_SNAPPY ${SNAPPY_FOUND})
set(HAVE_UUID ${HAVE_UUID_GENERATE})
set(HAVE_VALGRIND ${VALGRIND_FOUND})

set(SOCKET_LIBRARIES "")
if (WIN32)
  set(SOCKET_LIBRARIES "ws2_32")
endif()

cmake_push_check_state()
if(COMMONCRYPTO_FOUND)
  #set(CMAKE_REQUIRED_INCLUDES "${COMMONCRYPTO_INCUDE_DIR}")
  set(CMAKE_REQUIRED_LIBRARIES "${COMMONCRYPTO_LIBRARIES}")

  check_symbol_exists("CC_MD5_Update" "CommonCrypto/CommonDigest.h" HAVE_MD5_UPDATE)
  check_symbol_exists("CC_SHA1_Update" "CommonCrypto/CommonDigest.h" HAVE_SHA1_UPDATE)
  check_symbol_exists("CC_SHA224_Update" "CommonCrypto/CommonDigest.h" HAVE_SHA224_UPDATE)
  check_symbol_exists("CC_SHA256_Update" "CommonCrypto/CommonDigest.h" HAVE_SHA256_UPDATE)
  check_symbol_exists("CC_SHA384_Update" "CommonCrypto/CommonDigest.h" HAVE_SHA384_UPDATE)
  check_symbol_exists("CC_SHA512_Update" "CommonCrypto/CommonDigest.h" HAVE_SHA512_UPDATE)

  add_library(OpenSSL::Crypto UNKNOWN IMPORTED)
  set_target_properties(OpenSSL::Crypto PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES "${COMMONCRYPTO_INCLUDE_DIR}")
  set_target_properties(OpenSSL::Crypto PROPERTIES
    IMPORTED_LINK_INTERFACE_LANGUAGES "C"
    IMPORTED_LOCATION "${COMMONCRYPTO_LIBRARIES}")
endif()
if(OPENSSL_FOUND)
  #set(CMAKE_REQUIRED_INCLUDES "${OPENSSL_INCUDE_DIR}")
  #set(CMAKE_REQUIRED_LIBRARIES "${OPENSSL_LIBRARIES}")

  set(HAVE_OPENSSL ON CACHE INTERNAL "OpenSSL is available")
  set(CRYPTO_INCLUDE_DIR "${OPENSSL_INCLUDE_DIR}" CACHE INTERNAL "crypto include directory")
  set(CRYPTO_LIBRARIES "${OPENSSL_CRYPTO_LIBRARY}" CACHE INTERNAL "crypto libraries to link")
  set(CMAKE_REQUIRED_INCLUDES "${CMAKE_REQUIRED_INCLUDES};${CRYPTO_INCLUDE_DIR}")
  set(CMAKE_REQUIRED_LIBRARIES "${CMAKE_REQUIRED_LIBRARIES};${CRYPTO_LIBRARIES}")

  check_symbol_exists("MD5_Update" "openssl/md5.h" HAVE_MD5_UPDATE)
  check_symbol_exists("RIPEMD160_Update" "openssl/ripemd.h" HAVE_RIPEMD160_UPDATE)
  check_symbol_exists("SHA1_Update" "openssl/sha.h" HAVE_SHA1_UPDATE)
  check_symbol_exists("SHA224_Update" "openssl/sha.h" HAVE_SHA224_UPDATE)
  check_symbol_exists("SHA256_Update" "openssl/sha.h" HAVE_SHA256_UPDATE)
  check_symbol_exists("SHA384_Update" "openssl/sha.h" HAVE_SHA384_UPDATE)
  check_symbol_exists("SHA512_Update" "openssl/sha.h" HAVE_SHA512_UPDATE)
endif()
cmake_pop_check_state()

check_type_size(ssize_t SIZEOF_SSIZE_T LANGUAGE C)
if(NOT HAVE_SIZEOF_SSIZE_T)
  # Set a default value
  if(CMAKE_SIZEOF_VOID_P EQUAL 8)
    set(ssize_t "int64_t")
  else()
    set(ssize_t "int32_t")
  endif()
  set(SIZEOF_SSIZE_T ${CMAKE_SIZEOF_VOID_P})
endif()
check_type_size(char SIZEOF_CHAR LANGUAGE C)
check_type_size(short SIZEOF_SHORT LANGUAGE C)
check_type_size(int SIZEOF_INT LANGUAGE C)
check_type_size(long SIZEOF_LONG LANGUAGE C)
check_type_size(wchar_t SIZEOF_WCHAR_T LANGUAGE C)

if(MSVC)
  set(_Noreturn "__declspec(noreturn)")
  # C99 feature not present in MSVC
  set(restrict "__restrict")
  # C99 feature only available on C++ compiler in MSVC
  # https://docs.microsoft.com/en-us/cpp/cpp/inline-functions-cpp?view=vs-2015
  set(inline "__inline")
endif()

set(DIR_SEP  "/")
set(PATH_SEP ":")
set(DIR_SEP_STR  "/")
set(SO_PREFIX "${CMAKE_SHARED_LIBRARY_PREFIX}")
set(SO_EXT "${CMAKE_SHARED_LIBRARY_SUFFIX}")

include(GNUInstallDirs)

set(BINDIR "${CMAKE_INSTALL_FULL_BINDIR}")
set(LIBDIR "${CMAKE_INSTALL_FULL_LIBDIR}")
set(DATADIR "${CMAKE_INSTALL_FULL_DATADIR}")
set(DATA_DIR "${CMAKE_INSTALL_FULL_DATADIR}")
set(LOCALSTATEDIR "${CMAKE_INSTALL_FULL_LOCALSTATEDIR}")
if(WIN32)
  string(REPLACE "/" "\\\\" QXLOCALSTATEDIR "${LOCALSTATEDIR}") # Fix cmake conversions
endif()
set(MONETDB_PREFIX "${CMAKE_INSTALL_PREFIX}")
if(WIN32)
  string(REPLACE "/" "\\\\" MONETDB_PREFIX "${CMAKE_INSTALL_PREFIX}") # Fix cmake conversions
endif()

set(DATAROOTDIR "${CMAKE_INSTALL_FULL_DATAROOTDIR}")
set(BIN_DIR "${CMAKE_INSTALL_FULL_BINDIR}")
set(INCLUDEDIR "${CMAKE_INSTALL_FULL_INCLUDEDIR}")
set(INFODIR "${CMAKE_INSTALL_FULL_INFODIR}")
set(LIB_DIR "${CMAKE_INSTALL_FULL_LIBDIR}")
set(LIBEXECDIR "${CMAKE_INSTALL_FULL_LIBEXECDIR}")
set(LOCALSTATE_DIR "${CMAKE_INSTALL_FULL_LOCALSTATEDIR}")
# set(MANDIR "${CMAKE_INSTALL_FULL_MANDIR}")
set(SYSCONFDIR "${CMAKE_INSTALL_FULL_SYSCONFDIR}")
set(LOGDIR "${CMAKE_INSTALL_FULL_LOCALSTATEDIR}/log/monetdb"
  CACHE PATH
  "Where to put log files (default LOCALSTATEDIR/log/monetdb)")
set(PKGCONFIGDIR "${LIBDIR}/pkgconfig")
set(RUNDIR
  "${CMAKE_INSTALL_FULL_LOCALSTATEDIR}/run/monetdb"
  CACHE PATH
  "Where to put pid files (default LOCALSTATEDIR/run/monetdb)")

# Define custom target after the install directories variables.
# Some custom target will need these setting.
include(monetdb-custom-targets)

if(${CMAKE_SYSTEM_NAME} STREQUAL "Linux")
  add_definitions(-D_XOPEN_SOURCE -D_GNU_SOURCE)
  set(CMAKE_REQUIRED_DEFINITIONS "${CMAKE_REQUIRED_DEFINITIONS} -D_GNU_SOURCE") # required for tests
endif()

# Used for installing testing python module
execute_process(COMMAND "${Python3_EXECUTABLE}" "-c" "import distutils.sysconfig; print(distutils.sysconfig.get_python_lib(0,0,'${MONETDB_PREFIX}'))"
		RESULT_VARIABLE PY3_LIBDIR_CODE 
		OUTPUT_VARIABLE PYTHON3_SITEDIR 
		OUTPUT_STRIP_TRAILING_WHITESPACE)
if (PY3_LIBDIR_CODE) 
  message(WARNING "Could not determine MonetDB Python3 site-packages instalation directory")
endif()
set(PYTHON3_LIBDIR "${PYTHON3_SITEDIR}")
set(PYTHON "${Python3_EXECUTABLE}")

SET(CMAKE_SKIP_BUILD_RPATH  FALSE)
SET(CMAKE_BUILD_WITH_INSTALL_RPATH FALSE)
SET(CMAKE_INSTALL_RPATH "${CMAKE_INSTALL_FULL_LIBDIR}")
SET(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)

# required for some instalation files
set(PROGRAM_PERMISSIONS_DEFAULT
  OWNER_WRITE
  OWNER_READ
  OWNER_EXECUTE
  GROUP_READ
  GROUP_EXECUTE
  WORLD_READ
  WORLD_EXECUTE)

# Set host information
string(TOLOWER "${CMAKE_SYSTEM_PROCESSOR}" CMAKE_SYSTEM_PROCESSOR_LOWER)
string(TOLOWER "${CMAKE_SYSTEM_NAME}" CMAKE_SYSTEM_NAME_LOWER)
string(TOLOWER "${CMAKE_C_COMPILER_ID}" CMAKE_C_COMPILER_ID_LOWER)
set("HOST" "${CMAKE_SYSTEM_PROCESSOR_LOWER}-pc-${CMAKE_SYSTEM_NAME_LOWER}-${CMAKE_C_COMPILER_ID_LOWER}")

# Password hash algorithm
set(PASSWORD_BACKEND "SHA512"
  CACHE STRING
  "Password hash algorithm, one of MD5, SHA1, RIPEMD160, SHA224, SHA256, SHA384, SHA512, defaults to SHA512")

if(NOT ${PASSWORD_BACKEND} MATCHES "^MD5|SHA1|RIPEMD160|SHA224|SHA256|SHA384|SHA512$")
  message(FATAL_ERROR "PASSWORD_BACKEND invalid, choose one of MD5, SHA1, RIPEMD160, SHA224, SHA256, SHA384, SHA512")
endif()

find_program(BASH NAMES bash DOC "bash interpreter fullpath")

if(WIN32)
  configure_file(monetdb_config_win32.h.in
    ${CMAKE_CURRENT_BINARY_DIR}/monetdb_config.h)
  configure_file(unistd.h.in
    ${CMAKE_CURRENT_BINARY_DIR}/unistd.h)
else()
  configure_file(monetdb_config.h.in
    ${CMAKE_CURRENT_BINARY_DIR}/monetdb_config.h)
endif()

add_library(monetdb_config_header
  INTERFACE)

target_include_directories(monetdb_config_header
  INTERFACE
  $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}>
  $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/monetdb>)

install(TARGETS
  monetdb_config_header
  EXPORT monetdb_config_headerTargets
  COMPONENT monetdbdev)

install(EXPORT monetdb_config_headerTargets
  FILE monetdb_config_headerTargets.cmake
  NAMESPACE MonetDB::
  DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/cmake
  COMPONENT monetdbdev)

install(FILES
  ${CMAKE_CURRENT_BINARY_DIR}/monetdb_config.h
  DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/monetdb
  COMPONENT monetdbdev)

# [TODO]: install created python file
configure_file(create_include_object.py.in
  ${CMAKE_CURRENT_BINARY_DIR}/create_include_object.py
  @ONLY)

if(${CMAKE_SYSTEM_NAME} STREQUAL "Darwin")
  set(CMAKE_MODULE_LINKER_FLAGS "-Wl,-undefined,dynamic_lookup -Wl,-undefined,dynamic_lookup")
endif()

add_subdirectory(common)
add_subdirectory(clients)
add_subdirectory(gdk)
add_subdirectory(monetdb5)
add_subdirectory(sql)
add_subdirectory(geom)
add_subdirectory(tools)
if(TESTING)
  add_subdirectory(testing)
endif()

include(CMakePackageConfigHelpers)
configure_package_config_file(${CMAKE_CURRENT_SOURCE_DIR}/Config.cmake.in
  "${CMAKE_CURRENT_BINARY_DIR}/MonetDBConfig.cmake"
  INSTALL_DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/cmake)"
  NO_SET_AND_CHECK_MACRO
  NO_CHECK_REQUIRED_COMPONENTS_MACRO)

write_basic_package_version_file(
  "${CMAKE_CURRENT_BINARY_DIR}/MonetDBConfigVersion.cmake"
  VERSION "${MONETDB_VERSION}"
  COMPATIBILITY AnyNewerVersion)

install(FILES
  ${CMAKE_CURRENT_BINARY_DIR}/MonetDBConfig.cmake
  DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/cmake)

include(monetdb-packages)

include(CTest)
add_subdirectory(ctest)

if(CMAKE_SUMMARY)
  monetdb_cmake_summary()
endif()
