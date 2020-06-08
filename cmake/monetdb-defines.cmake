#[[
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
#]]

# This function should only run find functions. The resulting
# variables will have the correct scope. If you need to set
# additional variables, for example for legacy defines, do this
# in the "monetdb_macro_variables" macro.
function(monetdb_configure_defines)
  find_path(HAVE_SYS_TYPES_H "sys/types.h")
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
  find_path(HAVE_NETINET_IN_H "netinet/in.h")
  find_path(HAVE_POLL_H "poll.h")
  find_path(HAVE_PROCFS_H "procfs.h")
  find_path(HAVE_PWD_H "pwd.h")
  find_path(HAVE_STRINGS_H "strings.h")
  find_path(HAVE_STROPTS_H "stropts.h")
  find_path(HAVE_SYS_FILE_H "sys/file.h")
  find_path(HAVE_SYS_IOCTL_H "sys/ioctl.h")
  find_path(HAVE_SYS_SYSCTL_H "sys/sysctl.h")
  find_path(HAVE_SYS_MMAN_H "sys/mman.h")
  find_path(HAVE_SYS_PARAM_H "sys/param.h")
  find_path(HAVE_SYS_RESOURCE_H "sys/resource.h")
  find_path(HAVE_SYS_TIMES_H "sys/times.h")
  find_path(HAVE_SYS_UIO_H "sys/uio.h")
  find_path(HAVE_SYS_UN_H "sys/un.h")
  find_path(HAVE_SYS_UTIME_H "sys/utime.h")
  find_path(HAVE_SYS_WAIT_H "sys/wait.h")
  find_path(HAVE_TERMIOS_H "sys/termios.h")
  find_path(HAVE_UNISTD_H "unistd.h")
  find_path(HAVE_UUID_UUID_H "uuid/uuid.h")
  find_path(HAVE_WINSOCK_H "winsock2.h")
  find_path(HAVE_SEMAPHORE_H "semaphore.h")
  find_path(HAVE_GETOPT_H "getopt.h")

  check_include_file("stdatomic.h" HAVE_STDATOMIC_H)
  find_library(GETOPT_LIB "getopt.lib")

  check_symbol_exists("opendir" "dirent.h" HAVE_DIRENT_H)
  check_symbol_exists("gethostbyname" "netdb.h" HAVE_NETDB_H)
  check_symbol_exists("setsockopt" "sys/socket.h" HAVE_SYS_SOCKET_H)
  check_symbol_exists("gettimeofday" "sys/time.h" HAVE_SYS_TIME_H)
  # Linux specific, in the future, it might be ported to other platforms
  check_symbol_exists("S_ISREG" "sys/stat.h" HAVE_SYS_STAT_H)
  check_symbol_exists("getaddrinfo" "sys/types.h;sys/socket.h;netdb.h" UNIX_GETADDRINFO)
  check_symbol_exists("getaddrinfo" "ws2tcpip.h" WIN_GETADDRINFO)
  #check_symbol_exists("WSADATA" "winsock2.h" HAVE_WINSOCK_H)
  check_symbol_exists("fdatasync" "unistd.h" HAVE_FDATASYNC)
  # Some libc versions on Linux distributions don't have it
  check_symbol_exists("accept4"
    "sys/types.h;sys/socket.h" HAVE_ACCEPT4)
  check_symbol_exists("asctime_r" "time.h" HAVE_ASCTIME_R)
  check_symbol_exists("clock_gettime" "time.h" HAVE_CLOCK_GETTIME)
  check_symbol_exists("ctime_r" "time.h" HAVE_CTIME_R)
  check_symbol_exists("dispatch_semaphore_create"
    "dispatch/dispatch.h" HAVE_DISPATCH_SEMAPHORE_CREATE)
  # Linux specific, in the future, it might be ported to other platforms
  check_symbol_exists("fallocate" "fcntl.h" HAVE_FALLOCATE)
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
  # Some libc versions on Linux distributions don't have it
  check_symbol_exists("pipe2" "fcntl.h;unistd.h" HAVE_PIPE2)
  check_function_exists("poll" HAVE_POLL)
  check_symbol_exists("popen" "stdio.h" HAVE_POPEN)
  check_symbol_exists("posix_fadvise" "fcntl.h" HAVE_POSIX_FADVISE)
  # Some POSIX systems don't have it (e.g. Macos)
  check_symbol_exists("posix_fallocate" "fcntl.h" HAVE_POSIX_FALLOCATE)
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
  # Some libc versions on Linux distributions don't have it
  check_symbol_exists("semtimedop" "sys/types.h;sys/ipc.h;sys/sem.h" HAVE_SEMTIMEDOP)
  check_function_exists("pthread_kill" HAVE_PTHREAD_KILL)
  check_function_exists("pthread_sigmask" HAVE_PTHREAD_SIGMASK)
  check_symbol_exists("regcomp" "regex.h" HAVE_POSIX_REGEX)
endfunction()

macro(monetdb_macro_variables)
  # Set variables to define C macro's
  # These are related to the detected packages
  # These names are legacy. When the code is changed to use the cmake
  # variables, then they can be removed.
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
  set(HAVE_LIBLZ4 ${LZ4_FOUND})
  set(HAVE_PROJ ${PROJ_FOUND})
  set(HAVE_SNAPPY ${SNAPPY_FOUND})
  set(HAVE_FITS ${CFITSIO_FOUND})
  set(HAVE_UUID ${HAVE_UUID_GENERATE})
  set(HAVE_VALGRIND ${VALGRIND_FOUND})
  set(HAVE_NETCDF ${NETCDF_FOUND})
  set(HAVE_READLINE ${READLINE_FOUND})
  set(HAVE_LIBR ${LIBR_FOUND})
  set(RHOME "${LIBR_HOME}")
  set(HAVE_GEOM ${GEOS_FOUND})
  set(HAVE_SHP ${GDAL_FOUND})

  if(PY3INTEGRATION)
    set(HAVE_LIBPY3 "${Python3_NumPy_FOUND}")
  else()
    message(STATUS "Disable Py3integration, because required NumPy is missing")
  endif()
  if(Python3_Interpreter_FOUND)
    set(Python_EXECUTABLE "${Python3_EXECUTABLE}")
  endif()

  set(SOCKET_LIBRARIES "")
  if (WIN32)
    set(SOCKET_LIBRARIES "ws2_32")
  endif()

  if(UNIX_GETADDRINFO)
    set(HAVE_GETADDRINFO 1)
  endif()
  if(WIN_GETADDRINFO)
    set(HAVE_GETADDRINFO 1)
  endif()
  set(HAVE_CUDF
    ${CINTEGRATION}
    CACHE
    INTERNAL
    "C udfs extension is available")
  if(HAVE_GETOPT_H)
    set(HAVE_GETOPT 1)
  endif()
  # Check with STATIC_CODE_ANALYSIS
  # compiler options, profiling (google perf tools), valgrind
  set(ENABLE_STATIC_ANALYSIS
    "NO"
    CACHE
    STRING
    "Configure for static code analysis (use only if you know what you are doing)")
  # Check that posix regex is available when pcre is not found
  # "monetdb5/module/mal/pcre.c" assumes the regex library is available
  # as an alternative without checking this in the C code.
  if(NOT PCRE_FOUND AND NOT HAVE_POSIX_REGEX)
    message(FATAL_ERROR "PCRE library or GNU regex library not found but required for MonetDB5")
  endif()

  set(DIR_SEP  "/")
  set(PATH_SEP ":")
  set(DIR_SEP_STR  "/")
  set(SO_PREFIX "${CMAKE_SHARED_LIBRARY_PREFIX}")
  set(SO_EXT "${CMAKE_SHARED_LIBRARY_SUFFIX}")

  set(BINDIR "${CMAKE_INSTALL_FULL_BINDIR}")
  set(LIBDIR "${CMAKE_INSTALL_FULL_LIBDIR}")
  set(DATADIR "${CMAKE_INSTALL_FULL_DATADIR}")
  set(DATA_DIR "${CMAKE_INSTALL_FULL_DATADIR}")
  set(LOCALSTATEDIR "${CMAKE_INSTALL_FULL_LOCALSTATEDIR}")
  if(WIN32)
    # Fix cmake conversions
    string(REPLACE "/" "\\\\" QXLOCALSTATEDIR "${LOCALSTATEDIR}")
  endif()
  set(MONETDB_PREFIX "${CMAKE_INSTALL_PREFIX}")
  if(WIN32)
    # Fix cmake conversions
    string(REPLACE "/" "\\\\" MONETDB_PREFIX "${CMAKE_INSTALL_PREFIX}")
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
endmacro()

macro(monetdb_configure_crypto)
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
endmacro()

macro(monetdb_configure_sizes)
  # On C99, but we have to calculate the size
  check_type_size(size_t SIZEOF_SIZE_T LANGUAGE C)
  set(SIZEOF_VOID_P ${CMAKE_SIZEOF_VOID_P})
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
  check_type_size(socklen_t HAVE_SOCKLEN_T LANGUAGE C)

  if(INT128)
    cmake_push_check_state()
    check_type_size(__int128 SIZEOF___INT128 LANGUAGE C)
    check_type_size(__int128_t SIZEOF___INT128_T LANGUAGE C)
    check_type_size(__uint128_t SIZEOF___UINT128_T LANGUAGE C)
    if(HAVE_SIZEOF___INT128 OR HAVE_SIZEOF___INT128_T OR HAVE_SIZEOF___UINT128_T)
      set(HAVE_HGE TRUE)
      message(STATUS "Huge integers are available")
    else()
      message(STATUS "128-bit integers not supported by this compiler")
    endif()
    cmake_pop_check_state()
  endif()
endmacro()

macro(monetdb_configure_misc)
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
    message(FATAL_ERROR
      "PASSWORD_BACKEND invalid, choose one of MD5, SHA1, RIPEMD160, SHA224, SHA256, SHA384, SHA512")
  endif()

  # Used for installing testing python module (don't pass a location, else we need to strip this again)
  execute_process(COMMAND "${Python3_EXECUTABLE}" "-c" "import distutils.sysconfig; print(distutils.sysconfig.get_python_lib(0,0,''))"
    RESULT_VARIABLE PY3_LIBDIR_CODE
    OUTPUT_VARIABLE PYTHON3_SITEDIR
    OUTPUT_STRIP_TRAILING_WHITESPACE)
  if (PY3_LIBDIR_CODE)
    message(WARNING
      "Could not determine MonetDB Python3 site-packages instalation directory")
  endif()
  set(PYTHON3_LIBDIR "${PYTHON3_SITEDIR}")
  set(PYTHON "${Python3_EXECUTABLE}")

  if(MSVC)
    set(_Noreturn "__declspec(noreturn)")
    # C99 feature not present in MSVC
    set(restrict "__restrict")
    # C99 feature only available on C++ compiler in MSVC
    # https://docs.microsoft.com/en-us/cpp/cpp/inline-functions-cpp?view=vs-2015
    set(inline "__inline")
  endif()
endmacro()

# vim: set ts=2:sw=2:et
