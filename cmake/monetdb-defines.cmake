#[[
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
#]]

# This function should only run find functions. The resulting
# variables will have the correct scope. If you need to set
# additional variables, for example for legacy defines, do this
# in the "monetdb_macro_variables" macro.
function(monetdb_configure_defines)
  check_include_file("dispatch/dispatch.h" HAVE_DISPATCH_DISPATCH_H)
  check_include_file("dlfcn.h" HAVE_DLFCN_H)
  check_include_file("fcntl.h" HAVE_FCNTL_H)
# use find_path for getopt.h since we need the path on Windows
  find_path(HAVE_GETOPT_H "getopt.h")
  check_include_file("io.h" HAVE_IO_H)
  check_include_file("kvm.h" HAVE_KVM_H)
  check_include_file("libgen.h" HAVE_LIBGEN_H)
  check_include_file("libintl.h" HAVE_LIBINTL_H)
  check_include_file("mach/mach_init.h" HAVE_MACH_MACH_INIT_H)
  check_include_file("mach/task.h" HAVE_MACH_TASK_H)
  check_include_file("mach-o/dyld.h" HAVE_MACH_O_DYLD_H)
  check_include_file("netinet/in.h" HAVE_NETINET_IN_H)
  check_include_file("poll.h" HAVE_POLL_H)
  check_include_file("procfs.h" HAVE_PROCFS_H)
  check_include_file("pwd.h" HAVE_PWD_H)
  check_include_file("semaphore.h" HAVE_SEMAPHORE_H)
  check_include_file("stdatomic.h" HAVE_STDATOMIC_H)
  check_include_file("strings.h" HAVE_STRINGS_H)
  check_include_file("stropts.h" HAVE_STROPTS_H)
  check_include_file("sys/file.h" HAVE_SYS_FILE_H)
  check_include_file("sys/ioctl.h" HAVE_SYS_IOCTL_H)
  check_include_file("sys/mman.h" HAVE_SYS_MMAN_H)
  check_include_file("sys/param.h" HAVE_SYS_PARAM_H)
  check_include_file("sys/resource.h" HAVE_SYS_RESOURCE_H)
  check_include_file("sys/stat.h" HAVE_SYS_STAT_H)
  check_include_file("sys/sysctl.h" HAVE_SYS_SYSCTL_H)
  check_include_file("sys/termios.h" HAVE_TERMIOS_H)
  check_include_file("sys/times.h" HAVE_SYS_TIMES_H)
  check_include_file("sys/types.h" HAVE_SYS_TYPES_H)
  check_include_file("sys/uio.h" HAVE_SYS_UIO_H)
  check_include_file("sys/un.h" HAVE_SYS_UN_H)
  check_include_file("sys/wait.h" HAVE_SYS_WAIT_H)
  check_include_file("unistd.h" HAVE_UNISTD_H)
  check_include_file("uuid/uuid.h" HAVE_UUID_UUID_H)
  check_include_file("winsock2.h" HAVE_WINSOCK_H)

  find_library(GETOPT_LIB "getopt.lib")

  check_symbol_exists("opendir" "dirent.h" HAVE_DIRENT_H)
  check_symbol_exists("gethostbyname" "netdb.h" HAVE_NETDB_H)
  check_symbol_exists("setsockopt" "sys/socket.h" HAVE_SYS_SOCKET_H)
  check_symbol_exists("gettimeofday" "sys/time.h" HAVE_SYS_TIME_H)
  # Linux specific, in the future, it might be ported to other platforms
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
  cmake_push_check_state()
    set(CMAKE_REQUIRED_INCLUDES "${HAVE_GETOPT_H}")
    check_symbol_exists("getopt_long" "getopt.h" HAVE_GETOPT_LONG)
  cmake_pop_check_state()
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
  check_symbol_exists("stpcpy" "string.h" HAVE_STPCPY)
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
  cmake_push_check_state()
    set(CMAKE_REQUIRED_LIBRARIES "${CMAKE_THREAD_LIBS_INIT}")
    check_function_exists("pthread_kill" HAVE_PTHREAD_KILL)
    check_function_exists("pthread_sigmask" HAVE_PTHREAD_SIGMASK)
  cmake_pop_check_state()
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
  set(HAVE_ODBCINST ${ODBCinst_FOUND})
  set(HAVE_LIBR ${LIBR_FOUND})
  set(RHOME "${LIBR_HOME}")
  set(HAVE_GEOM ${GEOS_FOUND})
  set(HAVE_SHP ${GDAL_FOUND})

  if(PY3INTEGRATION)
    set(HAVE_LIBPY3 "${Python3_NumPy_FOUND}")
    set(PY3VER "${Python3_VERSION_MINOR}")
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
  # compiler options, profiling (google perf tools), valgrind
  # Check that posix regex is available when pcre is not found
  # "monetdb5/module/mal/pcre.c" assumes the regex library is available
  # as an alternative without checking this in the C code.
  if(NOT PCRE_FOUND AND NOT HAVE_POSIX_REGEX)
    message(FATAL_ERROR "PCRE library or GNU regex library not found but required for MonetDB5")
  endif()

  set(MAPI_PORT 50000)
  set(MAPI_PORT_STR "${MAPI_PORT}")

  set(DIR_SEP  "/")
  set(PATH_SEP ":")
  set(DIR_SEP_STR  "/")
  set(SO_PREFIX "${CMAKE_SHARED_LIBRARY_PREFIX}")
  set(SO_EXT "${CMAKE_SHARED_LIBRARY_SUFFIX}")

  set(BINDIR "${CMAKE_INSTALL_FULL_BINDIR}")
  set(LIBDIR "${CMAKE_INSTALL_FULL_LIBDIR}")
  set(DATADIR "${CMAKE_INSTALL_FULL_DATADIR}")
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
  set(INCLUDEDIR "${CMAKE_INSTALL_FULL_INCLUDEDIR}")
  set(INFODIR "${CMAKE_INSTALL_FULL_INFODIR}")
  set(LIBEXECDIR "${CMAKE_INSTALL_FULL_LIBEXECDIR}")
  # set(MANDIR "${CMAKE_INSTALL_FULL_MANDIR}")
  set(SYSCONFDIR "${CMAKE_INSTALL_FULL_SYSCONFDIR}")
  set(PKGCONFIGDIR "${LIBDIR}/pkgconfig")
endmacro()

macro(monetdb_configure_crypto)
  if(COMMONCRYPTO_FOUND)
    cmake_push_check_state()
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
    cmake_pop_check_state()
  endif()
  if(OPENSSL_FOUND)
    cmake_push_check_state()
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
    cmake_pop_check_state()
  endif()
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
  check_type_size("long int" SIZEOF_LONG_INT LANGUAGE C)
  check_type_size(double SIZEOF_DOUBLE LANGUAGE C)
  check_type_size(wchar_t SIZEOF_WCHAR_T LANGUAGE C)
  cmake_push_check_state()
  if(WIN32)
    set(CMAKE_EXTRA_INCLUDE_FILES "Ws2tcpip.h")
  endif()
  check_type_size(socklen_t HAVE_SOCKLEN_T LANGUAGE C)
  cmake_pop_check_state()

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

  if(ODBC_FOUND)
    cmake_push_check_state()
    set(CMAKE_REQUIRED_INCLUDES "${CMAKE_REQUIRED_INCLUDES};${ODBC_INCLUDE_DIRS}")
    if(WIN32)
      set(CMAKE_EXTRA_INCLUDE_FILES "${CMAKE_EXTRA_INCLUDE_FILES};Windows.h;sqlext.h;sqltypes.h")
      check_include_file("afxres.h" HAVE_AFXRES_H)
    else()
      set(CMAKE_EXTRA_INCLUDE_FILES "${CMAKE_EXTRA_INCLUDE_FILES};sql.h;sqltypes.h")
    endif()
    check_type_size(SQLLEN _SQLLEN LANGUAGE C)
    if(HAVE__SQLLEN)
      set(LENP_OR_POINTER_T "SQLLEN *")
    else()
      set(LENP_OR_POINTER_T "SQLPOINTER")
    endif()
    check_type_size(SQLWCHAR SIZEOF_SQLWCHAR LANGUAGE C)
    cmake_pop_check_state()
  endif()
endmacro()

macro(monetdb_configure_misc)
  # Set host information
  string(TOLOWER "${CMAKE_SYSTEM_PROCESSOR}" CMAKE_SYSTEM_PROCESSOR_LOWER)
  string(TOLOWER "${CMAKE_SYSTEM_NAME}" CMAKE_SYSTEM_NAME_LOWER)
  string(TOLOWER "${CMAKE_C_COMPILER_ID}" CMAKE_C_COMPILER_ID_LOWER)
  set("HOST" "${CMAKE_SYSTEM_PROCESSOR_LOWER}-pc-${CMAKE_SYSTEM_NAME_LOWER}-${CMAKE_C_COMPILER_ID_LOWER}")

  # Endianness
  TEST_BIG_ENDIAN(WORDS_BIGENDIAN)

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
