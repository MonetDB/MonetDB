#[[
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
#]]

# Detect required packages
find_package(BISON REQUIRED)
find_package(Iconv)
find_package(Threads)

if(${CMAKE_VERSION} VERSION_LESS "3.14.0")
  find_package(Python3 COMPONENTS Interpreter Development)
  find_package(NumPy)
  if(Python3_Interpreter_FOUND)
    set(Python_EXECUTABLE "${Python3_EXECUTABLE}")
  endif(Python3_Interpreter_FOUND)
else()
  find_package(Python3 COMPONENTS Interpreter Development NumPy)
  if(Python3_Interpreter_FOUND)
    set(Python_EXECUTABLE "${Python3_EXECUTABLE}")
  endif(Python3_Interpreter_FOUND)
endif()
if(PY3INTEGRATION)
  set(HAVE_LIBPY3 "${Python3_FOUND}")
endif(PY3INTEGRATION)

if(WIN32)
  find_library(GETOPT_LIB "getopt.lib")
endif()

if(WITH_LZMA)
  find_package(Lzma)
endif()

if(WITH_XML2)
  find_package(LibXml2)
endif()

if(WITH_PCRE)
  find_package(PCRE)
else()
  check_symbol_exists("regcomp" "regex.h" HAVE_POSIX_REGEX)
endif()
if(NOT PCRE_FOUND AND NOT HAVE_POSIX_REGEX)
  message(FATAL_ERROR "PCRE library or GNU regex library not found but required for MonetDB5")
endif()

if(${CMAKE_SYSTEM_NAME} STREQUAL "Darwin")
  find_package(CommonCrypto)
else()
  find_package(OpenSSL)
endif()

if(WITH_BZ2)
  find_package(BZip2)
endif()

if(WITH_CURL)
  find_package(CURL CONFIG)
endif()

if(WITH_ZLIB)
  find_package(ZLIB)
endif()

if(WITH_LZ4)
  find_package(LZ4)
endif()

if(WITH_PROJ)
  find_package(Proj)
endif()

if(WITH_SNAPPY)
  find_package(Snappy CONFIG)
endif()

if(WITH_UUID)
  find_package(UUID)
endif()

if(WITH_VALGRIND)
  find_package(Valgrind)
endif()

if(WITH_READLINE)
  find_package(Readline)
endif()

if(FITS)
  find_package(CFitsIO)
endif()

if(CINTEGRATION)
  set(HAVE_CUDF ON CACHE INTERNAL "C udfs extension is available")
endif()

if(NETCDF)
  find_package(NetCDF)
endif()

find_package(KVM)

if(GEOM)
  find_package(Geos)
endif()

if(SHP)
  if(NOT GEOS_FOUND)
    message(STATUS "Disable SHP, geom module required for ESRI Shapefile vault")
  else()
    find_package(GDAL)
  endif()
endif()

if(LIDAR)
  find_package(Lidar)
endif()

if(ODBC)
  find_package(ODBC)
  set(HAVE_ODBC "${ODBC_FOUND}")
  if(ODBC_FOUND)
    cmake_push_check_state()
    set(CMAKE_REQUIRED_INCLUDES "${CMAKE_REQUIRED_INCLUDES};${ODBC_INCLUDE_DIR}")
    if(WIN32)
      set(CMAKE_EXTRA_INCLUDE_FILES "${CMAKE_EXTRA_INCLUDE_FILES};Windows.h;sqlext.h;sqltypes.h")
      find_path(HAVE_AFXRES_H "afxres.h")
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
  endif(ODBC_FOUND)
endif(ODBC)

if(RINTEGRATION)
  find_package(LibR)
  set(HAVE_LIBR "${LIBR_FOUND}")
  set(RHOME "${LIBR_HOME}")
endif()

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

if(SANITIZER)
  if(${CMAKE_C_COMPILER_ID} STREQUAL "GNU")
    MT_addCompilerFlag("-fsanitize=address" "-fsanitize=address" "${CMAKE_C_FLAGS}" "all" CMAKE_C_FLAGS)
    MT_addCompilerFlag("-fno-omit-frame-pointer" "-fsanitize=address" "${CMAKE_C_FLAGS}" "all" CMAKE_C_FLAGS)
    add_definitions(-DNO_ATOMIC_INSTRUCTIONS)
  else()
    message(FATAL_ERROR "Sanitizer only supported with GCC")
  endif()
endif()

if(STRICT)
  if(${CMAKE_C_COMPILER_ID} MATCHES "^GNU|Clang|AppleClang$")
    MT_addCompilerFlag("-Werror" "-Werror" "${CMAKE_C_FLAGS}" "all" CMAKE_C_FLAGS)
    MT_addCompilerFlag("-Wall" "-Wall" "${CMAKE_C_FLAGS}" "all" CMAKE_C_FLAGS)
    MT_addCompilerFlag("-Wextra" "-Wextra" "${CMAKE_C_FLAGS}" "all" CMAKE_C_FLAGS)
    MT_addCompilerFlag("-W" "-W" "${CMAKE_C_FLAGS}" "all" CMAKE_C_FLAGS)
    MT_checkCompilerFlag("-Werror-implicit-function-declaration")

    MT_checkCompilerFlag("-Wpointer-arith")
    MT_checkCompilerFlag("-Wundef")
    MT_checkCompilerFlag("-Wformat=2")
    MT_checkCompilerFlag("-Wformat-overflow=1")
    MT_checkCompilerFlag("-Wno-format-truncation")
    MT_checkCompilerFlag("-Wno-format-nonliteral")
    #MT_checkCompilerFlag("-Wformat-signedness") 	-- numpy messes this up
    MT_checkCompilerFlag("-Wno-cast-function-type")
    MT_checkCompilerFlag("-Winit-self")
    MT_checkCompilerFlag("-Winvalid-pch")
    MT_checkCompilerFlag("-Wmissing-declarations")
    MT_checkCompilerFlag("-Wmissing-format-attribute")
    MT_checkCompilerFlag("-Wmissing-prototypes")
    # need this for clang 9.1.0 on Darwin:
    MT_checkCompilerFlag("-Wno-missing-field-initializers")
    MT_checkCompilerFlag("-Wold-style-definition")
    MT_checkCompilerFlag("-Wpacked")
    MT_checkCompilerFlag("-Wunknown-pragmas")
    MT_checkCompilerFlag("-Wvariadic-macros")
    MT_checkCompilerFlag("-Wstack-protector")
    MT_checkCompilerFlag("-fstack-protector-all")
    MT_checkCompilerFlag("-Wstack-protector")
    MT_checkCompilerFlag("-Wpacked-bitfield-compat")
    MT_checkCompilerFlag("-Wsync-nand")
    MT_checkCompilerFlag("-Wjump-misses-init")
    MT_checkCompilerFlag("-Wmissing-include-dirs")
    MT_checkCompilerFlag("-Wlogical-op")
    MT_checkCompilerFlag("-Wduplicated-cond")
    MT_checkCompilerFlag("-Wduplicated-branches")
    MT_checkCompilerFlag("-Wrestrict")
    MT_checkCompilerFlag("-Wnested-externs")

    # since we use values of type "int8_t" as subscript,
    # and int8_t may be defined as plain "char", we cannot
    # allow this warning (part of -Wall)
    MT_checkCompilerFlag("-Wno-char-subscripts")

    MT_checkCompilerFlag("-Wunreachable-code")

  elseif(${CMAKE_C_COMPILER_ID} STREQUAL "Intel")
    MT_addCompilerFlag("${COMPILER_OPTION}Wall" "${COMPILER_OPTION}Wall" "${CMAKE_C_FLAGS}" "all" CMAKE_C_FLAGS)
    MT_addCompilerFlag("${COMPILER_OPTION}Wcheck" "${COMPILER_OPTION}Wcheck" "${CMAKE_C_FLAGS}" "all" CMAKE_C_FLAGS)
    MT_addCompilerFlag("${COMPILER_OPTION}Werror-all" "${COMPILER_OPTION}Werror-all" "${CMAKE_C_FLAGS}" "all" CMAKE_C_FLAGS)
    MT_addCompilerFlag("${COMPILER_OPTION}${INTEL_OPTION_EXTRA}wd2259" "${COMPILER_OPTION}${INTEL_OPTION_EXTRA}wd2259" "${CMAKE_C_FLAGS}" "all" CMAKE_C_FLAGS)
  elseif(MSVC)
    MT_addCompilerFlag("/WX" "/WX" "${CMAKE_C_FLAGS}" "all" CMAKE_C_FLAGS)
  endif()
endif()

if(NOT MSVC)
  cmake_push_check_state()
  set(CMAKE_REQUIRED_FLAGS "${CMAKE_REQUIRED_FLAGS};-Wno-unreachable-code")
  # Warning don't add '-' or '/' to the output variable!
  check_c_source_compiles("int main(int argc,char** argv){(void)argc;(void)argv;return 0;}"
    COMPILER_Wnounreachablecode)
  cmake_pop_check_state()
endif()

if(NOT ASSERT)
  MT_checkCompilerFlag("-DNDEBUG=1")
endif()
