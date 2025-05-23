#[[
# SPDX-License-Identifier: MPL-2.0
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 2024, 2025 MonetDB Foundation;
# Copyright August 2008 - 2023 MonetDB B.V.;
# Copyright 1997 - July 2008 CWI.
#]]

function(monetdb_default_compiler_options)
  if (${FORCE_COLORED_OUTPUT})
    if ("${CMAKE_C_COMPILER_ID}" STREQUAL "GNU")
      add_compile_options("-fdiagnostics-color=always")
    elseif ("${CMAKE_C_COMPILER_ID}" MATCHES "^(Clang|AppleClang)$")
      add_compile_options("-fcolor-diagnostics")
    endif ()
  endif ()

  if(SANITIZER)
    if(${CMAKE_C_COMPILER_ID} STREQUAL "GNU")
      add_compile_options("-fsanitize=address")
      add_compile_options("-fno-omit-frame-pointer")
      add_compile_definitions(SANITIZER)
      if(${CMAKE_VERSION} VERSION_GREATER_EQUAL "3.13.0")
        add_link_options("-fsanitize=address")
      else()
        set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fsanitize=address" PARENT_SCOPE)
      endif()
    elseif ("${CMAKE_C_COMPILER_ID}" MATCHES "^(Clang|AppleClang)$")
      add_compile_options("-fsanitize=address")
      add_compile_options("-fno-omit-frame-pointer")
      add_compile_definitions(SANITIZER)
      if(${CMAKE_VERSION} VERSION_GREATER_EQUAL "3.13.0")
        add_link_options("-fsanitize=address")
      else()
        set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fsanitize=address" PARENT_SCOPE)
      endif()
    else()
      message(FATAL_ERROR "Sanitizer only supported with GCC")
    endif()
  endif()

  if(UNDEFINED)
    if(${CMAKE_C_COMPILER_ID} STREQUAL "GNU")
      add_compile_options("-fsanitize=undefined")
      add_compile_options("-fno-omit-frame-pointer")
      add_compile_definitions(UNDEFINED)
      if(${CMAKE_VERSION} VERSION_GREATER_EQUAL "3.13.0")
        add_link_options("-fsanitize=undefined")
      else()
        set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fsanitize=undefined" PARENT_SCOPE)
      endif()
    else()
      message(FATAL_ERROR "Sanitizer only supported with GCC")
    endif()
  endif()

  if(PGOTRAIN)
    if(${CMAKE_C_COMPILER_ID} STREQUAL "GNU")
        SET(PGO_COMPILE_FLAGS "-fprofile-generate=${CMAKE_BINARY_DIR}/profile-data")
    endif()
    if ("${CMAKE_C_COMPILER_ID}" MATCHES "^(Clang|AppleClang)$")
        SET(PGO_COMPILE_FLAGS "-fprofile-instr-generate")
    endif()
    SET( CMAKE_C_FLAGS  "${CMAKE_C_FLAGS} ${PGO_COMPILE_FLAGS}" PARENT_SCOPE)
  endif()

  if(PGOBUILD)
    if(NOT PGO_TRAINING_DIR)
        SET(PGO_TRAINING_DIR ../training)
    endif()
    SET(PGO_TRAINING_DATA ${CMAKE_BINARY_DIR}/${PGO_TRAINING_DIR}/profile-data)

    if(NOT EXISTS ${PGO_TRAINING_DATA})
        message(FATAL_ERROR "No profiling Data Found so can't Build. Ensure that the training run was executed in the training build directory. Training data expected in Directory: " ${PGO_TRAINING_DATA})
    endif()

    if(${CMAKE_C_COMPILER_ID} STREQUAL "GNU")
        SET(PGO_COMPILE_FLAGS "-fprofile-use=${PGO_TRAINING_DATA} -fprofile-correction -Wno-missing-profile -Wno-coverage-mismatch")
    endif()
    if ("${CMAKE_C_COMPILER_ID}" MATCHES "^(Clang|AppleClang)$")
        SET(PGO_COMPILE_FLAGS "-fprofile-instr-use")
    endif()

    SET( CMAKE_C_FLAGS  "${CMAKE_C_FLAGS} ${PGO_COMPILE_FLAGS}" PARENT_SCOPE )
  endif()

  if(STRICT)
    if(${CMAKE_C_COMPILER_ID} MATCHES "^(GNU|Clang|AppleClang)$")
      add_compile_options("-Werror")
      add_compile_options("-Wall")
      add_compile_options("-Wextra")
      add_compile_options("-W")

      add_option_if_available("-Werror-implicit-function-declaration")
      add_option_if_available("-Wpointer-arith")
      add_option_if_available("-Wundef")
      add_option_if_available("-Wformat=2")
      add_option_if_available("-Wformat-overflow=1")
      if(${CMAKE_C_COMPILER_ID} MATCHES "^GNU$")
	if(${CMAKE_C_COMPILER_VERSION} VERSION_LESS "9.5.0")
	  # on Ubuntu 20.04 with gcc 9.4.0 when building a Release
	  # version we get a warning (hence error) about possible
	  # buffer overflow in a call to snprintf, this option avoids
	  # that; I have no idea which version of gcc is safe, so the
	  # test may have to be refined
	  add_option_if_available("-Wno-format-truncation")
	endif()
      endif()
      add_option_if_available("-Wno-format-nonliteral")
      #add_option_if_available("-Wformat-signedness") 	-- numpy messes this up
      add_option_if_available("-Wno-cast-function-type")
      add_option_if_available("-Winit-self")
      add_option_if_available("-Winvalid-pch")
      add_option_if_available("-Wmissing-declarations")
      add_option_if_available("-Wmissing-format-attribute")
      add_option_if_available("-Wmissing-prototypes")
      # need this for gcc 4.8.5 on CentOS 7:
      add_option_if_available("-Wno-missing-braces")
      # need this for clang 9.1.0 on Darwin:
      add_option_if_available("-Wno-missing-field-initializers")
      add_option_if_available("-Wold-style-definition")
      add_option_if_available("-Wpacked")
      add_option_if_available("-Wunknown-pragmas")
      add_option_if_available("-Wvariadic-macros")
      add_option_if_available("-Wstack-protector")
      add_option_if_available("-fstack-protector-all")
      add_option_if_available("-Wpacked-bitfield-compat")
      add_option_if_available("-Wsync-nand")
      add_option_if_available("-Wmissing-include-dirs")
      add_option_if_available("-Wlogical-op")
      add_option_if_available("-Wduplicated-cond")
      add_option_if_available("-Wduplicated-branches")
      add_option_if_available("-Wrestrict")
      add_option_if_available("-Wnested-externs")
      add_option_if_available("-Wmissing-noreturn")
      add_option_if_available("-Wuninitialized")

      # since we use values of type "int8_t" as subscript,
      # and int8_t may be defined as plain "char", we cannot
      # allow this warning (part of -Wall)
      add_option_if_available("-Wno-char-subscripts")

      add_option_if_available("-Wunreachable-code")
    elseif(${CMAKE_C_COMPILER_ID} STREQUAL "Intel")
      if(WIN32)
        add_compile_options("/W3")
        add_compile_options("/Qdiag-disable:11074")
        add_compile_options("/Qdiag-disable:11075")
        add_compile_options("/Wcheck")
        add_compile_options("/Werror-all")
        add_compile_options("/${INTEL_OPTION_EXTRA}wd2259")
      else()
        add_compile_options("-Wall")
        add_compile_options("-Wcheck")
        add_compile_options("-Werror-all")
        add_compile_options("-${INTEL_OPTION_EXTRA}wd2259")
      endif()
    elseif(MSVC)
      add_compile_options("/WX")
    endif()
  endif()

  if(NOT MSVC)
    add_option_if_available("-Wno-unreachable-code")
  endif()

  if(NOT ASSERT)
     add_compile_definitions("NDEBUG=1")
  endif()
endfunction()
