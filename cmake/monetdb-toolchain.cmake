#[[
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
#]]

function(monetdb_default_toolchain)
  if(SANITIZER)
    if(${CMAKE_C_COMPILER_ID} STREQUAL "GNU")
      MT_addCompilerFlag("-fsanitize=address" "-fsanitize=address" "${CMAKE_C_FLAGS}" "all" CMAKE_C_FLAGS)
      MT_addCompilerFlag("-fno-omit-frame-pointer" "-fno-omit-frame-pointer" "${CMAKE_C_FLAGS}" "all" CMAKE_C_FLAGS)
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

  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS}" PARENT_SCOPE)

endfunction()
