if(BUILD_TESTING)
  add_test(NAME TestEcho
    COMMAND ${CMAKE_COMMAND} -E echo 'MonetDB Testing')

  add_subdirectory(monetdb5)
  add_subdirectory(tools)
endif()
