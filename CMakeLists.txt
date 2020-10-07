cmake_minimum_required (VERSION 2.6)

project(XrdPfcIntegrity)

macro( define_default variable value )
  if( NOT DEFINED ${variable} )
    set( ${variable} ${value} )
  endif()
endmacro()

define_default( PLUGIN_VERSION    5 )
add_definitions( -DXRDPLUGIN_SOVERSION="${PLUGIN_VERSION}" )

include(GNUInstallDirs)

set(CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake ${PROJECT_SOURCE_DIR}/src)

if(BUILD_TEST)
  set(GCC_COVERAGE_COMPILE_FLAGS "-g -O0 --coverage -fprofile-arcs -ftest-coverage -std=c++0x")
  set(GCC_COVERAGE_LINK_FLAGS    "--coverage -lgcov")
  set( CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} ${GCC_COVERAGE_COMPILE_FLAGS}" )
  set( CMAKE_EXE_LINKER_FLAGS  "${CMAKE_EXE_LINKER_FLAGS} ${GCC_COVERAGE_LINK_FLAGS}" )
  set(CMAKE_CXX_OUTPUT_EXTENSION_REPLACE 1)
  add_custom_target(gcov
     COMMAND ${CMAKE_MAKE_PROGRAM} test_page
     WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
  )
  add_custom_command(TARGET gcov
     COMMAND rm -f ${CMAKE_BINARY_DIR}/src/CMakeFiles/XrdOssIntegrity-5.dir/XrdOssIntegrity/*.gcda
     COMMAND LD_LIBRARY_PATH=./src ./tests/test_page
     COMMAND gcov ${CMAKE_SOURCE_DIR}/src/XrdOssIntegrity/XrdOssIntegrity*.cc -o ${CMAKE_BINARY_DIR}/src/CMakeFiles/XrdOssIntegrity-5.dir/XrdOssIntegrity
     WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
   )
   add_dependencies(gcov test_page XrdOssIntegrity-5)
else()
  set(CMAKE_CXX_FLAGS "-O2 -fPIC -pedantic -Wall -Wextra -Wno-unused-parameter -Wno-long-long -Wnon-virtual-dtor -std=c++0x -g")
endif()


execute_process(COMMAND ${CMAKE_C_COMPILER} -dumpversion OUTPUT_VARIABLE GCC_VERSION)

# include the directory itself as a path to include directories
set(CMAKE_INCLUDE_CURRENT_DIR ON)

find_package(XRootD REQUIRED)

add_subdirectory(src)

if(BUILD_TEST)
  add_subdirectory(tests)
endif()
