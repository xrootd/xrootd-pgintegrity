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

set(CMAKE_CXX_FLAGS "-O2 -fPIC -pedantic -Wall -Wextra -Wno-unused-parameter -Wno-long-long -fstack-protector-all -Wnon-virtual-dtor -std=c++1y -g")

execute_process(COMMAND ${CMAKE_C_COMPILER} -dumpversion OUTPUT_VARIABLE GCC_VERSION)

# include the directory itself as a path to include directories
set(CMAKE_INCLUDE_CURRENT_DIR ON)

find_package(XRootD REQUIRED)

add_subdirectory(src)

if(BUILD_TEST)
    add_subdirectory(tests)
endif()
