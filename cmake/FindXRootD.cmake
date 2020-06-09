# Try to find XROOTD
# Once done, this will define
#
# XROOTD_FOUND       - system has XRootD
# XROOTD_INCLUDES    - the XRootD include directory
# XROOTD_LIB_DIR     - the XRootD library directory
#
# XROOTD_DIR may be defined as a hint for where to look

IF( CMAKE_SYSTEM_PROCESSOR STREQUAL "x86_64" )
  set(LIBDESTINATION "lib64" CACHE STRING "The path suffix where to install libraries. Set this to lib64 if you have such a weird platform that you have to install the libs here.")
ELSE()
  set(LIBDESTINATION "lib" CACHE STRING "The path suffix where to install libraries. Set this to lib64 if you have such a weird platform that you have to install the libs here.")
ENDIF()

FIND_PATH(XROOTD_INCLUDES XrdVersion.hh
  HINTS
  ${XROOTD_DIR}
  $ENV{XROOTD_DIR}
  /usr
  /usr/local
  /opt/xrootd/
  PATH_SUFFIXES src include/xrootd
  PATHS /opt/xrootd
)

message("-- Found XROOTD includes: ${XROOTD_INCLUDES}")

FIND_LIBRARY(XROOTD_UTILS XrdUtils
  HINTS
  ${XROOTD_DIR}
  $ENV{XROOTD_DIR}
  /usr
  /usr/local
  /opt/xrootd/
  PATH_SUFFIXES build/src LIBDESTINATION
)


GET_FILENAME_COMPONENT( XROOTD_LIB_DIR ${XROOTD_UTILS} PATH )
message("-- Found XROOTD libs: ${XROOTD_LIB_DIR}")

INCLUDE(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(XRootD DEFAULT_MSG XROOTD_LIB_DIR XROOTD_INCLUDES)

