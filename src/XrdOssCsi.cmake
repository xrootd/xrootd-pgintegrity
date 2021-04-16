include_directories( ${XROOTD_INCLUDES} )
link_directories( ${XROOTD_LIB_DIR} )

#
# Source location
#
set(CSISRC ../submodules/xrootd/src/XrdOssCsi)

#-------------------------------------------------------------------------------
# Modules
#-------------------------------------------------------------------------------
set( LIB_XRD_OSSCSI  XrdOssCsi-${PLUGIN_VERSION} )

#-------------------------------------------------------------------------------
# Shared library version
#-------------------------------------------------------------------------------

#-------------------------------------------------------------------------------
# The XrdPfc library
#-------------------------------------------------------------------------------
add_library(
  ${LIB_XRD_OSSCSI}
  MODULE
  ${CSISRC}/XrdOssCsi.cc                      ${CSISRC}/XrdOssCsi.hh
  ${CSISRC}/XrdOssCsiFile.cc
  ${CSISRC}/XrdOssCsiFileAio.cc               ${CSISRC}/XrdOssCsiFileAio.hh
  ${CSISRC}/XrdOssCsiPages.cc                 ${CSISRC}/XrdOssCsiPages.hh
  ${CSISRC}/XrdOssCsiPagesUnaligned.cc
  ${CSISRC}/XrdOssCsiTagstoreFile.cc          ${CSISRC}/XrdOssCsiTagstoreFile.hh
  ${CSISRC}/XrdOssCsiRanges.cc                ${CSISRC}/XrdOssCsiRanges.hh
  ${CSISRC}/XrdOssCsiConfig.cc                ${CSISRC}/XrdOssCsiConfig.hh
  ${CSISRC}/XrdOssCsiCrcUtils.cc              ${CSISRC}/XrdOssCsiCrcUtils.hh
                                              ${CSISRC}/XrdOssCsiTagstore.hh
                                              ${CSISRC}/XrdOssHandler.hh
                                              ${CSISRC}/XrdOssCsiTrace.hh
  )

target_link_libraries(
  ${LIB_XRD_OSSCSI}
  XrdUtils
  XrdServer
  pthread )

set_target_properties(
  ${LIB_XRD_OSSCSI}
  PROPERTIES
  INTERFACE_LINK_LIBRARIES ""
  LINK_INTERFACE_LIBRARIES "" )

#-------------------------------------------------------------------------------
# Install
#-------------------------------------------------------------------------------
install(
  TARGETS ${LIB_XRD_OSSCSI}
  LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR} )
