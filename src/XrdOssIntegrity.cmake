include_directories( ${XROOTD_INCLUDES} )
link_directories( ${XROOTD_LIB_DIR} )

#-------------------------------------------------------------------------------
# Modules
#-------------------------------------------------------------------------------
set( LIB_XRD_OSSINTEGRITY  XrdOssIntegrity-${PLUGIN_VERSION} )

#-------------------------------------------------------------------------------
# Shared library version
#-------------------------------------------------------------------------------

#-------------------------------------------------------------------------------
# The XrdPfc library
#-------------------------------------------------------------------------------
add_library(
  ${LIB_XRD_OSSINTEGRITY}
  MODULE
  XrdOssIntegrity/XrdOssIntegrity.cc                      XrdOssIntegrity/XrdOssIntegrity.hh
  XrdOssIntegrity/XrdOssIntegrityFile.cc
  XrdOssIntegrity/XrdOssIntegrityFileAio.cc               XrdOssIntegrity/XrdOssIntegrityFileAio.hh
  XrdOssIntegrity/XrdOssIntegrityPages.cc                 XrdOssIntegrity/XrdOssIntegrityPages.hh
  XrdOssIntegrity/XrdOssIntegrityPagesUnaligned.cc
  XrdOssIntegrity/XrdOssIntegrityTagstoreFile.cc          XrdOssIntegrity/XrdOssIntegrityTagstoreFile.hh
  XrdOssIntegrity/XrdOssIntegrityRanges.cc                XrdOssIntegrity/XrdOssIntegrityRanges.hh
  XrdOssIntegrity/XrdOssIntegrityConfig.cc                XrdOssIntegrity/XrdOssIntegrityConfig.hh
                                                          XrdOssIntegrity/XrdOssIntegrityTagstore.hh
                                                          XrdOssIntegrity/XrdOssHandler.hh
                                                          XrdOssIntegrity/XrdOssIntegrityTrace.hh
  )

target_link_libraries(
  ${LIB_XRD_OSSINTEGRITY}
  XrdUtils
  XrdServer
  pthread )

set_target_properties(
  ${LIB_XRD_OSSINTEGRITY}
  PROPERTIES
  INTERFACE_LINK_LIBRARIES ""
  LINK_INTERFACE_LIBRARIES "" )

#-------------------------------------------------------------------------------
# Install
#-------------------------------------------------------------------------------
install(
  TARGETS ${LIB_XRD_OSSINTEGRITY}
  LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR} )
