
include( XRootDCommon )

#-------------------------------------------------------------------------------
# Shared library version
#-------------------------------------------------------------------------------
set( XRD_UTILS_VERSION   1.0.0 )
set( XRD_UTILS_SOVERSION 1 )
set( XRD_MAIN_VERSION    1.0.0 )
set( XRD_MAIN_SOVERSION  1 )
set( XRD_ZCRC32_VERSION   1.0.0 )
set( XRD_ZCRC32_SOVERSION 1 )

#-------------------------------------------------------------------------------
# The XrdSys library
#-------------------------------------------------------------------------------
add_library(
  XrdUtils
  SHARED

  #-----------------------------------------------------------------------------
  # XrdSys
  #-----------------------------------------------------------------------------
  XrdSys/XrdSysDNS.cc           XrdSys/XrdSysDNS.hh
  XrdSys/XrdSysDir.cc           XrdSys/XrdSysDir.hh
  XrdSys/XrdSysPlugin.cc        XrdSys/XrdSysPlugin.hh
  XrdSys/XrdSysPriv.cc          XrdSys/XrdSysPriv.hh
  XrdSys/XrdSysPlatform.cc      XrdSys/XrdSysPlatform.hh
  XrdSys/XrdSysPthread.cc       XrdSys/XrdSysPthread.hh
                                XrdSys/XrdSysSemWait.hh
  XrdSys/XrdSysTimer.cc         XrdSys/XrdSysTimer.hh
  XrdSys/XrdSysUtils.cc         XrdSys/XrdSysUtils.hh
  XrdSys/XrdSysXSLock.cc        XrdSys/XrdSysXSLock.hh
  XrdSys/XrdSysFAttr.cc         XrdSys/XrdSysFAttr.hh
                                XrdSys/XrdSysFAttrBsd.icc
                                XrdSys/XrdSysFAttrLnx.icc
                                XrdSys/XrdSysFAttrMac.icc
                                XrdSys/XrdSysFAttrSun.icc
  XrdSys/XrdSysIOEvents.cc      XrdSys/XrdSysIOEvents.hh
                                XrdSys/XrdSysIOEventsPollE.icc
                                XrdSys/XrdSysIOEventsPollPoll.icc
                                XrdSys/XrdSysIOEventsPollPort.icc
                                XrdSys/XrdSysAtomics.hh
                                XrdSys/XrdSysHeaders.hh
  XrdSys/XrdSysError.cc         XrdSys/XrdSysError.hh
  XrdSys/XrdSysLogger.cc        XrdSys/XrdSysLogger.hh

  #-----------------------------------------------------------------------------
  # XrdOuc
  #-----------------------------------------------------------------------------
  XrdOuc/XrdOuca2x.cc           XrdOuc/XrdOuca2x.hh
  XrdOuc/XrdOucArgs.cc          XrdOuc/XrdOucArgs.hh
                                XrdOuc/XrdOucCache.hh
  XrdOuc/XrdOucCacheData.cc     XrdOuc/XrdOucCacheData.hh
  XrdOuc/XrdOucCacheDram.cc     XrdOuc/XrdOucCacheDram.hh
  XrdOuc/XrdOucCacheReal.cc     XrdOuc/XrdOucCacheReal.hh
                                XrdOuc/XrdOucCacheSlot.hh
  XrdOuc/XrdOucCallBack.cc      XrdOuc/XrdOucCallBack.hh
  XrdOuc/XrdOucCRC.cc           XrdOuc/XrdOucCRC.hh
  XrdOuc/XrdOucEnv.cc           XrdOuc/XrdOucEnv.hh
                                XrdOuc/XrdOucHash.hh
                                XrdOuc/XrdOucHash.icc
  XrdOuc/XrdOucERoute.cc        XrdOuc/XrdOucERoute.hh
  XrdOuc/XrdOucExport.cc        XrdOuc/XrdOucExport.hh
  XrdOuc/XrdOucHashVal.cc
  XrdOuc/XrdOucMsubs.cc         XrdOuc/XrdOucMsubs.hh
  XrdOuc/XrdOucName2Name.cc     XrdOuc/XrdOucName2Name.hh
  XrdOuc/XrdOucN2NLoader.cc     XrdOuc/XrdOucN2NLoader.hh
  XrdOuc/XrdOucNList.cc         XrdOuc/XrdOucNList.hh
  XrdOuc/XrdOucNSWalk.cc        XrdOuc/XrdOucNSWalk.hh
  XrdOuc/XrdOucProg.cc          XrdOuc/XrdOucProg.hh
  XrdOuc/XrdOucPup.cc           XrdOuc/XrdOucPup.hh
  XrdOuc/XrdOucReqID.cc         XrdOuc/XrdOucReqID.hh
  XrdOuc/XrdOucSiteName.cc      XrdOuc/XrdOucSiteName.hh
  XrdOuc/XrdOucStream.cc        XrdOuc/XrdOucStream.hh
  XrdOuc/XrdOucString.cc        XrdOuc/XrdOucString.hh
  XrdOuc/XrdOucSxeq.cc          XrdOuc/XrdOucSxeq.hh
  XrdOuc/XrdOucTokenizer.cc     XrdOuc/XrdOucTokenizer.hh
  XrdOuc/XrdOucTPC.cc           XrdOuc/XrdOucTPC.hh
  XrdOuc/XrdOucTrace.cc         XrdOuc/XrdOucTrace.hh
  XrdOuc/XrdOucUtils.cc         XrdOuc/XrdOucUtils.hh
                                XrdOuc/XrdOucChain.hh
                                XrdOuc/XrdOucDLlist.hh
                                XrdOuc/XrdOucErrInfo.hh
                                XrdOuc/XrdOucLock.hh
                                XrdOuc/XrdOucPList.hh
                                XrdOuc/XrdOucRash.hh
                                XrdOuc/XrdOucRash.icc
                                XrdOuc/XrdOucTable.hh
                                XrdOuc/XrdOucTList.hh
                                XrdOuc/XrdOucXAttr.hh

  #-----------------------------------------------------------------------------
  # XrdNet
  #-----------------------------------------------------------------------------
  XrdNet/XrdNet.cc              XrdNet/XrdNet.hh
                                XrdNet/XrdNetOpts.hh
                                XrdNet/XrdNetPeer.hh
  XrdNet/XrdNetBuffer.cc        XrdNet/XrdNetBuffer.hh
  XrdNet/XrdNetCmsNotify.cc     XrdNet/XrdNetCmsNotify.hh
  XrdNet/XrdNetConnect.cc       XrdNet/XrdNetConnect.hh
  XrdNet/XrdNetLink.cc          XrdNet/XrdNetLink.hh
  XrdNet/XrdNetMsg.cc           XrdNet/XrdNetMsg.hh
  XrdNet/XrdNetSecurity.cc      XrdNet/XrdNetSecurity.hh
  XrdNet/XrdNetSocket.cc        XrdNet/XrdNetSocket.hh
  XrdNet/XrdNetWork.cc          XrdNet/XrdNetWork.hh

  #-----------------------------------------------------------------------------
  # XrdSut
  #-----------------------------------------------------------------------------
  XrdSut/XrdSutAux.cc           XrdSut/XrdSutAux.hh
  XrdSut/XrdSutCache.cc         XrdSut/XrdSutCache.hh
  XrdSut/XrdSutBucket.cc        XrdSut/XrdSutBucket.hh
  XrdSut/XrdSutBuckList.cc      XrdSut/XrdSutBuckList.hh
  XrdSut/XrdSutBuffer.cc        XrdSut/XrdSutBuffer.hh
  XrdSut/XrdSutPFile.cc         XrdSut/XrdSutPFile.hh
  XrdSut/XrdSutPFEntry.cc       XrdSut/XrdSutPFEntry.hh
  XrdSut/XrdSutRndm.cc          XrdSut/XrdSutRndm.hh
  XrdSut/XrdSutTrace.hh

  #-----------------------------------------------------------------------------
  # Xrd
  #-----------------------------------------------------------------------------
  Xrd/XrdBuffer.cc              Xrd/XrdBuffer.hh
  Xrd/XrdInet.cc                Xrd/XrdInet.hh
  Xrd/XrdInfo.cc                Xrd/XrdInfo.hh
  Xrd/XrdJob.hh
  Xrd/XrdLink.cc                Xrd/XrdLink.hh
  Xrd/XrdLinkMatch.cc           Xrd/XrdLinkMatch.hh
  Xrd/XrdPoll.cc                Xrd/XrdPoll.hh
                                Xrd/XrdPollDev.hh
                                Xrd/XrdPollDev.icc
                                Xrd/XrdPollE.hh
                                Xrd/XrdPollE.icc
                                Xrd/XrdPollPoll.hh
                                Xrd/XrdPollPoll.icc
  Xrd/XrdProtLoad.cc            Xrd/XrdProtLoad.hh
  Xrd/XrdProtocol.cc            Xrd/XrdProtocol.hh
  Xrd/XrdScheduler.cc           Xrd/XrdScheduler.hh
  Xrd/XrdStats.cc               Xrd/XrdStats.hh
                                Xrd/XrdTrace.hh

  #-----------------------------------------------------------------------------
  # XrdCks
  #-----------------------------------------------------------------------------
  XrdCks/XrdCksCalccrc32.cc        XrdCks/XrdCksCalccrc32.hh
  XrdCks/XrdCksCalcmd5.cc          XrdCks/XrdCksCalcmd5.hh
  XrdCks/XrdCksConfig.cc           XrdCks/XrdCksConfig.hh
  XrdCks/XrdCksLoader.cc           XrdCks/XrdCksLoader.hh
  XrdCks/XrdCksManager.cc          XrdCks/XrdCksManager.hh
                                   XrdCks/XrdCksCalcadler32.hh
                                   XrdCks/XrdCksCalc.hh
                                   XrdCks/XrdCksData.hh
                                   XrdCks/XrdCks.hh
                                   XrdCks/XrdCksXAttr.hh
)

target_link_libraries(
  XrdUtils
  pthread
  dl
  ${SOCKET_LIBRARY}
  ${SENDFILE_LIBRARY}
  ${EXTRA_LIBS} )

set_target_properties(
  XrdUtils
  PROPERTIES
  VERSION   ${XRD_UTILS_VERSION}
  SOVERSION ${XRD_UTILS_SOVERSION}
  LINK_INTERFACE_LIBRARIES "" )

#-------------------------------------------------------------------------------
# libz compatible CRC32
#-------------------------------------------------------------------------------
add_library(
  XrdCksCalczcrc32
  SHARED
  XrdCks/XrdCksCalczcrc32.cc )

target_link_libraries(
  XrdCksCalczcrc32
  XrdUtils
  ${ZLIB_LIBRARY} )

set_target_properties(
  XrdCksCalczcrc32
  PROPERTIES
  VERSION   ${XRD_ZCRC32_VERSION}
  SOVERSION ${XRD_ZCRC32_SOVERSION}
  LINK_INTERFACE_LIBRARIES "" )

#-------------------------------------------------------------------------------
# The helper lib
#-------------------------------------------------------------------------------
add_library(
  XrdMain
  SHARED
  Xrd/XrdConfig.cc          Xrd/XrdConfig.hh
  Xrd/XrdMain.cc )

target_link_libraries( XrdMain XrdUtils pthread )

set_target_properties(
  XrdMain
  PROPERTIES
  VERSION   ${XRD_MAIN_VERSION}
  SOVERSION ${XRD_MAIN_SOVERSION} )

#-------------------------------------------------------------------------------
# Install
#-------------------------------------------------------------------------------
install(
  TARGETS XrdUtils XrdMain XrdCksCalczcrc32
  LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR} )
