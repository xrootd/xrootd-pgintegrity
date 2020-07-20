//------------------------------------------------------------------------------
// Copyright (c) 2011-2014 by European Organization for Nuclear Research (CERN)
// Author: Lukasz Janyst <ljanyst@cern.ch>
//------------------------------------------------------------------------------
// This file is part of the XRootD software suite.
//
// XRootD is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// XRootD is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with XRootD.  If not, see <http://www.gnu.org/licenses/>.
//
// In applying this licence, CERN does not waive the privileges and immunities
// granted to it by virtue of its status as an Intergovernmental Organization
// or submit itself to any jurisdiction.
//------------------------------------------------------------------------------

#ifndef __XRD_CL_XROOTD_TRANSPORT_HH__
#define __XRD_CL_XROOTD_TRANSPORT_HH__

#include "XrdCl/XrdClPostMaster.hh"
#include "XProtocol/XProtocol.hh"
#include "XrdSec/XrdSecInterface.hh"
#include "XrdOuc/XrdOucEnv.hh"

#include <arpa/inet.h>

class XrdSysPlugin;
class XrdSecProtect;

namespace XrdCl
{
  class Tls;
  class Socket;
  struct XRootDChannelInfo;
  struct PluginUnloadHandler;

  //----------------------------------------------------------------------------
  //! XRootD related protocol queries
  //----------------------------------------------------------------------------
  struct XRootDQuery
  {
    static const uint16_t ServerFlags     = 1002; //!< returns server flags
    static const uint16_t ProtocolVersion = 1003; //!< returns the protocol version
    static const uint16_t HasDataEncryption = 1004; //!< data pass over encrypted streams
  };

  //----------------------------------------------------------------------------
  //! XRootD transport handler
  //----------------------------------------------------------------------------
  class XRootDTransport: public TransportHandler
  {
    public:
      //------------------------------------------------------------------------
      //! Constructor
      //------------------------------------------------------------------------
      XRootDTransport();

      //------------------------------------------------------------------------
      //! Destructor
      //------------------------------------------------------------------------
      ~XRootDTransport();

      //------------------------------------------------------------------------
      //! Read a message header from the socket, the socket is non-blocking,
      //! so if there is not enough data the function should return suRetry
      //! in which case it will be called again when more data arrives, with
      //! the data previously read stored in the message buffer
      //!
      //! @param message the message buffer
      //! @param socket  the socket
      //! @return        stOK & suDone if the whole message has been processed
      //!                stOK & suRetry if more data is needed
      //!                stError on failure
      //------------------------------------------------------------------------
      virtual Status GetHeader( Message *message, Socket *socket );

      //------------------------------------------------------------------------
      //! Read the message body from the socket, the socket is non-blocking,
      //! the method may be called multiple times - see GetHeader for details
      //!
      //! @param message the message buffer containing the header
      //! @param socket  the socket
      //! @return        stOK & suDone if the whole message has been processed
      //!                stOK & suRetry if more data is needed
      //!                stError on failure
      //------------------------------------------------------------------------
      virtual Status GetBody( Message *message, Socket *socket );

      //------------------------------------------------------------------------
      //! Initialize channel
      //------------------------------------------------------------------------
      virtual void InitializeChannel( const URL  &url,
                                      AnyObject  &channelData );

      //------------------------------------------------------------------------
      //! Finalize channel
      //------------------------------------------------------------------------
      virtual void FinalizeChannel( AnyObject &channelData );

      //------------------------------------------------------------------------
      //! HandShake
      //------------------------------------------------------------------------
      virtual Status HandShake( HandShakeData *handShakeData,
                                AnyObject     &channelData );

      //------------------------------------------------------------------------
      // @return true if handshake has been done and stream is connected,
      //         false otherwise
      //------------------------------------------------------------------------
      virtual bool HandShakeDone( HandShakeData *handShakeData,
                                  AnyObject     &channelData );

      //------------------------------------------------------------------------
      //! Check if the stream should be disconnected
      //------------------------------------------------------------------------
      virtual bool IsStreamTTLElapsed( time_t     time,
                                       AnyObject &channelData );

      //------------------------------------------------------------------------
      //! Check the stream is broken - ie. TCP connection got broken and
      //! went undetected by the TCP stack
      //------------------------------------------------------------------------
      virtual Status IsStreamBroken( time_t     inactiveTime,
                                     AnyObject &channelData );

      //------------------------------------------------------------------------
      //! Return the ID for the up stream this message should be sent by
      //! and the down stream which the answer should be expected at.
      //! Modify the message itself if necessary.
      //! If hint is non-zero then the message should be modified such that
      //! the answer will be returned via the hinted stream.
      //------------------------------------------------------------------------
      virtual PathID Multiplex( Message   *msg,
                                AnyObject &channelData,
                                PathID    *hint = 0 );

      //------------------------------------------------------------------------
      //! Return the ID for the up substream this message should be sent by
      //! and the down substream which the answer should be expected at.
      //! Modify the message itself if necessary.
      //! If hint is non-zero then the message should be modified such that
      //! the answer will be returned via the hinted stream.
      //------------------------------------------------------------------------
      virtual PathID MultiplexSubStream( Message   *msg,
                                         AnyObject &channelData,
                                         PathID    *hint = 0 );

      //------------------------------------------------------------------------
      //! Return a number of substreams per stream that should be created
      //------------------------------------------------------------------------
      virtual uint16_t SubStreamNumber( AnyObject &channelData );

      //------------------------------------------------------------------------
      //! Return the information whether a control connection needs to be
      //! valid before establishing other connections
      //------------------------------------------------------------------------
      virtual bool NeedControlConnection()
      {
        return true;
      }

      //------------------------------------------------------------------------
      //! Marshal the outgoing message
      //------------------------------------------------------------------------
      static Status MarshallRequest( Message *msg );

      //------------------------------------------------------------------------
      //! Unmarshall the request - sometimes the requests need to be rewritten,
      //! so we need to unmarshall them
      //------------------------------------------------------------------------
      static Status UnMarshallRequest( Message *msg );

      //------------------------------------------------------------------------
      //! Unmarshall the body of the incoming message
      //------------------------------------------------------------------------
      static Status UnMarshallBody( Message *msg, uint16_t reqType );

      //------------------------------------------------------------------------
      //!
      //------------------------------------------------------------------------
      static Status CheckStatusIntegrity( ServerResponseStatus *ms );

      //------------------------------------------------------------------------
      //! Unmarshall the header incoming message
      //------------------------------------------------------------------------
      static void UnMarshallHeader( Message *msg );

      //------------------------------------------------------------------------
      //! Log server error response
      //------------------------------------------------------------------------
      static void LogErrorResponse( const Message &msg );

      //------------------------------------------------------------------------
      //! Number of currently connected data streams
      //------------------------------------------------------------------------
      static uint16_t NbConnectedStrm( AnyObject &channelData );

      //------------------------------------------------------------------------
      //! The stream has been disconnected, do the cleanups
      //------------------------------------------------------------------------
      virtual void Disconnect( AnyObject &channelData,
                               uint16_t   subStreamId );

      //------------------------------------------------------------------------
      //! Query the channel
      //------------------------------------------------------------------------
      virtual Status Query( uint16_t   query,
                            AnyObject &result,
                            AnyObject &channelData );

      //------------------------------------------------------------------------
      //! Get the description of a message
      //------------------------------------------------------------------------
      static void SetDescription( Message *msg );

      //------------------------------------------------------------------------
      //! Check if the message invokes a stream action
      //------------------------------------------------------------------------
      virtual uint32_t MessageReceived( Message   *msg,
                                        uint16_t   subStream,
                                        AnyObject &channelData );

      //------------------------------------------------------------------------
      //! Notify the transport about a message having been sent
      //------------------------------------------------------------------------
      virtual void MessageSent( Message   *msg,
                                uint16_t   subStream,
                                uint32_t   bytesSent,
                                AnyObject &channelData );

      //------------------------------------------------------------------------
      //! Get signature for given message
      //------------------------------------------------------------------------
      virtual Status GetSignature( Message *toSign, Message *&sign,
                                   AnyObject &channelData );

      //------------------------------------------------------------------------
      //! Get signature for given message
      //------------------------------------------------------------------------
      virtual Status GetSignature( Message *toSign, Message *&sign,
                                   XRootDChannelInfo *info );

      //------------------------------------------------------------------------
      //! Wait until the program can safely exit
      //------------------------------------------------------------------------
      virtual void WaitBeforeExit();

      //------------------------------------------------------------------------
      //! @return : true if encryption should be turned on, false otherwise
      //------------------------------------------------------------------------
      virtual bool NeedEncryption( HandShakeData  *handShakeData,
                                  AnyObject      &channelData );

      struct ServerResponseInfo
      {
        bool isKxrStatus;   // response is kXR_status
        uint8_t *sid;       // pointer to streamid[2]
        uint16_t estatus;   // equivalent response type (based on resptype or status)
        uint32_t hlen;      // header length, excluding info or data portions

        enum XRequestTypes reqid; // requestid (only in case isKxrStatus)

        uint32_t idlen;     // length of info+data portions
        uint32_t rawdlen;   // length of data only portion
        uint32_t idavail;   // number of bytes actually available after start of info section
        bool hasallidata;   // all bytes expected from info section onwards are available
        char *idata;        // pointer to start of info section
        char *rawdata;      // pointer to start of data section
      };

      //------------------------------------------------------------------------
      //! Fills in a ServerResponseInfo objec with information from a buffer
      //! containing a server response.
      //------------------------------------------------------------------------
      static Status GetServerResponseInfo( char *buff, const uint32_t bufflen,
                                           const bool unmarshall,
                                           ServerResponseInfo &sri )
      {
        if (bufflen < 8)
          return Status( stError, errInternal );

        ServerResponseHeader *srp = (ServerResponseHeader *)buff;

        uint16_t hst = srp->status;
        uint32_t resplen = srp->dlen;

        sri.sid = srp->streamid;
        if (unmarshall)
        {
          hst = ntohs(hst);
          resplen = ntohl(resplen);
        }

        if (hst != kXR_status)
        {
          sri.isKxrStatus = false;
          sri.estatus = hst;
          sri.hlen = 8;
          sri.reqid = kXR_1stRequest;  // not available for non kXR_status response
          sri.idlen = resplen;         // for non kXR_status no info section
          sri.rawdlen = resplen;
          sri.idavail = bufflen - sri.hlen;
          sri.hasallidata = (sri.hlen+sri.idlen <= bufflen);
          sri.idata = buff + sri.hlen;
          sri.rawdata = sri.idata;
          return Status( stOK, suDone );
        }

        sri.hlen = 8 + XrdProto::kXR_statusBodyLen;

        if (resplen < XrdProto::kXR_statusBodyLen || resplen > INT_MAX)
          return Status( stError, errInternal );

        if (bufflen < sri.hlen)
          return Status( stError, errInternal );

        ServerResponseStatus *srsp = (ServerResponseStatus *)buff;
        sri.rawdlen = srsp->bdy.dlen;
        if (unmarshall)
        {
          sri.rawdlen = ntohl(sri.rawdlen);
        }
        sri.estatus = srsp->bdy.resptype;
        if (sri.estatus == XrdProto::kXR_FinalResult)
        {
          sri.estatus = kXR_ok;
        }
        else if (sri.estatus == XrdProto::kXR_PartialResult)
        {
          sri.estatus = kXR_oksofar;
        }
        sri.isKxrStatus = true;
        sri.reqid = (enum XRequestTypes)(srsp->bdy.requestid + kXR_1stRequest);
        sri.idlen = (resplen - XrdProto::kXR_statusBodyLen) + sri.rawdlen;
        sri.idata = buff + sri.hlen;
        sri.hasallidata = (sri.hlen+sri.idlen <= bufflen);
        sri.idavail = bufflen - sri.hlen;
        sri.rawdata = buff + 8 + resplen;
        return Status( stOK, suDone );
      }

    private:

      //------------------------------------------------------------------------
      // Read from socket until the message cursor it at (or beyond) target
      // Returns the number of bytes read or -1 in case of error
      //------------------------------------------------------------------------
      ssize_t ReadUntilCursor(Message *message, Socket *socket, uint32_t target,
                              Status &st);

      //------------------------------------------------------------------------
      // Hand shake the main stream
      //------------------------------------------------------------------------
      Status HandShakeMain( HandShakeData *handShakeData,
                            AnyObject     &channelData );

      //------------------------------------------------------------------------
      // Hand shake a parallel stream
      //------------------------------------------------------------------------
      Status HandShakeParallel( HandShakeData *handShakeData,
                                AnyObject     &channelData );

      //------------------------------------------------------------------------
      // Generate the message to be sent as an initial handshake
      // (handshake + kXR_protocol)
      //------------------------------------------------------------------------
      Message *GenerateInitialHSProtocol( HandShakeData     *hsData,
                                          XRootDChannelInfo *info,
                                          kXR_char           expect );

      //------------------------------------------------------------------------
      // Process the server initial handshake response
      //------------------------------------------------------------------------
      Status ProcessServerHS( HandShakeData     *hsData,
                              XRootDChannelInfo *info );

      //-----------------------------------------------------------------------
      // Process the protocol response
      //------------------------------------------------------------------------
      Status ProcessProtocolResp( HandShakeData     *hsData,
                                  XRootDChannelInfo *info );

      //------------------------------------------------------------------------
      // Generate the bind message
      //------------------------------------------------------------------------
      Message *GenerateBind( HandShakeData     *hsData,
                             XRootDChannelInfo *info );

      //------------------------------------------------------------------------
      // Generate the bind message
      //------------------------------------------------------------------------
      Status ProcessBindResp( HandShakeData     *hsData,
                              XRootDChannelInfo *info );

      //------------------------------------------------------------------------
      // Generate the login  message
      //------------------------------------------------------------------------
      Message *GenerateLogIn( HandShakeData     *hsData,
                              XRootDChannelInfo *info );

      //------------------------------------------------------------------------
      // Process the login response
      //------------------------------------------------------------------------
      Status ProcessLogInResp( HandShakeData     *hsData,
                               XRootDChannelInfo *info );

      //------------------------------------------------------------------------
      // Do the authentication
      //------------------------------------------------------------------------
      Status DoAuthentication( HandShakeData     *hsData,
                               XRootDChannelInfo *info );

      //------------------------------------------------------------------------
      // Get the initial credentials using one of the protocols
      //------------------------------------------------------------------------
      Status GetCredentials( XrdSecCredentials *&credentials,
                             HandShakeData      *hsData,
                             XRootDChannelInfo  *info );

      //------------------------------------------------------------------------
      // Clean up the data structures created for the authentication process
      //------------------------------------------------------------------------
      Status CleanUpAuthentication( XRootDChannelInfo *info );

      //------------------------------------------------------------------------
      // Clean up the data structures created for the protection purposes
      //------------------------------------------------------------------------
      Status CleanUpProtection( XRootDChannelInfo *info );

      //------------------------------------------------------------------------
      // Get the authentication function handle
      //------------------------------------------------------------------------
      XrdSecGetProt_t GetAuthHandler();

      //------------------------------------------------------------------------
      // Generate the end session message
      //------------------------------------------------------------------------
      Message *GenerateEndSession( HandShakeData     *hsData,
                                   XRootDChannelInfo *info );

      //------------------------------------------------------------------------
      // Process the end session response
      //------------------------------------------------------------------------
      Status ProcessEndSessionResp( HandShakeData     *hsData,
                                    XRootDChannelInfo *info );

      //------------------------------------------------------------------------
      // Get a string representation of the server flags
      //------------------------------------------------------------------------
      static std::string ServerFlagsToStr( uint32_t flags );

      //------------------------------------------------------------------------
      // Get a string representation of file handle
      //------------------------------------------------------------------------
      static std::string FileHandleToStr( const unsigned char handle[4] );

      friend struct PluginUnloadHandler;
      PluginUnloadHandler *pSecUnloadHandler;
  };
}

#endif // __XRD_CL_XROOTD_TRANSPORT_HANDLER_HH__
