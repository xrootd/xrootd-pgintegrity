#ifndef __XRDNETUTILS_HH__
#define __XRDNETUTILS_HH__
/******************************************************************************/
/*                                                                            */
/*                        X r d N e t U t i l s . h h                         */
/*                                                                            */
/* (c) 2013 by the Board of Trustees of the Leland Stanford, Jr., University  */
/*                            All Rights Reserved                             */
/*   Produced by Andrew Hanushevsky for Stanford University under contract    */
/*              DE-AC02-76-SFO0515 with the Department of Energy              */
/*                                                                            */
/* This file is part of the XRootD software suite.                            */
/*                                                                            */
/* XRootD is free software: you can redistribute it and/or modify it under    */
/* the terms of the GNU Lesser General Public License as published by the     */
/* Free Software Foundation, either version 3 of the License, or (at your     */
/* option) any later version.                                                 */
/*                                                                            */
/* XRootD is distributed in the hope that it will be useful, but WITHOUT      */
/* ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or      */
/* FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public       */
/* License for more details.                                                  */
/*                                                                            */
/* You should have received a copy of the GNU Lesser General Public License   */
/* along with XRootD in a file called COPYING.LESSER (LGPL license) and file  */
/* COPYING (GPL license).  If not, see <http://www.gnu.org/licenses/>.        */
/*                                                                            */
/* The copyright holder's institutional names and contributor's names may not */
/* be used to endorse or promote products derived from this software without  */
/* specific prior written permission of the institution or contributor.       */
/******************************************************************************/

struct sockaddr;
struct sockaddr_storage;
  
class XrdNetUtils
{
public:

//------------------------------------------------------------------------------
//! Decode an "encoded" address and place it sockaddr type structure.
//!
//! @param  sadr     address of structure that will hold the results.
//! @param  buff     address of buffer that holds the encoding.
//! @param  blen     length of the string (it need not be null terminated).
//!
//! @return > 0      the port number in host byte order.
//!         = 0      the port number was not set.
//!         < 0      the encoding was not correct.
//------------------------------------------------------------------------------

static int  Decode(struct sockaddr_storage *sadr, const char *buff, int blen);

//------------------------------------------------------------------------------
//! Encode the address and return it in a supplied buffer.
//!
//! @param  sadr     address of structure that holds the IPV4/6 address.
//! @param  buff     address of buffer to hold the null terminated encoding.
//! @param  blen     length of the buffer. It6 should be at least 40 bytes.
//! @param  port     optional port value to use as opposed to the one present
//!                  in sockaddr sadr. The port must be in host order.
//!
//! @return > 0      the length of the encoding less the null byte.
//!         = 0      current address format not supported for encoding.
//!         < 0      buffer is too small; abs(retval) bytes needed.
//------------------------------------------------------------------------------

static int  Encode(const struct sockaddr *sadr, char *buff, int blen, int port=-1);

//------------------------------------------------------------------------------
//! Determine if a hostname matches a pattern.
//!
//! @param  hName    the name of the host.
//! @param  pattern  the pattern to match against. The pattern may contain one
//!                  If the pattern contains a single asterisk, then the prefix
//!                  of hName is compared with the characters before the '*' and
//!                  the suffix of hName is compared with the character after.
//!                  If the pattern ends with a plus, the all then pattern is
//!                  taken as a hostname (less '+') and expanded to all possible
//!                  hostnames and each one is compared with hName. If the
//!                  pattern contains both, the asterisk rule is used first.
//!                  If it contains neither then strict equality is used.
//!
//! @return Success: True,  the pattern matches.
//!         Failure: False, no match found.
//------------------------------------------------------------------------------

static bool Match(const char *hName, const char *pattern);

//------------------------------------------------------------------------------
//! Parse an IP or host name specification.
//!
//! @param  hSpec    the name or IP address of the host. As one of the following
//!                  "[<ipv6>]:<port>", "<ipv4>:<port>", or "<name>:<port>".
//! @param  hName    place where the starting address of the host is placed.
//! @param  hNend    place where the ending   address+1 is placed. This will
//!                  point to either ']', ':', or a null byte.
//! @param  hPort    place where the starting address of the port is placed.
//!                  If no ":port" was found, this will contain *hNend.
//! @param  hNend    place where the ending   address+1 is placed. If no port
//!                  If no ":port" was found, this will contain *hNend.
//!
//! @return Success: True.
//!         Failure: False, hSpec is not valid. Some output parameters may have
//!                  been set but shlould be ignored.
//------------------------------------------------------------------------------

static bool Parse(char *hSpec, char **hName, char **hNend,
                               char **hPort, char **hPend);

//------------------------------------------------------------------------------
//! Obtain the numeric port associated with a file descriptor.
//!
//! @param  fd       the file descriptor number.
//! @param  eText    when not null, the reason for a failure is returned.
//!
//! @return Success: The positive port number.
//!         Failure: 0 is returned and if eText is not null, the error message.
//------------------------------------------------------------------------------

static int  Port(int fd, char **eText=0);

//------------------------------------------------------------------------------
//! Obtain the protocol identifier.
//!
//! @param  pName    the name of the protocol (e.g. "tcp").
//! @param  eText    when not null, the reason for a failure is returned.
//!
//! @return The protocol identifier.
//------------------------------------------------------------------------------

static int  ProtoID(const char *pName);

//------------------------------------------------------------------------------
//! Obtain the numeric port corresponding to a symbolic name.
//!
//! @param  sName    the name of the service.
//! @param  isUDP    if true, returns the UDP service port o/w the TCP service
//! @param  eText    when not null, the reason for a failure is returned.
//!
//! @return Success: The positive port number.
//!         Failure: 0 is returned and if eText is not null, the error message.
//------------------------------------------------------------------------------

static int  ServPort(const char *sName, bool isUDP=0, const char **eText=0);

//------------------------------------------------------------------------------
//! Constructor
//------------------------------------------------------------------------------

            XrdNetUtils() {}

//------------------------------------------------------------------------------
//! Destructor
//------------------------------------------------------------------------------

           ~XrdNetUtils() {}
private:

static int setET(char **errtxt, int rc);
};
#endif
