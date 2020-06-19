#ifndef _XRDOSSINTEGRITYTAGSTORE_H
#define _XRDOSSINTEGRITYTAGSTORE_H
/******************************************************************************/
/*                                                                            */
/*          X r d O s s I n t e g r i t y T a g s t o r e . h h               */
/*                                                                            */
/* (C) Copyright 2020 CERN.                                                   */
/*                                                                            */
/* This file is part of the XRootD software suite.                            */
/*                                                                            */
/* XRootD is free software: you can redistribute it and/or modify it under    */
/* the terms of the GNU Lesser General Public License as published by the     */
/* Free Software Foundation, either version 3 of the License, or (at your     */
/* option) any later version.                                                 */
/*                                                                            */
/* In applying this licence, CERN does not waive the privileges and           */
/* immunities granted to it by virtue of its status as an Intergovernmental   */
/* Organization or submit itself to any jurisdiction.                         */
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

#include "XrdOss/XrdOss.hh"

class XrdOssIntegrityTagstore
{
public:

   virtual ~XrdOssIntegrityTagstore() { }

   virtual int Open(const char *, off_t, int, XrdOucEnv &)=0;
   virtual int Close()=0;

   virtual int Fsync()=0;

   virtual ssize_t WriteTags(const uint32_t *, off_t, size_t)=0;
   virtual ssize_t ReadTags(uint32_t *, off_t, size_t)=0;

   virtual off_t GetTrackedTagSize() const=0;
   virtual off_t GetTrackedDataSize() const=0;

   virtual int SetTrackedSize(off_t)=0;
   virtual int ResetSizes(off_t)=0;
   virtual int Truncate(off_t,bool)=0;
};

#endif
