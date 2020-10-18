#ifndef _XRDOSSCSIPAGES_H
#define _XRDOSSCSIPAGES_H
/******************************************************************************/
/*                                                                            */
/*              X r d O s s I n t e g r i t y P a g e s . h h                 */
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

#include "XrdSys/XrdSysPthread.hh"

#include "XrdOssIntegrityTagstore.hh"
#include "XrdOssIntegrityRanges.hh"
#include <memory>
#include <mutex>
#include <utility>

class XrdOssCsiPages
{
public:
   typedef std::pair<off_t,off_t> Sizes_t;

   XrdOssCsiPages(const std::string &fn, std::unique_ptr<XrdOssCsiTagstore> ts, bool wh, bool am, const std::string &);
   ~XrdOssCsiPages() { (void)Close(); }

   int Open(const char *path, off_t dsize, int flags, XrdOucEnv &envP);
   int Close();

   int UpdateRange(XrdOssDF *, const void *, off_t, size_t, XrdOssCsiRangeGuard&);
   ssize_t VerifyRange(XrdOssDF *, const void *, off_t, size_t, XrdOssCsiRangeGuard&);
   void Flush();
   int Fsync();

   ssize_t FetchRange(XrdOssDF *, const void *, off_t, size_t, uint32_t *, uint64_t, XrdOssCsiRangeGuard&);
   int StoreRange(XrdOssDF *, const void *, off_t, size_t, uint32_t *, uint64_t, XrdOssCsiRangeGuard&);
   void LockTrackinglen(XrdOssCsiRangeGuard &, off_t, off_t, bool);

   bool IsReadOnly() const { return rdonly_; }
   int truncate(XrdOssDF *, off_t, XrdOssCsiRangeGuard&);
   int TrackedSizesGet(Sizes_t &, bool);
   int LockResetSizes(off_t);
   void TrackedSizeRelease();
   int VerificationStatus();

protected:
   ssize_t apply_sequential_aligned_modify(const void *, off_t, size_t, uint32_t *, bool, bool, uint32_t, uint32_t);
   std::unique_ptr<XrdOssCsiTagstore> ts_;
   XrdSysMutex rangeaddmtx_;
   XrdOssCsiRanges ranges_;
   bool writeHoles_;
   bool allowMissingTags_;
   bool hasMissingTags_;
   bool rdonly_;

   XrdSysCondVar tscond_;
   bool tsforupdate_;

   const std::string fn_;
   const std::string tident_;

   int LockSetTrackedSize(off_t);
   int LockTruncateSize(off_t,bool);
   int LockMakeUnverified();

   int UpdateRangeAligned(const void *, off_t, size_t, const Sizes_t &);
   int UpdateRangeUnaligned(XrdOssDF *, const void *, off_t, size_t, const Sizes_t &);
   int UpdateRangeHoleUntilPage(XrdOssDF *, off_t, const Sizes_t &);
   ssize_t VerifyRangeAligned(const void *, off_t, size_t, const Sizes_t &);
   ssize_t VerifyRangeUnaligned(XrdOssDF *, const void *, off_t, size_t, const Sizes_t &);
   ssize_t FetchRangeAligned(const void *, off_t, size_t, const Sizes_t &, uint32_t *, uint64_t);
   int StoreRangeAligned(const void *, off_t, size_t, const Sizes_t &, uint32_t *);

   static ssize_t fullread(XrdOssDF *fd, void *buff, const off_t off , const size_t sz)
   {
      ssize_t rret = maxread(fd, buff, off, sz);
      if (static_cast<size_t>(rret) != sz) return -EIO;
      return rret;
   }

   static ssize_t maxread(XrdOssDF *fd, void *buff, const off_t off , const size_t sz)
   {
      size_t toread = sz, nread = 0;
      uint8_t *p = (uint8_t*)buff;
      while(toread>0)
      {
         const ssize_t rret = fd->Read(&p[nread], off+nread, toread);
         if (rret<0) return rret;
         if (rret==0) break;
         toread -= rret;
         nread += rret;
      }
      return nread;
   }

   static const size_t stsize_ = 1024;
};

#endif
