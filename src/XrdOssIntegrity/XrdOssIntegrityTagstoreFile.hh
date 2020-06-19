#ifndef _XRDOSSINTEGRITYTAGSTOREFILE_H
#define _XRDOSSINTEGRITYTAGSTOREFILE_H
/******************************************************************************/
/*                                                                            */
/*        X r d O s s I n t e g r i t y T a g s t o r e F i l e . h h         */
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
#include "XrdOssIntegrityTagstore.hh"
#include "XrdOuc/XrdOucCRC.hh"

#include <memory>
#include <mutex>
#include <byteswap.h>

class XrdOssIntegrityTagstoreFile : public XrdOssIntegrityTagstore
{
public:
   XrdOssIntegrityTagstoreFile(std::unique_ptr<XrdOssDF> fd) : fd_(std::move(fd)), trackinglen_(0), isOpen(false) { }
   virtual ~XrdOssIntegrityTagstoreFile() { if (isOpen) { (void)Close(); } }

   virtual int Open(const char *, off_t, int, XrdOucEnv &) override;
   virtual int Close() override;

   virtual int Fsync() override;

   virtual ssize_t WriteTags(const uint32_t *, off_t, size_t) override;
   virtual ssize_t ReadTags(uint32_t *, off_t, size_t) override;

   virtual int Truncate(off_t, bool) override;

   virtual off_t GetTrackedTagSize() const override
   {
      if (!isOpen) return 0;
      return trackinglen_;
   }

   virtual off_t GetTrackedDataSize() const override
   {
      if (!isOpen) return 0;
      return actualsize_;
   }

   virtual int ResetSizes(const off_t size) override;

   virtual int SetTrackedSize(const off_t size) override
   {
      if (!isOpen) return -EBADF;
      if (size > actualsize_)
      {
         actualsize_ = size;
      }
      if (size != trackinglen_)
      {
         const int wtt = WriteTrackedTagSize(size);
         if (wtt<0) return wtt;
      }
      return 0;
   }

   static ssize_t fullread(XrdOssDF &fd, void *buff, const off_t off , const size_t sz)
   {
      size_t toread = sz, nread = 0;
      uint8_t *p = (uint8_t*)buff;
      while(toread>0)
      {
         const ssize_t rret = fd.Read(&p[nread], off+nread, toread);
         if (rret<0) return rret;
         if (rret==0) break;
         toread -= rret;
         nread += rret;
      }
      if (nread != sz) return -EIO;
      return nread;
   }

   static ssize_t fullwrite(XrdOssDF &fd, const void *buff, const off_t off , const size_t sz)
   {
      size_t towrite = sz, nwritten = 0;
      const uint8_t *p = (const uint8_t*)buff;
      while(towrite>0)
      {
         const ssize_t wret = fd.Write(&p[nwritten], off+nwritten, towrite);
         if (wret<0) return wret;
         towrite -= wret;
         nwritten += wret;
      }
      return nwritten;
   }

private:
   std::unique_ptr<XrdOssDF> fd_;
   off_t trackinglen_;
   off_t actualsize_;
   bool isOpen;
   bool machineIsBige_;
   bool fileIsBige_;

   ssize_t WriteTags_swap(const uint32_t *, off_t, size_t);
   ssize_t ReadTags_swap(uint32_t *, off_t, size_t);

   int WriteTrackedTagSize(const off_t size)
   {
      if (!isOpen) return -EBADF;
      uint64_t x = size;
      if (fileIsBige_ != machineIsBige_) x = bswap_64(x);
      uint8_t wbuf[12];
      memcpy(wbuf, &x, 8);
      uint32_t cv = XrdOucCRC::Calc32C(&x, 8, 0U);
      if (machineIsBige_ != fileIsBige_) cv = bswap_32(cv);
      memcpy(&wbuf[8], &cv, 4);
      ssize_t wret = fullwrite(*fd_, wbuf, 4, 12);
      if (wret<0) return wret;
      trackinglen_ = size;
      return 0;
   }
};

#endif
