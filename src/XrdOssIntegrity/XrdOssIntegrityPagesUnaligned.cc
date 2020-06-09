/******************************************************************************/
/*                                                                            */
/*     X r d O s s I n t e g r i t y P a g e s U n a l i g n e d . c c        */
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

#include "XrdOssIntegrityPages.hh"
#include "XrdOuc/XrdOucCRC.hh"
#include "XrdSys/XrdSysPageSize.hh"

#include <vector>

int XrdOssIntegrityPages::UpdateRangeHoleUntilPage(const off_t until, const Sizes_t &sizes)
{
   static const uint8_t bz[XrdSys::PageSize] = {0};
   static const uint32_t crczero = XrdOucCRC::Calc32C(bz, XrdSys::PageSize, 0U);
   static const std::vector<uint32_t> crc32Vec(1024, crczero);

   const off_t trackinglen = sizes.first;
   const off_t tracked_page = trackinglen / XrdSys::PageSize;
   if (until <= tracked_page) return 0;

   const size_t tracked_off = trackinglen % XrdSys::PageSize;

   // if last tracked page is before page "until" extend it
   if (tracked_off>0)
   {
      uint32_t prevtag;
      const ssize_t rret = ts_->ReadTags(&prevtag, tracked_page, 1);
      if (rret < 0) return rret;
      const uint32_t crc32c = XrdOucCRC::Calc32C(bz, XrdSys::PageSize - tracked_off, prevtag);
      const ssize_t wret = ts_->WriteTags(&crc32c, tracked_page, 1);
      if (wret < 0) return wret;
   }

   if (!writeHoles_) return 0;

   const off_t nAllEmpty = (tracked_off>0) ? (until - tracked_page - 1) : (until - tracked_page);
   const off_t firstEmpty = (tracked_off>0) ? (tracked_page + 1) : tracked_page;

   off_t towrite = nAllEmpty;
   off_t nwritten = 0;
   while(towrite>0)
   {
      const size_t nw = std::min(towrite, (off_t)crc32Vec.size());
      const ssize_t wret = ts_->WriteTags(&crc32Vec[0], firstEmpty+nwritten, nw);
      if (wret<0) return wret;
      towrite -= wret;
      nwritten += wret;
   }

   return 0;
}

int XrdOssIntegrityPages::UpdateRangeUnaligned(XrdOssDF *const fd, const void *buff, const off_t offset, const size_t blen, const Sizes_t &sizes)
{
   const off_t p1 = offset / XrdSys::PageSize;

   const off_t trackinglen = sizes.first;
   if (offset > trackinglen)
   {
      const int ret = UpdateRangeHoleUntilPage(p1, sizes);
      if (ret<0) return ret;
   }

   const size_t p1_off = offset % XrdSys::PageSize;
   const off_t p2 = (offset+blen) / XrdSys::PageSize;
   const size_t p2_off = (offset+blen) % XrdSys::PageSize;

   const off_t tracked_page = trackinglen / XrdSys::PageSize;
   const size_t tracked_off = trackinglen % XrdSys::PageSize;

   // deal with partial first page
   if (p1_off>0 || blen<XrdSys::PageSize)
   {
      const size_t bavail = (XrdSys::PageSize-p1_off > blen) ? blen : (XrdSys::PageSize-p1_off);
      uint8_t b[XrdSys::PageSize];
      if (p1 > tracked_page)
      {
         // the start of will have a number of implied zero bytes
         uint32_t crc32c;
         if (p1_off == 0)
         {
            crc32c = XrdOucCRC::Calc32C(buff, bavail, 0U);
         }
         else
         {
            memset(b, 0, p1_off);
            memcpy(&b[p1_off], buff, bavail);
            crc32c = XrdOucCRC::Calc32C(b, p1_off+bavail, 0U);
         }
         const ssize_t wret = ts_->WriteTags(&crc32c, p1, 1);
         if (wret<0) return wret;
      }
      else
      {
         if (tracked_off == 0 && p1_off == 0)
            {
               // appending at the start of empty block
               const uint32_t crc32c = XrdOucCRC::Calc32C(buff, bavail, 0U);
               const ssize_t wret = ts_->WriteTags(&crc32c, p1, 1);
               if (wret<0) return wret;
            }
            else if (tracked_off == p1_off)
            {
               // strictly appending: can recalc crc with new data
               // without rereeading existing partial block's data
               uint32_t crc32v;
               const ssize_t rret = ts_->ReadTags(&crc32v, p1, 1);
               if (rret<0) return rret;
               const uint32_t crc32c = XrdOucCRC::Calc32C(buff, bavail, crc32v);
               const ssize_t wret = ts_->WriteTags(&crc32c, p1, 1);
               if (wret<0) return wret;
            }
            else
            {
               // have to read some preexisting data and/or implied zero bytes
               size_t toread = (p1==tracked_page) ? tracked_off : XrdSys::PageSize;
               if (toread>0) {
               ssize_t rret = XrdOssIntegrityPages::fullread(fd, b, XrdSys::PageSize * p1, toread);
               if (rret<0) return -EIO;
               const uint32_t crc32c = XrdOucCRC::Calc32C(b, toread, 0U);
               uint32_t crc32v;
               rret = ts_->ReadTags(&crc32v, p1, 1);
               if (rret<0) return rret;
               if (crc32v != crc32c)
               {
                  return -EDOM;
               }
            }
            if (p1_off > toread)
            {
               memset(&b[toread], 0, p1_off-toread);
            }
            memcpy(&b[p1_off], buff, bavail);
            const uint32_t crc32c = XrdOucCRC::Calc32C(b, std::max(p1_off+bavail, toread), 0U);
            const ssize_t wret = ts_->WriteTags(&crc32c, p1, 1);
            if (wret<0) return wret;
         }
      }
   }

   // first (inclusive) and last (exclusive) full page
   const off_t fp = (p1_off != 0) ? p1+1 : p1;
   const off_t lp = p2;
   if (fp<lp)
   {
      const uint8_t *p = (uint8_t*)buff;
      const ssize_t aret = apply_sequential_aligned_modify(&p[p1_off ? XrdSys::PageSize-p1_off : 0], fp, lp-fp, nullptr, 0);
      if (aret<0) return aret;
   }

   // partial last page (if not already covered as being first page)
   if (p2>p1 && p2_off>0)
   {
      const uint8_t *p = (uint8_t*)buff;
      const uint32_t crc32c = XrdOucCRC::Calc32C(&p[blen-p2_off], p2_off, 0U);
      const ssize_t wret = ts_->WriteTags(&crc32c, p2, 1);
      if (wret<0) return wret;
   }

   return 0;
}

ssize_t XrdOssIntegrityPages::VerifyRangeUnaligned(XrdOssDF *const fd, const void *const buff, const off_t offset, const size_t blen, const Sizes_t &sizes)
{
   const off_t p1 = offset / XrdSys::PageSize;
   const size_t p1_off = offset % XrdSys::PageSize;
   const off_t p2 = (offset+blen) / XrdSys::PageSize;
   const size_t p2_off = (offset+blen) % XrdSys::PageSize;

   const off_t trackinglen = sizes.first;

   // deal with partial first page
   if (p1_off>0 || blen<XrdSys::PageSize)
   {
      size_t bavail = (trackinglen - (XrdSys::PageSize*p1) > XrdSys::PageSize) ? XrdSys::PageSize : (trackinglen - (XrdSys::PageSize*p1));
      uint8_t b[XrdSys::PageSize];
      ssize_t rret = XrdOssIntegrityPages::fullread(fd, b, XrdSys::PageSize*p1, bavail);
      if (rret<0) return rret;
      const size_t bcommon = (bavail - p1_off > blen) ? blen : (bavail-p1_off);
      if (memcmp(buff, &b[p1_off], bcommon))
      {
         return -EIO;
      }
      const uint32_t crc32calc = XrdOucCRC::Calc32C(b, bavail, 0U);
      uint32_t crc32v;
      rret = ts_->ReadTags(&crc32v, p1, 1);
      if (rret<0) return rret;
      if (crc32v != crc32calc)
      {
         return -EDOM;
      }
   }
    
   // first (inclusive) and last (exclusive) full page
   const off_t fp = (p1_off != 0) ? p1+1 : p1;
   const off_t lp = p2;
   if (fp<lp)
   {
      const uint8_t *p = (uint8_t*)buff;
      uint32_t calcbuf[1024],rbuf[1024];
      const size_t bufsz = sizeof(calcbuf)/sizeof(uint32_t);
      size_t toread = lp-fp;
      size_t nread = 0;
      while(toread>0)
      {
         const size_t rcnt = std::min(toread, bufsz);
         XrdOucCRC::Calc32C(&p[(p1_off ? XrdSys::PageSize-p1_off : 0)+XrdSys::PageSize*nread],rcnt*XrdSys::PageSize,calcbuf);
         const ssize_t rret = ts_->ReadTags(rbuf, fp+nread, rcnt);
         if (rret<0) return rret;
         if (memcmp(calcbuf, rbuf, 4*rret))
         {
            return -EDOM;
         }
         toread -= rret;
         nread += rret;
      }
   }

   // last partial page
   if (p2>p1 && p2_off > 0)
   {
      size_t bavail = (trackinglen - (XrdSys::PageSize*p2) > XrdSys::PageSize) ? XrdSys::PageSize : (trackinglen - (XrdSys::PageSize*p2));
      uint8_t b[XrdSys::PageSize];
      ssize_t rret = XrdOssIntegrityPages::fullread(fd, b, XrdSys::PageSize*p2, bavail);
      if (rret<0) return rret;
      const uint8_t *p = (uint8_t*)buff;
      if (memcmp(&p[blen-p2_off], b, p2_off))
      {
         return -EIO;
      }
      const uint32_t crc32calc = XrdOucCRC::Calc32C(b, bavail, 0U);
      uint32_t crc32v;
      rret = ts_->ReadTags(&crc32v, p2, 1);
      if (rret<0) return rret;
      if (crc32v != crc32calc)
      {
         return -EDOM;
      }
   }

   return blen;
}
