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

int XrdOssIntegrityPages::UpdateRangeHoleUntilPage(XrdOssDF *fd, const off_t until, const Sizes_t &sizes)
{
   static const uint8_t bz[XrdSys::PageSize] = {0};
   static const uint32_t crczero = XrdOucCRC::Calc32C(bz, XrdSys::PageSize, 0U);
   static const std::vector<uint32_t> crc32Vec(stsize_, crczero);

   const off_t trackinglen = sizes.first;
   const off_t tracked_page = trackinglen / XrdSys::PageSize;
   if (until <= tracked_page) return 0;

   const size_t tracked_off = trackinglen % XrdSys::PageSize;

   // if last tracked page is before page "until" extend it
   if (tracked_off>0)
   {
      if (fd == NULL) return -EIO;
      uint8_t b[XrdSys::PageSize];
      ssize_t rret = XrdOssIntegrityPages::maxread(fd, b, tracked_page*XrdSys::PageSize, XrdSys::PageSize);
      if (rret < 0) return rret;
      if (static_cast<size_t>(rret) < tracked_off) return -EIO;
      const ssize_t extra = rret - tracked_off;
      if (memcmp(bz, &b[tracked_off], extra)) return -EIO;
      uint32_t prevtag;
      rret = ts_->ReadTags(&prevtag, tracked_page, 1);
      if (rret < 0) return rret;
      uint32_t crc32c = XrdOucCRC::Calc32C(b, tracked_off, 0U);
      if (crc32c != prevtag) return -EDOM;
      crc32c = XrdOucCRC::Calc32C(bz, XrdSys::PageSize - tracked_off, prevtag);
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
      const int ret = UpdateRangeHoleUntilPage(fd, p1, sizes);
      if (ret<0) return ret;
   }

   const size_t p1_off = offset % XrdSys::PageSize;
   const off_t p2 = (offset+blen) / XrdSys::PageSize;
   const size_t p2_off = (offset+blen) % XrdSys::PageSize;

   const off_t tracked_page = trackinglen / XrdSys::PageSize;
   const size_t tracked_off = trackinglen % XrdSys::PageSize;

   bool hasprepage = false;
   uint32_t prepageval;

   // deal with partial first page
   if ( p1_off>0 || blen < static_cast<size_t>(XrdSys::PageSize) )
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
         hasprepage = true;
         prepageval = crc32c;
      }
      else
      {
         // the case (p1 == tracked_page && p1_off == 0 && bavail >= tracked_off) would not need
         // the read-modify-write cycle. However this case is sent to the aligned version of update.
         // Therefore it is not tested and optimised for here.

         // the case (p1 == tracked_page && tracked_off == p1_off) could also be done without the
         // read-modify-write cycle, as the previous tag cal be recalculated with extra data added. However
         // we prefer to fail during the write, if the page content is in error, since a subsequent read
         // will show this failure.

         // read some preexisting data and/or implied zero bytes
         const size_t toread = (p1==tracked_page) ? tracked_off : XrdSys::PageSize;
         if (toread>0)
         {
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
         hasprepage = true;
         prepageval = crc32c;
      }
   }

   // next page (if any)
   const off_t np = hasprepage ? p1+1 : p1;
   // next page starts at buffer offset
   const size_t npoff = hasprepage ? (XrdSys::PageSize - p1_off) : 0;

   // anything in next page?
   if (blen <= npoff)
   {
      // only need to write the first, partial page
      if (hasprepage)
      {
         const ssize_t wret = ts_->WriteTags(&prepageval, p1, 1);
         if (wret<0) return wret;
      }
      return 0;
   }

   const uint8_t *const p = (uint8_t*)buff;

   // see if there will be no old data to account for in the last page
   if (p2_off == 0 || (offset + blen >= static_cast<size_t>(trackinglen)))
   {
      // write prepage, calc and write full pages and last partial page
      const ssize_t aret = apply_sequential_aligned_modify(&p[npoff], np, blen-npoff, NULL, hasprepage, false, prepageval, 0U);
      if (aret<0) return aret;
      return 0;
   }

   // last page contains existing data that has to be read to modify it
   const size_t toread = (p2==tracked_page) ? tracked_off : XrdSys::PageSize;
   uint8_t b[XrdSys::PageSize];
   if (toread>0)
   {
      ssize_t rret = XrdOssIntegrityPages::fullread(fd, b, XrdSys::PageSize * p2, toread);
      if (rret<0) return -EIO;
      const uint32_t crc32c = XrdOucCRC::Calc32C(b, toread, 0U);
      uint32_t crc32v;
      rret = ts_->ReadTags(&crc32v, p2, 1);
      if (rret<0) return rret;
      if (crc32v != crc32c)
      {
         return -EDOM;
      }
   }
   memcpy(b,&p[blen-p2_off],p2_off);
   const uint32_t lastpageval = XrdOucCRC::Calc32C(b, std::max(p2_off,toread), 0U);

   // write prepage, calculate and write full pages, and write precomputed last page
   const ssize_t aret = apply_sequential_aligned_modify(&p[npoff], np, blen-npoff, NULL, hasprepage, true, prepageval, lastpageval);
   if (aret<0) return aret;

   return 0;
}

ssize_t XrdOssIntegrityPages::VerifyRangeUnaligned(XrdOssDF *const fd, const void *const buff, const off_t offset, const size_t blen, const Sizes_t &sizes)
{
   const off_t p1 = offset / XrdSys::PageSize;
   const size_t p1_off = offset % XrdSys::PageSize;
   const off_t p2 = (offset+blen) / XrdSys::PageSize;
   const size_t p2_off = (offset+blen) % XrdSys::PageSize;

   const off_t trackinglen = sizes.first;

   size_t ntagstoread = (p2_off>0) ? p2-p1+1 : p2-p1;
   size_t ntagsbase = p1;
   uint32_t tbuf[stsize_];
   const size_t tbufsz = sizeof(tbuf)/sizeof(uint32_t);

   size_t tcnt = std::min(ntagstoread, tbufsz);
   ssize_t rret = ts_->ReadTags(tbuf, ntagsbase, tcnt);
   if (rret<0) return rret;
   ntagstoread -= tcnt;

   // deal with partial first page
   if ( p1_off>0 || blen < static_cast<size_t>(XrdSys::PageSize) )
   {
      const size_t bavail = std::min(trackinglen - (XrdSys::PageSize*p1), (off_t)XrdSys::PageSize);
      uint8_t b[XrdSys::PageSize];
      rret = XrdOssIntegrityPages::fullread(fd, b, XrdSys::PageSize*p1, bavail);
      if (rret<0) return rret;
      const size_t bcommon = std::min(bavail - p1_off, blen);
      if (memcmp(buff, &b[p1_off], bcommon))
      {
         return -EIO;
      }
      const uint32_t crc32calc = XrdOucCRC::Calc32C(b, bavail, 0U);
      if (tbuf[0] != crc32calc)
      {
         return -EDOM;
      }
   }

   // first (inclusive) and last (exclusive) full page
   const off_t fp = (p1_off != 0) ? p1+1 : p1;
   const off_t lp = p2;
   if (fp<lp)
   {
      const uint8_t *const p = (uint8_t*)buff;
      uint32_t calcbuf[stsize_];
      const size_t cbufsz = sizeof(calcbuf)/sizeof(uint32_t);
      size_t toread = lp-fp;
      size_t nread = 0;
      while(toread>0)
      {
         const size_t ccnt = std::min(toread, cbufsz);
         XrdOucCRC::Calc32C(&p[(p1_off ? XrdSys::PageSize-p1_off : 0)+XrdSys::PageSize*nread],ccnt*XrdSys::PageSize,calcbuf);
         size_t tovalid = ccnt;
         size_t nvalid = 0;
         while(tovalid>0)
         {
            const size_t tidx=fp+nread+nvalid - ntagsbase;
            const size_t nv = std::min(tovalid, tbufsz-tidx);
            if (nv == 0)
            {
               ntagsbase += tbufsz;
               tcnt = std::min(ntagstoread, tbufsz);
               rret = ts_->ReadTags(tbuf, ntagsbase, tcnt);
               if (rret<0) return rret;
               ntagstoread -= tcnt;
               continue;
            }
            if (memcmp(&calcbuf[nvalid], &tbuf[tidx], 4*nv))
            {
               return -EDOM;
            }
            tovalid -= nv;
            nvalid += nv;
         }
         toread -= ccnt;
         nread += ccnt;
      }
   }

   // last partial page
   if (p2>p1 && p2_off > 0)
   {
      const size_t bavail = std::min(trackinglen - (XrdSys::PageSize*p2), (off_t)XrdSys::PageSize);
      uint8_t b[XrdSys::PageSize];
      ssize_t rret = XrdOssIntegrityPages::fullread(fd, b, XrdSys::PageSize*p2, bavail);
      if (rret<0) return rret;
      const uint8_t *const p = (uint8_t*)buff;
      if (memcmp(&p[blen-p2_off], b, p2_off))
      {
         return -EIO;
      }
      const uint32_t crc32calc = XrdOucCRC::Calc32C(b, bavail, 0U);
      size_t tidx = p2 - ntagsbase;
      if (tidx == tbufsz)
      {
         tidx = 0;
         ntagsbase = p2;
         rret = ts_->ReadTags(tbuf, ntagsbase, 1);
         if (rret<0) return rret;
         ntagstoread--;
      }
      if (tbuf[tidx] != crc32calc)
      {
         return -EDOM;
      }
   }

   return blen;
}
