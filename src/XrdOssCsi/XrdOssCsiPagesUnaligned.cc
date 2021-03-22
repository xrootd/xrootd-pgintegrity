/******************************************************************************/
/*                                                                            */
/*           X r d O s s C s i P a g e s U n a l i g n e d . c c              */
/*                                                                            */
/* (C) Copyright 2021 CERN.                                                   */
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

#include "XrdOssCsiTrace.hh"
#include "XrdOssCsiPages.hh"
#include "XrdOssCsiCrcUtils.hh"
#include "XrdOuc/XrdOucCRC.hh"
#include "XrdSys/XrdSysPageSize.hh"

#include <vector>
#include <assert.h>

extern XrdOucTrace  OssCsiTrace;
static XrdOssCsiCrcUtils CrcUtils;

//
// UpdateRangeHoleUntilPage
//
// Used pgWrite/Write (both aligned and unaligned cases) when extending a file
// with implied zeros after then current end of file and the new one.
// fd (data file descriptor pointer) required only when last page in file is partial.
//   current implementation does not use fd in this case, but requires it be set.
//
int XrdOssCsiPages::UpdateRangeHoleUntilPage(XrdOssDF *fd, const off_t until, const Sizes_t &sizes)
{
   EPNAME("UpdateRangeHoleUntilPage");

   static const uint32_t crczero = CrcUtils.crc32c_extendwith_zero(0u, XrdSys::PageSize);
   static const std::vector<uint32_t> crc32Vec(stsize_, crczero);

   const off_t trackinglen = sizes.first;
   const off_t tracked_page = trackinglen / XrdSys::PageSize;
   if (until <= tracked_page) return 0;

   const size_t tracked_off = trackinglen % XrdSys::PageSize;

   // if last tracked page is before page "until" extend it
   if (tracked_off>0)
   {
      if (fd == NULL)
      {
         TRACE(Warn, "Unexpected partially filled last page " << fn_);
         return -EDOM;
      }
      // assume tag for last page is correct; if not it can be discovered during a later read
      uint32_t prevtag;
      const ssize_t rret = ts_->ReadTags(&prevtag, tracked_page, 1);
      if (rret < 0)
      {
         TRACE(Warn, TagsReadError(tracked_page, 1, rret));
         return rret;
      }
      const uint32_t crc32c = CrcUtils.crc32c_extendwith_zero(prevtag, XrdSys::PageSize - tracked_off);
      const ssize_t wret = ts_->WriteTags(&crc32c, tracked_page, 1);
      if (wret < 0)
      {
         TRACE(Warn, TagsWriteError(tracked_page, 1, wret, " (prev)"));
         return wret;
      }
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
      if (wret<0)
      {
         TRACE(Warn, TagsWriteError(firstEmpty+nwritten, nw, wret, " (new)"));
         return wret;
      }
      towrite -= wret;
      nwritten += wret;
   }

   return 0;
}

// UpdateRangeUnaligned
// 
// Used by Write for various cases with mis-alignment that need checksum recalculation. See StoreRangeUnaligned for list of conditions.
//
int XrdOssCsiPages::UpdateRangeUnaligned(XrdOssDF *const fd, const void *buff, const off_t offset, const size_t blen, const Sizes_t &sizes)
{
   return StoreRangeUnaligned(fd, buff, offset, blen, sizes, NULL);
}

//
// used by StoreRangeUnaligned when the supplied data does not cover the whole of the first corresponding page in the file
//
int XrdOssCsiPages::StoreRangeUnaligned_preblock(XrdOssDF *const fd, const void *const buff, const size_t blen,
                                                 const off_t offset, const off_t trackinglen,
                                                 const uint32_t *const csvec, uint32_t &prepageval)
{
   EPNAME("StoreRangeUnaligned_preblock");
   const off_t p1 = offset / XrdSys::PageSize;
   const size_t p1_off = offset % XrdSys::PageSize;

   const off_t tracked_page = trackinglen / XrdSys::PageSize;
   const size_t tracked_off = trackinglen % XrdSys::PageSize;

   if (p1 > tracked_page)
   {
      // the start of will have a number of implied zero bytes
      uint32_t crc32c = CrcUtils.crc32c_extendwith_zero(0u, p1_off);
      if (csvec)
      {
         crc32c = CrcUtils.crc32c_combine(crc32c, csvec[0], blen);
      }
      else
      {
         crc32c = XrdOucCRC::Calc32C(buff, blen, crc32c);
      }
      prepageval = crc32c;
      return 0;
   }

   // we're appending, or appending within the last page after a gap of zeros
   if (p1 == tracked_page && p1_off >= tracked_off)
   {
      // appending: with or without some implied zeros.
      // can recalc crc with new data without re-reading existing partial block's data
      uint32_t crc32v = 0;
      if (tracked_off > 0)
      {
         const ssize_t rret = ts_->ReadTags(&crc32v, p1, 1);
         if (rret<0)
         {
            TRACE(Warn, TagsReadError(p1, 1, rret, " (append)"));
            return rret;
         }
      }
      const size_t nz = p1_off - tracked_off;
      uint32_t crc32c = crc32v;
      crc32c = CrcUtils.crc32c_extendwith_zero(crc32c, nz);
      if (csvec)
      {
         crc32c = CrcUtils.crc32c_combine(crc32c, csvec[0], blen);
      }
      else
      {
         crc32c = XrdOucCRC::Calc32C(buff, blen, crc32c);
      }
      prepageval = crc32c;
      return 0;
   }

   const size_t bavail = (p1==tracked_page) ? tracked_off : XrdSys::PageSize;

   // assert we're overwriting some (or all) of the previous data (other case was above)
   assert(p1_off < bavail);

   // case p1_off==0 && blen>=bavail is either handled by aligned case (p1==tracked_page)
   // or not sent to preblock, so will need to read some preexisting data
   assert(p1_off !=0 || blen<bavail);
   uint8_t b[XrdSys::PageSize];

   ssize_t rret = XrdOssCsiPages::fullread(fd, b, XrdSys::PageSize * p1, bavail);
   if (rret<0)
   {
      TRACE(Warn, PageReadError(bavail, XrdSys::PageSize * p1, rret));
      return rret;
   }
   uint32_t crc32c = XrdOucCRC::Calc32C(b, bavail, 0U);
   uint32_t crc32v;
   rret = ts_->ReadTags(&crc32v, p1, 1);
   if (rret<0)
   {
      TRACE(Warn, TagsReadError(p1, 1, rret, " (overwrite)"));
      return rret;
   }
   // this may be an implicit verification (e.g. pgWrite may return EDOM without Verify requested)
   // however, it's not clear if there is a meaningful way to crc a mismatching page during a partial update
   if (crc32v != crc32c)
   {
      TRACE(Warn, CRCMismatchError(bavail, XrdSys::PageSize*p1, crc32c, crc32v));
      return -EDOM;
   }

   crc32c = XrdOucCRC::Calc32C(b, p1_off, 0U);
   if (csvec)
   {
      crc32c = CrcUtils.crc32c_combine(crc32c, csvec[0], blen);
   }
   else
   {
      crc32c = XrdOucCRC::Calc32C(buff, blen, crc32c);
   }
   if (p1_off+blen < bavail)
   {
      const uint32_t cl = XrdOucCRC::Calc32C(&b[p1_off+blen], bavail-p1_off-blen, 0U);
      crc32c = CrcUtils.crc32c_combine(crc32c, cl, bavail-p1_off-blen);
   }
   prepageval = crc32c;
   return 0;
}

//
// used by StoreRangeUnaligned when the end of supplied data is not page aligned
// and is before the end of file
//
int XrdOssCsiPages::StoreRangeUnaligned_postblock(XrdOssDF *const fd, const void *const buff, const size_t blen,
                                                  const off_t offset, const off_t trackinglen,
                                                  const uint32_t *const csvec, uint32_t &lastpageval)
{
   EPNAME("StoreRangeUnaligned_postblock");

   const uint8_t *const p = (uint8_t*)buff;
   const off_t p2 = (offset+blen) / XrdSys::PageSize;
   const size_t p2_off = (offset+blen) % XrdSys::PageSize;

   const off_t tracked_page = trackinglen / XrdSys::PageSize;
   const size_t tracked_off = trackinglen % XrdSys::PageSize;

   // we should not be called in this case
   assert(p2_off != 0);

   // how much existing data this last (p2) page
   const size_t bavail = (p2==tracked_page) ? tracked_off : XrdSys::PageSize;

   // how much of that data will not be overwritten
   const size_t bremain = (p2_off < bavail) ? bavail-p2_off : 0;

   uint8_t b[XrdSys::PageSize];
   if (bremain>0)
   {
      // if any data will remain will need to use it to calculate the crc of the new p2 page.
      // read and verify it now.
      ssize_t rret = XrdOssCsiPages::fullread(fd, b, XrdSys::PageSize * p2, bavail);
      if (rret<0)
      {
         TRACE(Warn, PageReadError(bavail, XrdSys::PageSize * p2, rret));
         return rret;
      }
      const uint32_t crc32c = XrdOucCRC::Calc32C(b, bavail, 0U);
      uint32_t crc32v;
      rret = ts_->ReadTags(&crc32v, p2, 1);
      if (rret<0)
      {
         TRACE(Warn, TagsReadError(p2, 1, rret));
         return rret;
      }
      // this may be an implicit verification (e.g. pgWrite may return EDOM without Verify requested)
      // however, it's not clear if there is a meaningful way to crc a mismatching page during a partial update
      if (crc32v != crc32c)
      {
         TRACE(Warn, CRCMismatchError(bavail, XrdSys::PageSize*p2, crc32c, crc32v));
         return -EDOM;
      }
   }
   uint32_t crc32c = 0;
   if (csvec)
   {
      crc32c = csvec[(blen-1)/XrdSys::PageSize];
   }
   else
   {
      crc32c = XrdOucCRC::Calc32C(&p[blen-p2_off], p2_off, 0U);
   }
   if (bremain>0)
   {
      const uint32_t cl = XrdOucCRC::Calc32C(&b[p2_off], bremain, 0U);
      crc32c = CrcUtils.crc32c_combine(crc32c, cl, bremain);
   }
   lastpageval = crc32c;
   return 0;
}

//
// StoreRangeUnaligned
// 
// Used by pgWrite or Write (via UpdateRangeUnaligned) where the start of this update is not page aligned within the file
// OR where the end of this update is before the end of the file and is not page aligned
// OR where end of the file is not page aligned and this update starts after it
// i.e. where checksums of last current page of file, or the first or last pages after writing this buffer will need to be recomputed
//
int XrdOssCsiPages::StoreRangeUnaligned(XrdOssDF *const fd, const void *buff, const off_t offset, const size_t blen, const Sizes_t &sizes, const uint32_t *const csvec)
{
   EPNAME("StoreRangeUnaligned");
   const off_t p1 = offset / XrdSys::PageSize;

   const off_t trackinglen = sizes.first;
   if (offset > trackinglen)
   {
      const int ret = UpdateRangeHoleUntilPage(fd, p1, sizes);
      if (ret<0)
      {
         TRACE(Warn, "Error updating tags for holes, error=" << ret);
         return ret;
      }
   }

   const size_t p1_off = offset % XrdSys::PageSize;
   const size_t p2_off = (offset+blen) % XrdSys::PageSize;

   bool hasprepage = false;
   uint32_t prepageval;

   // deal with partial first page
   if ( p1_off>0 || blen < static_cast<size_t>(XrdSys::PageSize) )
   {
      const size_t bavail = (XrdSys::PageSize-p1_off > blen) ? blen : (XrdSys::PageSize-p1_off);
      const int ret = StoreRangeUnaligned_preblock(fd, buff, bavail, offset, trackinglen, csvec, prepageval);
      if (ret<0)
      {
         return ret;
      }
      hasprepage = true;
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
         if (wret<0)
         {
            TRACE(Warn, TagsWriteError(p1, 1, wret));
            return wret;
         }
      }
      return 0;
   }

   const uint8_t *const p = (uint8_t*)buff;
   const uint32_t *csp = csvec;
   if (csp && hasprepage) csp++;

   // see if there will be no old data to account for in the last page
   if (p2_off == 0 || (offset + blen >= static_cast<size_t>(trackinglen)))
   {
      // write any precomputed prepage, then write full pages and last partial page (computing or using supplied csvec)
      const ssize_t aret = apply_sequential_aligned_modify(&p[npoff], np, blen-npoff, csp, hasprepage, false, prepageval, 0U);
      if (aret<0)
      {
         TRACE(Warn, "Error updating tags, error=" << aret);
         return aret;
      }
      return 0;
   }

   // last page contains existing data that has to be read to modify it

   uint32_t lastpageval;
   const int ret = StoreRangeUnaligned_postblock(fd, &p[npoff], blen-npoff, offset+npoff, trackinglen, csp, lastpageval);
   if (ret<0)
   {
      return ret;
   }

   // write any precomputed prepage, then write full pages (computing or using supplied csvec) and finally write precomputed last page
   const ssize_t aret = apply_sequential_aligned_modify(&p[npoff], np, blen-npoff, csp, hasprepage, true, prepageval, lastpageval);
   if (aret<0)
   {
      TRACE(Warn, "Error updating tags, error=" << aret);
      return aret;
   }

   return 0;
}

// VerifyRangeUnaligned
// 
// Used by Read for various cases with mis-alignment. See FetchRangeUnaligned for list of conditions.
//
ssize_t XrdOssCsiPages::VerifyRangeUnaligned(XrdOssDF *const fd, const void *const buff, const off_t offset, const size_t blen, const Sizes_t &sizes)
{
  return FetchRangeUnaligned(fd, buff, offset, blen, sizes, NULL, XrdOssDF::Verify);
}

//
// used by FetchRangeUnaligned when only part of the data in the first page is needed, or the page is short
//
int XrdOssCsiPages::FetchRangeUnaligned_preblock(XrdOssDF *const fd, const void *const buff, const off_t offset, const size_t blen,
                                                 const off_t trackinglen, uint32_t *const tbuf, uint32_t *const csvec, const uint64_t opts)
{
   EPNAME("FetchRangeUnaligned_preblock");

   const off_t p1 = offset / XrdSys::PageSize;
   const size_t p1_off = offset % XrdSys::PageSize;

   // bavail is length of data in this page
   const size_t bavail = std::min(trackinglen - (XrdSys::PageSize*p1), (off_t)XrdSys::PageSize);

   // bcommon is length of data in this page that user wants
   const size_t bcommon = std::min(bavail - p1_off, blen);

   uint8_t b[XrdSys::PageSize];
   const uint8_t *ub = (uint8_t*)buff;
   if (bavail>bcommon)
   {
      // will need more data to either verify or return crc of the user's data
      // (in case of no verify and no csvec FetchRange() returns early)
      const ssize_t rret = XrdOssCsiPages::fullread(fd, b, XrdSys::PageSize*p1, bavail);
      if (rret<0)
      {
         TRACE(Warn, PageReadError(bavail, XrdSys::PageSize*p1, rret));
         return rret;
      }
      // if we're going to verify, make sure we just read the same overlapping data as that in the user's buffer
      if ((opts & XrdOssDF::Verify))
      {
         if (memcmp(buff, &b[p1_off], bcommon))
         {
            size_t badoff;
            for(badoff=0;badoff<bcommon;badoff++) { if (((uint8_t*)buff)[badoff] != b[p1_off+badoff]) break; }
            TRACE(Warn, ByteMismatchError(bavail, XrdSys::PageSize*p1+p1_off+badoff, ((uint8_t*)buff)[badoff], b[p1_off+badoff]));
            return -EDOM;
         }
      }
      ub = b;
   }
   // verify; based on whole block, or user's buffer (if it contains the whole block)
   if ((opts & XrdOssDF::Verify))
   {
      const uint32_t crc32calc = XrdOucCRC::Calc32C(ub, bavail, 0U);
      if (tbuf[0] != crc32calc)
      {
         TRACE(Warn, CRCMismatchError(bavail, XrdSys::PageSize*p1, crc32calc, tbuf[0]));
         return -EDOM;
      }
   }

   // if we're returning csvec values and this first block
   // needs adjustment because user requested a subset..
   if (bavail>bcommon && csvec)
   {
     // make sure csvec[0] corresponds to only the data the user wanted, not whole page.
     // if we have already verified the page + common part matches user's, take checksum of common.
     // (Use local copy of page, perhaps less chance of accidental concurrent modification than buffer)
     // Otherwise base on saved checksum.
     if ((opts & XrdOssDF::Verify))
     {
        csvec[0] = XrdOucCRC::Calc32C(&b[p1_off], bcommon, 0u);
     }
     else
     {
       // calculate expected user checksum based on block's recorded checksum, adjusting
       // for data not included in user's request. If either the returned data or the
       // data not included in the user's request are corrupt the returned checksum and
       // returned data will (probably) mismatch.

       // remove block data before p1_off from checksum
       uint32_t crc32c = XrdOucCRC::Calc32C(b, p1_off, 0u);
       csvec[0] = CrcUtils.crc32c_split2(csvec[0], crc32c, bavail-p1_off);

       // remove block data after p1_off+bcommon upto bavail
       crc32c = XrdOucCRC::Calc32C(&b[p1_off+bcommon], bavail-p1_off-bcommon, 0u);
       csvec[0] = CrcUtils.crc32c_split1(csvec[0], crc32c, bavail-p1_off-bcommon);
     }
   }
   return 0;
}

//
// used by FetchRangeUnaligned when only part of a page of data is needed from the last page
//
int XrdOssCsiPages::FetchRangeUnaligned_postblock(XrdOssDF *const fd, const void *const buff, const off_t offset, const size_t blen,
                                                 const off_t trackinglen, uint32_t *const tbuf, uint32_t *const csvec, const size_t tidx, const uint64_t opts)
{
   EPNAME("FetchRangeUnaligned_postblock");

   const off_t p2 = (offset+blen) / XrdSys::PageSize;
   const size_t p2_off = (offset+blen) % XrdSys::PageSize;

   // length of data in last (p2) page
   const size_t bavail = std::min(trackinglen - (XrdSys::PageSize*p2), (off_t)XrdSys::PageSize);

   // how much of that data is not being returned
   const size_t bremain = (p2_off < bavail) ? bavail-p2_off : 0;
   uint8_t b[XrdSys::PageSize];
   const uint8_t *ub = &((uint8_t*)buff)[blen-p2_off];
   if (bremain>0)
   {
      const ssize_t rret = XrdOssCsiPages::fullread(fd, b, XrdSys::PageSize*p2, bavail);
      if (rret<0)
      {
         TRACE(Warn, PageReadError(bavail, XrdSys::PageSize*p2, rret));
         return rret;
      }
      // if we're verifying make sure overlapping part of data just read matches user's buffer
      if ((opts & XrdOssDF::Verify))
      {
         const uint8_t *const p = (uint8_t*)buff;
         if (memcmp(&p[blen-p2_off], b, p2_off))
         {
            size_t badoff;
            for(badoff=0;badoff<p2_off;badoff++) { if (p[blen-p2_off+badoff] != b[badoff]) break; }
            TRACE(Warn, ByteMismatchError(bavail, XrdSys::PageSize*p2+badoff, p[blen-p2_off+badoff], b[badoff]));
            return -EDOM;
         }
      }
      ub = b;
   }
   if ((opts & XrdOssDF::Verify))
   {
      const uint32_t crc32calc = XrdOucCRC::Calc32C(ub, bavail, 0U);
      if (tbuf[tidx] != crc32calc)
      {
         TRACE(Warn, CRCMismatchError(bavail, XrdSys::PageSize*p2, crc32calc, tbuf[tidx]));
         return -EDOM;
      }
   }
   // if we're returning csvec and user only request part of page
   // adjust the crc
   if (csvec && bremain>0)
   {
      if ((opts & XrdOssDF::Verify))
      {
         // verified; calculate crc based on common part of page.
         csvec[tidx] = XrdOucCRC::Calc32C(b, p2_off, 0u);
      }
      else
      {
         // recalculate crc based on recorded checksum and adjusting for part of data not returned.
         // If either the returned data or the data not included in the user's request are
         // corrupt the returned checksum and returned data will (probably) mismatch.

         const uint32_t crc32c = XrdOucCRC::Calc32C(&b[p2_off], bremain, 0u);
         csvec[tidx] = CrcUtils.crc32c_split1(csvec[tidx], crc32c, bremain);
      }
   }

   return 0;
}

//
// FetchRangeUnaligned
//
// Used by pgRead/Read when reading a range not starting at a page boundary within the file
// OR when the length is not a multiple of the page-size and the read finishes not at the end of file.
//
ssize_t XrdOssCsiPages::FetchRangeUnaligned(XrdOssDF *const fd, const void *const buff, const off_t offset, const size_t blen, const Sizes_t &sizes, uint32_t *const csvec, const uint64_t opts)
{
   EPNAME("FetchRangeUnaligned");

   const off_t p1 = offset / XrdSys::PageSize;
   const size_t p1_off = offset % XrdSys::PageSize;
   const off_t p2 = (offset+blen) / XrdSys::PageSize;
   const size_t p2_off = (offset+blen) % XrdSys::PageSize;

   const off_t trackinglen = sizes.first;

   size_t ntagstoread = (p2_off>0) ? p2-p1+1 : p2-p1;
   size_t ntagsbase = p1;
   uint32_t tbufint[stsize_], *tbuf=0;
   size_t tbufsz = 0;
   if (!csvec)
   {
     tbuf = tbufint;
     tbufsz = sizeof(tbufint)/sizeof(uint32_t);
   }
   else
   {
     tbuf = csvec;
     tbufsz = ntagstoread;
   }

   size_t tcnt = std::min(ntagstoread, tbufsz);
   ssize_t rret = ts_->ReadTags(tbuf, ntagsbase, tcnt);
   if (rret<0)
   {
      TRACE(Warn, TagsReadError(ntagsbase, tcnt, rret, " (first)"));
      return rret;
   }
   ntagstoread -= tcnt;

   // deal with partial first page
   if ( p1_off>0 || blen < static_cast<size_t>(XrdSys::PageSize) )
   {
      const int ret = FetchRangeUnaligned_preblock(fd, buff, offset, blen, trackinglen, tbuf, csvec, opts);
      if (ret<0)
      {
         return ret;
      }
   }

   // first (inclusive) and last (exclusive) full page
   const off_t fp = (p1_off != 0) ? p1+1 : p1;
   const off_t lp = p2;

   // verify full pages if wanted
   if (fp<lp && (opts & XrdOssDF::Verify))
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
               assert(csvec == NULL);
               ntagsbase += tbufsz;
               tcnt = std::min(ntagstoread, tbufsz);
               rret = ts_->ReadTags(tbuf, ntagsbase, tcnt);
               if (rret<0)
               {
                  TRACE(Warn, TagsReadError(ntagsbase, tcnt, rret, " (mid)"));
                  return rret;
               }
               ntagstoread -= tcnt;
               continue;
            }
            if (memcmp(&calcbuf[nvalid], &tbuf[tidx], 4*nv))
            {
               size_t badpg;
               for(badpg=0;badpg<nv;badpg++) { if (memcmp(&calcbuf[nvalid+badpg], &tbuf[tidx+badpg],4)) break; }
               TRACE(Warn, CRCMismatchError(XrdSys::PageSize,
                                            XrdSys::PageSize*(ntagsbase+tidx+badpg),
                                            calcbuf[nvalid+badpg], tbuf[tidx+badpg]));
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
      // make sure we have last tag;
      // (should already have all of them if we're returning them in csvec)
      size_t tidx = p2 - ntagsbase;
      if (tidx >= tbufsz)
      {
         assert(csvec == NULL);
         tidx = 0;
         ntagsbase = p2;
         rret = ts_->ReadTags(tbuf, ntagsbase, 1);
         if (rret<0)
         {
            TRACE(Warn, TagsReadError(ntagsbase, 1, rret, " (last)"));
            return rret;
         }
         ntagstoread = 0;
      }

      const int ret = FetchRangeUnaligned_postblock(fd, buff, offset, blen, trackinglen, tbuf, csvec, tidx, opts);
      if (ret<0)
      {
         return ret;
      }
   }

   return blen;
}
