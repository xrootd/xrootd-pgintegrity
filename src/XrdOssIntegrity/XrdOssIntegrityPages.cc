/******************************************************************************/
/*                                                                            */
/*              X r d O s s I n t e g r i t y P a g e s . c c                 */
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

#include <assert.h>

XrdOssIntegrityPages::Sizes_t XrdOssIntegrityPages::TrackedSizesGet(const bool forupdate)
{
   XrdSysCondVarHelper lck(&tscond_);
   while (tsforupdate_)
   {
      tscond_.Wait();
   }
   off_t tagsize =  ts_->GetTrackedTagSize();
   off_t datasize =  ts_->GetTrackedDataSize();
   if (forupdate)
   {
      tsforupdate_ = true;
   }
   return std::make_pair(tagsize,datasize);
}

int XrdOssIntegrityPages::LockSetTrackedSize(const off_t sz)
{
   XrdSysCondVarHelper lck(&tscond_);
   return ts_->SetTrackedSize(sz);
}

int XrdOssIntegrityPages::LockResetSizes(const off_t sz)
{
   XrdSysCondVarHelper lck(&tscond_);
   return ts_->ResetSizes(sz);
}

int XrdOssIntegrityPages::LockTruncateSize(const off_t sz, const bool datatoo)
{
   XrdSysCondVarHelper lck(&tscond_);
   return ts_->Truncate(sz,datatoo);
}

void XrdOssIntegrityPages::TrackedSizeRelease()
{
   XrdSysCondVarHelper lck(&tscond_);
   assert(tsforupdate_ == true);

   tsforupdate_ = false;
   tscond_.Broadcast();
}

int XrdOssIntegrityPages::UpdateRange(XrdOssDF *const fd, const void *buff, const off_t offset, const size_t blen, XrdOssIntegrityRangeGuard &rg)
{
   if (offset<0)
   {
      return -EINVAL;
   }

   if (blen == 0)
   {
     return 0;
   }

   const Sizes_t sizes = rg.getTrackinglens();

   const off_t trackinglen = sizes.first;
   if (offset+blen > static_cast<size_t>(trackinglen))
   {
      LockSetTrackedSize(offset+blen);
      rg.unlockTrackinglen();
   }

   int ret;
   if ((offset % XrdSys::PageSize) != 0 ||
       (offset+blen < static_cast<size_t>(trackinglen) && (blen % XrdSys::PageSize) != 0) ||
       ((trackinglen % XrdSys::PageSize) !=0 && offset > trackinglen))
   {
      ret = UpdateRangeUnaligned(fd, buff, offset, blen, sizes);
   }
   else
   {
      ret = UpdateRangeAligned(buff, offset, blen, sizes);
   }

   return ret;
}

ssize_t XrdOssIntegrityPages::VerifyRange(XrdOssDF *const fd, const void *buff, const off_t offset, const size_t blen, XrdOssIntegrityRangeGuard &rg)
{
   if (offset<0)
   {
      return -EINVAL;
   }

   const Sizes_t sizes = rg.getTrackinglens();
   const off_t trackinglen = sizes.first;

   if (offset >= trackinglen)
   {
      return 0;
   }

   if (blen == 0)
   {
      // if offset if before the tracked len we should not be requested to verify zero bytes:
      // the file may have been truncated
      return -EIO;
   }

   size_t rlen = blen;
   if (offset+blen > static_cast<size_t>(trackinglen))
   {
      rlen = trackinglen - offset;
   }

   ssize_t vret;

   if ((offset % XrdSys::PageSize) != 0 || (offset+rlen != static_cast<size_t>(trackinglen) && (rlen % XrdSys::PageSize) != 0))
   {
      vret = VerifyRangeUnaligned(fd, buff, offset, rlen, sizes);
   }
   else
   {
      vret = VerifyRangeAligned(buff, offset, rlen, sizes);
   }

   return vret;
}

ssize_t XrdOssIntegrityPages::apply_sequential_aligned_modify(
   const void *const buff, const off_t startp, const size_t nbytes, uint32_t *csvec,
   const bool preblockset, const bool lastblockset, const uint32_t cspre, const uint32_t cslast)
{
   if (csvec && (preblockset || lastblockset))
   {
      return -EINVAL;
   }
   if (lastblockset && (nbytes % XrdSys::PageSize)==0)
   {
      return -EINVAL;
   }
   if (preblockset && startp==0)
   {
      return -EINVAL;
   }

   uint32_t calcbuf[stsize_];
   const size_t calcbufsz = sizeof(calcbuf)/sizeof(uint32_t);
   const uint8_t *const p = (uint8_t*)buff;

   bool dopre = preblockset;
   const off_t sp = preblockset ? startp-1 : startp;

   size_t blktowrite = ((nbytes+XrdSys::PageSize-1)/XrdSys::PageSize) + (preblockset ? 1 : 0);
   size_t nblkwritten = 0;
   size_t calcbytot = 0;
   while(blktowrite>0)
   {
      size_t blkwcnt = blktowrite;
      if (!csvec)
      {
         size_t cidx = 0;
         size_t calcbycnt = nbytes - calcbytot;
         if (nblkwritten == 0 && dopre)
         {
            calcbycnt = std::min(calcbycnt, (calcbufsz-1)*XrdSys::PageSize);
            blkwcnt = (calcbycnt+XrdSys::PageSize-1)/XrdSys::PageSize;
            calcbuf[cidx] = cspre;
            cidx++;
            blkwcnt++;
            dopre = false;
         }
         else
         {
            calcbycnt = std::min(calcbycnt, calcbufsz*XrdSys::PageSize);
            blkwcnt = (calcbycnt+XrdSys::PageSize-1)/XrdSys::PageSize;
         }
         if ((calcbycnt % XrdSys::PageSize)!=0 && lastblockset)
         {
            const size_t x = calcbycnt / XrdSys::PageSize;
            calcbycnt = XrdSys::PageSize * x;
            calcbuf[cidx + x] = cslast;
         }
         XrdOucCRC::Calc32C(&p[calcbytot], calcbycnt, &calcbuf[cidx]);
         calcbytot += calcbycnt;
      }
      const ssize_t wret = ts_->WriteTags(csvec ? &csvec[nblkwritten] : calcbuf, sp+nblkwritten, blkwcnt);
      if (wret<0) return wret;
      blktowrite -= blkwcnt;
      nblkwritten += blkwcnt;
   }
   return nblkwritten;
}

ssize_t XrdOssIntegrityPages::FetchRangeAligned(const void *const buff, const off_t offset, const size_t blen, const Sizes_t &sizes, uint32_t *const csvec, const uint64_t opts)
{
   if (csvec == NULL && !(opts & XrdOssDF::Verify))
   {
      // if the crc values are not wanted nor checks against data, then
      // there's nothing more to do here
      return blen;
   }

   uint32_t rdvec[stsize_],vrbuf[stsize_];

   const off_t p1 = offset / XrdSys::PageSize;
   const off_t p2 = (offset+blen) / XrdSys::PageSize;
   const size_t p2_off = (offset+blen) % XrdSys::PageSize;
   const size_t nfull = p2-p1;

   uint32_t *rdbuf;
   size_t rdbufsz;
   if (csvec == NULL)
   {
      // use fixed sized stack buffer
      rdbuf = rdvec;
      rdbufsz = sizeof(rdvec)/sizeof(uint32_t);
   }
   else
   {
      // use supplied buffer, assumed to be large enough
      rdbuf = csvec;
      rdbufsz = (p2_off==0) ? nfull : (nfull+1);
   }

   // always use stack based, fixed sized buffer for verify
   const size_t vrbufsz = sizeof(vrbuf)/sizeof(uint32_t);

   // pointer to data
   const uint8_t *const p = (uint8_t*)buff;
  
   // process full pages + any partial page
   size_t toread = (p2_off>0) ? nfull+1 : nfull;
   size_t nread = 0;
   while(toread>0)
   {
      const size_t rcnt = std::min(toread, rdbufsz-(nread%rdbufsz));
      const ssize_t rret = ts_->ReadTags(&rdbuf[nread%rdbufsz], p1+nread, rcnt);
      if (rret<0) return rret;
      if ((opts & XrdOssDF::Verify))
      {
         size_t toverif = rcnt;
         size_t nverif = 0;
         while(toverif>0)
         {
            const size_t vcnt = std::min(toverif, vrbufsz);
            const size_t databytes = (nread+nverif+vcnt <= nfull) ? (vcnt*XrdSys::PageSize) : ((vcnt-1)*XrdSys::PageSize+p2_off);
            XrdOucCRC::Calc32C(&p[XrdSys::PageSize*(nread+nverif)],databytes,vrbuf);
            if (memcmp(vrbuf, &rdbuf[(nread+nverif)%rdbufsz], 4*vcnt))
            {
               return -EDOM;
            }
            toverif -= vcnt;
            nverif += vcnt;
         }
      }
      toread -= rcnt;
      nread += rcnt;
   }

   return blen;
}

ssize_t XrdOssIntegrityPages::VerifyRangeAligned(const void *const buff, const off_t offset, const size_t blen, const Sizes_t &sizes)
{
   return FetchRangeAligned(buff,offset,blen,sizes,NULL,XrdOssDF::Verify);
}

int XrdOssIntegrityPages::StoreRangeAligned(const void *const buff, const off_t offset, const size_t blen, const Sizes_t &sizes, uint32_t *csvec)
{
   // if csvec given store those values
   // if no csvec then calculate against data and store

   const off_t p1 = offset / XrdSys::PageSize;
   const off_t trackinglen = sizes.first;

   if (offset > trackinglen)
   {
      const int ret = UpdateRangeHoleUntilPage(NULL, p1, sizes);
      if (ret<0) return ret;
   }

   const ssize_t aret = apply_sequential_aligned_modify(buff, p1, blen, csvec, false, false, 0U, 0U);
   if (aret<0) return aret;

   return 0;
}

int XrdOssIntegrityPages::UpdateRangeAligned(const void *const buff, const off_t offset, const size_t blen, const Sizes_t &sizes)
{
   return StoreRangeAligned(buff, offset, blen, sizes, NULL);
}

//
// LockTrackinglen: obtain current tracking counts and lock the following as necessary:
//                  tracking counts and file byte range [offset, offend)
//
// offset - byte offset to apply lock
// offend - end of range byte (excluding byte at end)
// rdonly - will be a read-only operation
//
void XrdOssIntegrityPages::LockTrackinglen(XrdOssIntegrityRangeGuard &rg, const off_t offset, const off_t offend, const bool rdonly)
{
   // in case of empty range the tracking len is not copied
   if (offset == offend) return;

   {
      XrdSysMutexHelper lck(rangeaddmtx_);
      const Sizes_t sizes = TrackedSizesGet(!rdonly);
      // tag tracking size: always less than or equal to actual tracked size
      const off_t trackinglen = sizes.first;

      const off_t p1 = (offset>trackinglen ? trackinglen : offset) / XrdSys::PageSize;
      bool unlock = false;
      if (!rdonly && offend <= trackinglen)
      {
         unlock = true;
      }

      off_t p2 = offend / XrdSys::PageSize;
      const size_t p2_off = offend % XrdSys::PageSize;

      // range is exclusive
      if (p2_off ==0) p2--;

      ranges_.AddRange(p1, p2, rg, rdonly);

      if (unlock)
      {
         TrackedSizeRelease();
      }
      rg.SetTrackingInfo(this, sizes, (!rdonly && !unlock));
   }

   rg.Wait();
}

int XrdOssIntegrityPages::truncate(XrdOssDF *const fd, const off_t len, XrdOssIntegrityRangeGuard &rg)
{
   if (len<0) return -EINVAL;

   const Sizes_t sizes = rg.getTrackinglens();

   const off_t trackinglen = sizes.first;
   const off_t p_until = len / XrdSys::PageSize;
   const size_t p_off = len % XrdSys::PageSize;

   if (len>trackinglen)
   {
      int ret = UpdateRangeHoleUntilPage(fd,p_until,sizes);
      if (ret<0) return ret;
   }

   if (len != trackinglen && p_off != 0)
   {
      const off_t tracked_page = trackinglen / XrdSys::PageSize;
      const size_t tracked_off = trackinglen % XrdSys::PageSize;
      size_t toread = tracked_off;
      if (len>trackinglen)
      {
         if (p_until != tracked_page) toread = 0;
      }
      else
      {
         if (p_until != tracked_page) toread = XrdSys::PageSize;
      }
      uint8_t b[XrdSys::PageSize];
      if (toread>0)
      {
         ssize_t rret = XrdOssIntegrityPages::fullread(fd, b, p_until*XrdSys::PageSize, toread);
         if (rret<0) return rret;
         const uint32_t crc32c = XrdOucCRC::Calc32C(b, toread, 0U);
         uint32_t crc32v;
         rret = ts_->ReadTags(&crc32v, p_until, 1);
         if (rret<0) return rret;
         if (crc32v != crc32c)
         {
            return -EDOM;
         }
      }
      if (p_off > toread)
      {
         memset(&b[toread],0,p_off-toread);
      }
      const uint32_t crc32c = XrdOucCRC::Calc32C(b, p_off, 0U);
      const ssize_t wret = ts_->WriteTags(&crc32c, p_until, 1);
      if (wret < 0) return wret;
   }

   LockTruncateSize(len,true);
   rg.unlockTrackinglen();
   return 0;
}

ssize_t XrdOssIntegrityPages::FetchRange(
   XrdOssDF *const fd, const void *buff, const off_t offset, const size_t blen,
   uint32_t *csvec, const uint64_t opts, XrdOssIntegrityRangeGuard &rg)
{
   if (offset<0)
   {
      return -EINVAL;
   }

   // these methods require page aligned offset.
   if ((offset & XrdSys::PageMask)) return -EINVAL;

   const Sizes_t sizes = rg.getTrackinglens();
   const off_t trackinglen = sizes.first;

   if (offset >= trackinglen)
   {
      return 0;
   }

   if (blen == 0)
   {
      // if offset if before the tracked len we should not be requested to verify zero bytes:
      // the file may have been truncated
      return -EIO;
   }

   size_t rlen = blen;
   if (offset+blen > static_cast<size_t>(trackinglen))
   {
      rlen = trackinglen - offset;
   }

   // rlen must be multiple of pagesize or short due to eof
   if ((rlen % XrdSys::PageSize) != 0 && offset+rlen != static_cast<size_t>(trackinglen)) return -EINVAL;

   return FetchRangeAligned(buff,offset,blen,sizes,csvec,opts);
}

int XrdOssIntegrityPages::StoreRange(XrdOssDF *const fd, const void *buff, const off_t offset, const size_t blen, uint32_t *csvec, XrdOssIntegrityRangeGuard &rg)
{
   if (offset<0)
   {
      return -EINVAL;
   }

   if (blen == 0)
   {
      return 0;
   }

   // these methods require page aligned offset.
   if ((offset & XrdSys::PageMask)) return -EINVAL;

   const Sizes_t sizes = rg.getTrackinglens();
   const off_t trackinglen = sizes.first;

   // blen must be multiple of pagesize or short for page at eof
   if ((blen % XrdSys::PageSize) != 0 && offset+blen < static_cast<size_t>(trackinglen)) return -EINVAL;

   // if the last page is partially filled can not write past it
   if ((trackinglen % XrdSys::PageSize) !=0 && offset > trackinglen) return -EINVAL;

   if (offset+blen > static_cast<size_t>(trackinglen))
   {
      LockSetTrackedSize(offset+blen);
      rg.unlockTrackinglen();
   }

   return StoreRangeAligned(buff,offset,blen,sizes,csvec);
}
