/******************************************************************************/
/*                                                                            */
/*              X r d O s s I n t e g r i t y F i l e . c c                   */
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

#include "XrdOssIntegrity.hh"
#include "XrdOssIntegrityTagstoreFile.hh"
#include "XrdOssIntegrityPages.hh"
#include "XrdOssIntegrityRanges.hh"
#include "XrdOuc/XrdOucEnv.hh"
#include "XrdOuc/XrdOuca2x.hh"
#include "XrdSfs/XrdSfsAio.hh"
#include "XrdSys/XrdSysPageSize.hh"
#include "XrdVersion.hh"
#include "XrdSfs/XrdSfsAio.hh"

#include <string>
#include <algorithm>

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <limits.h>

// storage for class members
std::mutex XrdOssIntegrityFile::pumtx_;
std::condition_variable XrdOssIntegrityFile::pucond_;
std::unordered_map<std::string, std::shared_ptr<XrdOssIntegrityPages> > XrdOssIntegrityFile::pumap_;

XrdOssIntegrityFile::~XrdOssIntegrityFile()
{
   Close();
}

int XrdOssIntegrityFile::Close(long long *retsz)
{
   pages_.reset();
   int cpret = 0;
   {
      std::lock_guard<std::mutex> guard(pumtx_);
      auto mapidx = pumap_.find(ipath_);
      if (mapidx != pumap_.end())
      {
         if (mapidx->second.use_count() == 1)
         {
            cpret = mapidx->second->Close();
            pumap_.erase(ipath_);
         }
      }
   }
   ipath_.clear();
   const int csret = successor_->Close(retsz);
   if (cpret<0) return cpret;
   return csret;
}

off_t XrdOssIntegrityFile::getMmap(void **addr)
{
   if (addr) *addr = 0;
   return 0;
}

int XrdOssIntegrityFile::createPageUpdater(const char *path, const int Oflag, XrdOucEnv &Env)
{
   const std::string ipath_ = std::string(path) + ".xrdt";

   XrdOucEnv newEnv;
   newEnv.Put("oss.cgroup", config_->xrdtSpaceName().c_str());

   char *tmp;
   long long cgSize=0;
   if ((tmp = Env.Get("oss.asize")) && XrdOuca2x::a2sz(config_->err(),"invalid asize",tmp,&cgSize,0))
   {
      cgSize=0;
   }

   if (cgSize>0)
   {
      char size_str[32];
      sprintf(size_str, "%lld", 16+4*((cgSize+XrdSys::PageSize-1)/XrdSys::PageSize));
      newEnv.Put("oss.asize",  size_str);
   }

   // tag file always opened O_RDWR as the Tagstore/Pages object associated will be shared
   // between any File instances which concurrently access the file
   // (some of which may be RDWR, some RDONLY)
   int tagFlags = O_RDWR|O_CREAT;
   if ((Oflag & O_EXCL) || (Oflag & O_TRUNC)) tagFlags |= O_TRUNC;

   const int crOpts = XRDOSS_mkpath;
   const int ret = parentOss_->Create(tident_, ipath_.c_str(), 0600, newEnv, (tagFlags<<8)|crOpts);
   if (ret != XrdOssOK && ret != -ENOTSUP)
   {
      return ret;
   }

   std::unique_ptr<XrdOssDF> integFile(parentOss_->newFile(tident_));
   std::unique_ptr<XrdOssIntegrityTagstore> ts(new XrdOssIntegrityTagstoreFile(std::move(integFile)));
   std::shared_ptr<XrdOssIntegrityPages> pages(new XrdOssIntegrityPages(std::move(ts), config_->fillFileHole()));

   struct stat sb;
   const int sstat = successor_->Fstat(&sb);
   if (sstat<0)
   {
      return sstat;
   }

   const int puret = pages->Open(ipath_.c_str(), sb.st_size, tagFlags, newEnv);
   if (puret<0)
   {
      return puret;
   }

   pages_ = pages;
   return XrdOssOK;
}

int XrdOssIntegrityFile::Open(const char *path, const int Oflag, const mode_t Mode, XrdOucEnv &Env)
{
   char cxid[4];

   if (pages_)
   {
      // already open
      return -EINVAL;
   }

   if (!path)
   {
      return -EINVAL;
   }
   if (XrdOssIntegrity::isTagFile(path))
   {
      if ((Oflag & O_CREAT)) return -EPERM;
      return -ENOENT;
   }

   ipath_ = std::string(path) + ".xrdt";

   int dflags = Oflag;
   if ((dflags & O_ACCMODE) == O_WRONLY)
   {
      // for non-aligned writes it may be needed to do read-modify-write
      dflags &= ~O_ACCMODE;
      dflags |= O_RDWR;
   }

   rdonly_ = true;
   if ((dflags & O_ACCMODE) != O_RDONLY)
   {
      rdonly_ = false;
   }

   const int dataret = successor_->Open(path, dflags, Mode, Env);
   if (dataret != XrdOssOK)
   {
      return dataret;
   }

   if (successor_->isCompressed(cxid)>0)
   {
      successor_->Close();
      return -ENOTSUP;
   }

   {
      std::unique_lock<std::mutex> lk(pumtx_);
      while(1)
      {
         auto mapidx = pumap_.find(ipath_);
         if (mapidx == pumap_.end())
         {
            pumap_.insert(std::make_pair(ipath_, std::shared_ptr<XrdOssIntegrityPages>()));
            break;
         }
         if (mapidx->second)
         {
            if ((Oflag & O_EXCL))
            {
               // data file was just created, it
               // should not have had a tag file in use
               successor_->Close();
               return -EIO;
            }
            pages_ = mapidx->second;
            break;
         }
         pucond_.wait(lk);
      }
   }

   if (pages_)
   {
      return XrdOssOK;
   }

   int oret = createPageUpdater(path, Oflag, Env);

   if (oret<0)
   {
      successor_->Close();
   }

   {
      std::lock_guard<std::mutex> guard(pumtx_);
      auto mapidx = pumap_.find(ipath_);
      if (oret<0)
      {
         pumap_.erase(mapidx);
      }
      else
      {
         mapidx->second = pages_;
      }
      pucond_.notify_all();
   }

   if (oret<0)
   {
      return oret;
   }

   return XrdOssOK;
}

ssize_t XrdOssIntegrityFile::Read(off_t offset, size_t blen)
{
   return successor_->Read(offset, blen);
}

ssize_t XrdOssIntegrityFile::Read(void *buff, off_t offset, size_t blen)
{
   if (!pages_) return -EBADF;

   XrdOssIntegrityRangeGuard rg;
   pages_->LockRange(rg, offset, blen, true);

   const ssize_t bread = successor_->Read(buff, offset, blen);
   if (bread<0 || blen==0) return bread;

   const ssize_t puret = pages_->VerifyRange(successor_, buff, offset, bread, rg);
   if (puret<0) return puret;
   if (puret != bread)
   {
      return -EIO;
   }
   return bread;
}

ssize_t XrdOssIntegrityFile::ReadRaw(void *buff, off_t offset, size_t blen)
{
   if (!pages_) return -EBADF;

   XrdOssIntegrityRangeGuard rg;
   pages_->LockRange(rg, offset, blen, true);

   const ssize_t bread = successor_->ReadRaw(buff, offset, blen);
   if (bread<0 || blen==0) return bread;

   const ssize_t puret = pages_->VerifyRange(successor_, buff, offset, bread, rg);
   if (puret<0) return puret;
   if (puret != bread)
   {
      return -EIO;
   }
   return bread;
}

ssize_t XrdOssIntegrityFile::ReadV(XrdOucIOVec *readV, int n)
{
   if (!pages_) return -EBADF;
   if (n==0) return 0;

   XrdOssIntegrityRangeGuard rg;
   off_t start = readV[0].offset;
   off_t end = start + (off_t)readV[0].size;
   for(int i=1; i<n; i++)
   {
      const off_t p1 = readV[i].offset;
      const off_t p2 = p1 + (off_t)readV[i].size;
      if (p1<start) start = p1;
      if (p2>end) end = p2;
   }
   pages_->LockRange(rg, start, end-start+1, true);

   // standard OSS gives -ESPIPE in case of partial read of an element
   ssize_t rret = successor_->ReadV(readV, n);
   if (rret<0) return rret;
   for (int i=0; i<n; i++)
   {
      if (readV[i].size == 0) continue;
      ssize_t puret = pages_->VerifyRange(successor_, readV[i].data, readV[i].offset, readV[i].size, rg);
      if (puret<0) return puret;
      if (puret != readV[i].size)
      {
         return -EIO;
      }
   }
   return rret;
}

ssize_t XrdOssIntegrityFile::Write(const void *buff, off_t offset, size_t blen)
{
   if (!pages_) return -EBADF;
   if (rdonly_) return -EBADF;

   XrdOssIntegrityRangeGuard rg;
   pages_->LockRange(rg, offset, blen, false);

   int puret = pages_->UpdateRange(successor_, buff, offset, blen, rg);
   if (puret<0) return (ssize_t)puret;
   ssize_t towrite = blen;
   ssize_t bwritten = 0;
   const uint8_t *p = (uint8_t*)buff;
   while(towrite>0)
   {
      ssize_t wret = successor_->Write(&p[bwritten], offset+bwritten, towrite);
      if (wret<0) return wret;
      towrite -= wret;
      bwritten += wret;
   }
   return bwritten;
}

ssize_t XrdOssIntegrityFile::WriteV(XrdOucIOVec *writeV, int n)
{
   if (!pages_) return -EBADF;
   if (rdonly_) return -EBADF;
   if (n==0) return 0;

   XrdOssIntegrityRangeGuard rg;
   off_t start = writeV[0].offset;
   off_t end = start + (off_t)writeV[0].size;
   for(int i=1; i<n; i++)
   {
      const off_t p1 = writeV[i].offset;
      const off_t p2 = p1 + (off_t)writeV[i].size;
      if (p1<start) start = p1;
      if (p2>end) end = p2;
   }
   pages_->LockRange(rg, start, end-start+1, false);

   for (int i=0; i<n; i++)
   {
      int ret = pages_->UpdateRange(successor_, writeV[i].data, writeV[i].offset, writeV[i].size, rg);
      if (ret<0) return ret;
   }
   // standard OSS gives -ESPIPE in case of partial write of an element
   return successor_->WriteV(writeV, n);
}

ssize_t XrdOssIntegrityFile::pgRead (void *buffer, off_t offset, size_t rdlen, uint32_t *csvec, uint64_t opts)
{
   if (!pages_) return -EBADF;

   // this is a tighter restriction that FetchRange requires
   if ((rdlen % XrdSys::PageSize) != 0) return -EINVAL;

   XrdOssIntegrityRangeGuard rg;
   pages_->LockRange(rg, offset, rdlen, true);

   ssize_t toread = rdlen;
   ssize_t bread = 0;
   uint8_t *const p = (uint8_t*)buffer;
   while(toread>0)
   {
      ssize_t rret = successor_->Read(&p[bread], offset+bread, toread);
      if (rret<0) return rret;
      if (rret==0) break;
      toread -= rret;
      bread += rret;
   }
   ssize_t puret = pages_->FetchRange(successor_, buffer, offset, bread, csvec, opts, rg);
   if (puret<0) return puret;
   if (puret != bread)
   {
      return -EIO;
   }
   return bread;
}

ssize_t XrdOssIntegrityFile::pgWrite(void *buffer, off_t offset, size_t wrlen, uint32_t *csvec, uint64_t opts)
{
   if (!pages_) return -EBADF;

   XrdOssIntegrityRangeGuard rg;
   pages_->LockRange(rg, offset, wrlen, false);

   int puret = pages_->StoreRange(successor_, buffer, offset, wrlen, csvec, opts, rg);
   if (puret<0) return (ssize_t)puret;
   ssize_t towrite = wrlen;
   ssize_t bwritten = 0;
   const uint8_t *p = (uint8_t*)buffer;
   while(towrite>0)
   {
      ssize_t wret = successor_->Write(&p[bwritten], offset+bwritten, towrite);
      if (wret<0) return wret;
      towrite -= wret;
      bwritten += wret;
   }
   return bwritten;
}

int XrdOssIntegrityFile::Fsync()
{
   if (!pages_) return -EBADF;

   const int psret = pages_->Fsync();
   const int ssret = successor_->Fsync();
   if (psret<0) return psret;
   return ssret;
}

int XrdOssIntegrityFile::Ftruncate(unsigned long long flen)
{
   if (!pages_) return -EBADF;
   if (rdonly_) return -EBADF;

   XrdOssIntegrityRangeGuard rg;
   pages_->LockRange(rg, flen, LLONG_MAX-flen, false);
   int ret = pages_->truncate(successor_, flen, rg);
   if (ret<0) return ret;
   return successor_->Ftruncate(flen);
}

int XrdOssIntegrityFile::Fstat(struct stat *buff)
{
   if (!pages_) return -EBADF;
   XrdOssIntegrityPages::Sizes_t sizes = pages_->TrackedSizesGet(false);
   int ret = successor_->Fstat(buff);
   if (ret<0) return ret;
   buff->st_size = std::max(sizes.first, sizes.second);
   return 0;
}
