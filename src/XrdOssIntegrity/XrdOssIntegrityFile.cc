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
#include "XrdOuc/XrdOucCRC.hh"
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
XrdSysMutex XrdOssIntegrityFile::pumtx_;
std::unordered_map<std::string, std::shared_ptr<XrdOssIntegrityFile::puMapItem_t> > XrdOssIntegrityFile::pumap_;

//
// expects the caller still has a shared_ptr to the XrdOssIntegrityPages object.
// The underlying object will be closed and the mapentry removed if no others hold a copy.
//
int XrdOssIntegrityFile::pageMapClose(const std::string &tpath)
{
   std::shared_ptr<puMapItem_t> pmi;
   {
      XrdSysMutexHelper lck(pumtx_);
      auto mapidx = pumap_.find(tpath);
      if (mapidx != pumap_.end())
      {
         pmi = mapidx->second;
      }
   }

   int cpret = 0;
   if (pmi)
   {
      pmi->cond.Lock();
      while(pmi->inprogress)
      {
         pmi->cond.Wait();
      }

      if (pmi->pages.use_count() == 2)
      {
         cpret = pmi->pages->Close();
         pmi->pages.reset();
      }

      if (!pmi->pages)
      {
         XrdSysMutexHelper lck(pumtx_);
         auto mapidx = pumap_.find(tpath);
         if (mapidx != pumap_.end())
         {
            if (pmi.use_count() == 2)
            {
               pumap_.erase(mapidx);
            }
         }
      }
      pmi->cond.UnLock();
   }
   return cpret;
}

int XrdOssIntegrityFile::pageMapOpen(const std::string &tpath, const int Oflag, XrdOucEnv &Env, std::shared_ptr<XrdOssIntegrityPages> &retpages)
{
   std::shared_ptr<puMapItem_t> pmi;
   {
      XrdSysMutexHelper lck(pumtx_);
      auto mapidx = pumap_.find(tpath);
      if (mapidx == pumap_.end())
      {
         pmi.reset(new puMapItem_t());
         pumap_.insert(std::make_pair(tpath, pmi));
      }
      else
      {
         pmi = mapidx->second;
      }
   }

   pmi->cond.Lock();
   while(pmi->inprogress)
   {
      pmi->cond.Wait();
   }

   if (pmi->pages)
   {
      retpages = pmi->pages;
      pmi->cond.UnLock();
      return XrdOssOK;
   }
     
   pmi->inprogress = true;
   pmi->cond.UnLock();

   const int oret = createPageUpdater(tpath, Oflag, Env, retpages);

   pmi->cond.Lock();
   pmi->inprogress = false;
   if (oret>=0)
   {
      pmi->pages = retpages;
   }
   else
   {
      XrdSysMutexHelper lck(pumtx_);
      auto mapidx = pumap_.find(tpath);
      if (mapidx != pumap_.end())
      {
         if (pmi.use_count() == 2)
         {
            pumap_.erase(mapidx);
         }
      }
   }
   pmi->cond.Broadcast();
   pmi->cond.UnLock();

   return oret;
}

XrdOssIntegrityFile::~XrdOssIntegrityFile()
{
   if (pages_)
   {
      (void)Close();
   }
}

int XrdOssIntegrityFile::Close(long long *retsz)
{
   if (!pages_)
   {
      return -EBADF;
   }

   // wait for any ongoing aios to finish
   aioWait();

   const int cpret = pageMapClose(tpath_);

   pages_.reset();
   tpath_.clear();
   const int csret = successor_->Close(retsz);
   if (cpret<0) return cpret;
   return csret;
}

int XrdOssIntegrityFile::createPageUpdater(const std::string &tpath, const int Oflag, XrdOucEnv &Env, std::shared_ptr<XrdOssIntegrityPages> &retpages)
{
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
      sprintf(size_str, "%lld", 20+4*((cgSize+XrdSys::PageSize-1)/XrdSys::PageSize));
      newEnv.Put("oss.asize",  size_str);
   }

   // get information about data file
   struct stat sb;
   const int sstat = successor_->Fstat(&sb);
   if (sstat<0)
   {
      return sstat;
   }

   // tag file always opened O_RDWR as the Tagstore/Pages object associated will be shared
   // between any File instances which concurrently access the file
   // (some of which may be RDWR, some RDONLY)
   int tagFlags = O_RDWR;

   // if data file was truncated do same to tag file and let it be recreated for empty data file
   if ((Oflag & O_TRUNC)) tagFlags |= O_TRUNC;

   // If the datafile is new, should try to create tag file.
   // If O_CREAT|O_EXCL was given datafile is new, or if datasize is zero also try to create
   if (((Oflag & O_CREAT) && (Oflag & O_EXCL)) || sb.st_size==0)
   {
      tagFlags |= O_CREAT;
   }

   if ((tagFlags & O_CREAT))
   {
      const int crOpts = XRDOSS_mkpath;
      const int ret = parentOss_->Create(tident_, tpath.c_str(), 0600, newEnv, (tagFlags<<8)|crOpts);
      if (ret != XrdOssOK && ret != -ENOTSUP && ret != -EROFS)
      {
         return ret;
      }
   }

   std::unique_ptr<XrdOssDF> integFile(parentOss_->newFile(tident_));
   std::unique_ptr<XrdOssIntegrityTagstore> ts(new XrdOssIntegrityTagstoreFile(std::move(integFile)));
   std::shared_ptr<XrdOssIntegrityPages> pages(new XrdOssIntegrityPages(std::move(ts), config_->fillFileHole(), config_->allowMissingTags()));

   int puret = pages->Open(tpath.c_str(), sb.st_size, tagFlags, newEnv);
   if (puret<0)
   {
      if (puret == -EROFS && rdonly_)
      {
         // try to open tag file readonly
         puret = pages->Open(tpath.c_str(), sb.st_size, O_RDONLY, newEnv);
      }
   }

   if (puret<0)
   {
      return puret;
   }

   retpages = pages;
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

   tpath_ = std::string(path) + ".xrdt";

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

   const int oret = pageMapOpen(tpath_, Oflag, Env, pages_);
   if (oret<0)
   {
      successor_->Close();
      return oret;
   }

   if (pages_->IsReadOnly() && !rdonly_)
   {
      (void)pageMapClose(tpath_);
      pages_.reset();
      successor_->Close();
      return -EROFS;
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
   pages_->LockTrackinglen(rg, offset, offset+blen, true);

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
   pages_->LockTrackinglen(rg, offset, offset+blen, true);

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
   pages_->LockTrackinglen(rg, start, end, true);

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
   pages_->LockTrackinglen(rg, offset, offset+blen, false);

   int puret = pages_->UpdateRange(successor_, buff, offset, blen, rg);
   if (puret<0)
   {
      rg.ReleaseAll();
      resyncSizes();
      return (ssize_t)puret;
   }
   ssize_t towrite = blen;
   ssize_t bwritten = 0;
   const uint8_t *p = (uint8_t*)buff;
   while(towrite>0)
   {
      ssize_t wret = successor_->Write(&p[bwritten], offset+bwritten, towrite);
      if (wret<0)
      {
         rg.ReleaseAll();
         resyncSizes();
         return wret;
      }
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
   pages_->LockTrackinglen(rg, start, end, false);

   for (int i=0; i<n; i++)
   {
      int ret = pages_->UpdateRange(successor_, writeV[i].data, writeV[i].offset, writeV[i].size, rg);
      if (ret<0)
      {
         rg.ReleaseAll();
         resyncSizes();
         return ret;
      }
   }
   // standard OSS gives -ESPIPE in case of partial write of an element
   int ret = successor_->WriteV(writeV, n);
   if (ret<0)
   {
      rg.ReleaseAll();
      resyncSizes();
   }
   return ret;
}

ssize_t XrdOssIntegrityFile::pgRead(void *buffer, off_t offset, size_t rdlen, uint32_t *csvec, uint64_t opts)
{
   if (!pages_) return -EBADF;

   // this is a tighter restriction that FetchRange requires
   if ((rdlen % XrdSys::PageSize) != 0) return -EINVAL;

   XrdOssIntegrityRangeGuard rg;
   pages_->LockTrackinglen(rg, offset, offset+rdlen, true);

   ssize_t toread = rdlen;
   ssize_t bread = 0;
   uint8_t *const p = (uint8_t*)buffer;
   do
   {
      ssize_t rret = successor_->Read(&p[bread], offset+bread, toread);
      if (rret<0) return rret;
      if (rret==0) break;
      toread -= rret;
      bread += rret;
   } while(toread>0 && (bread % XrdSys::PageSize)!=0);
   if (rdlen == 0) return bread;

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
   if (rdonly_) return -EBADF;
   uint64_t pgopts = opts;

   // do verify before taking locks to allow for faster fail
   if (csvec && (opts & XrdOssDF::Verify))
   {
      uint32_t valcs;
      if (XrdOucCRC::Ver32C((void *)buffer, wrlen, csvec, valcs)>=0)
      {
         return -EDOM;
      }
   }

   XrdOssIntegrityRangeGuard rg;
   pages_->LockTrackinglen(rg, offset, offset+wrlen, false);

   int puret = pages_->StoreRange(successor_, buffer, offset, wrlen, csvec, pgopts, rg);
   if (puret<0) {
      rg.ReleaseAll();
      resyncSizes();
      return (ssize_t)puret;
   }
   ssize_t towrite = wrlen;
   ssize_t bwritten = 0;
   const uint8_t *p = (uint8_t*)buffer;
   do
   {
      ssize_t wret = successor_->Write(&p[bwritten], offset+bwritten, towrite);
      if (wret<0)
      {
         rg.ReleaseAll();
         resyncSizes();
         return wret;
      }
      towrite -= wret;
      bwritten += wret;
   } while(towrite>0);
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
   pages_->LockTrackinglen(rg, flen, LLONG_MAX, false);
   int ret = pages_->truncate(successor_, flen, rg);
   if (ret<0)
   {
      rg.ReleaseAll();
      resyncSizes();
      return ret;
   }
   ret = successor_->Ftruncate(flen);
   if (ret<0)
   {
      rg.ReleaseAll();
      resyncSizes();
   }
   return ret;
}

int XrdOssIntegrityFile::Fstat(struct stat *buff)
{
   if (!pages_) return -EBADF;
   XrdOssIntegrityPages::Sizes_t sizes;
   const int tsret = pages_->TrackedSizesGet(sizes, false);
   const int fsret = successor_->Fstat(buff);
   if (fsret<0) return fsret;
   if (tsret<0) return 0;
   buff->st_size = std::max(sizes.first, sizes.second);
   return 0;
}

int XrdOssIntegrityFile::resyncSizes()
{
   XrdOssIntegrityRangeGuard rg;
   pages_->LockTrackinglen(rg, 0, LLONG_MAX, false);
   struct stat sbuff;
   int ret = successor_->Fstat(&sbuff);
   if (ret<0) return ret;
   pages_->LockResetSizes(sbuff.st_size);
   return 0;
}

void XrdOssIntegrityFile::Flush()
{
   pages_->Flush();
   successor_->Flush();
}

int XrdOssIntegrityFile::VerificationStatus() const
{
   if (!pages_) return 0;
   return pages_->VerificationStatus();
}
