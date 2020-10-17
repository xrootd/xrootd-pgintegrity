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
#include "XrdOssIntegrityTrace.hh"
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
#include <assert.h>

extern XrdSysError  OssIntegrityEroute;
extern XrdOucTrace  OssIntegrityTrace;

// storage for class members
XrdSysMutex XrdOssIntegrityFile::pumtx_;
std::unordered_map<std::string, std::shared_ptr<XrdOssIntegrityFile::puMapItem_t> > XrdOssIntegrityFile::pumap_;

//
// If no others hold a pointer to Pages object, close it and remoe the pagemap info object.
//
int XrdOssIntegrityFile::pageMapClose()
{
   if (!pmi_) return -EBADF;
   bool doclose = false;

   XrdSysMutexHelper lck(pmi_->mtx);
   {
      XrdSysMutexHelper lck2(pumtx_);
      pmi_->busy--;
      if (pmi_->busy == 0)
      {
         if (!pmi_->unlinked)
         {
            auto mapidx = pumap_.find(pmi_->tpath);
            assert(mapidx != pumap_.end());
            pumap_.erase(mapidx);
         }
         doclose = true;
      }
   }

   int cpret = 0;
   if (doclose)
   {
      if (pmi_->pages)
      {
         cpret = pmi_->pages->Close();
         pmi_->pages.reset();
      }
   }

   pmi_->mtx.UnLock();
   pmi_.reset();

   return cpret;
}

int XrdOssIntegrityFile::pageAndFileOpen(const char *fn, const int dflags, const int Oflag, const mode_t Mode, XrdOucEnv &Env)
{
   if (pmi_) return -EBADF;

   {
      std::string tpath = std::string(fn) + ".xrdt";
      XrdSysMutexHelper lck(pumtx_);
      auto mapidx = pumap_.find(tpath);
      if (mapidx == pumap_.end())
      {
         pmi_.reset(new puMapItem_t());
         pmi_->dpath = std::string(fn);
         pmi_->tpath = tpath;
         pumap_.insert(std::make_pair(tpath, pmi_));
      }
      else
      {
         pmi_ = mapidx->second;
         assert(!pmi_->unlinked);
      }
      pmi_->busy++;
   }

   XrdSysMutexHelper lck(pmi_->mtx);
   if (pmi_->unlinked)
   {
     pmi_->busy--;
     // filename replaced since check, try again
     lck.UnLock();
     pmi_.reset();
     return pageAndFileOpen(fn, dflags, Oflag, Mode, Env);
   }

   if ((dflags & O_TRUNC) && pmi_->pages)
   {
      // asked to truncate but the file is already open: becomes difficult to sync.
      // So, return error
      return -ETXTBSY;
   }

   const int dataret = successor_->Open(pmi_->dpath.c_str(), dflags, Mode, Env);
   int pageret = XrdOssOK;
   if (dataret == XrdOssOK)
   {
      if (pmi_->pages)
      {
         return XrdOssOK;
      }
     
      pageret = createPageUpdater(Oflag, Env);
      if (pageret == XrdOssOK)
      {
         return XrdOssOK;
      }
   }

   // failed to open the datafile or create the page object.
   // close datafile if needed
   if (dataret == XrdOssOK)
   {
      (void) successor_->Close();
   }

   XrdSysMutexHelper lck2(pumtx_);
   pmi_->busy--;
   auto mapidx = pumap_.find(pmi_->tpath);
   if (pmi_->busy == 0)
   {
      assert(mapidx != pumap_.end());
      pumap_.erase(mapidx);
   }
   pmi_->mtx.UnLock();
   pmi_.reset();

   return (dataret != XrdOssOK) ? dataret : pageret;
}

XrdOssIntegrityFile::~XrdOssIntegrityFile()
{
   if (pmi_)
   {
      (void)Close();
   }
}

int XrdOssIntegrityFile::Close(long long *retsz)
{
   if (!pmi_)
   {
      return -EBADF;
   }

   // wait for any ongoing aios to finish
   aioWait();

   const int cpret = pageMapClose();

   const int csret = successor_->Close(retsz);
   if (cpret<0) return cpret;
   return csret;
}

int XrdOssIntegrityFile::createPageUpdater(const int Oflag, XrdOucEnv &Env)
{
   XrdOucEnv newEnv;
   newEnv.Put("oss.cgroup", config_.xrdtSpaceName().c_str());

   char *tmp;
   long long cgSize=0;
   if ((tmp = Env.Get("oss.asize")) && XrdOuca2x::a2sz(OssIntegrityEroute,"invalid asize",tmp,&cgSize,0))
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
      const int ret = parentOss_->Create(tident_, pmi_->tpath.c_str(), 0600, newEnv, (tagFlags<<8)|crOpts);
      if (ret != XrdOssOK && ret != -ENOTSUP && ret != -EROFS)
      {
         return ret;
      }
   }

   std::unique_ptr<XrdOssDF> integFile(parentOss_->newFile(tident_));
   std::unique_ptr<XrdOssIntegrityTagstore> ts(new XrdOssIntegrityTagstoreFile(pmi_->dpath, std::move(integFile), tident_));
   std::unique_ptr<XrdOssIntegrityPages> pages(new XrdOssIntegrityPages(pmi_->dpath, std::move(ts), config_.fillFileHole(), config_.allowMissingTags(), tident_));

   int puret = pages->Open(pmi_->tpath.c_str(), sb.st_size, tagFlags, newEnv);
   if (puret<0)
   {
      if (puret == -EROFS && rdonly_)
      {
         // try to open tag file readonly
         puret = pages->Open(pmi_->tpath.c_str(), sb.st_size, O_RDONLY, newEnv);
      }
   }

   if (puret<0)
   {
      return puret;
   }

   pmi_->pages = std::move(pages);
   return XrdOssOK;
}

int XrdOssIntegrityFile::Open(const char *path, const int Oflag, const mode_t Mode, XrdOucEnv &Env)
{
   char cxid[4];

   if (pmi_)
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

   const int oret = pageAndFileOpen(path, dflags, Oflag, Mode, Env);
   if (oret<0)
   {
      return oret;
   }

   if (successor_->isCompressed(cxid)>0)
   {
      (void)Close();
      return -ENOTSUP;
   }

   if (Pages()->IsReadOnly() && !rdonly_)
   {
      (void)Close();
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
   if (!pmi_) return -EBADF;

   XrdOssIntegrityRangeGuard rg;
   Pages()->LockTrackinglen(rg, offset, offset+blen, true);

   const ssize_t bread = successor_->Read(buff, offset, blen);
   if (bread<0 || blen==0) return bread;

   const ssize_t puret = Pages()->VerifyRange(successor_, buff, offset, bread, rg);
   if (puret<0) return puret;
   if (puret != bread)
   {
      return -EIO;
   }
   return bread;
}

ssize_t XrdOssIntegrityFile::ReadRaw(void *buff, off_t offset, size_t blen)
{
   if (!pmi_) return -EBADF;

   XrdOssIntegrityRangeGuard rg;
   Pages()->LockTrackinglen(rg, offset, offset+blen, true);

   const ssize_t bread = successor_->ReadRaw(buff, offset, blen);
   if (bread<0 || blen==0) return bread;

   const ssize_t puret = Pages()->VerifyRange(successor_, buff, offset, bread, rg);
   if (puret<0) return puret;
   if (puret != bread)
   {
      return -EIO;
   }
   return bread;
}

ssize_t XrdOssIntegrityFile::ReadV(XrdOucIOVec *readV, int n)
{
   if (!pmi_) return -EBADF;
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
   Pages()->LockTrackinglen(rg, start, end, true);

   // standard OSS gives -ESPIPE in case of partial read of an element
   ssize_t rret = successor_->ReadV(readV, n);
   if (rret<0) return rret;
   for (int i=0; i<n; i++)
   {
      if (readV[i].size == 0) continue;
      ssize_t puret = Pages()->VerifyRange(successor_, readV[i].data, readV[i].offset, readV[i].size, rg);
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
   if (!pmi_) return -EBADF;
   if (rdonly_) return -EBADF;

   XrdOssIntegrityRangeGuard rg;
   Pages()->LockTrackinglen(rg, offset, offset+blen, false);

   int puret = Pages()->UpdateRange(successor_, buff, offset, blen, rg);
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
   if (!pmi_) return -EBADF;
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
   Pages()->LockTrackinglen(rg, start, end, false);

   for (int i=0; i<n; i++)
   {
      int ret = Pages()->UpdateRange(successor_, writeV[i].data, writeV[i].offset, writeV[i].size, rg);
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
   if (!pmi_) return -EBADF;

   // this is a tighter restriction that FetchRange requires
   if ((rdlen % XrdSys::PageSize) != 0) return -EINVAL;

   XrdOssIntegrityRangeGuard rg;
   Pages()->LockTrackinglen(rg, offset, offset+rdlen, true);

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

   ssize_t puret = Pages()->FetchRange(successor_, buffer, offset, bread, csvec, opts, rg);
   if (puret<0) return puret;
   if (puret != bread)
   {
      return -EIO;
   }
   return bread;
}

ssize_t XrdOssIntegrityFile::pgWrite(void *buffer, off_t offset, size_t wrlen, uint32_t *csvec, uint64_t opts)
{
   if (!pmi_) return -EBADF;
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
   Pages()->LockTrackinglen(rg, offset, offset+wrlen, false);

   int puret = Pages()->StoreRange(successor_, buffer, offset, wrlen, csvec, pgopts, rg);
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
   if (!pmi_) return -EBADF;

   const int psret = Pages()->Fsync();
   const int ssret = successor_->Fsync();
   if (psret<0) return psret;
   return ssret;
}

int XrdOssIntegrityFile::Ftruncate(unsigned long long flen)
{
   if (!pmi_) return -EBADF;
   if (rdonly_) return -EBADF;

   XrdOssIntegrityRangeGuard rg;
   Pages()->LockTrackinglen(rg, flen, LLONG_MAX, false);
   int ret = Pages()->truncate(successor_, flen, rg);
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
   if (!pmi_) return -EBADF;
   XrdOssIntegrityPages::Sizes_t sizes;
   const int tsret = Pages()->TrackedSizesGet(sizes, false);
   const int fsret = successor_->Fstat(buff);
   if (fsret<0) return fsret;
   if (tsret<0) return 0;
   buff->st_size = std::max(sizes.first, sizes.second);
   return 0;
}

int XrdOssIntegrityFile::resyncSizes()
{
   XrdOssIntegrityRangeGuard rg;
   Pages()->LockTrackinglen(rg, 0, LLONG_MAX, false);
   struct stat sbuff;
   int ret = successor_->Fstat(&sbuff);
   if (ret<0) return ret;
   Pages()->LockResetSizes(sbuff.st_size);
   return 0;
}

void XrdOssIntegrityFile::Flush()
{
   if (!pmi_) return;

   Pages()->Flush();
   successor_->Flush();
}

int XrdOssIntegrityFile::VerificationStatus()
{
   if (!pmi_) return 0;
   return Pages()->VerificationStatus();
}

int XrdOssIntegrityFile::FRename(const char *newname, XrdOucEnv *old_env, XrdOucEnv *new_env)
{
   if (!pmi_) return -EBADF;
   if (rdonly_) return -EBADF;

   std::string inew(newname);
   inew += ".xrdt";

   // in case target name exists get its mapinfo
   std::shared_ptr<puMapItem_t> newpmi;
   {
      XrdSysMutexHelper lck(pumtx_);
      auto mapidx = pumap_.find(inew);
      if (mapidx != pumap_.end())
      {
         newpmi = mapidx->second;
         assert(!newpmi->unlinked);
      }
   }

   // rename to self, do nothing
   if (newpmi == pmi_) return 0;

   XrdSysMutexHelper lck(newpmi ? &newpmi->mtx : NULL);
   if (newpmi && newpmi->unlinked)
   {
     // something overwrote the target file since we checked
     lck.UnLock();
     return FRename(newname, old_env, new_env);
   }

   XrdSysMutexHelper lck2(pmi_->mtx);
   if (pmi_->unlinked)
   {
      // if we've already been unlinked, can not rename
      return -ENOENT;
   }

   std::string oldtag = pmi_->tpath;
   std::string olddata = pmi_->dpath;

   const int sret = parentOss_->Rename(pmi_->dpath.c_str(), newname, old_env, new_env);
   if (sret<0) return sret;

   const int iret = parentOss_->Rename(pmi_->tpath.c_str(), inew.c_str(), old_env, new_env);
   if (iret<0)
   {
      if (iret == -ENOENT)
      {
         // if there is no tag file for oldfile, but newfile existed previously with a tag file,
         // we don't want to be left with the previously existing tagfile
         (void) parentOss_->Unlink(inew.c_str(), 0, new_env);
      }
      else
      {
         (void) parentOss_->Rename(newname, pmi_->dpath.c_str(), new_env, old_env);
         return iret;
      }
   }

   if (newpmi)
   {
      newpmi->unlinked = true;
   }

   {
      XrdSysMutexHelper lck3(pumtx_);
      auto mapidx_new = pumap_.find(inew);
      if (mapidx_new != pumap_.end()) pumap_.erase(mapidx_new);

      auto mapidx = pumap_.find(pmi_->tpath);
      assert(mapidx != pumap_.end());

      pumap_.erase(mapidx);
      pumap_.insert(std::make_pair(inew, pmi_));
      pmi_->dpath = newname;
      pmi_->tpath = inew;
   }

   return XrdOssOK;
}

int XrdOssIntegrityFile::FUnlink(int Opts, XrdOucEnv *eP)
{
   if (!pmi_) return -EBADF;
   if (rdonly_) return -EBADF;

   int utret = 0;

   XrdSysMutexHelper lck(pmi_->mtx);
   if (!pmi_->unlinked)
   {
      const int uret = parentOss_->Unlink(pmi_->dpath.c_str(), Opts, eP);
      if (uret != XrdOssOK) return uret;

      utret = parentOss_->Unlink(pmi_->tpath.c_str(), Opts, eP);

      XrdSysMutexHelper lck2(pumtx_);
      auto mapidx = pumap_.find(pmi_->tpath);
      assert(mapidx != pumap_.end());
      pumap_.erase(mapidx);
   }

   pmi_->unlinked = true;

   return (utret == -ENOENT) ? 0 : utret;
}
