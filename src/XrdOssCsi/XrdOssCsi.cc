/******************************************************************************/
/*                                                                            */
/*                        X r d O s s C s i . c c                             */
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

#include "XrdOssCsiTrace.hh"
#include "XrdOssCsi.hh"
#include "XrdOssCsiConfig.hh"
#include "XrdOuc/XrdOucEnv.hh"
#include "XrdVersion.hh"

#include <string>
#include <memory>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <limits.h>
#include <assert.h>

XrdVERSIONINFO(XrdOssAddStorageSystem2,XrdOssCsi)

XrdSysError OssCsiEroute(0, "osscsi_");
XrdOucTrace OssCsiTrace(&OssCsiEroute);

XrdScheduler *XrdOssCsi::Sched_;

// skip tag files in directory listing
int XrdOssCsiDir::Readdir(char *buff, int blen)
{
   int ret;
   do
   {
      ret = successor_->Readdir(buff, blen);
      if (ret<0) return ret;
   } while(XrdOssCsi::isTagFile(buff));
   return ret;
}

int XrdOssCsi::Init(XrdSysLogger *lP, const char *cP, const char *params, XrdOucEnv *env)
{
   OssCsiEroute.logger(lP);

   int cret = config_.Init(OssCsiEroute, cP, params, env);
   if (cret != XrdOssOK)
   {
      return cret;
   }

   if ( ! env ||
        ! (Sched_ = (XrdScheduler*) env->GetPtr("XrdScheduler*")))
   {
      Sched_ = new XrdScheduler;
      Sched_->Start();
   }

   return XrdOssOK;
}

int XrdOssCsi::Unlink(const char *path, int Opts, XrdOucEnv *eP)
{
   if (isTagFile(path)) return -ENOENT;

   // get mapinfo entries for file
   std::shared_ptr<XrdOssCsiFile::puMapItem_t> pmi;
   {
      const std::string tpath = std::string(path) + ".xrdt";
      XrdOssCsiFile::mapTake(tpath, pmi);
   }

   int utret = 0;

   XrdSysMutexHelper lck(pmi->mtx);
   pmi->dpath = path;
   if (!pmi->unlinked)
   {
      const int uret = successor_->Unlink(path, Opts, eP);
      if (uret != XrdOssOK)
      {
         XrdOssCsiFile::mapReleaseLocked(pmi,&lck);
         return uret;
      }

      utret = successor_->Unlink(pmi->tpath.c_str(), Opts, eP);
   }

   pmi->unlinked = true;
   XrdOssCsiFile::mapReleaseLocked(pmi,&lck);

   return (utret == -ENOENT) ? 0 : utret;
}

int XrdOssCsi::Rename(const char *oldname, const char *newname,
                      XrdOucEnv  *old_env, XrdOucEnv  *new_env)
{
   if (isTagFile(oldname) || isTagFile(newname)) return -ENOENT;

   std::string inew(newname);
   inew += ".xrdt";

   std::string iold(oldname);
   iold += ".xrdt";

   // get mapinfo entries for both old and possibly existing newfile
   std::shared_ptr<XrdOssCsiFile::puMapItem_t> newpmi,pmi;
   XrdOssCsiFile::mapTake(inew, newpmi);
   XrdOssCsiFile::mapTake(iold   , pmi);

   // rename to self, do nothing
   if (newpmi == pmi)
   {
      XrdOssCsiFile::mapReleaseLocked(pmi);
      XrdOssCsiFile::mapReleaseLocked(newpmi);
      return 0;
   }

   // take in consistent order
   XrdSysMutexHelper lck(NULL), lck2(NULL);
   if (newpmi > pmi)
   {
     lck.Lock(&newpmi->mtx);
     lck2.Lock(&pmi->mtx);
   }
   else
   {
     lck2.Lock(&pmi->mtx);
     lck.Lock(&newpmi->mtx);
   }

   if (pmi->unlinked || newpmi->unlinked)
   {
      // something overwrote the source or target file since we checked
      XrdOssCsiFile::mapReleaseLocked(pmi,&lck2);
      XrdOssCsiFile::mapReleaseLocked(newpmi,&lck);
      return Rename(oldname, newname, old_env, new_env);
   }

   const int sret = successor_->Rename(oldname, newname, old_env, new_env);
   if (sret<0)
   {
      XrdOssCsiFile::mapReleaseLocked(pmi,&lck2);
      XrdOssCsiFile::mapReleaseLocked(newpmi,&lck);
      return sret;
   }

   const int iret = successor_->Rename(iold.c_str(), inew.c_str(), old_env, new_env);
   if (iret<0)
   {
      if (iret == -ENOENT)
      {
         // if there is no tag file for oldfile, but newfile existed previously with a tag file,
         // we don't want to be left with the previously existing tagfile
         (void) successor_->Unlink(inew.c_str(), 0, new_env);
      }
      else
      {
         (void) successor_->Rename(newname, oldname, new_env, old_env);
         XrdOssCsiFile::mapReleaseLocked(pmi,&lck2);
         XrdOssCsiFile::mapReleaseLocked(newpmi,&lck);
         return iret;
      }
   }

   if (newpmi)
   {
      newpmi->unlinked = true;
   }

   {
      XrdSysMutexHelper lck3(XrdOssCsiFile::pumtx_);
      auto mapidx_new = XrdOssCsiFile::pumap_.find(inew);
      if (mapidx_new != XrdOssCsiFile::pumap_.end()) XrdOssCsiFile::pumap_.erase(mapidx_new);

      auto mapidx = XrdOssCsiFile::pumap_.find(iold);
      assert(mapidx != XrdOssCsiFile::pumap_.end());

      XrdOssCsiFile::pumap_.erase(mapidx);
      XrdOssCsiFile::pumap_.insert(std::make_pair(inew, pmi));
      pmi->dpath = newname;
      pmi->tpath = inew;
   }
         
   XrdOssCsiFile::mapReleaseLocked(pmi,&lck2);
   XrdOssCsiFile::mapReleaseLocked(newpmi,&lck);

   return XrdOssOK;
}

int XrdOssCsi::Truncate(const char *path, unsigned long long size, XrdOucEnv *envP)
{
   if (isTagFile(path)) return -ENOENT;

   std::unique_ptr<XrdOssDF> fp(newFile("xrdt"));
   XrdOucEnv   myEnv;
   int ret = fp->Open(path, O_RDWR, 0600, myEnv);
   if (ret != XrdOssOK)
   {
      return ret;
   }
   ret = fp->Ftruncate(size);
   if (ret != XrdOssOK)
   {
      return ret;
   }
   long long retsz=0;
   fp->Close(&retsz);
   return XrdOssOK;
}

int XrdOssCsi::Reloc(const char *tident, const char *path,
                     const char *cgName, const char *anchor)
{
   if (isTagFile(path)) return -ENOENT;
   return successor_->Reloc(tident, path, cgName, anchor);
}

int XrdOssCsi::Mkdir(const char *path, mode_t mode, int mkpath, XrdOucEnv *envP)
{
   if (isTagFile(path)) return -ENOENT;
   return successor_->Mkdir(path, mode, mkpath, envP);
}

int XrdOssCsi::Create(const char *tident, const char *path, mode_t access_mode,
                      XrdOucEnv &env, int Opts)
{
   if (isTagFile(path)) return -EPERM;

   // get mapinfo entries for file
   std::shared_ptr<XrdOssCsiFile::puMapItem_t> pmi;
   {
      const std::string tpath = std::string(path) + ".xrdt";
      XrdOssCsiFile::mapTake(tpath, pmi);
   }

   XrdSysMutexHelper lck(pmi->mtx);
   if (pmi->unlinked)
   {
      XrdOssCsiFile::mapReleaseLocked(pmi,&lck);
      return Create(tident, path, access_mode, env, Opts);
   }

   const bool isTrunc = ((Opts>>8)&O_TRUNC) ? true : false;

   if (isTrunc && pmi->pages)
   {
      // asked to truncate but the file is already open: becomes difficult to sync.
      // So, return error
      XrdOssCsiFile::mapReleaseLocked(pmi, &lck);
      return -ETXTBSY;
   }

   const int ret = successor_->Create(tident, path, access_mode, env, Opts);
   XrdOssCsiFile::mapReleaseLocked(pmi, &lck);

   return ret;
}

int XrdOssCsi::Chmod(const char *path, mode_t mode, XrdOucEnv *envP)
{
   if (isTagFile(path)) return -ENOENT;
   return successor_->Chmod(path, mode, envP);
}

int XrdOssCsi::Remdir(const char *path, int Opts, XrdOucEnv *eP)
{
   if (isTagFile(path)) return -ENOENT;
   return successor_->Remdir(path, Opts, eP);
}

int XrdOssCsi::Stat(const char *path, struct stat *buff, int opts,
                    XrdOucEnv  *EnvP)
{
   if (isTagFile(path)) return -ENOENT;
   return successor_->Stat(path, buff, opts, EnvP);
}

int XrdOssCsi::StatPF(const char *path, struct stat *buff, int opts)
{
   if (isTagFile(path)) return -ENOENT;
   if (!(opts & XrdOss::PF_dStat)) return successor_->StatPF(path, buff, opts);

   buff->st_rdev = 0;
   const int pfret = successor_->StatPF(path, buff, opts);
   if (pfret != XrdOssOK)
   {
      return pfret;
   }

   std::unique_ptr<XrdOssCsiFile> fp((XrdOssCsiFile*)newFile("xrdt"));
   XrdOucEnv   myEnv;
   const int oret = fp->Open(path, O_RDONLY, 0600, myEnv);
   if (oret != XrdOssOK)
   {
      return oret;
   }
   const int vs = fp->VerificationStatus();

   long long retsz=0;
   fp->Close(&retsz);

   buff->st_rdev &= ~(XrdOss::PF_csVer | XrdOss::PF_csVun);
   buff->st_rdev |= static_cast<dev_t>(vs);
   return XrdOssOK;
}

int XrdOssCsi::StatXA(const char *path, char *buff, int &blen,
                         XrdOucEnv *envP)
{
   if (isTagFile(path)) return -ENOENT;
   return successor_->StatXA(path, buff, blen, envP);
}


XrdOss *XrdOssAddStorageSystem2(XrdOss       *curr_oss,
                                XrdSysLogger *Logger,
                                const char   *config_fn,
                                const char   *parms,
                                XrdOucEnv    *envP)
{
   XrdOssCsi *myOss = new XrdOssCsi(curr_oss);
   if (myOss->Init(Logger, config_fn, parms, envP) != XrdOssOK)
   {
      delete myOss;
      return NULL;
   }
   return (XrdOss*)myOss;
}
