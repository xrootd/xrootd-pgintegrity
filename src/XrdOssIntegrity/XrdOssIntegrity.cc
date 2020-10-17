/******************************************************************************/
/*                                                                            */
/*                X r d O s s I n t e g r i t y . c c                         */
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

#include "XrdOssIntegrityTrace.hh"
#include "XrdOssIntegrity.hh"
#include "XrdOssIntegrityConfig.hh"
#include "XrdOuc/XrdOucEnv.hh"
#include "XrdVersion.hh"

#include <string>
#include <memory>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <limits.h>

XrdVERSIONINFO(XrdOssAddStorageSystem2,XrdOssIntegrity)

XrdSysError OssIntegrityEroute(0, "ossintegrity_");
XrdOucTrace OssIntegrityTrace(&OssIntegrityEroute);

XrdScheduler *XrdOssIntegrity::Sched_;

// skip tag files in directory listing
int XrdOssIntegrityDir::Readdir(char *buff, int blen)
{
   int ret;
   do
   {
      ret = successor_->Readdir(buff, blen);
      if (ret<0) return ret;
   } while(XrdOssIntegrity::isTagFile(buff));
   return ret;
}

int XrdOssIntegrity::Init(XrdSysLogger *lP, const char *cP, const char *params, XrdOucEnv *env)
{
   OssIntegrityEroute.logger(lP);

   int cret = config_.Init(OssIntegrityEroute, cP, params, env);
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

int XrdOssIntegrity::Unlink(const char *path, int Opts, XrdOucEnv *eP)
{
   if (isTagFile(path)) return -ENOENT;

   std::unique_ptr<XrdOssIntegrityFile> fp((XrdOssIntegrityFile*)newFile("xrdt"));
   XrdOucEnv   myEnv;
   const int oret = fp->Open(path, O_RDWR, 0600, myEnv);
   if (oret != XrdOssOK)
   {
      return oret;
   }

   const int ret = fp->FUnlink(Opts, eP);

   long long retsz=0;
   fp->Close(&retsz);

   return ret;
}

int XrdOssIntegrity::Rename(const char *oldname, const char *newname,
                      XrdOucEnv  *old_env, XrdOucEnv  *new_env)
{
   if (isTagFile(oldname) || isTagFile(newname)) return -ENOENT;

   std::unique_ptr<XrdOssIntegrityFile> fp((XrdOssIntegrityFile*)newFile("xrdt"));
   XrdOucEnv   myEnv;
   const int oret = fp->Open(oldname, O_RDWR, 0600, myEnv);
   if (oret != XrdOssOK)
   {
      return oret;
   }

   const int rnret = fp->FRename(newname, old_env, new_env);

   long long retsz=0;
   fp->Close(&retsz);

   return rnret;
}

int XrdOssIntegrity::Truncate(const char *path, unsigned long long size, XrdOucEnv *envP)
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

int XrdOssIntegrity::Reloc(const char *tident, const char *path,
                     const char *cgName, const char *anchor)
{
   if (isTagFile(path)) return -ENOENT;
   return successor_->Reloc(tident, path, cgName, anchor);
}

int XrdOssIntegrity::Mkdir(const char *path, mode_t mode, int mkpath, XrdOucEnv *envP)
{
   if (isTagFile(path)) return -ENOENT;
   return successor_->Mkdir(path, mode, mkpath, envP);
}

int XrdOssIntegrity::Create(const char *tident, const char *path, mode_t access_mode,
                      XrdOucEnv &env, int Opts)
{
   if (isTagFile(path)) return -EPERM;

   // don't let create do any truncating
   // instead let the open do it: as it is aware of the tag file
   int dopts = Opts;
   dopts = (((dopts>>8)&(~O_TRUNC))<<8)|(dopts&0xff);

   const int iret = successor_->Create(tident, path, access_mode, env, dopts);
   if (iret != XrdOssOK) return iret;

   // If create did want truncate we make sure give open the
   // chance to truncated now, as subsequently the user may not give O_TRUNC when
   // opening the file. If it is was a new empty file (without truncate) it will
   // be zero length and we'll try to make the tag file at open, no need to do it here.

   bool isTrunc = ((Opts>>8)&O_TRUNC) ? true : false;
   if (!isTrunc) return XrdOssOK;

   const int flags = O_RDWR|O_CREAT|O_TRUNC;

   std::unique_ptr<XrdOssIntegrityFile> fp((XrdOssIntegrityFile*)newFile(tident));
   XrdOucEnv   myEnv;
   const int oret = fp->Open(path, flags, access_mode, myEnv);
   if (oret == XrdOssOK)
   {
      long long retsz=0;
      fp->Close(&retsz);
   }
   return XrdOssOK;
}

int XrdOssIntegrity::Chmod(const char *path, mode_t mode, XrdOucEnv *envP)
{
   if (isTagFile(path)) return -ENOENT;
   return successor_->Chmod(path, mode, envP);
}

int XrdOssIntegrity::Remdir(const char *path, int Opts, XrdOucEnv *eP)
{
   if (isTagFile(path)) return -ENOENT;
   return successor_->Remdir(path, Opts, eP);
}

int XrdOssIntegrity::Stat(const char *path, struct stat *buff, int opts,
                    XrdOucEnv  *EnvP)
{
   if (isTagFile(path)) return -ENOENT;
   return successor_->Stat(path, buff, opts, EnvP);
}

int XrdOssIntegrity::StatPF(const char *path, struct stat *buff, int opts)
{
   if (isTagFile(path)) return -ENOENT;
   if (!(opts & XrdOss::PF_dStat)) return successor_->StatPF(path, buff, opts);

   buff->st_rdev = 0;
   const int pfret = successor_->StatPF(path, buff, opts);
   if (pfret != XrdOssOK)
   {
      return pfret;
   }

   std::unique_ptr<XrdOssIntegrityFile> fp((XrdOssIntegrityFile*)newFile("xrdt"));
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

int XrdOssIntegrity::StatXA(const char *path, char *buff, int &blen,
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
   XrdOssIntegrity *myOss = new XrdOssIntegrity(curr_oss);
   if (myOss->Init(Logger, config_fn, parms, envP) != XrdOssOK)
   {
      delete myOss;
      return NULL;
   }
   return (XrdOss*)myOss;
}
