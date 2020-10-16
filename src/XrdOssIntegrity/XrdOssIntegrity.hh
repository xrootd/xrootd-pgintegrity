#ifndef _XRDOSSINTEGRITY_H
#define _XRDOSSINTEGRITY_H
/******************************************************************************/
/*                                                                            */
/*                   X r d O s s I n t e g r i t y . h h                      */
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

#include "Xrd/XrdScheduler.hh"
#include "XrdOssHandler.hh"
#include "XrdOssIntegrityConfig.hh"
#include "XrdOssIntegrityPages.hh"

#include <memory>
#include <unordered_map>

// forward decl
class XrdOssIntegrityFileAio;
class XrdOssIntegrityFileAioJob;

class XrdOssIntegrityFileAioStore
{
public:
   XrdOssIntegrityFileAioStore() : list_(NULL) { }
   ~XrdOssIntegrityFileAioStore();

   std::mutex mtx_;
   XrdOssIntegrityFileAio *list_;
};

class XrdOssIntegrityDir : public XrdOssDFHandler
{
public:

                XrdOssIntegrityDir(XrdOss *parent, const char *tid, std::shared_ptr<XrdOssIntegrityConfig> cf) : XrdOssDFHandler(parent->newDir(tid)), config_(cf) { }
virtual        ~XrdOssIntegrityDir() { }

virtual int     Readdir(char *buff, int blen) /* override */;

private:
   std::shared_ptr<XrdOssIntegrityConfig> config_;
};

class XrdOssIntegrityFile : public XrdOssDFHandler
{
friend class XrdOssIntegrityFileAio;
friend class XrdOssIntegrityFileAioJob;
public:

virtual int     Close(long long *retsz=0) /* override */;
virtual int     Open(const char *, int, mode_t, XrdOucEnv &) /* override */;

virtual off_t   getMmap(void **addr) /* override */ { if (addr) *addr = 0; return 0; }
virtual int     getFD() /* override */ { return -1; }

virtual void    Flush() /* override */;
virtual int     Fstat(struct stat *) /* override */;
virtual int     Fsync() /* override */;
virtual int     Fsync(XrdSfsAio *) /* override */;
virtual int     Ftruncate(unsigned long long) /* override */;

virtual ssize_t Read(off_t, size_t) /* override */;
virtual ssize_t Read(void *, off_t, size_t) /* override */;
virtual int     Read(XrdSfsAio *) /* override */;
virtual ssize_t ReadRaw(void *, off_t, size_t) /* override */;
virtual ssize_t ReadV(XrdOucIOVec *readV, int n) /* override */;

virtual ssize_t Write(const void *, off_t, size_t) /* override */;
virtual int     Write(XrdSfsAio *) /* override */;
virtual ssize_t WriteV(XrdOucIOVec *writeV, int n) /* override */;

virtual ssize_t pgRead (void*, off_t, size_t, uint32_t*, uint64_t) /* override */;
virtual int     pgRead (XrdSfsAio*, uint64_t) /* override */;
virtual ssize_t pgWrite(void*, off_t, size_t, uint32_t*, uint64_t) /* override */;
virtual int     pgWrite(XrdSfsAio*, uint64_t) /* override */;

                XrdOssIntegrityFile(XrdOss *parent, const char *tid, std::shared_ptr<XrdOssIntegrityConfig> cf) :
                    XrdOssDFHandler(parent->newFile(tid)), parentOss_(parent), tident_(tid), config_(cf),
                    rdonly_(false), aioCntCond_(0), aioCnt_(0), aioCntWaiters_(0) { }
virtual        ~XrdOssIntegrityFile();

        void    aioInc() { XrdSysCondVarHelper lck(&aioCntCond_); ++aioCnt_; }
        void    aioDec()
        {
           XrdSysCondVarHelper lck(&aioCntCond_);
           if (--aioCnt_ == 0 && aioCntWaiters_>0)
              aioCntCond_.Broadcast();
        }
        void    aioWait()
        {
           XrdSysCondVarHelper lck(&aioCntCond_);
           ++aioCntWaiters_;
           while(aioCnt_>0)
           {
              aioCntCond_.Wait();
           }
           --aioCntWaiters_;
        }

        int VerificationStatus();

        int FRename(const char *, XrdOucEnv *old_env=0, XrdOucEnv *new_env=0);

        int FUnlink(int, XrdOucEnv *);

        XrdOssIntegrityPages *Pages() {
           return pmi_->pages.get();
        }

        struct puMapItem_t {
           int busy;                              // access under map's lock
           XrdSysMutex mtx;
           std::unique_ptr<XrdOssIntegrityPages> pages;
           std::string dpath;
           std::string tpath;
           bool unlinked;

           puMapItem_t() : busy(0), unlinked(false) { }
        };

static  XrdSysMutex pumtx_;
static  std::unordered_map<std::string, std::shared_ptr<puMapItem_t> > pumap_;

private:
        XrdOss *parentOss_;
        const char *tident_;
        std::shared_ptr<puMapItem_t> pmi_;
        XrdOssIntegrityFileAioStore aiostore_;
        std::shared_ptr<XrdOssIntegrityConfig> config_;
        bool rdonly_;

        int resyncSizes();
        int pageMapClose();
        int pageAndFileOpen(const char *, const int, const int, const mode_t, XrdOucEnv &);
        int createPageUpdater(const std::string &, int, XrdOucEnv &, std::unique_ptr<XrdOssIntegrityPages> &);

        XrdSysCondVar aioCntCond_;
        int           aioCnt_;
        int           aioCntWaiters_;
};

class XrdOssIntegrity : public XrdOssHandler
{
public:
virtual XrdOssDF *newDir(const char *tident) /* override */ { return (XrdOssDF *)new XrdOssIntegrityDir(successor_, tident, config_); }
virtual XrdOssDF *newFile(const char *tident) /* override */ { return (XrdOssDF *)new XrdOssIntegrityFile(successor_, tident, config_); }

virtual int       Init(XrdSysLogger *, const char *) /* override */ { return XrdOssOK; }
virtual int       Init(XrdSysLogger *, const char *, XrdOucEnv *) /* override */;

virtual uint64_t  Features() /* override */ { return (successor_->Features() | XRDOSS_HASFSCS); }

virtual int       Unlink(const char *path, int Opts=0, XrdOucEnv *eP=0) /* override */;
virtual int       Rename(const char *oldname, const char *newname,
                      XrdOucEnv  *old_env=0, XrdOucEnv  *new_env=0) /* override */;
virtual int       Truncate(const char *path, unsigned long long size,
                        XrdOucEnv *envP=0) /* override */;
virtual int       Reloc(const char *tident, const char *path,
                     const char *cgName, const char *anchor=0) /* override */;
virtual int       Mkdir(const char *path, mode_t mode, int mkpath=0, XrdOucEnv *envP=0) /* override */;
virtual int       Create(const char *tident, const char *path, mode_t access_mode,
                      XrdOucEnv &env, int Opts=0) /* override */;
virtual int       Chmod(const char *path, mode_t mode, XrdOucEnv *envP=0) /* override */;
virtual int       Remdir(const char *path, int Opts=0, XrdOucEnv *eP=0) /* override */;
virtual int       Stat(const char *path, struct stat *buff, int opts=0,
                    XrdOucEnv  *EnvP=0) /* override */;
virtual int       StatPF(const char *path, struct stat *buff, int opts) /* override */;
virtual int       StatPF(const char *path, struct stat *buff) /* override */ { return StatPF(path, buff, 0);}
virtual int       StatXA(const char *path, char *buff, int &blen,
                         XrdOucEnv *envP=0) /* override */;

                XrdOssIntegrity(XrdOss *successor, std::shared_ptr<XrdOssIntegrityConfig> cf) : XrdOssHandler(successor), config_(cf) { }
virtual        ~XrdOssIntegrity() { }

   static bool isTagFile(const char *p)
   {
      if (p && strlen(p)>=5 && !strcmp(&p[strlen(p)-5],".xrdt")) return true;
      return false;
   }

   static XrdScheduler *Sched_;

private:
   std::shared_ptr<XrdOssIntegrityConfig> config_;
};

extern "C" XrdOss *XrdOssAddStorageSystem2(XrdOss       *curr_oss,
                                           XrdSysLogger *Logger,
                                           const char   *config_fn,
                                           const char   *parms,
                                           XrdOucEnv    *envP);

#endif
