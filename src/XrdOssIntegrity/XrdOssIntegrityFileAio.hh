#ifndef _XRDOSSINTEGRITYFILEAIO_H
#define _XRDOSSINTEGRITYFILEAIO_H
/******************************************************************************/
/*                                                                            */
/*           X r d O s s I n t e g r i t y F i l e A i o . h h                */
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
#include "XrdSfs/XrdSfsAio.hh"
#include "XrdOssIntegrity.hh"

#include <mutex>
#include <thread>

class XrdOssIntegrityFileAioJob : public XrdJob
{
public:

   XrdOssIntegrityFileAioJob() { }
   virtual ~XrdOssIntegrityFileAioJob() { }

   void Init(XrdOssIntegrityFile *fp, XrdOssIntegrityFileAio *nio, XrdSfsAio *aiop, bool isPg, bool read)
   {
      fp_ =fp;
      nio_ =nio;
      aiop_ = aiop;
      pg_ = isPg;
      read_ = read;
   }

   void DoIt() override
   {
      if (read_) { DoItRead(); }
      else { DoItWrite(); }
   }
   
   void DoItRead();
   void DoItWrite();

private:
   XrdOssIntegrityFile *fp_;
   XrdOssIntegrityFileAio *nio_;
   XrdSfsAio *aiop_;
   bool pg_;
   bool read_;
};

class XrdOssIntegrityFileAio : public XrdSfsAio
{
friend XrdOssIntegrityFileAioStore;
public:

   XrdOssIntegrityRangeGuard rg_;
   uint64_t pgOpts_;

   virtual void doneRead() override
   {
      successor_->Result = this->Result;
      if (successor_->Result<0 || this->sfsAio.aio_nbytes==0)
      {
         rg_.ReleaseAll();
         successor_->doneRead();
         return;
      }
      SchedReadJob();
   }

   virtual void doneWrite() override
   { 
      successor_->Result = this->Result;
      if (successor_->Result<0)
      {
         rg_.ReleaseAll();
         file_->resyncSizes();
         successor_->doneWrite();
         return;
      }
      // in case there was a short write during the async write, finish
      // writing the data now, otherwise the crc values will be inconsistent
      ssize_t towrite = this->sfsAio.aio_nbytes - this->Result;
      ssize_t nwritten = this->Result;
      const char *p = (const char*)this->sfsAio.aio_buf;
      while(towrite>0)
      {
         const ssize_t wret = file_->successor_->Write(&p[nwritten], this->sfsAio.aio_offset+nwritten, towrite);
         if (wret<0)
         {
            successor_->Result = wret;
            rg_.ReleaseAll();
            file_->resyncSizes();
            successor_->doneWrite();
            return;
         }
         towrite -= wret;
         nwritten += wret;
      }
      successor_->Result = nwritten;
      rg_.ReleaseAll();
      successor_->doneWrite();
   }

   virtual void Recycle()
   {
      rg_.ReleaseAll();
      successor_->Recycle();
      successor_ = nullptr;
      file_ = nullptr;
      if (store_)
      {
         std::lock_guard<std::mutex> guard(store_->mtx_);
         next_ = store_->list_;
         store_->list_ = this;
      }
      else
      {
         delete this;
      }
   }
  
   void Init(XrdSfsAio *successor, XrdOssIntegrityFile *file, bool isPgOp, uint64_t opts, bool isread)
   {
      successor_ = successor;
      this->sfsAio.aio_fildes = successor->sfsAio.aio_fildes;
      this->sfsAio.aio_buf = successor->sfsAio.aio_buf;
      this->sfsAio.aio_nbytes = successor->sfsAio.aio_nbytes;
      this->sfsAio.aio_offset = successor->sfsAio.aio_offset;
      this->sfsAio.aio_reqprio = successor->sfsAio.aio_reqprio;
      this->cksVec = successor->cksVec;
      this->TIdent = successor->TIdent;
      file_ = file;
      isPgOp_ = isPgOp;
      pgOpts_ = opts;
      Sched_ = XrdOssIntegrity::Sched_;
      job_.Init(file, this, successor, isPgOp, isread);
   }

   static XrdOssIntegrityFileAio *Alloc(XrdOssIntegrityFileAioStore *store)
   {
      XrdOssIntegrityFileAio *p=nullptr;
      if (store)
      {
         std::lock_guard<std::mutex> guard(store->mtx_);
         if ((p = store->list_)) store->list_ = p->next_;
      }
      if (!p) p = new XrdOssIntegrityFileAio(store);
      return p;
   }

   int SchedWriteJob()
   {
      Sched_->Schedule((XrdJob *)&job_);
      return 0;
   }

   void SchedReadJob()
   {
      Sched_->Schedule((XrdJob *)&job_);
   }

   XrdOssIntegrityFileAio(XrdOssIntegrityFileAioStore *store) : store_(store) { }
   ~XrdOssIntegrityFileAio() { }

private:
   XrdOssIntegrityFileAioStore *store_;
   XrdSfsAio *successor_;
   XrdOssIntegrityFile *file_;
   bool isPgOp_;
   XrdOssIntegrityFileAioJob job_;
   XrdScheduler *Sched_;
   XrdOssIntegrityFileAio *next_;
};

void XrdOssIntegrityFileAioJob::DoItRead()
{
   // this job runs after async Read
   ssize_t puret;
   if (pg_)
   {
      puret = fp_->pages_->FetchRange(fp_->successor_,
                                      (void *)nio_->sfsAio.aio_buf,
                                      (off_t)nio_->sfsAio.aio_offset,
                                      (size_t)nio_->Result,
                                      (uint32_t*)nio_->cksVec,
                                      nio_->pgOpts_,
                                      nio_->rg_);
   }
   else
   {
      puret = fp_->pages_->VerifyRange(fp_->successor_,
                                       (void *)nio_->sfsAio.aio_buf,
                                       (off_t)nio_->sfsAio.aio_offset,
                                       (size_t)nio_->Result,
                                       nio_->rg_);
   }
   if (puret<0)
   {
      aiop_->Result = puret;
   }
   if (puret != aiop_->Result)
   {
      aiop_->Result = -EIO;
   }
   nio_->rg_.ReleaseAll();
   aiop_->doneRead();
}

void XrdOssIntegrityFileAioJob::DoItWrite()
{
   // this job runs before async Write
   int puret;
   if (pg_) {
      puret = fp_->pages_->StoreRange(fp_->successor_,
                                      (const void *)aiop_->sfsAio.aio_buf, (off_t)aiop_->sfsAio.aio_offset,
                                      (size_t)aiop_->sfsAio.aio_nbytes, (uint32_t*)aiop_->cksVec, nio_->rg_);

   }
   else
   {
      puret = fp_->pages_->UpdateRange(fp_->successor_,
                                       (const void *)aiop_->sfsAio.aio_buf, (off_t)aiop_->sfsAio.aio_offset,
                                       (size_t)aiop_->sfsAio.aio_nbytes, nio_->rg_);
   }
   if (puret<0)
   {
      nio_->rg_.ReleaseAll();
      fp_->resyncSizes();
      aiop_->Result = puret;
      aiop_->doneWrite();
      nio_->Recycle();
      return;
   }

   const int ret = fp_->successor_->Write(nio_);
   if (ret<0)
   {
      nio_->rg_.ReleaseAll();
      fp_->resyncSizes();
      aiop_->Result = ret;
      aiop_->doneWrite();
      nio_->Recycle();
      return;
   }

   return;
}

#endif
