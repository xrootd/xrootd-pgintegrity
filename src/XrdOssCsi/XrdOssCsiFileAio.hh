#ifndef _XRDOSSCSIFILEAIO_H
#define _XRDOSSCSIFILEAIO_H
/******************************************************************************/
/*                                                                            */
/*                 X r d O s s C s i F i l e A i o . h h                      */
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

#include "Xrd/XrdScheduler.hh"
#include "XrdSfs/XrdSfsAio.hh"
#include "XrdOssCsi.hh"
#include "XrdSys/XrdSysPageSize.hh"

#include <mutex>
#include <thread>

class XrdOssCsiFileAioJob : public XrdJob
{
public:

   XrdOssCsiFileAioJob() { }
   virtual ~XrdOssCsiFileAioJob() { }

   void Init(XrdOssCsiFile *fp, XrdOssCsiFileAio *nio, XrdSfsAio *aiop, bool isPg, bool read)
   {
      fp_   = fp;
      nio_  = nio;
      aiop_ = aiop;
      pg_   = isPg;
      read_ = read;
   }

   void DoIt() /* override */
   {
      if (read_) { DoItRead(); }
      else { DoItWrite(); }
   }
   
   void DoItRead();
   void DoItWrite();

private:
   XrdOssCsiFile *fp_;
   XrdOssCsiFileAio *nio_;
   XrdSfsAio *aiop_;
   bool pg_;
   bool read_;
};

class XrdOssCsiFileAio : public XrdSfsAio
{
friend class XrdOssCsiFileAioStore;
public:

   XrdOssCsiRangeGuard rg_;
   uint64_t pgOpts_;

   virtual void doneRead() /* override */
   {
      parentaio_->Result = this->Result;
      if (parentaio_->Result<0 || this->sfsAio.aio_nbytes==0)
      {
         parentaio_->doneRead();
         Recycle();
         return;
      }

      //
      // if this is a pg operation and this was a short read, try to complete,
      // otherwise caller will have to deal with joining csvec values from repeated reads
      //
      ssize_t toread = this->sfsAio.aio_nbytes - this->Result;
      ssize_t nread = this->Result;

      if (!isPgOp_)
      {
         // not a pg operation, no need to read more
         toread = 0;
      }
      char *p = (char*)this->sfsAio.aio_buf;
      while(toread>0)
      {
         const ssize_t rret = file_->successor_->Read(&p[nread], this->sfsAio.aio_offset+nread, toread);
         if (rret == 0) break;
         if (rret<0)
         {
            parentaio_->Result = rret;
            parentaio_->doneRead();
            Recycle();
            return;
         }
         toread -= rret;
         nread += rret;
      }
      parentaio_->Result = nread;

      // schedule the fetchrange
      SchedReadJob();
   }

   virtual void doneWrite() /* override */
   {
      parentaio_->Result = this->Result;
      if (parentaio_->Result<0)
      {
         rg_.ReleaseAll();
         file_->resyncSizes();
         parentaio_->doneWrite();
         Recycle();
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
            parentaio_->Result = wret;
            rg_.ReleaseAll();
            file_->resyncSizes();
            parentaio_->doneWrite();
            Recycle();
            return;
         }
         towrite -= wret;
         nwritten += wret;
      }
      parentaio_->Result = nwritten;
      parentaio_->doneWrite();
      Recycle();
   }

   virtual void Recycle()
   {
      rg_.ReleaseAll();
      parentaio_ = NULL;
      XrdOssCsiFile *f = file_;
      file_ = NULL;
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
      if (f)
      {
         f->aioDec();
      }
   }
  
   void Init(XrdSfsAio *aiop, XrdOssCsiFile *file, bool isPgOp, uint64_t opts, bool isread)
   {
      parentaio_               = aiop;
      this->sfsAio.aio_fildes  = aiop->sfsAio.aio_fildes;
      this->sfsAio.aio_buf     = aiop->sfsAio.aio_buf;
      this->sfsAio.aio_nbytes  = aiop->sfsAio.aio_nbytes;
      this->sfsAio.aio_offset  = aiop->sfsAio.aio_offset;
      this->sfsAio.aio_reqprio = aiop->sfsAio.aio_reqprio;
      this->cksVec             = aiop->cksVec;
      this->TIdent             = aiop->TIdent;
      file_                    = file;
      isPgOp_                  = isPgOp;
      pgOpts_                  = opts;
      Sched_                   = XrdOssCsi::Sched_;
      job_.Init(file, this, aiop, isPgOp, isread);
      file_->aioInc();
   }

   static XrdOssCsiFileAio *Alloc(XrdOssCsiFileAioStore *store)
   {
      XrdOssCsiFileAio *p=NULL;
      if (store)
      {
         std::lock_guard<std::mutex> guard(store->mtx_);
         if ((p = store->list_)) store->list_ = p->next_;
      }
      if (!p) p = new XrdOssCsiFileAio(store);
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

   XrdOssCsiFileAio(XrdOssCsiFileAioStore *store) : store_(store) { }
   ~XrdOssCsiFileAio() { }

private:
   XrdOssCsiFileAioStore *store_;
   XrdSfsAio *parentaio_;
   XrdOssCsiFile *file_;
   bool isPgOp_;
   XrdOssCsiFileAioJob job_;
   XrdScheduler *Sched_;
   XrdOssCsiFileAio *next_;
};

void XrdOssCsiFileAioJob::DoItRead()
{
   // this job runs after async Read
   // range was already locked read-only before the read
   ssize_t puret;
   if (pg_)
   {
      puret = fp_->Pages()->FetchRange(fp_->successor_,
                                      (void *)nio_->sfsAio.aio_buf,
                                      (off_t)nio_->sfsAio.aio_offset,
                                      (size_t)nio_->Result,
                                      (uint32_t*)nio_->cksVec,
                                      nio_->pgOpts_,
                                      nio_->rg_);
   }
   else
   {
      puret = fp_->Pages()->VerifyRange(fp_->successor_,
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
      aiop_->Result = -EDOM;
   }
   aiop_->doneRead();
   nio_->Recycle();
}

void XrdOssCsiFileAioJob::DoItWrite()
{
   // this job runs before async Write

   // lock range
   fp_->Pages()->LockTrackinglen(nio_->rg_, (off_t)aiop_->sfsAio.aio_offset,
                                (off_t)(aiop_->sfsAio.aio_offset+aiop_->sfsAio.aio_nbytes), false);
   int puret;
   if (pg_) {
      puret = fp_->Pages()->StoreRange(fp_->successor_,
                                      (const void *)aiop_->sfsAio.aio_buf, (off_t)aiop_->sfsAio.aio_offset,
                                      (size_t)aiop_->sfsAio.aio_nbytes, (uint32_t*)aiop_->cksVec, nio_->pgOpts_, nio_->rg_);

   }
   else
   {
      puret = fp_->Pages()->UpdateRange(fp_->successor_,
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
