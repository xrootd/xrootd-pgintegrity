#ifndef _XRDOSSINTEGRITYRANGES_H
#define _XRDOSSINTEGRITYRANGES_H
/******************************************************************************/
/*                                                                            */
/*            X r d O s s I n t e g r i t y R a n g e s . h h                 */
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

#include "XrdSys/XrdSysPthread.hh"

#include <mutex>
#include <list>
#include <condition_variable>
#include <memory>

// forward decl
class XrdOssIntegrityPages;

struct XrdOssIntegrityRange_s
{
   off_t start;
   off_t end;
   bool rdonly;
   int nBlockedBy;
   std::mutex mtx;
   std::condition_variable cv;
   XrdOssIntegrityRange_s *next;
};

class XrdOssIntegrityRanges;

class XrdOssIntegrityRangeGuard
{
public:
   XrdOssIntegrityRangeGuard() : r_(nullptr), rp_(nullptr), pages_(nullptr) { }
   ~XrdOssIntegrityRangeGuard();

   void SetRange(XrdOssIntegrityRanges *r, XrdOssIntegrityRange_s *rp)
   {
      r_ = r;
      rp_ = rp;
      pages_ = nullptr;
      trackinglenlocked_ = false;
   }

   const std::pair<off_t,off_t>& getTrackinglens() const
   {
      return trackingsizes_;
   }

   void SetTrackingInfo(XrdOssIntegrityPages *p, const std::pair<off_t,off_t> &tsizes, bool locked)
   {
      trackingsizes_ = tsizes;
      if (locked)
      {
         trackinglenlocked_ = true;
         pages_ = p;
      }
   }

   void Wait();

   void unlockTrackinglen();
   void ReleaseAll();

private:
   XrdOssIntegrityRanges *r_;
   XrdOssIntegrityRange_s *rp_;
   XrdOssIntegrityPages *pages_;
   std::pair<off_t,off_t> trackingsizes_;
   bool trackinglenlocked_;
};


class XrdOssIntegrityRanges
{
public:
   XrdOssIntegrityRanges() : allocList_(nullptr) { }

   ~XrdOssIntegrityRanges()
   {
      XrdOssIntegrityRange_s *p;
      while((p = allocList_))
      {
         allocList_ = allocList_->next;
         delete p;
      }
   }

   void AddRange(const off_t start, const off_t end, XrdOssIntegrityRangeGuard &rg, bool rdonly)
   {
      std::unique_lock<std::mutex> lck(rmtx_);
    
      int nblocking = 0;
      for(auto &r: ranges_)
      {
         if (r->start <= end && start <= r->end)
         {
            if (!(rdonly && r->rdonly))
            {
               nblocking++;
            }
         }
      }

      XrdOssIntegrityRange_s *nr = AllocRange();
      nr->start = start;
      nr->end = end;
      nr->rdonly = rdonly;
      nr->nBlockedBy = nblocking;
      ranges_.push_back(nr);
      lck.unlock();

      rg.SetRange(this, nr);
   }

   void Wait(XrdOssIntegrityRange_s *rp)
   {
      std::unique_lock<std::mutex> l(rp->mtx);
      while (rp->nBlockedBy>0)
      {
         rp->cv.wait(l);
      }
   }

   void RemoveRange(XrdOssIntegrityRange_s *rp)
   {
      std::lock_guard<std::mutex> guard(rmtx_);
      for(auto itr=ranges_.begin();itr!=ranges_.end();++itr)
      {
         if (*itr == rp)
         {
            ranges_.erase(itr);
            break;
         }
      }

      for(auto &r: ranges_)
      {
         if (r->start <= rp->end && rp->start <= r->end)
         {
            if (!(rp->rdonly && r->rdonly))
            {
               std::unique_lock<std::mutex> l(r->mtx);
               r->nBlockedBy--;
               if (r->nBlockedBy == 0)
               {
                  r->cv.notify_one();
               }
            }
         }
     }

     RecycleRange(rp);
     rp = nullptr;
   }

private:
   std::mutex rmtx_;
   std::list<XrdOssIntegrityRange_s *> ranges_;
   XrdOssIntegrityRange_s *allocList_;

   // must be called with rmtx_ locked
   XrdOssIntegrityRange_s* AllocRange()
   {
      XrdOssIntegrityRange_s *p;
      if ((p = allocList_)) allocList_ = p->next;
      if (!p) p = new XrdOssIntegrityRange_s();
      p->next = nullptr;
      return p;
   }

   // must be called with rmtx_ locked
   void RecycleRange(XrdOssIntegrityRange_s* rp)
   {
     rp->next = allocList_;
     allocList_ = rp;
   }
};

#endif
