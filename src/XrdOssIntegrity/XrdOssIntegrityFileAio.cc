/******************************************************************************/
/*                                                                            */
/*           X r d O s s I n t e g r i t y F i l e A i o . c c                */
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
#include "XrdOssIntegrityPages.hh"
#include "XrdOssIntegrityFileAio.hh"
#include "XrdOuc/XrdOucCRC.hh"

#include <string>
#include <algorithm>
#include <mutex>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <limits.h>

XrdOssIntegrityFileAioStore::~XrdOssIntegrityFileAioStore()
{
   XrdOssIntegrityFileAio *p;
   while((p=list_))
   {
      list_ = list_->next_;
      delete p;
   }
}

int XrdOssIntegrityFile::Read(XrdSfsAio *aiop)
{
   if (!pmi_) return -EBADF;

   XrdOssIntegrityFileAio *nio = XrdOssIntegrityFileAio::Alloc(&aiostore_);
   nio->Init(aiop, this, false, 0, true);
   pmi_->pages->LockTrackinglen(nio->rg_, (off_t)aiop->sfsAio.aio_offset,
                                     (off_t)(aiop->sfsAio.aio_offset+aiop->sfsAio.aio_nbytes), true);
   return successor_->Read(nio);
}

int XrdOssIntegrityFile::Write(XrdSfsAio *aiop)
{
   if (!pmi_) return -EBADF;
   if (rdonly_) return -EBADF;

   XrdOssIntegrityFileAio *nio = XrdOssIntegrityFileAio::Alloc(&aiostore_);
   nio->Init(aiop, this, false, 0, false);
   // pages will be locked when write is scheduled
   return nio->SchedWriteJob();
}

int XrdOssIntegrityFile::pgRead (XrdSfsAio *aioparm, uint64_t opts)
{
   if (!pmi_) return -EBADF;

   // this is a tighter restriction that FetchRange requires
   if ((aioparm->sfsAio.aio_nbytes % XrdSys::PageSize) !=0) return -EINVAL;

   XrdOssIntegrityFileAio *nio = XrdOssIntegrityFileAio::Alloc(&aiostore_);
   nio->Init(aioparm, this, true, opts, true);
   pmi_->pages->LockTrackinglen(nio->rg_, (off_t)aioparm->sfsAio.aio_offset,
                                     (off_t)(aioparm->sfsAio.aio_offset+aioparm->sfsAio.aio_nbytes), true);
   return successor_->Read(nio);
}

int XrdOssIntegrityFile::pgWrite(XrdSfsAio *aioparm, uint64_t opts)
{
   if (!pmi_) return -EBADF;
   if (rdonly_) return -EBADF;
   uint64_t pgopts = opts;

   // do verify	before taking locks to allow for fast fail
   if (aioparm->cksVec && (opts & XrdOssDF::Verify))
   {
      uint32_t valcs;
      if (XrdOucCRC::Ver32C((void *)aioparm->sfsAio.aio_buf, (size_t)aioparm->sfsAio.aio_nbytes, (uint32_t*)aioparm->cksVec, valcs)>=0)
      {
         return -EDOM;
      }
   }

   XrdOssIntegrityFileAio *nio = XrdOssIntegrityFileAio::Alloc(&aiostore_);
   nio->Init(aioparm, this, true, pgopts, false);
   // pages will be locked when write is scheduled
   return nio->SchedWriteJob();
}

int XrdOssIntegrityFile::Fsync(XrdSfsAio *aiop)
{
   aiop->Result = this->Fsync();
   aiop->doneWrite();
   return 0;
}
