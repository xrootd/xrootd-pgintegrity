#ifndef __XRDPFC_IO_ENTIRE_FILE_HH__
#define __XRDPFC_IO_ENTIRE_FILE_HH__
//----------------------------------------------------------------------------------
// Copyright (c) 2014 by Board of Trustees of the Leland Stanford, Jr., University
// Author: Alja Mrak-Tadel, Matevz Tadel, Brian Bockelman
//----------------------------------------------------------------------------------
// XRootD is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// XRootD is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with XRootD.  If not, see <http://www.gnu.org/licenses/>.
//----------------------------------------------------------------------------------

#include <string>

#include "XrdSys/XrdSysPthread.hh"
#include "XrdPfcIO.hh"
#include "XrdPfc.hh"
#include "XrdPfcStats.hh"
#include "XrdPfcFile.hh"

class XrdSysError;
class XrdOssDF;
class XfcStats;
class XrdOucIOVec;

namespace XrdPfc
{
//----------------------------------------------------------------------------
//! \brief Downloads original file into a single file on local disk.
//! Handles read requests as they come along.
//----------------------------------------------------------------------------
class IOEntireFile : public IO
{
public:
   IOEntireFile(XrdOucCacheIO *io, XrdOucCacheStats &stats, Cache &cache);

   ~IOEntireFile();

   //------------------------------------------------------------------------
   //! Check if File was opened successfully.
   //------------------------------------------------------------------------
   bool HasFile() const { return m_file != 0; }

   //---------------------------------------------------------------------
   //! Pass Read request to the corresponding File object.
   //!
   //! @param Buffer
   //! @param Offset
   //! @param Length
   //!
   //! @return number of bytes read
   //---------------------------------------------------------------------
   using XrdOucCacheIO::Read;

   virtual int Read(char *Buffer, long long Offset, int Length);

   //---------------------------------------------------------------------
   //! Pass ReadV request to the corresponding File object.
   //!
   //! @param readV
   //! @param n number of XrdOucIOVecs
   //!
   //! @return total bytes read
   //---------------------------------------------------------------------
   using XrdOucCacheIO::ReadV;

   virtual int ReadV(const XrdOucIOVec *readV, int n);

   //---------------------------------------------------------------------
   //! Pass pgRead request to the corresponding File object.
   //!
   //! @param  buff  pointer to buffer where the bytes are to be placed.
   //! @param  offs  The offset where the read is to start. It must be
   //!               page aligned.
   //! @param  rdlen The number of bytes to read. The amount must be an
   //!               integral number of XrdSys::PageSize bytes.
   //! @param  csvec A vector whose entries which will be filled with the
   //!               corresponding CRC32C checksum for each page; sized to:
   //!               (rdlen/XrdSys::PageSize + (rdlen%XrdSys::PageSize != 0).
   //! @param  opts  Processing options.
   //!
   //! @return >= 0      The number of bytes placed in buffer.
   //! @return -errno    File could not be read, return value is the reason.
   //-----------------------------------------------------------------------------
   using XrdOucCacheIO::pgRead;

   virtual int pgRead(char      *buff,
                      long long  offs,
                      int        rdlen,
                      uint32_t  *csvec,
                      uint64_t   opts=0) override;

   //! \brief Abstract virtual method of XrdPfcIO
   //! Called to check if destruction needs to be done in a separate task.
   bool ioActive() /* override */;

   //! \brief Abstract virtual method of XrdPfcIO
   //! Called to destruct the IO object after it is no longer used.
   void DetachFinalize() /* override */;
   
   virtual int  Fstat(struct stat &sbuff);

   virtual long long FSize();

private:
   File        *m_file;
   struct stat *m_localStat;
   int initCachedStat(const char* path);
};

}
#endif

