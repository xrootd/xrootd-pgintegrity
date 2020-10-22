/******************************************************************************/
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

#include "XrdOss/XrdOss.hh"
#include "XrdOss/XrdOssDefaultSS.hh"
#include "XrdOuc/XrdOucEnv.hh"
#include "XrdSys/XrdSysLogger.hh"
#include "XrdVersion.hh"

#include <gtest/gtest.h>

#include <thread>
#include <memory>
#include <iostream>
#include <dlfcn.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <cstdint>
#include <stdlib.h>

#define TMPFN "/tmp/xrdosscsi_testfile_concurrent"

namespace integrationTests {

class osscsi_pageConcurrent : public ::testing::Test {

public:
  void thread(int idx, int *thrret) {
    std::unique_ptr<uint8_t[]> buf(new uint8_t[sizeof(m_b)]);
    std::string tid = "mytesttid" + std::to_string((long long)idx);
    std::unique_ptr<XrdOssDF> file(m_oss->newFile(tid.c_str()));
    int ret = file->Open(TMPFN, O_RDWR, 0600, m_env);
    if (ret != 0) {
      *thrret = 1;
      return;
    }
    
    for(int i=0;i<2000;i++) {
      off_t off = double(rand())/RAND_MAX * sizeof(m_b);
      size_t len = 1 + double(rand())/RAND_MAX * sizeof(m_b);
      len = std::min(len,sizeof(m_b)-off);
      size_t bufidx = double(rand())/RAND_MAX * sizeof(m_b);
      bufidx = std::min(bufidx, sizeof(m_b)-len);
      ssize_t res=0;
      switch(rand() % 4) {
        case 0:
          off &= 0xfffff000;
          len &= 0xfffff000;
          res = file->pgWrite(&m_b[bufidx],off,len,NULL,0);
          // pgWrite can fail if current EOF if not on page boundary
          if (res==-ESPIPE) res=0;
          break;
        case 1:
          off &= 0xfffff000;
          len &= 0xfffff000;
          res = file->pgRead(&buf[0],off,len,NULL,XrdOssDF::Verify);
          break;
        case 2:
          res = file->Read(&buf[0],off,len);
          break;
        case 3:
          res = file->Write(&m_b[bufidx],off,len);
          break;
      }
      if (res<0) {
        *thrret = 1;
        return;
      }
    }
    file->Close();
  }
  
protected:

  virtual void SetUp() {
    m_fdnull = open("/dev/null", O_WRONLY);
    ASSERT_TRUE(m_fdnull >= 0);

    const char *config_fn = NULL;
    m_logger = new XrdSysLogger(m_fdnull,0);

    XrdVERSIONINFODEF(v, "testint", XrdVNUMBER,XrdVERSION);
    XrdOss *ossP = XrdOssDefaultSS(m_logger, config_fn, v);

    m_libp = dlopen("libXrdOssCsi-5.so",RTLD_NOW|RTLD_GLOBAL);
    ASSERT_TRUE( m_libp != NULL );

    XrdOssAddStorageSystem2_t oss2P=NULL;
    oss2P = reinterpret_cast<XrdOssAddStorageSystem2_t>(dlsym(m_libp, "XrdOssAddStorageSystem2"));

    ASSERT_TRUE( oss2P != NULL );

    m_oss = oss2P(ossP, m_logger, config_fn, "prefix=", &m_env);
    ASSERT_TRUE(m_oss != NULL );

    m_file = m_oss->newFile("mytesttid");
    ASSERT_TRUE(m_file != NULL);
    m_fileopen = false;

    uint32_t x,m;
    x = 1;
    m = 0x7fffffff;
    for(size_t i=0;i<sizeof(m_b);i++) {
      x = (48271ULL * x)%m;
      m_b[i] = x;
    }

    resetfile();
  }

  virtual void TearDown() {
    closefile();
    delete m_file;
    m_oss->Unlink(TMPFN);
    delete m_oss;
    m_file = NULL;
    m_oss = NULL;
    dlclose(m_libp);
    m_libp = NULL;
    delete m_logger;
    m_logger = NULL;
    close(m_fdnull);
    m_fdnull = -1;
  }

  void resetfile() {
    closefile();
    int ret = m_file->Open(TMPFN, O_RDWR|O_CREAT|O_TRUNC, 0600, m_env);
    ASSERT_TRUE(ret == XrdOssOK);
    m_fileopen = true;
  }

  void closefile() {
    if (m_fileopen) {
      m_file->Close();
      m_fileopen = false;
    }
  }

  int m_fdnull;
  XrdSysLogger *m_logger;
  void *m_libp;
  XrdOucEnv m_env;
  XrdOss *m_oss;
  XrdOssDF *m_file;
  uint8_t m_b[4096*256];
  bool m_fileopen;
  const static int NTHR = 16;
};

TEST_F(osscsi_pageConcurrent,concurrent) {
  ASSERT_TRUE((m_oss->Features() & XRDOSS_HASFSCS) != 0);

  closefile();

  std::vector<std::thread> thr;
  std::vector<int> res(NTHR);
  for(int i=0;i<NTHR;i++) {
    thr.emplace_back(&osscsi_pageConcurrent::thread, this, i, &res[i]);
  }
  for(int i=0;i<NTHR;i++) {
    thr[i].join();
    ASSERT_TRUE(res[i]==0);
  }
}

} // namespace integrationTests

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
