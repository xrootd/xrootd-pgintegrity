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

#include "XrdOuc/XrdOucCRC.hh"
#include "XrdOss/XrdOss.hh"
#include "XrdOss/XrdOssDefaultSS.hh"
#include "XrdOuc/XrdOucEnv.hh"
#include "XrdSys/XrdSysLogger.hh"
#include "XrdVersion.hh"

#include <gtest/gtest.h>

#include <iostream>
#include <string>
#include <cstdint>

#include <dlfcn.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#define TMPFN "/tmp/xrdosscsi_testfile_verified"

namespace integrationTests {

class osscsi_verifiedTest : public ::testing::Test {
protected:

  virtual void SetUp() {
    m_fdnull = open("/dev/null", O_WRONLY);
    ASSERT_TRUE(m_fdnull >= 0);

    m_libp = dlopen("libXrdOssCsi-5.so",RTLD_NOW|RTLD_GLOBAL);
    ASSERT_TRUE( m_libp != NULL );

    openplugin(true);

    uint32_t x,m;
    x = 1;
    m = 0x7fffffff;
    for(size_t i=0;i<sizeof(m_b);i++) {
      x = (48271ULL * x)%m;
      m_b[i] = x;
    }

    int ret = openfile(O_RDWR|O_CREAT|O_TRUNC);
    ASSERT_TRUE(ret == XrdOssOK);
  }

  virtual void TearDown() {
    if (m_oss) {
      m_oss->Unlink(TMPFN);
    }
    closeplugin();
    dlclose(m_libp);
    m_libp = NULL;
    close(m_fdnull);
    m_fdnull = -1;
  }

  void openplugin(bool allowmissing) {
    const char *config_fn = NULL;
    std::string params = "prefix=";
    if (!allowmissing) params += " nomissing";

    m_logger = new XrdSysLogger(m_fdnull,0);

    XrdVERSIONINFODEF(v, "testint", XrdVNUMBER,XrdVERSION);
    XrdOss *ossP = XrdOssDefaultSS(m_logger, config_fn, v);

    XrdOssAddStorageSystem2_t oss2P=NULL;
    oss2P = reinterpret_cast<XrdOssAddStorageSystem2_t>(dlsym(m_libp, "XrdOssAddStorageSystem2"));

    ASSERT_TRUE( oss2P != NULL );

    m_oss = oss2P(ossP, m_logger, config_fn, params.c_str(), &m_env);
    ASSERT_TRUE(m_oss != NULL );

    m_file = m_oss->newFile("mytesttid");
    ASSERT_TRUE(m_file != NULL);
    m_fileopen = false;
  }

  void closeplugin() {
    closefile();
    delete m_file;
    delete m_oss;
    delete m_logger;
    m_file = NULL;
    m_oss = NULL;
    m_logger = NULL;
  }

  int openfile(int oflags) {
    closefile();
    fileFlags_ = oflags;
    int ret = m_file->Open(TMPFN, fileFlags_, 0600, m_env);
    if (ret != XrdOssOK) return ret;
    m_fileopen = true;
    return ret;
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
  uint8_t m_b[4096*4];
  bool m_fileopen;
  int fileFlags_;
};

TEST_F(osscsi_verifiedTest,verified) {
  uint32_t csvec[2];
  XrdOucCRC::Calc32C((void *)m_b, 8192, csvec);
  ssize_t ret = m_file->pgWrite(m_b, 0, 8192, csvec, XrdOssDF::Verify);
  ASSERT_TRUE(ret == 8192);
  struct stat sbuff;
  memset(&sbuff,0,sizeof(sbuff));
  m_oss->StatPF(TMPFN, &sbuff, XrdOss::PF_dStat);
  ASSERT_TRUE(sbuff.st_rdev == XrdOss::PF_csVer);
}

TEST_F(osscsi_verifiedTest,unverified) {
  ssize_t ret = m_file->pgWrite(m_b, 0, 8192, NULL, 0);
  ASSERT_TRUE(ret == 8192);
  struct stat sbuff;
  memset(&sbuff,0,sizeof(sbuff));
  m_oss->StatPF(TMPFN, &sbuff, XrdOss::PF_dStat);
  ASSERT_TRUE(sbuff.st_rdev == XrdOss::PF_csVun);
}

TEST_F(osscsi_verifiedTest,unverified2) {
  ssize_t ret = m_file->Write(m_b, 0, 8192);
  ASSERT_TRUE(ret == 8192);
  struct stat sbuff;
  memset(&sbuff,0,sizeof(sbuff));
  m_oss->StatPF(TMPFN, &sbuff, XrdOss::PF_dStat);
  ASSERT_TRUE(sbuff.st_rdev == XrdOss::PF_csVun);
}

TEST_F(osscsi_verifiedTest,downgrade) {
  uint32_t csvec[2];
  XrdOucCRC::Calc32C((void *)m_b, 8192, csvec);
  ssize_t ret = m_file->pgWrite(m_b, 0, 8192, csvec, XrdOssDF::Verify);
  ASSERT_TRUE(ret == 8192);
  struct stat sbuff;
  memset(&sbuff,0,sizeof(sbuff));
  m_oss->StatPF(TMPFN, &sbuff, XrdOss::PF_dStat);
  ASSERT_TRUE(sbuff.st_rdev == XrdOss::PF_csVer);
  // downgrade to unverified because of pgWrite without csvec
  ret = m_file->pgWrite(m_b, 0, 8192, NULL, 0);
  ASSERT_TRUE(ret == 8192);
  memset(&sbuff,0,sizeof(sbuff));
  m_oss->StatPF(TMPFN, &sbuff, XrdOss::PF_dStat);
  ASSERT_TRUE(sbuff.st_rdev == XrdOss::PF_csVun);
  // check still unverified after using pgWrite with csvec
  ret = m_file->pgWrite(m_b, 0, 8192, csvec, XrdOssDF::Verify);
  ASSERT_TRUE(ret == 8192);
  m_oss->StatPF(TMPFN, &sbuff, XrdOss::PF_dStat);
  ASSERT_TRUE(sbuff.st_rdev == XrdOss::PF_csVun);
}

TEST_F(osscsi_verifiedTest,downgrade2) {
  uint32_t csvec[2];
  XrdOucCRC::Calc32C((void *)m_b, 8192, csvec);
  ssize_t ret = m_file->pgWrite(m_b, 0, 8192, csvec, XrdOssDF::Verify);
  ASSERT_TRUE(ret == 8192);
  struct stat sbuff;
  memset(&sbuff,0,sizeof(sbuff));
  m_oss->StatPF(TMPFN, &sbuff, XrdOss::PF_dStat);
  ASSERT_TRUE(sbuff.st_rdev == XrdOss::PF_csVer);
  // downgrade to unverified because of Write
  ret = m_file->Write(m_b, 0, 8192);
  ASSERT_TRUE(ret == 8192);
  memset(&sbuff,0,sizeof(sbuff));
  m_oss->StatPF(TMPFN, &sbuff, XrdOss::PF_dStat);
  ASSERT_TRUE(sbuff.st_rdev == XrdOss::PF_csVun);
  // check still unverified after using pgWrite with csvec
  ret = m_file->pgWrite(m_b, 0, 8192, csvec, XrdOssDF::Verify);
  ASSERT_TRUE(ret == 8192);
  m_oss->StatPF(TMPFN, &sbuff, XrdOss::PF_dStat);
  ASSERT_TRUE(sbuff.st_rdev == XrdOss::PF_csVun);
}

TEST_F(osscsi_verifiedTest,downgrade3) {
  uint32_t csvec[2],csvec2[2];
  XrdOucCRC::Calc32C((void *)m_b, 8192, csvec);
  ssize_t ret = m_file->pgWrite(m_b, 0, 8192, csvec, XrdOssDF::Verify);
  ASSERT_TRUE(ret == 8192);
  struct stat sbuff;
  memset(&sbuff,0,sizeof(sbuff));
  m_oss->StatPF(TMPFN, &sbuff, XrdOss::PF_dStat);
  ASSERT_TRUE(sbuff.st_rdev == XrdOss::PF_csVer);
  // not downgraded because of doCalc on pgWrite
  ret = m_file->pgWrite(m_b, 0, 8192, csvec2, XrdOssDF::doCalc);
  ASSERT_TRUE(ret == 8192);
  memset(&sbuff,0,sizeof(sbuff));
  m_oss->StatPF(TMPFN, &sbuff, XrdOss::PF_dStat);
  ASSERT_TRUE(sbuff.st_rdev == XrdOss::PF_csVer);
  ASSERT_TRUE(memcmp(csvec, csvec2, 8)==0);
  // still not downgraded because of doCalc on pgWrite
  ret = m_file->pgWrite(m_b, 0, 8192, NULL, XrdOssDF::doCalc);
  ASSERT_TRUE(ret == 8192);
  memset(&sbuff,0,sizeof(sbuff));
  m_oss->StatPF(TMPFN, &sbuff, XrdOss::PF_dStat);
  ASSERT_TRUE(sbuff.st_rdev == XrdOss::PF_csVer);
}

TEST_F(osscsi_verifiedTest,nochecksums) {
  uint32_t csvec[2],csvec2[2];
  uint8_t buf[8192];
  XrdOucCRC::Calc32C((void *)m_b, 8192, csvec);
  ssize_t ret = m_file->pgWrite(m_b, 0, 8192, csvec, XrdOssDF::Verify);
  ASSERT_TRUE(ret == 8192);
  closefile();
  unlink(TMPFN ".xrdt");
  // missing is allowed by default
  int iret = openfile(O_RDWR);
  ASSERT_TRUE(iret == XrdOssOK);
  ret = m_file->pgRead(buf, 0, 8192, csvec2, XrdOssDF::Verify);
  ASSERT_TRUE(ret == 8192);
  ASSERT_TRUE(memcmp(buf, m_b, 8192)==0);
  ASSERT_TRUE(memcmp(csvec, csvec2, 8) == 0);
  struct stat sbuff;
  memset(&sbuff,0,sizeof(sbuff));
  m_oss->StatPF(TMPFN, &sbuff, XrdOss::PF_dStat);
  ASSERT_TRUE(sbuff.st_rdev == 0);
}

TEST_F(osscsi_verifiedTest,nochecksumsnomissing) {
  uint32_t csvec[2],csvec2[2];
  uint8_t buf[8192];
  XrdOucCRC::Calc32C((void *)m_b, 8192, csvec);
  ssize_t ret = m_file->pgWrite(m_b, 0, 8192, csvec, XrdOssDF::Verify);
  ASSERT_TRUE(ret == 8192);
  closefile();
  unlink(TMPFN ".xrdt");
  closeplugin();

  // use plugin without allowing for missing tags
  openplugin(false);

  // open existing datafile with missing tag.
  int iret = openfile(O_RDWR);
  ASSERT_TRUE(iret == -EDOM);

  // try to create datafile: expect to fail best datafile does exist
  iret = openfile(O_RDWR|O_CREAT|O_EXCL);
  ASSERT_TRUE(iret == -EEXIST);

  // try to optionally create file: create isn't needed for the datafile
  // but the tag file is missing: however it should not be created
  iret = openfile(O_RDWR|O_CREAT);
  ASSERT_TRUE(iret == -EDOM);
  struct stat sbuff;
  iret = stat(TMPFN ".xrdt",&sbuff);
  const int err = errno;
  ASSERT_TRUE(iret<0 && err==ENOENT);
  closefile();
  closeplugin();
  // allow missing tag files again
  openplugin(true);
  iret = openfile(O_RDWR);
  ASSERT_TRUE(iret == XrdOssOK);
  ret = m_file->pgRead(buf, 0, 8192, csvec2, XrdOssDF::Verify);
  ASSERT_TRUE(ret == 8192);
  ASSERT_TRUE(memcmp(buf, m_b, 8192)==0);
  ASSERT_TRUE(memcmp(csvec, csvec2, 8)==0);
}

} // namespace integrationTests

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
