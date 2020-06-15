#include "XrdOss/XrdOss.hh"
#include "XrdOss/XrdOssDefaultSS.hh"
#include "XrdOuc/XrdOucEnv.hh"
#include "XrdSys/XrdSysLogger.hh"
#include "XrdVersion.hh"

#include <gtest/gtest.h>

#include <iostream>
#include <dlfcn.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <cstdint>

#define TMPFN "/tmp/xrdossintegrity_testfile"

namespace integrationTests {

class ossintegrity_pageTest : public ::testing::Test {
protected:

  virtual void SetUp() {
    m_fdnull = open("/dev/null", O_WRONLY);
    ASSERT_TRUE(m_fdnull >= 0);

    const char *config_fn = nullptr;
    XrdSysLogger logger(m_fdnull,0);

    XrdVERSIONINFODEF(v, "testint", XrdVNUMBER,XrdVERSION);
    XrdOss *ossP = XrdOssDefaultSS(&logger, config_fn, v);

    m_libp = dlopen("libXrdOssIntegrity-5.so",RTLD_NOW|RTLD_GLOBAL);
    ASSERT_TRUE( m_libp != nullptr );

    XrdOssAddStorageSystem2_t oss2P=nullptr;
    oss2P = reinterpret_cast<XrdOssAddStorageSystem2_t>(dlsym(m_libp, "XrdOssAddStorageSystem2"));

    ASSERT_TRUE( oss2P != nullptr );

    m_oss = oss2P(ossP, &logger, config_fn, "", &m_env);
    ASSERT_TRUE(m_oss != nullptr );

    m_file = m_oss->newFile("mytesttid");
    ASSERT_TRUE(m_file != nullptr);
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
    m_file = nullptr;
    m_oss = nullptr;
    dlclose(m_libp);
    m_libp = nullptr;
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
  void *m_libp;
  XrdOucEnv m_env;
  XrdOss *m_oss;
  XrdOssDF *m_file;
  uint8_t m_b[4096*4];
  bool m_fileopen;
};

TEST_F(ossintegrity_pageTest,onepage) {
  ssize_t ret = m_file->Write(m_b, 0, 4096);
  ASSERT_TRUE(ret == 4096);
  uint8_t rbuf[4096];
  uint32_t csvec[1];
  ret = m_file->pgRead(rbuf, 0, 4096, csvec, XrdOssDF::Verify);
  ASSERT_TRUE(ret == 4096);
  ASSERT_TRUE( memcmp(rbuf,m_b,4096)==0 );
  ASSERT_TRUE(csvec[0] == 0x353125d0);
}

TEST_F(ossintegrity_pageTest,twopages) {
  ssize_t ret = m_file->Write(m_b, 0, 8192);
  ASSERT_TRUE(ret == 8192);
  uint8_t rbuf[8192];
  uint32_t csvec[2];
  ret = m_file->pgRead(rbuf, 0, 8192, csvec, XrdOssDF::Verify);
  ASSERT_TRUE(ret == 8192);
  ASSERT_TRUE( memcmp(rbuf,m_b,8192)==0 );
  ASSERT_TRUE(csvec[0] == 0x353125d0);
  ASSERT_TRUE(csvec[1] == 0x68547dba);
}

TEST_F(ossintegrity_pageTest,oneandpartpage) {
  ssize_t ret = m_file->Write(m_b, 0, 6143);
  ASSERT_TRUE(ret == 6143);
  uint8_t rbuf[8192];
  uint32_t csvec[2];
  ret = m_file->pgRead(rbuf, 0, 8192, csvec, XrdOssDF::Verify);
  ASSERT_TRUE(ret == 6143);
  ASSERT_TRUE( memcmp(rbuf,m_b,6143)==0 );
  ASSERT_TRUE(csvec[0] == 0x353125d0);
  ASSERT_TRUE(csvec[1] == 0x7bf5fca1);
}

TEST_F(ossintegrity_pageTest,upperpartpage) {
  ssize_t ret = m_file->Write(&m_b[2049], 2049, 2047);
  ASSERT_TRUE(ret == 2047);
  uint8_t rbuf[4096];
  uint32_t csvec[1];
  ret = m_file->pgRead(rbuf, 0, 4096, csvec, XrdOssDF::Verify);
  ASSERT_TRUE(ret == 4096);
  uint8_t cbuf[4096];
  memset(cbuf,0,2049);
  memcpy(&cbuf[2049],&m_b[2049],2047);
  ASSERT_TRUE( memcmp(rbuf,cbuf,4096)==0 );
  ASSERT_TRUE(csvec[0] == 0xfe965ca0);
}

TEST_F(ossintegrity_pageTest,pagewithhole) {
  ssize_t ret = m_file->Write(m_b, 0, 1024);
  ASSERT_TRUE(ret == 1024);
  ret = m_file->Write(&m_b[2048], 2048, 2048);
  ASSERT_TRUE(ret == 2048);
  uint8_t rbuf[4096];
  uint32_t csvec[1];
  ret = m_file->pgRead(rbuf, 0, 4096, csvec, XrdOssDF::Verify);
  ASSERT_TRUE(ret == 4096);
  uint8_t cbuf[4096];
  memcpy(cbuf, m_b, 1024);
  memset(&cbuf[1024],0,1024);
  memcpy(&cbuf[2048],&m_b[2048],2048);
  ASSERT_TRUE(memcmp(rbuf,cbuf,4096) == 0);
  ASSERT_TRUE(csvec[0] == 0xf573261e);
}

TEST_F(ossintegrity_pageTest,pagewithholefilled) {
  ssize_t ret = m_file->Write(m_b, 0, 1024);
  ASSERT_TRUE(ret == 1024);
  ret = m_file->Write(&m_b[2048], 2048, 2048);
  ASSERT_TRUE(ret == 2048);
  ret = m_file->Write(&m_b[1024], 1024, 1024);
  ASSERT_TRUE(ret == 1024);
  uint8_t rbuf[4096];
  uint32_t csvec[1];
  ret = m_file->pgRead(rbuf, 0, 4096, csvec, XrdOssDF::Verify);
  ASSERT_TRUE(ret == 4096);
  ASSERT_TRUE(memcmp(rbuf,m_b,4096) == 0);
  ASSERT_TRUE(csvec[0] == 0x353125d0);
}

TEST_F(ossintegrity_pageTest,extendtotwo) {
  ssize_t ret = m_file->Write(m_b, 0, 2047);
  ASSERT_TRUE(ret == 2047);
  ret = m_file->Write(&m_b[4096], 4096, 2049);
  ASSERT_TRUE(ret == 2049);
  uint8_t rbuf[8192];
  uint32_t csvec[2];
  ret = m_file->pgRead(rbuf, 0, 8192, csvec, XrdOssDF::Verify);
  ASSERT_TRUE(ret == 6145);
  uint8_t cbuf[6145];
  memcpy(cbuf, m_b, 2047);
  memset(&cbuf[2047],0,2049);
  memcpy(&cbuf[4096],&m_b[4096],2049);
  ASSERT_TRUE( memcmp(rbuf,cbuf,6145)==0);
  ASSERT_TRUE(csvec[0] == 0xe7625dd6);
  ASSERT_TRUE(csvec[1] == 0x3e455e47);
}

TEST_F(ossintegrity_pageTest,threepartial) {
  ssize_t ret = m_file->Write(&m_b[2049], 2049, 8193);
  ASSERT_TRUE(ret == 8193);
  uint8_t rbuf[12288];
  uint32_t csvec[3];
  ret = m_file->pgRead(rbuf, 0, 12288, csvec, XrdOssDF::Verify);
  ASSERT_TRUE(ret == 10242);
  uint8_t cbuf[10242];
  memset(cbuf,0,2049);
  memcpy(&cbuf[2049],&m_b[2049],8193);
  ASSERT_TRUE( memcmp(rbuf, cbuf, 10242) == 0);
  ASSERT_TRUE(csvec[0] == 0xfe965ca0);
  ASSERT_TRUE(csvec[1] == 0x68547dba);
  ASSERT_TRUE(csvec[2] == 0x8bb57f35);
}

TEST_F(ossintegrity_pageTest,threepartial2) {
  ssize_t ret = m_file->Write(&m_b[2049], 2049, 8193);
  ASSERT_TRUE(ret == 8193);
  ret = m_file->Write(&m_b[10],10,10);
  ASSERT_TRUE(ret == 10);
  ret = m_file->Write(&m_b[12268], 12268, 10);
  ASSERT_TRUE(ret == 10);
  uint8_t rbuf[12288];
  uint32_t csvec[3];
  ret = m_file->pgRead(rbuf, 0, 12288, csvec, XrdOssDF::Verify);
  ASSERT_TRUE(ret == 12278);
  uint8_t cbuf[12278];
  memset(cbuf,0,12278);
  memcpy(&cbuf[10],&m_b[10],10);
  memcpy(&cbuf[2049],&m_b[2049],8193);
  memcpy(&cbuf[12268],&m_b[12268],10);
  ASSERT_TRUE( memcmp(rbuf, cbuf, 12278) == 0);
  ASSERT_TRUE(csvec[0] == 0x0f1c284f);
  ASSERT_TRUE(csvec[1] == 0x68547dba);
  ASSERT_TRUE(csvec[2] == 0xb851d608);
}

TEST_F(ossintegrity_pageTest,readpartial) {
  ssize_t ret = m_file->Write(m_b, 0, 16384);
  ASSERT_TRUE(ret == 16384);
  uint8_t rbuf[12289];
  ret = m_file->Read(rbuf, 2049, 12289);
  ASSERT_TRUE(ret == 12289);
  ASSERT_TRUE(memcmp(rbuf,&m_b[2049],12289)==0);
}

TEST_F(ossintegrity_pageTest,extendwrite) {
  ssize_t ret = m_file->Write(m_b,0,4000);
  ASSERT_TRUE(ret == 4000);
  ret = m_file->Write(&m_b[4200], 4200, 10);
  ASSERT_TRUE(ret == 10);
  ret = m_file->Write(&m_b[4096], 4096, 4096);
  ASSERT_TRUE(ret == 4096);
  ret = m_file->Write(&m_b[12288], 12288, 4096);
  ASSERT_TRUE(ret == 4096);
  ret = m_file->Write(&m_b[5000], 16384, 50);
  ASSERT_TRUE(ret == 50);
  ret = m_file->Write(&m_b[6000], 16434, 50);
  ASSERT_TRUE(ret == 50);
  uint8_t rbuf[16484];
  ret = m_file->Read(rbuf,0,16484);
  ASSERT_TRUE(ret == 16484);
  uint8_t cbuf[16484];
  memset(cbuf,0,16484);
  memcpy(cbuf,m_b,4000);
  memcpy(&cbuf[4096], &m_b[4096],4096);
  memcpy(&cbuf[12288], &m_b[12288],4096);
  memcpy(&cbuf[16384], &m_b[5000], 50);
  memcpy(&cbuf[16434], &m_b[6000], 50);
  ASSERT_TRUE(memcmp(cbuf,rbuf,16484)==0);
}

TEST_F(ossintegrity_pageTest,badcrc) {
  uint32_t csvec[4] = { 0x1, 0x2, 0x3, 0x4 };
  ssize_t ret = m_file->pgWrite(m_b, 0, 16384, csvec, XrdOssDF::Verify);
  ASSERT_TRUE(ret == -EDOM);
  ret = m_file->pgWrite(m_b, 0,	16384, csvec, 0);
  ASSERT_TRUE(ret == 16384);
  uint8_t rbuf[16384];
  uint32_t csvec2[4];
  ret = m_file->pgRead(rbuf, 0, 16384, csvec2, 0);
  ASSERT_TRUE(ret == 16384);
  ASSERT_TRUE(memcmp(csvec, csvec2, 4*4) == 0);
  ret = m_file->pgRead(rbuf, 0,	16384, csvec2, XrdOssDF::Verify);
  ASSERT_TRUE(ret == -EDOM);
  ret = m_file->pgWrite(&m_b[4096], 4096, 4096, nullptr, 0);
  ASSERT_TRUE(ret == 4096);
  ret = m_file->pgRead(rbuf, 4096, 4096, csvec2, XrdOssDF::Verify);
  ASSERT_TRUE(ret == 4096);
  ASSERT_TRUE(memcmp(rbuf, &m_b[4096], 4096)==0);
  ASSERT_TRUE(csvec2[0] == 0x68547dba);
  ret = m_file->pgRead(rbuf, 0, 16384, csvec2, 0);
  ASSERT_TRUE(ret == 16384);
  ASSERT_TRUE(memcmp(rbuf, m_b, 16384)==0);
  ASSERT_TRUE(csvec2[0] == 0x1);
  ASSERT_TRUE(csvec2[1] == 0x68547dba);
  ASSERT_TRUE(csvec2[2] == 0x3);
  ASSERT_TRUE(csvec2[3] == 0x4);
  ret = m_file->Write(m_b,0,100);
  ASSERT_TRUE(ret == -EDOM);
  ret = m_file->Write(&m_b[4096],4096,100);
  ASSERT_TRUE(ret == 100);
  ret = m_file->Write(&m_b[8192],8192,8192);
  ASSERT_TRUE(ret == 8192);
  ret = m_file->pgRead(rbuf, 0, 16384, csvec2, 0);
  ASSERT_TRUE(ret == 16384);
  ASSERT_TRUE(memcmp(rbuf, m_b, 16384)==0);
  ASSERT_TRUE(csvec2[0] == 0x1);
  ASSERT_TRUE(csvec2[1] == 0x68547dba);
  ASSERT_TRUE(csvec2[2] == 0x210896db);
  ASSERT_TRUE(csvec2[3] == 0x2d2b98b0);
  ret = m_file->pgRead(rbuf, 4096, 12288, csvec, XrdOssDF::Verify);
  ASSERT_TRUE(ret == 12288);
  ASSERT_TRUE(memcmp(csvec,&csvec2[1],3*4)==0);
}

TEST_F(ossintegrity_pageTest,truncate) {
  ssize_t ret = m_file->Ftruncate(16384);
  ASSERT_TRUE(ret == 0);
  uint8_t rbuf[16384];
  ret = m_file->Read(rbuf, 0, 16384);
  ASSERT_TRUE(ret == 16384);
  uint8_t rbuf2[16384];
  memset(rbuf2, 0, 16384);
  ASSERT_TRUE(memcmp(rbuf,rbuf2,16384)==0);
  ret = m_file->Write(&m_b[10000], 10000, 100);
  ASSERT_TRUE(ret == 100);
  ret = m_file->Ftruncate(10050);
  ASSERT_TRUE(ret == 0);
  ret = m_file->Ftruncate(10100);
  ASSERT_TRUE(ret == 0);
  uint32_t csvec[1];
  ret = m_file->pgRead(rbuf, 8192, 4096, csvec, XrdOssDF::Verify);
  ASSERT_TRUE(ret == 1908);
  memset(rbuf2, 0, 1908);
  memcpy(&rbuf2[1808], &m_b[10000], 50);
  ASSERT_TRUE(memcmp(rbuf, rbuf2, 1908)==0);
  ASSERT_TRUE(csvec[0] == 0x45b62822);
}

TEST_F(ossintegrity_pageTest,partialwrite) {
  ssize_t ret = m_file->Write(m_b, 0, 12289);
  ASSERT_TRUE(ret == 12289);
  ret = m_file->Write(&m_b[2049], 2047, 8194);
  ASSERT_TRUE(ret == 8194);
  uint8_t rbuf[12289];
  ret = m_file->Read(rbuf, 0, 12289);
  ASSERT_TRUE(ret == 12289);
  uint8_t cbuf[12289];
  memcpy(cbuf,m_b,12289);
  memcpy(&cbuf[2047],&m_b[2049],8194);
  ASSERT_TRUE(memcmp(rbuf,cbuf,12289)==0);
}

TEST_F(ossintegrity_pageTest,pgwriteverifyabort) {
  ssize_t ret = m_file->Write(m_b, 0, 12288);
  ASSERT_TRUE(ret == 12288);
  uint8_t buf[20480];
  memset(buf,0,4097);
  ret = m_file->Write(buf, 12288, 4097);
  ASSERT_TRUE(ret == 4097);
  uint32_t csvec[5];
  ret = m_file->pgRead(buf, 0, 20480, csvec, XrdOssDF::Verify);
  ASSERT_TRUE(ret == 16385);
  memset(buf,0,12288);
  csvec[0] = csvec[3];
  csvec[1] = csvec[3];
  ret = m_file->pgWrite(buf, 8192, 12288, csvec, XrdOssDF::Verify);
  ASSERT_TRUE(ret == -EDOM);
  ret = m_file->Read(buf, 0, 20480);
  ASSERT_TRUE(ret == 16385);
  ASSERT_TRUE(memcmp(buf, m_b, 12288) == 0);
  csvec[2] = csvec[3];
  memset(buf,0,12288);
  ret = m_file->pgWrite(buf, 0, 12288, csvec, XrdOssDF::Verify);
  ASSERT_TRUE(ret == 12288);
}

} // namespace integrationTests

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
