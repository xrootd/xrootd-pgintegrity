add_executable(
  test_page
  testpage.cc)

add_executable(
  test_verified
  testverified.cc)

add_executable(
  test_concurrent
  testconcurrent.cc)

target_include_directories(
  test_page
  PUBLIC
   ${XROOTD_INCLUDES} )

target_include_directories(
  test_verified
  PUBLIC
   ${XROOTD_INCLUDES} )

target_include_directories(
  test_concurrent
  PUBLIC
   ${XROOTD_INCLUDES} )

target_link_libraries(
  test_page
  XrdUtils
  XrdServer
  dl
  gtest )

target_link_libraries(
  test_verified
  XrdUtils
  XrdServer
  dl
  gtest )

target_link_libraries(
  test_concurrent
  XrdUtils
  XrdServer
  dl
  pthread
  gtest )
