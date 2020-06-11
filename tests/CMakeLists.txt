add_executable(
  test_page
  testpage.cc)

target_include_directories(
  test_page
  PUBLIC
   ${XROOTD_INCLUDES} )

target_link_libraries(
  test_page
  XrdUtils
  XrdServer
  dl
  gtest )
