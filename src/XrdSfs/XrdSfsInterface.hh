#define SFS_LCLPRFY    "/="
#define SFS_LCLROOT(x) !strncmp(x, SFS_LCLPRFX, SFS_LCLPLEN-1) \
                       && (*(x+SFS_LCLPLEN-1) == '/' || *(x+SFS_LCLPLEN-1) == 0)
{
  (void)Func; (void)csName; (void)path; (void)eInfo; (void)client;
  (void)opaque;
  eInfo.setErrInfo(ENOTSUP, "Not supported.");
  return SFS_ERROR;
}
virtual void           Disc(const XrdSecEntity     *client = 0)
{
  (void)client;
}
                             const XrdSecEntity     *client = 0)
{
  (void)cmd; (void)args; (void)eInfo; (void)client;
  return SFS_OK;
}
//! @param  eInfo  - The object where error info or results are to be returned.
//!                  This is legacy and the error onject may be used as well.
                            const XrdSecEntity     *client = 0)
{
  (void)cmd; (void)alen; (void)args; (void)client;
  return SFS_OK;
}
                                XrdSfsXferSize     size)
{
  (void)sfDio; (void)offset; (void)size;
  return SFS_OK;
}
virtual void           setXio(XrdSfsXio *xioP) { (void)xioP; }
                            {(void)buf;
                             error.setErrInfo(ENOTSUP, "Not supported.");