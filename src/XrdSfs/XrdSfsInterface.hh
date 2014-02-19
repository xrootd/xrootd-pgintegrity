// The following macros are used for dealing with special local paths
//
#define SFS_LCLPRFX    "/=/"
#define SFS_LCLPLEN    3
#define SFS_LCLPATH(x) !strncmp(x, SFS_LCLPRFX, SFS_LCLPLEN)

//-----------------------------------------------------------------------------
//! Notify filesystem that a client has disconnected.
//!
//! @param  client - Client's identify (see common description).
//-----------------------------------------------------------------------------

virtual void           Disc(const XrdSecEntity     *client = 0) {}
