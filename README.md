Modified XrdPfc and a new plugin XrdOssIntegrity
================================================

XrdPfc
======

Modified XrdPfc (Xcache) to extend use of pgRead & pgWrite:

pgRead is used to fetch data from the origin server for the purpose of
caching. Doing so should bring the benefit for non-TLS connections of the
the integrity check done by the client using per-page CRC values.

Data is passed to the cache's file system along with the per-page checksum
values by using pgWrite, allowing a file system featuring XRDOSS_HASFSCS
to store those values without recalculation.

On subsequent read from the cache errors which indicate cehcksum mismatch, or
related errors from a filesystem featuring XRDOSS_HASFSCS, will make
the cache consider the specific cache-block invalid. It will be fetched
from the origin server, in the usual way as for a cache miss.

XrdOssIntegrity
===============

A stackable Oss plugin that adds the filesystem checksum (XRDOSS_HASFSCS)
feature to an Oss by storing per-page CRC32C values as a separate file as
well as the data file.

Usage:
------

e.g. with an Xcache, specifying 'tags' as the space name for the files
containing the CRC32C values:

```
pfc.osslib ++ /usr/lib64/libXrdOssIntegrity.so nofill space=tags
```

e.g. on a stanalone server

```
ofs.osslib ++ /usr/lib64/libXrdOssIntegrity.so
```
