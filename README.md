Modified XrdPfc and a new plugin XrdOssIntegrity
================================================

XrdPfc
======

Modified XrdPfc (Xcache) to extend use of pgRead & pgWrite:

Xcache is modified so that pgRead is used to fetch data from the origin
server in order to cache it. Once XrdCl supports the method this should
bring the benefit for non-TLS connections of the integrity check done
by the client using per-page CRC values.

Data is passed to the cache's file system along with the per-page checksum
values by using pgWrite, allowing a file system featuring XRDOSS_HASFSCS
to store those values without recalculation.

On subsequent read from the cache, errors which indicate checksum mismatch, or
related errors from a filesystem featuring XRDOSS_HASFSCS, will make
the cache treat the specific cache-block as missing. It will be fetched
from the origin server in the usual way for a cache miss.

XrdOssIntegrity
===============

A stackable Oss plugin that adds the filesystem checksum (XRDOSS_HASFSCS)
feature to an Oss by storing per-page CRC32C values as a separate file as
well as the data file. Write() and Read() calls update or check against
the stored CRC32C values.

Usage:
------

e.g. with an Xcache, using 'tags' as the space name for the files
containing the CRC32C values:

```
pfc.osslib ++ /usr/lib64/libXrdOssIntegrity.so nofill space=tags
```

e.g. on a stanalone server

```
ofs.osslib ++ /usr/lib64/libXrdOssIntegrity.so
```

Options

```
nofill
When writing to a point after the current end of file the space between, a hole,
will contain zeros. Adding the option 'nofill' indicates the XrdOssIntegrity should
skip writing the page tags with the CRC32C value for the implied zero pages.
Not filling will result in subsequent reads of the hole pages giving checksum
errors. Usually this is not what is wanted, unless it is known that hole pages
should not be read. Partial write of pages implies a read-modify-write, which
would also fail.

space=name
The Oss space name to be used for the files containing the CRC32C values.
```
