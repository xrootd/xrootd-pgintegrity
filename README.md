XrdPfc refetch and a new plugin XrdOssIntegrity
===============================================

XrdPfc
======

XrdPfc (Xcache) in branch pgpfc to extend use of pgRead & pgWrite:

The Xcache module has been modified so that pgRead is used to fetch
data from the origin server in order to cache it. Once XrdCl supports the
method using pgRead should bring the benefit of the integrity check done
by the client using per-page CRC values.

Data is passed to the cache's file system along with the per-page checksum
values by using pgWrite, allowing a file system featuring XRDOSS_HASFSCS
to store those values without recalculation.

On subsequent read from the cache, errors which indicate checksum mismatch
will make the cache treat the specific cache-block as missing. It will be fetched
from the origin server in the usual way for a cache miss.

XrdOssIntegrity
===============

A stackable Oss plugin that adds the filesystem checksum (XRDOSS_HASFSCS)
feature to an Oss by storing per-page CRC32C values as a separate file.
Write() and Read() calls update or check against the stored CRC32C values.

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

nomissing
This option requires the files containing the CRC32C values to exist for previously
existing datafiles. If they do not an error will be returned when tyring to open the datafile.
Conversely, the default behaviour if a CRC file does not exist is that CRC values
will not be written or verfieid as the datafile is accessed. If CRC values are requested
they will be calculated from the data currently in the datafile.

space=name
The Oss space name to be used for the files containing the CRC32C values.
```
