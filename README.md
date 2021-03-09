XrdOssCsi
=========

XrdOssCsi is a stacked Oss plugin that adds the filesystem checksum (XRDOSS_HASFSCS)
feature to an Oss by storing per-page CRC32C values as a separate file.
Write() and Read() calls update or check against the stored CRC32C values. pgWrite()
and pgRead() can be used to directly write or read the values.

Usage:
------

e.g. with an Xcache, using 'tags' as the space name for the files
containing the CRC32C values:

```
pfc.osslib ++ /usr/lib64/libXrdOssCsi.so nofill space=tags
```

e.g. on a stanalone server

```
ofs.osslib ++ /usr/lib64/libXrdOssCsi.so
```

Options

```
nofill
The option 'nofill' indicates the XrdOssCsi should skip writing the page
tags with the CRC32C value for implied zero pages: When writing to a point
after the current end of file the space between, a hole, will contain zeros.
Not filling will result in subsequent reads of the hole pages giving checksum
errors. Usually this is not what is wanted, unless it is known that hole pages
should not be read. Partial write of pages implies a read-modify-write, which
would also fail.

nomissing
This option requires the files containing the CRC32C values to exist for
previously existing datafiles with non-zero length. If the tag file does not exist an
error will be returned. The default behaviour if a CRC file does not exist
for an existing datafile is that CRC values will not be written or verfieid as
the datafile is accessed. If CRC values are requested they will be calculated
from the data currently in the datafile. In either case, empty datafiles which
are opened with create or truncation will have their tag files created if needed.

space=name
The Oss space name to be used for the files containing the CRC32C values.

prefix=/directory
The files containing the CRC32C values will be stored under the given base
directory as files ending in ".xrdt". The files are organised in
directories to match the datafiles. By default the base directory is
"/.xrdt". The syntax "prefix=" with no value has the special meaning that
the .xrdt files will be stored inside the same directories as the datafiles.
Accessing or listing the .xrdt files through xrootd is not supported.

nopgextend
This option prevents pgWrite from writing past the current end-of-file
when the file length is not a multiple of the page size. This approximates
the behaviour of an original version of pgWrite. The check is not applied
in case of a missing tagfile.
```
