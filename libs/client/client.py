import XRootD
from pyxrootd import client
from XRootD.responses import XRootDStatus, StatInfo, StatInfoVFS, \
                             LocationInfo, DirectoryList, ProtocolInfo

class Client(object):
  # Doing both the fast backend and the pythonic frontend
  # Moving the border of the bindings, more python-heavy
  # Syntactic sugar
  # There are now 2 client objects... hmmm
  # This is more stable and self-documenting
  # Remove keywords in libpyxrootd? Let Python handle them up here?
  # I can put enums and handlers inside the actual class
  # Only unmodifiable immutable objects can live here, there's no way we're
  # reimplementing any methods...
  """The client class
  
  :param url: The URL of the server to connect with
  :type  url: string
  """
  
  def __init__(self, url):
    self.__fs = client.FileSystem(url)

  @property
  def url(self):
    """The server URL object, instance of :mod:`XRootD.client.URL`"""
    return self.__fs.url

  def locate(self, path, flags, timeout=0, callback=None):
    """Locate a file.

    :param  path: path to the file to be located
    :type   path: string
    :param flags: An `ORed` combination of :mod:`XRootD.enums.OpenFlags`
    :returns:     :class:`XRootD.responses.XRootDStatus` and 
    """
    if callback:
      callback = XRootD.client.CallbackWrapper(callback, LocationInfo)
      return XRootDStatus(self.__fs.locate(path, flags, timeout, callback))
    
    status, response = self.__fs.locate(path, flags, timeout)
    if response: response = LocationInfo(response)
    return XRootDStatus(status), response

  def deeplocate(self, path, flags, timeout=0, callback=None):
    """Locate a file, recursively locate all disk servers.

    :param  path: path to the file to be located
    :type   path: string
    :param flags: An `ORed` combination of :mod:`XRootD.enums.OpenFlags`
    :returns:     tuple containing status and location info (see above)
    """
    if callback:
      callback = XRootD.client.CallbackWrapper(callback, LocationInfo)
      return XRootDStatus(self.__fs.deeplocate(path, flags, timeout, callback))
    
    status, response = self.__fs.deeplocate(path, flags, timeout)
    if response: response = LocationInfo(response)
    return XRootDStatus(status), response

  def mv(self, source, dest, timeout=0, callback=None):
    """Move a directory or a file.

    :param source: the file or directory to be moved
    :type  source: string
    :param   dest: the new name
    :type    dest: string
    :returns:      tuple containing status dictionary and None
    """
    if callback:
      callback = XRootD.client.CallbackWrapper(callback, None)
      return XRootDStatus(self.__fs.mv(source, dest, timeout, callback))
    
    status, response = self.__fs.mv(source, dest, timeout)
    return XRootDStatus(status), None
    
  def query(self, querycode, arg, timeout=0, callback=None):
    """Obtain server information.
    
    :param querycode: the query code as specified in
                      :mod:`XRootD.enums.QueryCode`
    :param       arg: query argument
    :type        arg: string
    :returns:         the query response or None if there was an error
    :rtype:           string
    
    .. note::
      For more information about XRootD query codes and arguments, see 
      `the relevant section in the protocol reference 
      <http://xrootd.slac.stanford.edu/doc/prod/XRdv299.htm#_Toc337053385>`_.
    """
    if callback:
      callback = XRootD.client.CallbackWrapper(callback, None)
      return XRootDStatus(self.__fs.query(querycode, arg, timeout, callback))
    
    status, response = self.__fs.query(querycode, arg, timeout)
    return XRootDStatus(status), response
    
  def truncate(self, path, size, timeout=0, callback=None):
    """Truncate a file.
    
    :param path: path to the file to be truncated
    :type  path: string
    :param size: file size
    :type  size: integer
    :returns:    tuple containing status dictionary and None
    """
    if callback:
      callback = XRootD.client.CallbackWrapper(callback, None)
      return XRootDStatus(self.__fs.truncate(path, size, timeout, callback))
    
    status, response = self.__fs.truncate(path, size, timeout)
    return XRootDStatus(status), None
    
  def rm(self, path, timeout=0, callback=None):
    """Remove a file.
    
    :param path: path to the file to be removed
    :type  path: string
    :returns:    tuple containing status dictionary and None
    """
    if callback:
      callback = XRootD.client.CallbackWrapper(callback, None)
      return XRootDStatus(self.__fs.rm(path, timeout, callback))
    
    status, response = self.__fs.rm(path, timeout)
    return XRootDStatus(status), None

  def mkdir(self, path, flags=0, mode=0, timeout=0, callback=None):
    """Create a directory.
    
    :param  path: path to the directory to create
    :type   path: string
    :param flags: An `ORed` combination of :mod:`XRootD.enums.MkDirFlags`
                  where the default is `MkDirFlags.NONE`
    :param  mode: the initial file access mode, an `ORed` combination of
                  :mod:`XRootD.enums.AccessMode` where the default is
                  `AccessMode.NONE`
    :returns:     tuple containing status dictionary and None
    """
    if callback:
      callback = XRootD.client.CallbackWrapper(callback, None)
      return XRootDStatus(self.__fs.mkdir(path, flags, mode, timeout, callback))
    
    status, response = self.__fs.mkdir(path, flags, mode, timeout)
    return XRootDStatus(status), None
    
  def rmdir(self, path, timeout=0, callback=None):
    """Remove a directory.
    
    :param path: path to the directory to remove
    :type  path: string
    :returns:    tuple containing status dictionary and None
    """
    if callback:
      callback = XRootD.client.CallbackWrapper(callback, None)
      return XRootDStatus(self.__fs.rmdir(path, timeout, callback))
    
    status, response = self.__fs.rmdir(path, timeout)
    return XRootDStatus(status), None
      
  def chmod(self, path, mode=0, timeout=0, callback=None):
    """Change access mode on a directory or a file.
    
    :param path: path to the file/directory to change access mode
    :type  path: string
    :param mode: An `OR`ed` combination of :mod:`XRootD.enums.AccessMode`
                 where the default is `AccessMode.NONE`
    :returns:    tuple containing status dictionary and None
    """
    if callback:
      callback = XRootD.client.CallbackWrapper(callback, None)
      return XRootDStatus(self.__fs.chmod(path, mode, timeout, callback))
    
    status, response = self.__fs.chmod(path, mode, timeout)
    return XRootDStatus(status), None
    
  def ping(self, timeout=0, callback=None):
    """Check if the server is alive.
    
    :returns: tuple containing status dictionary and None
    """
    if callback:
      callback = XRootD.client.CallbackWrapper(callback, None)
      return XRootDStatus(self.__fs.ping(timeout, callback))
    
    status, response = self.__fs.ping(timeout)
    return XRootDStatus(status), None
    
  def stat(self, path, timeout=0, callback=None):
    """Obtain status information for a path.
    
    :param path: path to the file/directory to stat
    :type  path: string
    :returns:    tuple containing status dictionary and stat info
                 dictionary (see below)
    """
    if callback:
      callback = XRootD.client.CallbackWrapper(callback, StatInfo)
      return XRootDStatus(self.__fs.stat(path, timeout, callback))
    
    status, response = self.__fs.stat(path, timeout)
    if response: response = StatInfo(response)
    return XRootDStatus(status), response
  
  def statvfs(self, path, timeout=0, callback=None):
    """Obtain status information for a Virtual File System.
    
    :param path: path to the file/directory to stat
    :type  path: string
    :returns:    tuple containing status dictionary and statvfs info
                 dictionary (see below)
    """
    if callback:
      callback = XRootD.client.CallbackWrapper(callback, StatInfoVFS)
      return XRootDStatus(self.__fs.statvfs(path, timeout, callback))
    
    status, response = self.__fs.statvfs(path, timeout)
    if response: response = StatInfoVFS(response)
    return XRootDStatus(status), response
    
  def protocol(self, timeout=0, callback=None):
    """Obtain server protocol information.
    
    :returns: tuple containing status dictionary and protocol info
              dictionary (see below)
    """
    if callback:
      callback = XRootD.client.CallbackWrapper(callback, ProtocolInfo)
      return XRootDStatus(self.__fs.protocol(timeout, callback))
    
    status, response = self.__fs.protocol(timeout)
    if response: response = ProtocolInfo(response)
    return XRootDStatus(status), response
    
  def dirlist(self, path, flags=0, timeout=0, callback=None):
    """List entries of a directory.
    
    :param  path: path to the directory to list
    :type   path: string
    :param flags: An `ORed` combination of :mod:`XRootD.enums.DirListFlags`
                  where the default is `DirListFlags.NONE`
    :returns:     tuple containing status dictionary and directory
                  list info dictionary (see below)
    """
    if callback:
      callback = XRootD.client.CallbackWrapper(callback, DirectoryList)
      return XRootDStatus(self.__fs.dirlist(path, flags, timeout, callback))
    
    status, response = self.__fs.dirlist(path, flags, timeout)
    if response: response = DirectoryList(response)
    return XRootDStatus(status), response
    
  def sendinfo(self, info, timeout=0, callback=None):
    """Send info to the server (up to 1024 characters).
    
    :param info: the info string to be sent
    :type  info: string
    :returns:    tuple containing status dictionary and None
     """
    if callback:
      callback = XRootD.client.CallbackWrapper(callback, None)
      return XRootDStatus(self.__fs.sendinfo(info, timeout, callback))
    
    status, response = self.__fs.sendinfo(info, timeout)
    return XRootDStatus(status), response
    
  def prepare(self, files, flags, priority=0, timeout=0, callback=None):
    """Prepare one or more files for access.
    
    :param    files: list of files to be prepared
    :type     files: list
    :param    flags: An `ORed` combination of
                     :mod:`XRootD.enums.PrepareFlags`
    :param priority: priority of the request 0 (lowest) - 3 (highest)
    :type  priority: integer
    :returns:        tuple containing status dictionary and None
     """
    if callback:
      callback = XRootD.client.CallbackWrapper(callback, None)
      return XRootDStatus(self.__fs.prepare(files, flags, priority, timeout, 
                                            callback))
    
    status, response = self.__fs.prepare(files, flags, priority, timeout)
    return XRootDStatus(status), response

