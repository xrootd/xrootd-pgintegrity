#ifndef __XRDPFC_CACHE_HH__
#define __XRDPFC_CACHE_HH__
//----------------------------------------------------------------------------------
// Copyright (c) 2014 by Board of Trustees of the Leland Stanford, Jr., University
// Author: Alja Mrak-Tadel, Matevz Tadel, Brian Bockelman
//----------------------------------------------------------------------------------
// XRootD is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// XRootD is distributed in the hope that it will be useful,
// but WITHOUT ANY emacs WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with XRootD.  If not, see <http://www.gnu.org/licenses/>.
//----------------------------------------------------------------------------------
#include <string>
#include <list>
#include <map>
#include <set>

#include "Xrd/XrdScheduler.hh"
#include "XrdVersion.hh"
#include "XrdSys/XrdSysPthread.hh"
#include "XrdOuc/XrdOucCache.hh"
#include "XrdOuc/XrdOucCallBack.hh"
#include "XrdCl/XrdClDefaultEnv.hh"

#include "XrdPfcFile.hh"
#include "XrdPfcDecision.hh"

class XrdOucStream;
class XrdSysError;
class XrdSysTrace;

namespace XrdCl
{
class Log;
}

namespace XrdPfc
{
class File;
class IO;

class DataFsState;
}


namespace XrdPfc
{

//----------------------------------------------------------------------------
//! Contains parameters configurable from the xrootd config file.
//----------------------------------------------------------------------------
struct Configuration
{
   Configuration() :
      m_hdfsmode(false),
      m_allow_xrdpfc_command(false),
      m_data_space("public"),
      m_meta_space("public"),
      m_diskTotalSpace(-1),
      m_diskUsageLWM(-1),
      m_diskUsageHWM(-1),
      m_fileUsageBaseline(-1),
      m_fileUsageNominal(-1),
      m_fileUsageMax(-1),
      m_purgeInterval(300),
      m_purgeColdFilesAge(-1),
      m_purgeColdFilesPeriod(-1),
      m_accHistorySize(20),
      m_dirStatsMaxDepth(-1),
      m_dirStatsStoreDepth(-1),
      m_dirStats(false),
      m_bufferSize(1024*1024),
      m_RamAbsAvailable(0),
      m_NRamBuffers(-1),
      m_wqueue_blocks(16),
      m_wqueue_threads(4),
      m_prefetch_max_blocks(10),
      m_hdfsbsize(128*1024*1024),
      m_flushCnt(2000)
   {}

   bool are_file_usage_limits_set()    const { return m_fileUsageMax > 0; }
   bool is_age_based_purge_in_effect() const { return m_purgeColdFilesAge > 0; }
   bool is_purge_plugin_set_up()       const { return false; }

   void calculate_fractional_usages(long long du, long long fu, double &frac_du, double &frac_fu);

   // This might become more complicated with per-dir purge policy
   bool are_dirstats_enabled() const { return m_dirStats; }

   bool m_hdfsmode;                     //!< flag for enabling block-level operation
   bool m_allow_xrdpfc_command;         //!< flag for enabling access to /xrdpfc-command/ functionality.

   std::string m_username;              //!< username passed to oss plugin
   std::string m_data_space;            //!< oss space for data files
   std::string m_meta_space;            //!< oss space for metadata files (cinfo)

   long long m_diskTotalSpace;          //!< total disk space on configured partition or oss space
   long long m_diskUsageLWM;            //!< cache purge - disk usage low water mark
   long long m_diskUsageHWM;            //!< cache purge - disk usage high water mark
   long long m_fileUsageBaseline;       //!< cache purge - files usage baseline
   long long m_fileUsageNominal;        //!< cache purge - files usage nominal
   long long m_fileUsageMax;            //!< cache purge - files usage maximum
   int       m_purgeInterval;           //!< sleep interval between cache purges
   int       m_purgeColdFilesAge;       //!< purge files older than this age
   int       m_purgeColdFilesPeriod;    //!< peform cold file purge every this many purge cycles
   int       m_accHistorySize;          //!< max number of entries in access history part of cinfo file

   std::set<std::string> m_dirStatsDirs;     //!< directories for which stat reporting was requested
   std::set<std::string> m_dirStatsDirGlobs; //!< directory globs for which stat reporting was requested
   int       m_dirStatsMaxDepth;        //!< maximum depth for statistics write out
   int       m_dirStatsStoreDepth;      //!< depth to which statistics should be collected
   bool      m_dirStats;                //!< is directory access / usage statistics enabled

   long long m_bufferSize;              //!< prefetch buffer size, default 1MB
   long long m_RamAbsAvailable;         //!< available from configuration
   int       m_NRamBuffers;             //!< number of total in-memory cache blocks, cached
   int       m_wqueue_blocks;           //!< maximum number of blocks written per write-queue loop
   int       m_wqueue_threads;          //!< number of threads writing blocks to disk
   int       m_prefetch_max_blocks;     //!< maximum number of blocks to prefetch per file

   long long m_hdfsbsize;               //!< used with m_hdfsmode, default 128MB
   long long m_flushCnt;                //!< nuber of unsynced blcoks on disk before flush is called
};

//------------------------------------------------------------------------------

struct TmpConfiguration
{
   std::string m_diskUsageLWM;
   std::string m_diskUsageHWM;
   std::string m_fileUsageBaseline;
   std::string m_fileUsageNominal;
   std::string m_fileUsageMax;
   std::string m_flushRaw;

   TmpConfiguration() :
      m_diskUsageLWM("0.90"), m_diskUsageHWM("0.95"),
      m_flushRaw("")
   {}
};

//==============================================================================

struct SplitParser
{
   char       *str;
   const char *delim;
   char       *state;
   bool        first;

   SplitParser(const std::string &s, const char *d) :
      str(strdup(s.c_str())), delim(d), state(0), first(true)
   {}
   ~SplitParser() { free(str); }

   char* get_token()
   {
      if (first) { first = false; return strtok_r(str, delim, &state); }
      else       { return strtok_r(0, delim, &state); }
   }

   char* get_reminder_with_delim()
   {
      if (first) { return str; }
      else       { *(state - 1) = delim[0]; return state - 1; }
   }

   char *get_reminder()
   {
      return first ? str : state;
   }

   int fill_argv(std::vector<char*> &argv)
   {
      if (!first) return 0;
      int dcnt = 0; { char *p = str; while (*p) { if (*(p++) == delim[0]) ++dcnt; } }
      argv.reserve(dcnt + 1);
      int argc = 0;
      char *i = strtok_r(str, delim, &state);
      while (i)
      {
         ++argc;
         argv.push_back(i);
         // printf("  arg %d : '%s'\n", argc, i);
         i = strtok_r(0, delim, &state);
      }
      return argc;
   }
};

struct PathTokenizer : private SplitParser
{
   std::vector<const char*>  m_dirs;
   const char               *m_reminder;
   int                       m_n_dirs;

   PathTokenizer(const std::string &path, int max_depth, bool parse_as_lfn) :
      SplitParser(path, "/"),
      m_reminder (0)
   {
      // If parse_as_lfn is true store final token into reminder, regardless of maxdepth.
      // This assumes the last token is a file name (and full path if lfn, including the file name).

      m_dirs.reserve(max_depth);

      char *t;
      for (int i = 0; i < max_depth; ++i)
      {
         t = get_token();
         if (t == 0) break;
         m_dirs.emplace_back(t);
      }
      if (parse_as_lfn && (t == 0 || * get_reminder() == 0))
      {
         m_reminder = m_dirs.back();
         m_dirs.pop_back();
      }
      else
      {
         m_reminder = get_reminder();
      }
      m_n_dirs = (int) m_dirs.size();
   }

   int get_n_dirs()
   {
      return m_n_dirs;
   }

   const char *get_dir(int pos)
   {
      if (pos >= m_n_dirs) return 0;
      return m_dirs[pos];
   }

   std::string make_path()
   {
      std::string res;
      for (std::vector<const char*>::iterator i = m_dirs.begin(); i != m_dirs.end(); ++i)
      {
         res += "/";
         res += *i;
      }
      if (m_reminder != 0)
      {
         res += "/";
         res += m_reminder;
      }
      return res;
   }

   void deboog()
   {
      printf("PathTokenizer::deboog size=%d\n", m_n_dirs);
      for (int i = 0; i < m_n_dirs; ++i)
      {
         printf("   %2d: %s\n", i, m_dirs[i]);
      }
      printf("  rem: %s\n", m_reminder);
   }
};


//==============================================================================
// Cache
//==============================================================================

//----------------------------------------------------------------------------
//! Attaches/creates and detaches/deletes cache-io objects for disk based cache.
//----------------------------------------------------------------------------
class Cache : public XrdOucCache
{
public:
   //---------------------------------------------------------------------
   //! Constructor
   //---------------------------------------------------------------------
   Cache(XrdSysLogger *logger);

   //---------------------------------------------------------------------
   //! Obtain a new IO object that fronts existing XrdOucCacheIO.
   //---------------------------------------------------------------------
   using XrdOucCache::Attach;

   virtual XrdOucCacheIO *Attach(XrdOucCacheIO *, int Options = 0);

   //---------------------------------------------------------------------
   // Virtual function of XrdOucCache. Used for redirection to a local
   // file on a distributed FS.
   virtual int LocalFilePath(const char *url, char *buff=0, int blen=0,
                             LFP_Reason why=ForAccess, bool forall=false);

   //---------------------------------------------------------------------
   // Virtual function of XrdOucCache. Used for deferred open.
   virtual int  Prepare(const char *url, int oflags, mode_t mode);

   // virtual function of XrdOucCache.
   virtual int  Stat(const char *url, struct stat &sbuff);

   // virtual function of XrdOucCache.
   virtual int  Unlink(const char *url);

   //--------------------------------------------------------------------
   //! \brief Makes decision if the original XrdOucCacheIO should be cached.
   //!
   //! @param & URL of file
   //!
   //! @return decision if IO object will be cached.
   //--------------------------------------------------------------------
   bool Decide(XrdOucCacheIO*);

   //------------------------------------------------------------------------
   //! Reference XrdPfc configuration
   //------------------------------------------------------------------------
   const Configuration& RefConfiguration() const { return m_configuration; }

   //---------------------------------------------------------------------
   //! \brief Parse configuration file
   //!
   //! @param config_filename    path to configuration file
   //! @param parameters         optional parameters to be passed
   //!
   //! @return parse status
   //---------------------------------------------------------------------
   bool Config(const char *config_filename, const char *parameters);

   //---------------------------------------------------------------------
   //! Singleton creation.
   //---------------------------------------------------------------------
   static Cache &CreateInstance(XrdSysLogger *logger);

  //---------------------------------------------------------------------
   //! Singleton access.
   //---------------------------------------------------------------------
   static Cache &GetInstance();

   //---------------------------------------------------------------------
   //! Version check.
   //---------------------------------------------------------------------
   static bool VCheck(XrdVersionInfo &urVersion) { return true; }

   //---------------------------------------------------------------------
   //! Thread function running disk cache purge periodically.
   //---------------------------------------------------------------------
   void Purge();

   //---------------------------------------------------------------------
   //! Remove file from cache unless it is currently open.
   //---------------------------------------------------------------------
   int  UnlinkUnlessOpen(const std::string& f_name);

   //---------------------------------------------------------------------
   //! Add downloaded block in write queue.
   //---------------------------------------------------------------------
   void AddWriteTask(Block* b, bool from_read);

   //---------------------------------------------------------------------
   //!  \brief Remove blocks from write queue which belong to given prefetch.
   //! This method is used at the time of File destruction.
   //---------------------------------------------------------------------
   void RemoveWriteQEntriesFor(File *f);

   //---------------------------------------------------------------------
   //! Separate task which writes blocks from ram to disk.
   //---------------------------------------------------------------------
   void ProcessWriteTasks();

   bool RequestRAMBlock();

   void RAMBlockReleased();

   void RegisterPrefetchFile(File*);
   void DeRegisterPrefetchFile(File*);

   File* GetNextFileToPrefetch();

   void Prefetch();

   XrdOss* GetOss() const { return m_output_fs; }

   bool IsFileActiveOrPurgeProtected(const std::string&);
   
   File* GetFile(const std::string&, IO*, long long off = 0, long long filesize = 0);

   void  ReleaseFile(File*, IO*);

   void ScheduleFileSync(File* f) { schedule_file_sync(f, false, false); }

   void FileSyncDone(File*, bool high_debug);
   
   XrdSysError* GetLog()   { return &m_log;  }
   XrdSysTrace* GetTrace() { return m_trace; }

   void ExecuteCommandUrl(const std::string& command_url);

   static XrdScheduler *schedP;

private:
   bool ConfigParameters(std::string, XrdOucStream&, TmpConfiguration &tmpc);
   bool ConfigXeq(char *, XrdOucStream &);
   bool xdlib(XrdOucStream &);
   bool xtrace(XrdOucStream &);

   bool cfg2bytes(const std::string &str, long long &store, long long totalSpace, const char *name);

   int  UnlinkCommon(const std::string& f_name, bool fail_if_open);

   static Cache        *m_factory;      //!< this object

   XrdSysError       m_log;             //!< XrdPfc namespace logger
   XrdSysTrace      *m_trace;
   const char       *m_traceID;

   XrdOucCacheStats  m_ouc_stats;       //!<
   XrdOss           *m_output_fs;       //!< disk cache file system

   std::vector<XrdPfc::Decision*> m_decisionpoints;       //!< decision plugins

   std::map<std::string, long long> m_filesInQueue;

   Configuration m_configuration;           //!< configurable parameters

   XrdSysCondVar m_prefetch_condVar;        //!< lock for vector of prefetching files
   bool          m_prefetch_enabled;        //!< set to true when prefetching is enabled

   XrdSysMutex m_RAMblock_mutex;            //!< lock for allcoation of RAM blocks
   int         m_RAMblocks_used;
   bool        m_isClient;                  //!< True if running as client

   struct WriteQ
   {
      WriteQ() : condVar(0), writes_between_purges(0), size(0) {}

      XrdSysCondVar     condVar;      //!< write list condVar
      std::list<Block*> queue;        //!< container
      long long         writes_between_purges; //!< upper bound on amount of bytes written between two purge passes
      int               size;         //!< current size of write queue
   };

   WriteQ m_writeQ;

   // active map, purge delay set
   typedef std::map<std::string, File*>               ActiveMap_t;
   typedef ActiveMap_t::iterator                      ActiveMap_i;
   typedef std::multimap<std::string, XrdPfc::Stats>  StatsMMap_t;
   typedef StatsMMap_t::iterator                      StatsMMap_i;
   typedef std::set<std::string>                      FNameSet_t;

   ActiveMap_t      m_active;
   StatsMMap_t      m_closed_files_stats;
   FNameSet_t       m_purge_delay_set;
   bool             m_in_purge;
   XrdSysCondVar    m_active_cond;

   void inc_ref_cnt(File*, bool lock, bool high_debug);
   void dec_ref_cnt(File*, bool high_debug);

   void schedule_file_sync(File*, bool ref_cnt_already_set, bool high_debug);

   // prefetching
   typedef std::vector<File*>  PrefetchList;
   PrefetchList m_prefetchList;

   // directory state for access / usage info and quotas
   DataFsState *m_fs_state;

   void copy_out_active_stats_and_update_data_fs_state();
};

}

#endif
