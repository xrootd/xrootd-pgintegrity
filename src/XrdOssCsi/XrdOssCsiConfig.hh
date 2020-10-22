#ifndef _XRDOSSCSICONFIG_H
#define _XRDOSSCSICONFIG_H
/******************************************************************************/
/*                                                                            */
/*                   X r d O s s C s i C o n f i g . h h                      */
/*                                                                            */
/* (C) Copyright 2020 CERN.                                                   */
/*                                                                            */
/* This file is part of the XRootD software suite.                            */
/*                                                                            */
/* XRootD is free software: you can redistribute it and/or modify it under    */
/* the terms of the GNU Lesser General Public License as published by the     */
/* Free Software Foundation, either version 3 of the License, or (at your     */
/* option) any later version.                                                 */
/*                                                                            */
/* In applying this licence, CERN does not waive the privileges and           */
/* immunities granted to it by virtue of its status as an Intergovernmental   */
/* Organization or submit itself to any jurisdiction.                         */
/*                                                                            */
/* XRootD is distributed in the hope that it will be useful, but WITHOUT      */
/* ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or      */
/* FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public       */
/* License for more details.                                                  */
/*                                                                            */
/* You should have received a copy of the GNU Lesser General Public License   */
/* along with XRootD in a file called COPYING.LESSER (LGPL license) and file  */
/* COPYING (GPL license).  If not, see <http://www.gnu.org/licenses/>.        */
/*                                                                            */
/* The copyright holder's institutional names and contributor's names may not */
/* be used to endorse or promote products derived from this software without  */
/* specific prior written permission of the institution or contributor.       */
/******************************************************************************/

#include "XrdOss/XrdOss.hh"
#include "XrdOuc/XrdOucStream.hh"
#include "XrdOuc/XrdOucEnv.hh"
#include "XrdSys/XrdSysLogger.hh"

#include <string>

class TagPath
{
public:

   TagPath() : prefix_("/.xrdt"), suffix_(".xrdt") { calcPrefixElements(); }
   ~TagPath() { }

   bool isTagFile(const char *path)
   {
      if (!path || !*path) return false;
      const std::string s(path);
      // if prefix_ set, the test is to match if "path" is equal to or a subpath of perfix_
      if (!prefix_.empty())
      {
         if (s.find(prefix_) == 0)
         {
            if (prefix_.length() == s.length()) return true;
            if (s[prefix_.length()] == '/') return true;
         }
         return false;
      }
      // prefix_ not set, test is if "path" ends with suffix_
      const size_t haystack = s.length();
      const size_t needle = suffix_.length();
      if (haystack >= needle && s.substr(haystack-needle, std::string::npos) == suffix_) return true;
      return false;
   }

   int SetPrefix(XrdSysError &Eroute, const std::string &v)
   { 
      prefix_ = v;
      prefixstart_.clear();
      prefixend_.clear();
      if (prefix_.empty()) return XrdOssOK;

      if (v[0] != '/')
      {
         Eroute.Emsg("Config","prefix must be empty or start with /");
         return 1;
      }
      if (v[v.length()-1] == '/')
      {
         Eroute.Emsg("Config","prefix must not end with /");
         return 1;
      }
      calcPrefixElements();
      return XrdOssOK;
   }

   bool hasPrefix() { return !prefix_.empty(); }

   std::string makeBaseDir(const char *path)
   {
      if (!path || !*path || prefix_.empty()) return std::string();
      std::string p(path);
      while(p.length()>0 && p[p.length()-1] == '/') p.erase( p.end()-1 );
      return prefix_ + p;
   }

   bool matchPrefixDir(const char *path)
   {
      if (!path || !*path) return false;
      std::string p(path);
      while(p.length()>1 && p[p.length()-1] == '/') p.erase( p.end()-1 );
      if (prefixstart_ == p) return true;
      return false;
   }

   std::string getPrefixName()
   {
      return prefixend_;
   }

   std::string makeTagFilename(const char *path)
   {
      if (!path || !*path) return std::string();
      std::string p(path);
      while(p.length()>1 && p[p.length()-1] == '/') p.erase( p.end()-1 );
      return prefix_ + p + suffix_;
   }

   std::string prefix_;

private:
   void calcPrefixElements()
   {
      const size_t idx = prefix_.rfind("/");
      prefixstart_ = prefix_.substr(0,idx);
      if (prefixstart_.empty()) prefixstart_="/";
      prefixend_ = prefix_.substr(idx+1,std::string::npos);
   }
      
   std::string prefixstart_;
   std::string prefixend_;
   std::string suffix_;
};

class XrdOssCsiConfig
{
public:

  XrdOssCsiConfig() : fillFileHole_(true), xrdtSpaceName_("public"), allowMissingTags_(true) { }
  ~XrdOssCsiConfig() { }

  int Init(XrdSysError &, const char *, const char *, XrdOucEnv *);

  bool fillFileHole() const { return fillFileHole_; }

  std::string xrdtSpaceName() const { return xrdtSpaceName_; }

  bool allowMissingTags() const { return allowMissingTags_; }

  TagPath tagParam_;

private:
  int readConfig(XrdSysError &, const char *);

  int ConfigXeq(char *, XrdOucStream &, XrdSysError &);

  int xtrace(XrdOucStream &, XrdSysError &);

  bool fillFileHole_;
  std::string xrdtSpaceName_;
  bool allowMissingTags_;
};

#endif
