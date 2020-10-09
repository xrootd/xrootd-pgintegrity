#ifndef _XRDOSSINTEGRITYCONFIG_H
#define _XRDOSSINTEGRITYCONFIG_H
/******************************************************************************/
/*                                                                            */
/*             X r d O s s I n t e g r i t y C o n f i g . h h                */
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

#include "XrdOuc/XrdOucEnv.hh"
#include "XrdSys/XrdSysLogger.hh"

#include <string>

class XrdOssIntegrityConfig
{
public:

  XrdOssIntegrityConfig(XrdSysLogger *logger) : fillFileHole_(true), xrdtSpaceName_("public"), allowMissingTags_(true), err_(logger, "XrdOssIntegrity_") { }
  ~XrdOssIntegrityConfig() { }

  int Init(XrdSysLogger *, const char *, const char *, XrdOucEnv *);

  bool fillFileHole() const { return fillFileHole_; }

  std::string xrdtSpaceName() const { return xrdtSpaceName_; }

  bool allowMissingTags() const { return allowMissingTags_; }

  XrdSysError &err() { return err_; }

private:
  bool fillFileHole_;
  std::string xrdtSpaceName_;
  bool allowMissingTags_;
  XrdSysError err_;
};

#endif
