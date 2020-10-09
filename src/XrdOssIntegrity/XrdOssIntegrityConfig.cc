/******************************************************************************/
/*                                                                            */
/*             X r d O s s I n t e g r i t y C o n f i g . c c                */
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

#include "XrdOssIntegrityConfig.hh"
#include "XrdOss/XrdOss.hh"

#include <sstream>
#include <string>
#include <vector>

int XrdOssIntegrityConfig::Init(XrdSysLogger *Logger, const char *config_fn, const char *parms, XrdOucEnv *envP)
{
   XrdSysError err(Logger, "");

   err.Say("++++++ Integrity adding OSS layer initialization started.");

   std::stringstream ss(parms ? parms : "");
   std::vector<std::string> tokens;
   std::string item; 
      
   while(std::getline(ss, item, ' '))
   {
      std::string value;
      const auto idx = item.find('=');
      if (idx != std::string::npos)
      {
         value = item.substr(idx+1, std::string::npos);
         item.erase(idx, std::string::npos);
      }
      if (item == "nofill")
      {
         fillFileHole_ = false;
      }
      else if (item == "space" && !value.empty())
      {
         xrdtSpaceName_ = value;
      }
      else if (item == "nomissing")
      {
         allowMissingTags_ = false;
      }
   }

   err.Say("       compute file holes: ", fillFileHole_ ? "yes" : "no");
   err.Say("       space: ", xrdtSpaceName_.c_str());
   err.Say("       allow files without CRCs: ", allowMissingTags_ ? "yes" : "no");

   err.Say("++++++ Integrity adding OSS layer initialization completed.");

   return XrdOssOK;
}
