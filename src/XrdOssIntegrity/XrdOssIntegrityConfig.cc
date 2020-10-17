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
#include "XrdOssIntegrityTrace.hh"

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <sstream>
#include <string>
#include <vector>

extern XrdOucTrace  OssIntegrityTrace;

#define TS_Xeq(x,m)    if (!strcmp(x,var)) return m(Config, Eroute);

int XrdOssIntegrityConfig::Init(XrdSysError &Eroute, const char *config_fn, const char *parms, XrdOucEnv *envP)
{
   Eroute.Say("++++++ Integrity adding OSS layer initialization started.");

   std::stringstream ss(parms ? parms : "");
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

   OssIntegrityTrace.What = TRACE_Warn;
   if (getenv("XRDDEBUG")) OssIntegrityTrace.What = TRACE_ALL;
   readConfig(Eroute, config_fn);

   Eroute.Say("       compute file holes: ", fillFileHole_ ? "yes" : "no");
   Eroute.Say("       space: ", xrdtSpaceName_.c_str());
   Eroute.Say("       allow files without CRCs: ", allowMissingTags_ ? "yes" : "no");
   Eroute.Say("       trace level: ", std::to_string(OssIntegrityTrace.What).c_str());

   Eroute.Say("++++++ Integrity adding OSS layer initialization completed.");

   return XrdOssOK;
}

int XrdOssIntegrityConfig::readConfig(XrdSysError &Eroute, const char *ConfigFN)
{
   char *var;
   int cfgFD, retc, NoGo = XrdOssOK;
   XrdOucEnv myEnv;
   XrdOucStream Config(&Eroute, getenv("XRDINSTANCE"), &myEnv, "=====> ");

   if( !ConfigFN || !*ConfigFN)
   {
      Eroute.Say("Config warning: config file not specified; defaults assumed.");
      return XrdOssOK;
   }

   if ( (cfgFD = open(ConfigFN, O_RDONLY, 0)) < 0)
   {
      Eroute.Emsg("Config", errno, "open config file", ConfigFN);
      return 1;
   }

   Config.Attach(cfgFD);
   static const char *cvec[] = { "*** ossintegrity plugin config:", 0 };
   Config.Capture(cvec);

   while((var = Config.GetMyFirstWord()))
   {
      if (!strncmp(var, "csi.", 4))
      {
         if (ConfigXeq(var+4, Config, Eroute))
         {
            Config.Echo(); NoGo = 1;
         }
      }
   }

   if ((retc = Config.LastError()))
      NoGo = Eroute.Emsg("Config", retc, "read config file", ConfigFN);

   Config.Close();

   return NoGo;
}

int XrdOssIntegrityConfig::ConfigXeq(char *var, XrdOucStream &Config, XrdSysError &Eroute)
{
   TS_Xeq("trace",         xtrace);
   return 0;
}

int XrdOssIntegrityConfig::xtrace(XrdOucStream &Config, XrdSysError &Eroute)
{
    char *val;
    static struct traceopts {const char *opname; int opval;} tropts[] =
       {
        {"all",      TRACE_ALL},
        {"debug",    TRACE_Debug},
        {"warn",     TRACE_Warn}
       };
    int i, neg, trval = 0, numopts = sizeof(tropts)/sizeof(struct traceopts);

    if (!(val = Config.GetWord()))
       {Eroute.Emsg("Config", "trace option not specified"); return 1;}
    while (val)
         {if (!strcmp(val, "off")) trval = 0;
             else {if ((neg = (val[0] == '-' && val[1]))) val++;
                   for (i = 0; i < numopts; i++)
                       {if (!strcmp(val, tropts[i].opname))
                           {if (neg) trval &= ~tropts[i].opval;
                               else  trval |=  tropts[i].opval;
                            break;
                           }
                       }
                   if (i >= numopts)
                      Eroute.Say("Config warning: ignoring invalid trace option '",val,"'.");
                  }
          val = Config.GetWord();
         }
    OssIntegrityTrace.What = trval;
    return 0;
}
