/* Copyright 2016 Codership Oy <http://www.codership.com>

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA */

#include <wsrep.h>
#include <limits.h>
#include <stdio.h>
#include <string.h>
#include "mysql/plugin.h"

#include "m_ctype.h"
#include "m_string.h"
#include "my_dbug.h"
#include "my_inttypes.h"
#include "my_macros.h"
#include "my_sys.h"

#include "wsrep_mysqld.h"


static int wsrep_plugin_init(void *p MY_ATTRIBUTE((unused)))
{
  WSREP_DEBUG("wsrep_plugin_init()");
  return 0;
}

static int wsrep_plugin_deinit(void *p MY_ATTRIBUTE((unused)))
{
  WSREP_DEBUG("wsrep_plugin_deinit()");
  return 0;
}

struct Mysql_replication wsrep_plugin= {
  MYSQL_REPLICATION_INTERFACE_VERSION
};

mysql_declare_plugin(wsrep)
{
  MYSQL_REPLICATION_PLUGIN,
  &wsrep_plugin,
  "wsrep",
  "Codership Oy",
  "Wsrep replication plugin",
  PLUGIN_LICENSE_GPL,
  wsrep_plugin_init,   /* Plugin Init */
  NULL,                /* Plugin Check uninstall */
  wsrep_plugin_deinit, /* Plugin Deinit */
  0x0100,
  NULL, /* Status variables */
  NULL, /* System variables */
  NULL, /* config options                  */
  0,    /* flags                           */
}
mysql_declare_plugin_end;
