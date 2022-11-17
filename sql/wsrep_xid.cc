/* Copyright 2019 Codership Oy <http://www.codership.com>

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02111-1301 USA
 */

//! @file some utility functions and classes not directly related to replication

#include "wsrep_xid.h"
#include "my_byteorder.h" // int8store
#include <wsrep.h>
#include "sql_class.h"
#include "sql_plugin.h"
#include "mysql/plugin.h"
#include "handler.h"

#include <mysql/service_wsrep.h>

#include <algorithm> /* std::sort() */

void wsrep_xid_init(XID* xid, const wsrep::gtid& wsgtid, const wsrep_local_gtid_t& gtid)
{
  xid->reset();

  xid->set_format_id(1);
  xid->set_gtrid_length(WSREP_XID_GTRID_LEN_V_3);
  xid->set_bqual_length(0);

  char data[XIDDATASIZE];

  memset(data, 0, XIDDATASIZE);
  memcpy(data, WSREP_XID_PREFIX, WSREP_XID_PREFIX_LEN);
  data[WSREP_XID_VERSION_OFFSET] = WSREP_XID_VERSION_3;
  memcpy(data + WSREP_XID_UUID_OFFSET,  wsgtid.id().data(),sizeof(wsrep::id));
  int8store(data + WSREP_XID_SEQNO_OFFSET, wsgtid.seqno().get());
  memcpy(data + WSREP_XID_RPL_GTID_OFFSET, &gtid, sizeof(wsrep_local_gtid_t));
  xid->set_data(data, XIDDATASIZE);
}

extern "C"
int wsrep_is_wsrep_xid(const void* xid_ptr)
{
  const XID* xid = reinterpret_cast<const XID*>(xid_ptr);
  const unsigned char version = (xid->get_data() + WSREP_XID_VERSION_OFFSET)[0];
  return (xid->get_format_id()     == 1                   &&
          xid->get_bqual_length()  == 0                   &&
          !memcmp(xid->get_data(), WSREP_XID_PREFIX, WSREP_XID_PREFIX_LEN) &&
          (((version == WSREP_XID_VERSION_1                         ||
             version == WSREP_XID_VERSION_2)                        &&
            xid->get_gtrid_length()  == WSREP_XID_GTRID_LEN_V_1_2)  ||
           (version == WSREP_XID_VERSION_3                          &&
            xid->get_gtrid_length()  == WSREP_XID_GTRID_LEN_V_3)));
}

const unsigned char* wsrep_xid_uuid(const xid_t* xid)
{
  assert(xid);
  static wsrep::id const undefined;
  if (wsrep_is_wsrep_xid(xid))
    return reinterpret_cast<const unsigned char*>
      (xid->get_data() + WSREP_XID_UUID_OFFSET);
  else
    return static_cast<const unsigned char*>(wsrep::id::undefined().data());
}

const wsrep::id& wsrep_xid_uuid(const XID& xid)
{
  static_assert(sizeof(wsrep::id) == sizeof(wsrep_uuid_t),
                "wsrep::id size mismatch");
  return *reinterpret_cast<const wsrep::id*>(wsrep_xid_uuid(&xid));
}

long long wsrep_xid_seqno(const xid_t* xid)
{
  assert(xid);
  long long ret = wsrep::seqno::undefined().get();
  if (wsrep_is_wsrep_xid(xid)) {
    uchar version= (xid->get_data() + WSREP_XID_VERSION_OFFSET)[0];
    switch (version) {
    case WSREP_XID_VERSION_1:
      memcpy(&ret, xid->get_data() + WSREP_XID_SEQNO_OFFSET, sizeof ret);
      break;
    case WSREP_XID_VERSION_2:
    case WSREP_XID_VERSION_3:
      ret = sint8korr(xid->get_data() + WSREP_XID_SEQNO_OFFSET);
      break;
    default:
      break;
    }
  }
  return ret;
}

wsrep::seqno wsrep_xid_seqno(const XID& xid)
{
  return wsrep::seqno(wsrep_xid_seqno(&xid));
}

struct set_SE_checkpoint_arg
{
  XID *xid;
  bool full_sync;
};

static bool set_SE_checkpoint(THD* thd MY_ATTRIBUTE((unused)),
                              plugin_ref plugin, void* arg_ptr)
{
  const set_SE_checkpoint_arg* arg=
    static_cast<set_SE_checkpoint_arg*>(arg_ptr);
  const XID* xid= arg->xid;
  const bool full_sync= arg->full_sync;
  handlerton* hton= plugin_data<handlerton*>(plugin);

  if (hton->wsrep_set_checkpoint) {
    const unsigned char* uuid = wsrep_xid_uuid(xid);
    char uuid_str[40] = {0, };
    wsrep_uuid_print((const wsrep_uuid_t*)uuid, uuid_str, sizeof(uuid_str));
    WSREP_DEBUG("Set WSREPXid for InnoDB:  %s:%lld",
                uuid_str, (long long)wsrep_xid_seqno(xid));
    hton->wsrep_set_checkpoint(hton, xid, full_sync);
  }
  return false;
}

bool wsrep_set_SE_checkpoint(XID& xid, bool full_sync)
{
  set_SE_checkpoint_arg arg{ &xid, full_sync };
  return plugin_foreach(NULL, set_SE_checkpoint, MYSQL_STORAGE_ENGINE_PLUGIN,
                        &arg);
}

bool wsrep_set_SE_checkpoint(const wsrep::gtid& wsgtid,
                             const wsrep_local_gtid_t& gtid, bool full_sync)
{
  XID xid;
  wsrep_xid_init(&xid, wsgtid, gtid);
  return wsrep_set_SE_checkpoint(xid, full_sync);
}

static bool get_SE_checkpoint(THD* thd MY_ATTRIBUTE((unused)), plugin_ref plugin, void* arg)
{
  XID* xid = reinterpret_cast<XID*>(arg);
  handlerton* hton = plugin_data<handlerton*>(plugin);

  if (hton->wsrep_get_checkpoint) {
    hton->wsrep_get_checkpoint(hton, xid);
    wsrep_uuid_t uuid;
    memcpy(&uuid, wsrep_xid_uuid(xid), sizeof(uuid));
    char uuid_str[40]= {0, };
    wsrep_uuid_print(&uuid, uuid_str, sizeof(uuid_str));
    WSREP_DEBUG("Read WSREPXid from InnoDB:  %s:%lld",
                uuid_str, (long long)wsrep_xid_seqno(xid));
  }
  return false;
}

bool wsrep_get_SE_checkpoint(XID& xid)
{
  return plugin_foreach(NULL, get_SE_checkpoint, MYSQL_STORAGE_ENGINE_PLUGIN,
                        &xid);
}

static bool wsrep_get_SE_checkpoint_common(XID& xid)
{
  xid.set_format_id(-1);
  if (wsrep_get_SE_checkpoint(xid))
    return false;
  if (xid.get_format_id() == -1 || xid.is_null())
    return false;
  if (!wsrep_is_wsrep_xid(&xid)) {
    WSREP_WARN("Read non-wsrep XID from storage engines.");
    return false;
  }
  return true;
}

template<>
wsrep::gtid wsrep_get_SE_checkpoint()
{
  XID xid;
  if (!wsrep_get_SE_checkpoint_common(xid))
    return wsrep::gtid();
  return wsrep::gtid(wsrep_xid_uuid(xid),wsrep_xid_seqno(xid));
}

template<>
wsrep_local_gtid_t wsrep_get_SE_checkpoint()
{
  XID xid;
  wsrep_local_gtid_t gtid;
  if (!wsrep_get_SE_checkpoint_common(xid))
    return wsrep_local_gtid_manager.undefined();
  unsigned char version = (xid.get_data() + WSREP_XID_VERSION_OFFSET)[0];
  if (version == WSREP_XID_VERSION_3)
    memcpy(&gtid, xid.get_data() + WSREP_XID_RPL_GTID_OFFSET, sizeof(wsrep_local_gtid_t));
  return gtid;
}

/*
  Sort order for XIDs. Wsrep XIDs are sorted according to
  seqno in ascending order. Non-wsrep XIDs are considered
  equal among themselves and greater than with respect
  to wsrep XIDs.
 */
struct Wsrep_xid_cmp
{
  bool operator()(const struct st_xarecover_txn& left,
                  const struct st_xarecover_txn& right) const
  {
    const bool left_is_wsrep= wsrep_is_wsrep_xid(&left.id);
    const bool right_is_wsrep= wsrep_is_wsrep_xid(&right.id);
    if (left_is_wsrep && right_is_wsrep)
      return (wsrep_xid_seqno(&left.id) < wsrep_xid_seqno(&right.id));
    else if (left_is_wsrep)
      return true;
    else
      return false;
  }
};

void wsrep_sort_xid_array(struct st_xarecover_txn *array, int len)
{
  std::sort(array, array + len, Wsrep_xid_cmp());
}
