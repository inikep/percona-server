/* Copyright (C) 2015 Codership Oy <info@codership.com>

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License along
   with this program; if not, write to the Free Software Foundation, Inc.,
   51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA. */

#ifndef WSREP_XID_H
#define WSREP_XID_H

#include <my_config.h>

#ifdef WITH_WSREP

#include "wsrep_mysqld.h"
#include "wsrep/gtid.hpp"

typedef struct xid_t XID;

/*
 * WSREPXid
 */
#define WSREP_XID_PREFIX "WSREPXi"
#define WSREP_XID_PREFIX_LEN 7
#define WSREP_XID_VERSION_OFFSET WSREP_XID_PREFIX_LEN
#define WSREP_XID_VERSION_1 'd'
#define WSREP_XID_VERSION_2 'e'
#define WSREP_XID_VERSION_3 'f'
#define WSREP_XID_UUID_OFFSET 8
#define WSREP_XID_SEQNO_OFFSET (WSREP_XID_UUID_OFFSET + sizeof(wsrep_uuid_t))
#define WSREP_XID_GTRID_LEN_V_1_2 (WSREP_XID_SEQNO_OFFSET + sizeof(wsrep_seqno_t))
#define WSREP_XID_RPL_GTID_OFFSET (WSREP_XID_SEQNO_OFFSET + sizeof(wsrep_seqno_t))
#define WSREP_XID_GTRID_LEN_V_3 (WSREP_XID_RPL_GTID_OFFSET + sizeof(wsrep_local_gtid_t))

void wsrep_xid_init(xid_t*, const wsrep::gtid&, const wsrep_local_gtid_t&);
const wsrep::id& wsrep_xid_uuid(const XID&);
wsrep::seqno wsrep_xid_seqno(const XID&);

template<typename T> T wsrep_get_SE_checkpoint();
bool wsrep_set_SE_checkpoint(const wsrep::gtid& gtid, const wsrep_local_gtid_t&,
                             bool full_sync);
//void wsrep_get_SE_checkpoint(XID&);             /* uncomment if needed */
//void wsrep_set_SE_checkpoint(XID&);             /* uncomment if needed */

void wsrep_sort_xid_array(struct st_xarecover_txn *array, int len);

#endif /* WITH_WSREP */
#endif /* WSREP_UTILS_H */
