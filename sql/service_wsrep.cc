/* Copyright 2018 Codership Oy <info@codership.com>

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#include <wsrep.h>
#include "sql_class.h"
#include "debug_sync.h"

#include "mysql/service_wsrep.h"
#include "wsrep/key.hpp"
#include "wsrep_thd.h"
#include "wsrep_trans_observer.h"
#include "wsrep_binlog.h" // wsrep_commit_will_write_binlog

extern "C" bool wsrep_on(const THD *thd)
{
  return bool(WSREP(thd));
}

extern "C" bool wsrep_is_ready()
{
  return bool(!WSREP_ON || wsrep_ready);
}

extern "C" void wsrep_thd_LOCK(THD *thd)
{
  mysql_mutex_lock(&thd->LOCK_thd_data);
}

extern "C" void wsrep_thd_UNLOCK(THD *thd)
{
  mysql_mutex_unlock(&thd->LOCK_thd_data);
}

extern "C" const char* wsrep_thd_client_state_str(const THD *thd)
{
  return wsrep::to_c_string(thd->wsrep_cs().state());
}

extern "C" void wsrep_thd_xid(
    const THD*, void *xid MY_ATTRIBUTE((unused)), size_t)
{
  assert(0);
  return;
}

extern "C" const char* wsrep_thd_client_mode_str(const THD *thd)
{
  return wsrep::to_c_string(thd->wsrep_cs().mode());
}

extern "C" const char* wsrep_thd_transaction_state_str(const THD *thd)
{
  return wsrep::to_c_string(thd->wsrep_cs().transaction().state());
}

extern "C" void wsrep_lock_thd_query(THD *thd, bool lock)
{
  if (lock)
    mysql_mutex_lock(&thd->LOCK_thd_query);
  else
    mysql_mutex_unlock(&thd->LOCK_thd_query);
}

extern "C" const char *wsrep_thd_query(THD *thd)
{
  if (!thd) {
    return NULL;
  }
  return thd->query().str;
}

extern "C" query_id_t wsrep_thd_transaction_id(const THD *thd)
{
  return thd->wsrep_cs().transaction().id().get();
}

extern "C" long long wsrep_thd_trx_seqno(const THD *thd)
{
  const wsrep::client_state& cs= thd->wsrep_cs();
  if (cs.mode() == wsrep::client_state::m_toi)
  {
    return cs.toi_meta().seqno().get();
  }
  else
  {
    return cs.transaction().ws_meta().seqno().get();
  }
}

extern "C" void wsrep_thd_self_abort(THD *thd)
{
  thd->wsrep_cs().bf_abort(wsrep::seqno(0));
}

extern "C" const char* wsrep_get_sr_table_name()
{
  return wsrep_sr_table_name_full;
}

extern "C" bool wsrep_get_debug()
{
  return wsrep_debug;
}

extern "C" bool wsrep_thd_is_local(const THD *thd)
{
  return ((thd->system_thread == NON_SYSTEM_THREAD          ||
	   thd->system_thread == SYSTEM_THREAD_EVENT_WORKER ||
	   thd->system_thread == SYSTEM_THREAD_SLAVE_SQL    ||
	   thd->system_thread == SYSTEM_THREAD_SLAVE_WORKER) &&
	  thd->wsrep_cs().mode() == wsrep::client_state::m_local);
  //  return thd->wsrep_cs().mode() == wsrep::client_state::m_local;
}

extern "C" bool wsrep_thd_is_async_slave(const THD *thd)
{
  return (WSREP_ON && !thd->wsrep_applier &&
          (thd->system_thread == SYSTEM_THREAD_SLAVE_SQL    ||
           thd->system_thread == SYSTEM_THREAD_SLAVE_WORKER));
}

extern "C" bool wsrep_thd_is_applying(const THD *thd)
{
  return thd->wsrep_cs().mode() == wsrep::client_state::m_high_priority;
}

extern "C" bool wsrep_thd_is_toi(const THD *thd)
{
  return thd->wsrep_cs().mode() == wsrep::client_state::m_toi;
}

extern "C" bool wsrep_thd_is_local_toi(const THD *thd)
{
  return thd->wsrep_cs().mode() == wsrep::client_state::m_toi &&
         thd->wsrep_cs().toi_mode() == wsrep::client_state::m_local;

}

extern "C" bool wsrep_thd_is_in_rsu(const THD *thd)
{
  return thd->wsrep_cs().mode() == wsrep::client_state::m_rsu;
}

extern "C" bool wsrep_thd_is_BF(THD *thd, bool sync)
{
  bool status = false;
  if (thd && WSREP(thd))
  {
    if (sync) mysql_mutex_lock(&thd->LOCK_thd_data);
    status = (wsrep_thd_is_applying(thd) || wsrep_thd_is_toi(thd));
    if (sync) mysql_mutex_unlock(&thd->LOCK_thd_data);
  }
  return status;
}

extern "C" bool wsrep_thd_is_SR(const THD *thd)
{
  return thd && thd->wsrep_cs().transaction().is_streaming();
}

extern "C" void wsrep_handle_SR_rollback(THD *bf_thd __attribute__((unused)),
                                         THD *victim_thd)
{
  /*
    Since MySQL 8.0.18 the InnoDB rollback processing has changed
    so that the BF thd context is not available when the locks are
    released for transaction. Therefore the bf_thd should always be
    NULL here in MySQL.
  */
  assert(!bf_thd);
  /*
    We should always be in victim_thd context, either client session is
    rolling back or rollbacker thread should be in control.
  */
  assert(victim_thd);
  assert(current_thd == victim_thd);

  /* Defensive measure to avoid crash in production. */
  if (!victim_thd) return;

  WSREP_DEBUG("handle rollback, for deadlock: thd %u trx_id %lu frags %lu conf %s",
              victim_thd->thread_id(),
              victim_thd->wsrep_trx_id(),
              victim_thd->wsrep_sr().fragments_certified(),
              wsrep_thd_transaction_state_str(victim_thd));
  DEBUG_SYNC(victim_thd, "wsrep_before_SR_rollback");
  wsrep_thd_self_abort(victim_thd);
}

extern "C" bool wsrep_thd_bf_abort(THD *bf_thd, THD *victim_thd,
                                   bool signal)
{
  DBUG_EXECUTE_IF("sync.before_wsrep_thd_abort",
                 {
                   const char act[]=
                     "now "
                     "SIGNAL sync.before_wsrep_thd_abort_reached "
                     "WAIT_FOR signal.before_wsrep_thd_abort";
                   assert(!debug_sync_set_action(bf_thd,
                                                      STRING_WITH_LEN(act)));
                 };);
  bool ret= wsrep_bf_abort(bf_thd, victim_thd);
  /*
    Send awake signal if victim was BF aborted or does not
    have wsrep on. Note that this should never interrupt RSU
    as RSU has paused the provider.
   */
  if ((ret || !wsrep_on(victim_thd)) && signal)
  {
    mysql_mutex_lock(&victim_thd->LOCK_thd_data);
    if (victim_thd->wsrep_aborter && victim_thd->wsrep_aborter != bf_thd->thread_id())
    {
      WSREP_DEBUG("victim is killed already by %u, skipping awake",
                  victim_thd->wsrep_aborter);
      mysql_mutex_unlock(&victim_thd->LOCK_thd_data);
      return false;
    }
    victim_thd->wsrep_aborter = bf_thd->thread_id();

    victim_thd->awake(THD::KILL_QUERY);
    mysql_mutex_unlock(&victim_thd->LOCK_thd_data);
  }
  return ret;
}

extern "C" bool wsrep_thd_skip_locking(const THD *thd)
{
  return thd && thd->wsrep_skip_locking;
}

extern "C" bool wsrep_thd_order_before(const THD *left, const THD *right)
{
  if (left && right &&
      wsrep_thd_trx_seqno(left) > 0 &&
      wsrep_thd_trx_seqno(left) < wsrep_thd_trx_seqno(right)) {
    WSREP_DEBUG("BF conflict, order: %lld %lld\n",
                (long long)wsrep_thd_trx_seqno(left),
                (long long)wsrep_thd_trx_seqno(right));
    return true;
  }
  WSREP_DEBUG("waiting for BF, trx order: %lld %lld\n",
              (long long)wsrep_thd_trx_seqno(left),
              (long long)wsrep_thd_trx_seqno(right));
  return false;
}

extern "C" bool wsrep_thd_is_aborting(const MYSQL_THD thd)
{
  mysql_mutex_assert_owner(&thd->LOCK_thd_data);
  if (thd != 0)
  {
    const wsrep::client_state& cs(thd->wsrep_cs());
    const enum wsrep::transaction::state tx_state(cs.transaction().state());
    switch (tx_state)
    {
    case wsrep::transaction::s_must_abort:
      return (cs.state() == wsrep::client_state::s_exec ||
              cs.state() == wsrep::client_state::s_result);
    case wsrep::transaction::s_aborting:
    case wsrep::transaction::s_aborted:
      return true;
    default:
      return false;
    }
  }
  return false;
}

static inline enum wsrep::key::type
map_key_type(enum Wsrep_service_key_type type)
{
  switch (type)
  {
  case WSREP_SERVICE_KEY_SHARED:    return wsrep::key::shared;
  case WSREP_SERVICE_KEY_REFERENCE: return wsrep::key::reference;
  case WSREP_SERVICE_KEY_UPDATE:    return wsrep::key::update;
  case WSREP_SERVICE_KEY_EXCLUSIVE: return wsrep::key::exclusive;
  }
  return wsrep::key::exclusive;
}

extern "C" int wsrep_thd_append_key(THD *thd,
                                    const struct wsrep_key* key,
                                    int n_keys,
                                    enum Wsrep_service_key_type key_type)
{
  Wsrep_client_state& client_state(thd->wsrep_cs());
  assert(client_state.transaction().active());
  int ret = 0;
  for (int i = 0; i < n_keys && ret == 0; ++i) {
    wsrep::key wsrep_key(map_key_type(key_type));
    for (size_t kp = 0; kp < key[i].key_parts_num; ++kp) {
      wsrep_key.append_key_part(key[i].key_parts[kp].ptr, key[i].key_parts[kp].len);
    }
    ret = client_state.append_key(wsrep_key);
  }
  /*
    In case of `wsrep_gtid_mode` when WS will be replicated, we need to set
    `server_id` for events that are going to be written in IO.
  */
  if (!ret &&  wsrep_gtid_mode &&
      !thd->slave_thread && !wsrep_thd_is_applying(thd))
    thd->server_id = wsrep_local_gtid_manager.server_id;
  return ret;
}

extern "C" void wsrep_commit_ordered(THD *thd)
{
  if (wsrep_is_active(thd) &&
      (thd->wsrep_trx().state() == wsrep::transaction::s_committing) &&
      !wsrep_commit_will_write_binlog(thd))
    thd->wsrep_cs().ordered_commit();
}

extern "C" bool wsrep_thd_set_wsrep_aborter(THD *bf_thd, THD *victim_thd)
{
  if (!bf_thd)
  {
    victim_thd->wsrep_aborter = 0;
    WSREP_DEBUG("wsrep_thd_set_wsrep_aborter resetting wsrep_aborter");
    return false;
  }
  if (victim_thd->wsrep_aborter && victim_thd->wsrep_aborter != bf_thd->thread_id())
  {
    return true;
  }
  victim_thd->wsrep_aborter = bf_thd->thread_id();
  WSREP_DEBUG("wsrep_thd_set_wsrep_aborter setting wsrep_aborter %u",
              victim_thd->wsrep_aborter);
  return false;
}

/*
  Get auto increment variables for THD. Use global settings for
  applier threads.
 */
extern "C" void wsrep_thd_auto_increment_variables(THD* thd,
                                                   unsigned long long* offset,
                                                   unsigned long long* increment)
{
  if (!wsrep_thd_is_local(thd) &&
      thd->wsrep_trx().state() != wsrep::transaction::s_replaying)
  {
    *offset = global_system_variables.auto_increment_offset;
    *increment = global_system_variables.auto_increment_increment;
  }
  else
  {
    *offset = thd->variables.auto_increment_offset;
    *increment = thd->variables.auto_increment_increment;
  }
}

extern "C" void  wsrep_thd_set_PA_unsafe(THD *thd)
{
  if (thd && thd->wsrep_cs().mark_transaction_pa_unsafe())
  {
    WSREP_DEBUG("session does not have active transaction, can not mark as PA unsafe");
  }
}
