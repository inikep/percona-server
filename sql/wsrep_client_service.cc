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
#include "wsrep_server_state.h"
#include "wsrep_client_service.h"
#include "wsrep_high_priority_service.h"
#include "wsrep_binlog.h"  /* wsrep_dump_rbr_buf() */
#include "wsrep_schema.h"  /* remove_fragments() */
#include "wsrep_thd.h"
#include "wsrep_xid.h"
#include "wsrep_trans_observer.h"

#include "sql_base.h"    /* close_temporary_table() */
#include "sql_class.h"   /* THD */
#include "sql_parse.h"   /* stmt_causes_implicit_commit() */
#include "rpl_filter.h"  /* binlog_filter */
#include "rpl_rli.h"     /* Relay_log_info */
#include "transaction.h" /* trans_commit()... */
#include "log.h"      /* stmt_has_updated_trans_table() */
#include "log_event.h"
#include "binlog.h" // binlog_cache_mngr
#include "debug_sync.h"

namespace
{


void debug_sync_caller(THD* thd __attribute__((unused)),
                       const char* sync_point __attribute__((unused)))
{
#ifdef ENABLED_DEBUG_SYNC
  if (opt_debug_sync_timeout)
    debug_sync(thd, sync_point, strlen(sync_point));
#endif /* ENABLED_DEBUG_SYNC */
}
}

Wsrep_client_service::Wsrep_client_service(THD* thd,
                                           Wsrep_client_state& client_state)
  : wsrep::client_service()
  , m_thd(thd)
  , m_client_state(client_state)
{ }

void Wsrep_client_service::store_globals()
{
  wsrep_store_threadvars(m_thd);
}

void Wsrep_client_service::reset_globals()
{
  wsrep_reset_threadvars(m_thd);
}

bool Wsrep_client_service::interrupted(
  wsrep::unique_lock<wsrep::mutex>& lock WSREP_UNUSED) const
{
  assert(m_thd == current_thd);
  mysql_mutex_lock(&m_thd->LOCK_thd_data);
  bool ret= (m_thd->killed != THD::NOT_KILLED);
  mysql_mutex_unlock(&m_thd->LOCK_thd_data);

  if (ret)
  {
#ifdef OUT
      WSREP_DEBUG("wsrep state is interrupted, THD::killed %d trx state %d",
                  m_thd->killed,  m_thd->wsrep_trx().state());
#endif
  }
  return ret;
}

int Wsrep_client_service::prepare_data_for_replication()
{
  assert(m_thd == current_thd);
  DBUG_ENTER("Wsrep_client_service::prepare_data_for_replication");
  size_t data_len= 0;

  if (wsrep_prepare_data_for_replication(m_thd, &data_len)) {
    DBUG_RETURN(1);
  }

  if (data_len == 0)
  {
    if (m_thd->get_stmt_da()->is_ok()              &&
        m_thd->get_stmt_da()->affected_rows() > 0  &&
        !binlog_filter->is_on() &&
        !m_thd->wsrep_trx().is_streaming())
    {
      WSREP_DEBUG("empty rbr buffer, query: %s, "
                  "affected rows: %llu, "
                  "changed tables: %d, "
                  "sql_log_bin: %d",
                  WSREP_QUERY(m_thd),
                  m_thd->get_stmt_da()->affected_rows(),
                  stmt_has_updated_trans_table(m_thd->get_transaction()->ha_trx_info(Transaction_ctx::STMT)),
                  m_thd->variables.sql_log_bin);
    }
    else
    {
      WSREP_DEBUG("empty rbr buffer, query: %s", WSREP_QUERY(m_thd));
    }
  }
  DBUG_RETURN(0);
}


void Wsrep_client_service::cleanup_transaction()
{
  assert(m_thd == current_thd);
  if (WSREP_EMULATE_BINLOG(m_thd)) wsrep_thd_binlog_trx_reset(m_thd);
  m_thd->wsrep_affected_rows= 0;
}

int Wsrep_client_service::prepare_fragment_for_replication(
  wsrep::mutable_buffer &buffer, size_t &log_position)
{
  assert(m_thd == current_thd);
  DBUG_ENTER("Wsrep_client_service::prepare_fragment_for_replication");
  if (wsrep_prepare_fragment_for_replication(m_thd, buffer))
  {
    DBUG_RETURN(1);
  }
  log_position = wsrep_get_binlog_cache_size(m_thd);
  DBUG_RETURN(0);
}

int Wsrep_client_service::remove_fragments()
{
  DBUG_ENTER("Wsrep_client_service::remove_fragments");
  debug_sync_caller(m_thd, "wsrep_before_fragment_removal");
  if (wsrep_schema->remove_fragments(m_thd,
                                     Wsrep_server_state::instance().id(),
                                     m_thd->wsrep_trx().id(),
                                     m_thd->wsrep_sr().fragments()))
  {
    WSREP_DEBUG("Failed to remove fragments from SR storage for transaction "
                "%u, %llu",
                m_thd->thread_id(), m_thd->wsrep_trx().id().get());
    DBUG_RETURN(1);
  }
  DBUG_RETURN(0);
}

bool Wsrep_client_service::statement_allowed_for_streaming() const
{
  /*
    Todo: Decide if implicit commit is allowed with streaming
    replication.
    !stmt_causes_implicit_commit(m_thd, CF_IMPLICIT_COMMIT_BEGIN);
  */
  return true;
}

size_t Wsrep_client_service::bytes_generated() const
{
  size_t pending_rows_event_length= 0;
  if (Rows_log_event* ev= m_thd->binlog_get_pending_rows_event(true))
  {
    pending_rows_event_length= ev->get_data_size();
  }
  return wsrep_get_binlog_cache_size(m_thd) + pending_rows_event_length;
}

void Wsrep_client_service::will_replay()
{
  assert(m_thd == current_thd);
  mysql_mutex_lock(&LOCK_wsrep_replaying);
  ++wsrep_replaying;
  mysql_mutex_unlock(&LOCK_wsrep_replaying);
}

void Wsrep_client_service::signal_replayed()
{
  assert(m_thd == current_thd);
  mysql_mutex_lock(&LOCK_wsrep_replaying);
  --wsrep_replaying;
  assert(wsrep_replaying >= 0);
  mysql_cond_broadcast(&COND_wsrep_replaying);
  mysql_mutex_unlock(&LOCK_wsrep_replaying);
}

extern int init_wsrep_thread(THD*);
extern void deinit_wsrep_thread(THD*);

enum wsrep::provider::status Wsrep_client_service::replay()
{

  assert(m_thd == current_thd);
  DBUG_ENTER("Wsrep_client_service::replay");

  /*
    Allocate separate THD for replaying to avoid tampering
    original THD state during replication event applying.
   */
  THD *replayer_thd= new THD(true, true);
  replayer_thd->thread_stack= m_thd->thread_stack;
  init_wsrep_thread(replayer_thd);

  enum wsrep::provider::status ret;
  {
    Wsrep_replayer_service replayer_service(replayer_thd, m_thd);
    wsrep::provider& provider(replayer_thd->wsrep_cs().provider());
    ret= provider.replay(replayer_thd->wsrep_trx().ws_handle(),
                         &replayer_service);
    replayer_service.replay_status(ret);
  }

  wsrep_reset_threadvars(replayer_thd);
  deinit_wsrep_thread(replayer_thd);
  delete replayer_thd;
  wsrep_store_threadvars(m_thd);

  DBUG_RETURN(ret);
}

enum wsrep::provider::status Wsrep_client_service::replay_unordered()
{
  assert(0);
  return wsrep::provider::error_not_implemented;
}

void Wsrep_client_service::wait_for_replayers(wsrep::unique_lock<wsrep::mutex>& lock)
{
  assert(m_thd == current_thd);
  lock.unlock();
  mysql_mutex_lock(&LOCK_wsrep_replaying);
  /* We need to check if the THD is BF aborted during condition wait.
     Because the aborter does not know which condition this thread is waiting,
     use timed wait and check if the THD is BF aborted in the loop. */
  while (wsrep_replaying > 0 && !wsrep_is_bf_aborted(m_thd))
  {
    struct timespec wait_time;
    set_timespec_nsec(&wait_time, 10000000L);
    mysql_cond_timedwait(&COND_wsrep_replaying, &LOCK_wsrep_replaying,
                         &wait_time);
  }
  mysql_mutex_unlock(&LOCK_wsrep_replaying);
  lock.lock();
}

enum wsrep::provider::status Wsrep_client_service::commit_by_xid()
{
  assert(0);
  return wsrep::provider::error_not_implemented;
}

void Wsrep_client_service::debug_sync(const char* sync_point)
{
  assert(m_thd == current_thd);
  debug_sync_caller(m_thd, sync_point);
}

void Wsrep_client_service::debug_crash(const char* crash_point
                                       __attribute__((unused)))
{
  DBUG_EXECUTE_IF(crash_point, DBUG_SUICIDE(); );
}

int Wsrep_client_service::bf_rollback()
{
  assert(m_thd == current_thd);
  DBUG_ENTER("Wsrep_client_service::rollback");

  int ret= (trans_rollback_stmt(m_thd) || trans_rollback(m_thd));
  if (m_thd->locked_tables_mode && m_thd->lock)
  {
    m_thd->locked_tables_list.unlock_locked_tables(m_thd);
    m_thd->variables.option_bits&= ~OPTION_TABLE_LOCK;
  }
  if (m_thd->global_read_lock.is_acquired())
  {
    m_thd->global_read_lock.unlock_global_read_lock(m_thd);
  }
  m_thd->mdl_context.release_transactional_locks();
  m_thd->mdl_context.release_explicit_locks();

  DBUG_RETURN(ret);
}
