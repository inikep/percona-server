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
#include "wsrep_high_priority_service.h"
#include "wsrep_server_state.h"
#include "wsrep_applier.h"
#include "wsrep_binlog.h"
#include "wsrep_schema.h"
#include "wsrep_xid.h"
#include "wsrep_trans_observer.h"

#include "sql_class.h" /* THD */
#include "sql_base.h" /* close_temporary_tables */
#include "transaction.h"
#include "debug_sync.h"
/* RLI */
#include "rpl_rli.h"
#define NUMBER_OF_FIELDS_TO_IDENTIFY_COORDINATOR 1
#define NUMBER_OF_FIELDS_TO_IDENTIFY_WORKER 2
#include "rpl_replica.h"
#include "rpl_mi.h"
#include "rpl_msr.h" /* wsrep replication channel name */
#include "mysql/plugin.h" // thd_proc_info

namespace
{
/*
  Scoped mode for applying non-transactional write sets (TOI)
 */
class Wsrep_non_trans_mode
{
public:
  Wsrep_non_trans_mode(THD* thd, const wsrep::ws_meta& ws_meta)
    : m_thd(thd)
    , m_option_bits(thd->variables.option_bits)
    , m_server_status(thd->server_status)
    , m_tx_priority(thd->tx_priority)
    , m_thd_tx_priority(thd->thd_tx_priority)
  {
    m_thd->variables.option_bits&= ~OPTION_BEGIN;
    m_thd->server_status&= ~SERVER_STATUS_IN_TRANS;
    m_thd->wsrep_cs().enter_toi_mode(ws_meta);
    m_thd->tx_priority= 1;
    m_thd->thd_tx_priority= 1;
  }
  ~Wsrep_non_trans_mode()
  {
    m_thd->variables.option_bits= m_option_bits;
    m_thd->server_status= m_server_status;
    m_thd->tx_priority= m_tx_priority;
    m_thd->thd_tx_priority= m_thd_tx_priority;
    m_thd->wsrep_cs().leave_toi_mode();
  }
private:
  Wsrep_non_trans_mode(const Wsrep_non_trans_mode&);
  Wsrep_non_trans_mode& operator=(const Wsrep_non_trans_mode&);
  THD* m_thd;
  ulonglong m_option_bits;
  uint m_server_status;
  int m_tx_priority;
  int m_thd_tx_priority;
};
}

#include "rpl_info_factory.h"
static Relay_log_info* wsrep_relay_log_init(
   const char* log_fname MY_ATTRIBUTE((unused)))
{
  uint rli_option = INFO_REPOSITORY_DUMMY;
  Relay_log_info *rli= NULL;
  /* using named "wsrep" channel to not interfere with unnamed
     async replication channel
   */
  rli = Rpl_info_factory::create_rli(rli_option, false,
            channel_map.get_wsrep_replication_channel_name(), true);
  if (!rli)
  {
    WSREP_ERROR("Failed to create RLI for wsrep thread, aborting");
    unireg_abort(1);
  }
  rli->set_rli_description_event(
      new Format_description_log_event());

  rli->current_mts_submode= new Mts_submode_wsrep();
  rli->deferred_events_collecting= false;
  return (rli);
}

void wsrep_setup_fk_checks(THD* thd)
{
  if (wsrep_slave_FK_checks == false)
    thd->variables.option_bits|= OPTION_NO_FOREIGN_KEY_CHECKS;
  else
    thd->variables.option_bits&= ~OPTION_NO_FOREIGN_KEY_CHECKS;
}

void wsrep_setup_uk_checks(THD* thd)
{
  if (wsrep_slave_UK_checks == false)
    thd->variables.option_bits|= OPTION_RELAXED_UNIQUE_CHECKS;
  else
    thd->variables.option_bits&= ~OPTION_RELAXED_UNIQUE_CHECKS;
}

static void wsrep_setup_uk_and_fk_checks(THD* thd)
{
  /* Tune FK and UK checking policy. These are reset back to original
     in Wsrep_high_priority_service destructor. */
  wsrep_setup_fk_checks(thd);
  wsrep_setup_uk_checks(thd);
}

static int apply_events(THD*                       thd,
                        Relay_log_info*            rli,
                        const wsrep::const_buffer& data,
                        wsrep::mutable_buffer&     err)
{
  int const ret= wsrep_apply_events(thd, rli, data.data(), data.size());
  if (ret || thd->wsrep_has_ignored_error)
  {
    if (ret)
    {
      wsrep_store_error(thd, err);
    }
    wsrep_dump_rbr_buf_with_header(thd, data.data(), data.size());
  }
  return ret;
}

/****************************************************************************
                         High priority service
*****************************************************************************/

Wsrep_high_priority_service::Wsrep_high_priority_service(THD* thd)
  : wsrep::high_priority_service(Wsrep_server_state::instance())
  , wsrep::high_priority_context(thd->wsrep_cs())
  , m_thd(thd)
  , m_rli()
{
  m_shadow.option_bits  = thd->variables.option_bits;
  m_shadow.server_status= thd->server_status;
  m_shadow.vio          = thd->wsrep_get_vio();
  m_shadow.tx_isolation = thd->variables.transaction_isolation;
  m_shadow.db           = thd->db();
  m_shadow.user_time    = thd->user_time;
  m_shadow.row_count_func= thd->get_row_count_func();
  m_shadow.wsrep_applier= thd->wsrep_applier;
  m_shadow.tx_priority     = thd->tx_priority;
  m_shadow.thd_tx_priority = thd->thd_tx_priority;

  /* Disable general logging on applier threads */
  thd->variables.option_bits |= OPTION_LOG_OFF;
  /* Enable binlogging if opt_log_replica_updates is set */
  if (opt_log_replica_updates)
    thd->variables.option_bits|= OPTION_BIN_LOG;
  else
    thd->variables.option_bits&= ~(OPTION_BIN_LOG);

  thd->wsrep_set_vio(NULL);
  thd->reset_db(NULL_CSTR);
  thd->clear_error();
  thd->variables.transaction_isolation= ISO_READ_COMMITTED;
  thd->tx_isolation          = ISO_READ_COMMITTED;
  thd->tx_priority = 1;
  thd->thd_tx_priority = 1;

  /* Make THD wsrep_applier so that it cannot be killed */
  thd->wsrep_applier= true;

  if (!thd->wsrep_rli) thd->wsrep_rli= wsrep_relay_log_init("wsrep_relay");
  m_rli= m_thd->wsrep_rli;
  m_rli->info_thd= m_thd;
  assert(!m_thd->rli_slave);
  m_thd->rli_slave = m_thd->wsrep_rli;
  thd_proc_info(thd, "wsrep applier idle");
}

Wsrep_high_priority_service::~Wsrep_high_priority_service()
{
  THD* thd= m_thd;
  thd->variables.option_bits = m_shadow.option_bits;
  thd->server_status         = m_shadow.server_status;
  thd->wsrep_set_vio(m_shadow.vio);
  thd->variables.transaction_isolation= m_shadow.tx_isolation;
  thd->user_time             = m_shadow.user_time;
  thd->reset_db(m_shadow.db);
  thd->tx_priority = m_shadow.tx_priority;
  thd->thd_tx_priority = m_shadow.thd_tx_priority;

  assert(thd->rli_slave == thd->wsrep_rli);
  thd->rli_slave = NULL;

  delete thd->wsrep_rli->current_mts_submode;
  thd->wsrep_rli->current_mts_submode = 0;
  //if (thd->wsrep_rli)
  //  delete thd->wsrep_rli->mi;
  delete thd->wsrep_rli;
  thd->wsrep_rli= NULL;
  wsrep_set_apply_format(thd, nullptr); /* Deletes format descrption event */
  thd->set_row_count_func(m_shadow.row_count_func);
  thd->wsrep_applier         = m_shadow.wsrep_applier;
}

int Wsrep_high_priority_service::start_transaction(
  const wsrep::ws_handle& ws_handle, const wsrep::ws_meta& ws_meta)
{
  DBUG_ENTER(" Wsrep_high_priority_service::start_transaction");
  m_thd->reset_for_next_command();
  /* Make sure that the thd is not in mst mode when the applying
     starts to avoid implicit commit in trans_begin(). */
  m_thd->variables.option_bits&= ~(OPTION_BEGIN | OPTION_NOT_AUTOCOMMIT);
  m_thd->server_status&= ~SERVER_STATUS_IN_TRANS;
  /* Calling trans_begin() here is not necessary. In MySQL each write
     set has BEGIN statement as the first event, which will start the
     transaction. */
  DBUG_RETURN(m_thd->wsrep_cs().start_transaction(ws_handle, ws_meta));
}

const wsrep::transaction& Wsrep_high_priority_service::transaction() const
{
  DBUG_ENTER(" Wsrep_high_priority_service::transaction");
  DBUG_RETURN(m_thd->wsrep_trx());
}

int Wsrep_high_priority_service::next_fragment(const wsrep::ws_meta& ws_meta)
{
  DBUG_ENTER(" Wsrep_high_priority_service::next_fragment");
  DBUG_RETURN(m_thd->wsrep_cs().next_fragment(ws_meta));
}

int Wsrep_high_priority_service::adopt_transaction(
  const wsrep::transaction& transaction)
{
  DBUG_ENTER(" Wsrep_high_priority_service::adopt_transaction");
  /* Adopt transaction first to set up transaction meta data for
     trans begin. If trans_begin() fails for some reason, roll back
     the wsrep transaction before return. */
  m_thd->wsrep_cs().adopt_transaction(transaction);
  m_thd->reset_for_next_command();
  int ret= trans_begin(m_thd);
  if (ret)
  {
    m_thd->wsrep_cs().before_rollback();
    m_thd->wsrep_cs().after_rollback();
  }
  DBUG_RETURN(ret);
}


int Wsrep_high_priority_service::append_fragment_and_commit(
  const wsrep::ws_handle& ws_handle,
  const wsrep::ws_meta& ws_meta,
  const wsrep::const_buffer& data,
  const wsrep::xid& xid WSREP_UNUSED)
{
  DBUG_ENTER("Wsrep_high_priority_service::append_fragment_and_commit");
  int ret= start_transaction(ws_handle, ws_meta);
  /*
    Start transaction explicitly to avoid early commit via
    trans_commit_stmt() in append_fragment()
  */
  ret= ret || trans_begin(m_thd);
  ret= ret || wsrep_schema->append_fragment(m_thd,
                                            ws_meta.server_id(),
                                            ws_meta.transaction_id(),
                                            ws_meta.seqno(),
                                            ws_meta.flags(),
                                            data);

  if (!ret)
  {
    ret= m_thd->wsrep_cs().prepare_for_ordering(ws_handle,
                                                ws_meta, true);
  }

  ret= ret || trans_commit(m_thd);
  ret= ret || (m_thd->wsrep_cs().after_applying(), 0);
  m_thd->mdl_context.release_transactional_locks();

  m_thd->mem_root->Clear();

  thd_proc_info(m_thd, "wsrep applier committed");

  DBUG_RETURN(ret);
}

int Wsrep_high_priority_service::remove_fragments(const wsrep::ws_meta& ws_meta)
{
  DBUG_ENTER("Wsrep_high_priority_service::remove_fragments");
  int ret= wsrep_schema->remove_fragments(m_thd,
                                          ws_meta.server_id(),
                                          ws_meta.transaction_id(),
                                          m_thd->wsrep_sr().fragments());
  DBUG_RETURN(ret);
}

int Wsrep_high_priority_service::commit(const wsrep::ws_handle& ws_handle,
                                        const wsrep::ws_meta& ws_meta)
{
  DBUG_ENTER("Wsrep_high_priority_service::commit");
  THD* thd= m_thd;
  assert(thd->wsrep_trx().active());
  thd->wsrep_cs().prepare_for_ordering(ws_handle, ws_meta, true);
  thd_proc_info(thd, "committing");

  const bool is_ordered= !ws_meta.seqno().is_undefined();
  int ret= trans_commit(thd);

  if (ret == 0)
  {
    thd->wsrep_rli->cleanup_context(thd, 0);
  }

  m_thd->mdl_context.release_transactional_locks();

  thd_proc_info(thd, "wsrep applier committed");

  if (!is_ordered)
  {
    m_thd->wsrep_cs().before_rollback();
    m_thd->wsrep_cs().after_rollback();
  }
  else if (m_thd->wsrep_trx().state() == wsrep::transaction::s_executing)
  {
    /*
      Wsrep commit was ordered but it did not go through commit time
      hooks and remains active. Cycle through commit hooks to release
      commit order and to make cleanup happen in after_applying() call.

      This is a workaround for CTAS with empty result set.
    */
    WSREP_DEBUG("Commit not finished for applier %u", thd->thread_id());
    ret= ret || m_thd->wsrep_cs().before_commit() ||
      m_thd->wsrep_cs().ordered_commit() ||
      m_thd->wsrep_cs().after_commit();
  }

  thd->lex->sql_command= SQLCOM_END;

  thd->mem_root->Clear();

  must_exit_= check_exit_status();
  DBUG_RETURN(ret);
}

int Wsrep_high_priority_service::rollback(const wsrep::ws_handle& ws_handle,
                                          const wsrep::ws_meta& ws_meta)
{
  DBUG_ENTER("Wsrep_high_priority_service::rollback");
  if (ws_meta.ordered())
  {
    m_thd->wsrep_cs().prepare_for_ordering(ws_handle, ws_meta, false);
  }
  else
  {
     assert(ws_meta == wsrep::ws_meta());
     assert(ws_handle == wsrep::ws_handle());
  }
  int ret= (trans_rollback_stmt(m_thd) || trans_rollback(m_thd));
  m_thd->mdl_context.release_transactional_locks();
  m_thd->mdl_context.release_explicit_locks();

  m_thd->mem_root->Clear();

  DBUG_RETURN(ret);
}

int Wsrep_high_priority_service::apply_toi(const wsrep::ws_meta& ws_meta,
                                           const wsrep::const_buffer& data,
                                           wsrep::mutable_buffer& err)
{
  DBUG_ENTER("Wsrep_high_priority_service::apply_toi");
  THD* thd = m_thd;
  Wsrep_non_trans_mode non_trans_mode(thd, ws_meta);

  wsrep::client_state& client_state(thd->wsrep_cs());
  assert(client_state.in_toi());

  thd_proc_info(thd, "wsrep applier toi");

  WSREP_DEBUG("Wsrep_high_priority_service::apply_toi: %lld",
              client_state.toi_meta().seqno().get());

  DBUG_EXECUTE_IF("sync.wsrep_apply_toi",
                  {
                    const char act[]=
                      "now "
                      "SIGNAL sync.wsrep_apply_toi_reached "
                      "WAIT_FOR signal.wsrep_apply_toi";
                    assert(!debug_sync_set_action(thd,
                                                       STRING_WITH_LEN(act)));
                  };);

  int ret = apply_events(thd, m_rli, data, err);
  thd->wsrep_has_ignored_error = false;
  trans_commit(thd);

  close_temporary_tables(thd);
  
  wsrep_local_gtid_manager.signal_waiters(thd->wsrep_current_gtid_seqno, false);
  wsrep_set_SE_checkpoint(client_state.toi_meta().gtid(),
                          wsrep_local_gtid_manager.gtid(), true);

  must_exit_ = check_exit_status();

  DBUG_RETURN(ret);
}

void Wsrep_high_priority_service::store_globals()
{
  wsrep_store_threadvars(m_thd);
  m_thd->wsrep_cs().acquire_ownership();
}

void Wsrep_high_priority_service::reset_globals()
{
  wsrep_reset_threadvars(m_thd);
}

void Wsrep_high_priority_service::switch_execution_context(wsrep::high_priority_service& orig_high_priority_service)
{
  DBUG_ENTER("Wsrep_high_priority_service::switch_execution_context");
  Wsrep_high_priority_service&
    orig_hps= static_cast<Wsrep_high_priority_service&>(orig_high_priority_service);
  m_thd->thread_stack= orig_hps.m_thd->thread_stack;
  DBUG_VOID_RETURN;
}

int Wsrep_high_priority_service::log_dummy_write_set(const wsrep::ws_handle& ws_handle,
                                                     const wsrep::ws_meta& ws_meta,
                                                     wsrep::mutable_buffer& err)
{
  DBUG_ENTER("Wsrep_high_priority_service::log_dummy_write_set");
  int ret= 0;
  DBUG_PRINT("info",
             ("Wsrep_high_priority_service::log_dummy_write_set: seqno=%lld",
              ws_meta.seqno().get()));
  DBUG_EXECUTE_IF("sync.wsrep_log_dummy_write_set",
                  {
                    const char act[]=
                      "now "
                      "SIGNAL sync.wsrep_log_dummy_write_set_reached ";
                    assert(!debug_sync_set_action(m_thd,
                                                       STRING_WITH_LEN(act)));
                  };);
  if (ws_meta.ordered())
  {
    wsrep::client_state& cs(m_thd->wsrep_cs());
    if (!cs.transaction().active())
    {
      cs.start_transaction(ws_handle, ws_meta);
    }
    adopt_apply_error(err);
    WSREP_DEBUG("Log dummy write set %lld", ws_meta.seqno().get());
    ret= cs.provider().commit_order_enter(ws_handle, ws_meta);
    if (!(ret && opt_log_replica_updates && wsrep_gtid_mode))
    {
      cs.before_rollback();
      cs.after_rollback();
    }

    if (!WSREP_EMULATE_BINLOG(m_thd))
    {
        Wsrep_server_state::instance().provider().wait_for_gtid(
            wsrep::gtid(ws_meta.group_id(),
            wsrep::seqno(ws_meta.seqno().get() - 1)), 10);
    }

    wsrep_set_SE_checkpoint(ws_meta.gtid(), wsrep_local_gtid_manager.gtid(),
                            false);
    ret= ret || cs.provider().commit_order_leave(ws_handle, ws_meta, err);
    cs.after_applying();
  }
  DBUG_RETURN(ret);
}

void Wsrep_high_priority_service::adopt_apply_error(wsrep::mutable_buffer& err)
{
  m_thd->wsrep_cs().adopt_apply_error(err);
}

void Wsrep_high_priority_service::debug_crash(const char* crash_point
                                              __attribute__((unused)))
{
  assert(m_thd == current_thd);
  DBUG_EXECUTE_IF(crash_point, DBUG_SUICIDE(););
}

/****************************************************************************
                           Applier service
*****************************************************************************/

Wsrep_applier_service::Wsrep_applier_service(THD* thd)
  : Wsrep_high_priority_service(thd)
{
  thd->wsrep_applier_service= this;
  thd->wsrep_cs().open(wsrep::client_id(thd->thread_id()));
  thd->wsrep_cs().before_command();
  thd->wsrep_cs().debug_log_level(wsrep_debug);

}

Wsrep_applier_service::~Wsrep_applier_service()
{
  m_thd->wsrep_cs().after_command_before_result();
  m_thd->wsrep_cs().after_command_after_result();
  m_thd->wsrep_cs().close();
  m_thd->wsrep_cs().cleanup();
}

int Wsrep_applier_service::apply_write_set(const wsrep::ws_meta& ws_meta,
                                           const wsrep::const_buffer& data,
                                           wsrep::mutable_buffer& err)
{
  DBUG_ENTER("Wsrep_applier_service::apply_write_set");
  THD* thd= m_thd;

  assert(thd->wsrep_trx().active());
  assert(thd->wsrep_trx().state() == wsrep::transaction::s_executing);

  thd_proc_info(thd, "applying write set");
  /* moved dbug sync point here, after possible THD switch for SR transactions
     has ben done
  */
  /* Allow tests to block the applier thread using the DBUG facilities */
  DBUG_EXECUTE_IF("sync.wsrep_apply_cb",
                 {
                   const char act[]=
                     "now "
                     "SIGNAL sync.wsrep_apply_cb_reached "
                     "WAIT_FOR signal.wsrep_apply_cb";
                   assert(!debug_sync_set_action(thd,
                                                      STRING_WITH_LEN(act)));
                 };);

  wsrep_setup_uk_and_fk_checks(thd);

  /* this wsrep_skip_hooks trick is needed to support CTAS execution in
     applying.
     We mark the transaction as non-replicable, to prevent wsrep commit time
     hooks be called. This transaction will finally commit with
     Wsrep_high_priority_service::commit
  */
  thd->get_transaction()->m_flags.wsrep_skip_hooks= true;
  int const ret= apply_events(thd, m_rli, data, err);
  thd->get_transaction()->m_flags.wsrep_skip_hooks= false;

  close_temporary_tables(thd);
  if (!ret && !(ws_meta.flags() & wsrep::provider::flag::commit))
  {
    thd->wsrep_cs().fragment_applied(ws_meta.seqno());
  }
  thd_proc_info(thd, "wsrep applied write set");
  DBUG_RETURN(ret);
}

int Wsrep_applier_service::apply_nbo_begin(const wsrep::ws_meta&,
                                           const wsrep::const_buffer&,
                                           wsrep::mutable_buffer&)
{
  DBUG_ENTER("Wsrep_applier_service::apply_nbo_begin");
  DBUG_RETURN(0);
}

void Wsrep_applier_service::after_apply()
{
  DBUG_ENTER("Wsrep_applier_service::after_apply");
  wsrep_after_apply(m_thd);
  DBUG_VOID_RETURN;
}

bool Wsrep_applier_service::check_exit_status() const
{
  bool ret= false;
  mysql_mutex_lock(&LOCK_wsrep_slave_threads);
  if (wsrep_slave_count_change < 0)
  {
    ++wsrep_slave_count_change;
    ret= true;
  }
  mysql_mutex_unlock(&LOCK_wsrep_slave_threads);
  return ret;
}

/****************************************************************************
                           Replayer service
*****************************************************************************/

Wsrep_replayer_service::Wsrep_replayer_service(THD* replayer_thd, THD* orig_thd)
  : Wsrep_high_priority_service(replayer_thd)
  , m_orig_thd(orig_thd)
  , m_da_shadow()
  , m_replay_status()
{
  WSREP_DEBUG("Wsrep_replayer_service, thd %u orig thd %u",
	      replayer_thd->thread_id(), orig_thd->thread_id());
  /* Response must not have been sent to client */
  assert(!orig_thd->get_stmt_da()->is_sent());
  /* Replaying should happen always from after_statement() hook
     after rollback, which should guarantee that there are no
     transactional locks */
  assert(!orig_thd->mdl_context.has_transactional_locks());

  /* Make a shadow copy of diagnostics area and reset */
  m_da_shadow.status= orig_thd->get_stmt_da()->status();
  if (m_da_shadow.status == Diagnostics_area::DA_OK)
  {
    m_da_shadow.affected_rows= orig_thd->get_stmt_da()->affected_rows();
    m_da_shadow.last_insert_id= orig_thd->get_stmt_da()->last_insert_id();
    strmake(m_da_shadow.message, orig_thd->get_stmt_da()->message_text(),
            sizeof(m_da_shadow.message) - 1);
  }
  orig_thd->get_stmt_da()->reset_diagnostics_area();

  /* Release explicit locks */
  if (orig_thd->locked_tables_mode && orig_thd->lock)
  {
    WSREP_WARN("releasing table lock for replaying (%u)",
               orig_thd->thread_id());
    orig_thd->locked_tables_list.unlock_locked_tables(orig_thd);
    orig_thd->variables.option_bits&= ~(OPTION_TABLE_LOCK);
  }

  thd_proc_info(orig_thd, "wsrep replaying trx");

  /* Copy thd vars from orig_thd before reset, otherwise reset
     for orig thd clears thread local storage before copy. */
  wsrep_assign_from_threadvars(replayer_thd);
  wsrep_reset_threadvars(orig_thd);
  wsrep_store_threadvars(replayer_thd);
  wsrep_open(replayer_thd);
  wsrep_before_command(replayer_thd);
  replayer_thd->wsrep_cs().clone_transaction_for_replay(orig_thd->wsrep_trx());
}

Wsrep_replayer_service::~Wsrep_replayer_service()
{
  THD* replayer_thd= m_thd;
  THD* orig_thd= m_orig_thd;

  /* Switch execution context back to original. */
  wsrep_after_apply(replayer_thd);
  wsrep_after_command_ignore_result(replayer_thd);
  wsrep_close(replayer_thd);
  wsrep_reset_threadvars(replayer_thd);
  wsrep_store_threadvars(orig_thd);

  assert(!orig_thd->get_stmt_da()->is_sent());
  assert(!orig_thd->get_stmt_da()->is_set());

  if (m_replay_status == wsrep::provider::success)
  {
    assert(replayer_thd->wsrep_cs().current_error() == wsrep::e_success);
    orig_thd->killed= THD::NOT_KILLED;
    my_ok(orig_thd, m_da_shadow.affected_rows, m_da_shadow.last_insert_id);
  }
  else if (m_replay_status == wsrep::provider::error_certification_failed)
  {
    wsrep_override_error(orig_thd, ER_LOCK_DEADLOCK);
  }
  else
  {
    assert(0);
    WSREP_ERROR("trx_replay failed for: %d, schema: %s, query: %s",
                m_replay_status,
                orig_thd->db().str, WSREP_QUERY(orig_thd));
    unireg_abort(1);
  }
}

int Wsrep_replayer_service::apply_write_set(const wsrep::ws_meta& ws_meta,
                                            const wsrep::const_buffer& data,
                                            wsrep::mutable_buffer& err)
{
  DBUG_ENTER("Wsrep_replayer_service::apply_write_set");
  THD* thd= m_thd;

  assert(thd->wsrep_trx().active());
  assert(thd->wsrep_trx().state() == wsrep::transaction::s_replaying);

  /* Allow tests to block the applier thread using the DBUG facilities */
  DBUG_EXECUTE_IF("sync.wsrep_replay_cb",
                 {
                   const char act[]=
                     "now "
                     "SIGNAL sync.wsrep_replay_cb_reached "
                     "WAIT_FOR signal.wsrep_replay_cb";
                   assert(!debug_sync_set_action(thd,
                                                      STRING_WITH_LEN(act)));
                 };);
  wsrep_setup_uk_and_fk_checks(thd);

  /*
    Skip wsrep hooks during write set applying. The write set may
    contain statements which cause implicit commit (like BEGIN),
    which should not cause wsrep commit.
  */
  thd->get_transaction()->m_flags.wsrep_skip_hooks= true;

  int ret= 0;
  if (!wsrep::starts_transaction(ws_meta.flags()))
  {
    assert(thd->wsrep_trx().is_streaming());
    ret= wsrep_schema->replay_transaction(thd,
                                          m_rli,
                                          ws_meta,
                                          thd->wsrep_sr().fragments());
  }

  ret= ret || apply_events(thd, m_rli, data, err);
  thd->get_transaction()->m_flags.wsrep_skip_hooks= false;

  close_temporary_tables(thd);
  if (!ret && !(ws_meta.flags() & wsrep::provider::flag::commit))
  {
    thd->wsrep_cs().fragment_applied(ws_meta.seqno());
  }

  thd_proc_info(thd, "wsrep replayed write set");
  DBUG_RETURN(ret);
}
