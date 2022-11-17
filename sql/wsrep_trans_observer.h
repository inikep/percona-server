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

#ifndef WSREP_TRANS_OBSERVER_H
#define WSREP_TRANS_OBSERVER_H

#include "mysql/service_wsrep.h"
#include "wsrep_server_state.h"
#include "wsrep_applier.h" /* wsrep_apply_error */
#include "wsrep_xid.h"
#include "wsrep_thd.h"
#include "my_dbug.h"

#include "binlog.h" // wsrep_is_binlog_cache_empty
#include "table.h" // TABLE
#include "sql_lex.h" // LEX

class THD;

/*
   Return true if THD has active wsrep transaction.
 */
static inline bool wsrep_is_active(THD* thd)
{
  return (thd->wsrep_cs().state() != wsrep::client_state::s_none  &&
	  thd->wsrep_cs().transaction().active());
}

/*
  Return true if transaction is ordered.
 */
static inline bool wsrep_is_ordered(THD* thd)
{
  return thd->wsrep_trx().ordered();
}

/*
  Return true if transaction has been BF aborted but has not been
  rolled back yet.

  It is required that the caller holds thd->LOCK_thd_data.
*/
static inline bool wsrep_must_abort(THD* thd)
{
  mysql_mutex_assert_owner(&thd->LOCK_thd_data);
  return (thd->wsrep_trx().state() == wsrep::transaction::s_must_abort);
}

/*
  Return true if the transaction must be replayed.
 */
static inline bool wsrep_must_replay(THD* thd)
{
  return (thd->wsrep_trx().state() == wsrep::transaction::s_must_replay);
}
/*
  Return true if transaction has not been committed.

  Note that we don't require thd->LOCK_thd_data here. Calling this method
  makes sense only from codepaths which are past ordered_commit state
  and the wsrep transaction is immune to BF aborts at that point.
*/
static inline bool wsrep_not_committed(THD* thd)
{
  return (thd->wsrep_trx().state() != wsrep::transaction::s_committed);
}

/*
  Return true if the THD is preparing a SR transaction.
 */
static inline bool wsrep_is_committing(THD* thd)
{
  return (thd->wsrep_trx().state() == wsrep::transaction::s_committing);
}

/*
  Return true if THD is either committing a transaction or statement
  is autocommit.
 */
static inline bool wsrep_is_real(THD* thd, bool all)
{
  if (thd->lex->sql_command == SQLCOM_CREATE_TABLE)
  {
    return (all && thd->get_transaction()->is_active(Transaction_ctx::SESSION)) ||
      (!all && thd->get_transaction()->is_active(Transaction_ctx::STMT) &&
       thd->get_transaction()->is_active(Transaction_ctx::SESSION)) ||
      (thd->wsrep_trx().state() == wsrep::transaction::s_must_abort ||
       thd->wsrep_trx().state() == wsrep::transaction::s_aborting);
  }
  return all || !thd->get_transaction()->is_active(Transaction_ctx::SESSION);
}

/*
  Check if a transaction has generated changes.
 */
static inline bool wsrep_has_changes(THD* thd)
{
  return (!wsrep_is_binlog_cache_empty(thd) && !thd->wsrep_trx().is_empty());
}

/*
  Check if an active transaction has been BF aborted.
 */
static inline bool wsrep_is_bf_aborted(THD* thd)
{
  return (thd->wsrep_trx().active() && thd->wsrep_trx().bf_aborted());
}

static inline int wsrep_check_pk(THD* thd)
{

  if (!wsrep_certify_nonPK)
  {
    for (TABLE* table= thd->open_tables; table != NULL; table= table->next)
    {
      if (table->key_info == NULL || table->s->primary_key == MAX_KEY)
      {
        WSREP_DEBUG("No primary key found for table %s.%s",
                    table->s->db.str, table->s->table_name.str);
        wsrep_override_error(thd, ER_LOCK_DEADLOCK);
        return 1;
      }
    }
  }
  return 0;
}

static inline bool wsrep_streaming_enabled(THD* thd)
{
  return (thd->wsrep_sr().fragment_size() > 0);
}

/*
  Return number of fragments succesfully certified for the
  current statement.
 */
static inline size_t wsrep_fragments_certified_for_stmt(THD* thd)
{
    return thd->wsrep_trx().fragments_certified_for_statement();
}

static inline int wsrep_start_transaction(THD* thd, wsrep_trx_id_t trx_id)
{
  return ((thd->wsrep_cs().state() != wsrep::client_state::s_none &&
           Wsrep_server_state::instance().state() !=
           Wsrep_server_state::s_disconnected) ?
          thd->wsrep_cs().start_transaction(wsrep::transaction_id(trx_id)) :
          0);
}

/**/
static inline int wsrep_start_trx_if_not_started(THD* thd)
{
  int ret= 0;
  if (thd->is_bootstrap_system_thread()) return 0;
  assert(thd->wsrep_next_trx_id() != WSREP_UNDEFINED_TRX_ID);
  assert(thd->wsrep_cs().mode() == Wsrep_client_state::m_local);
  if (thd->wsrep_trx().active() == false)
  {
    ret= wsrep_start_transaction(thd, thd->wsrep_next_trx_id());
  }
  return ret;
}

/*
  Called after each row operation.

  Return zero on succes, non-zero on failure.
 */
static inline int wsrep_after_row(THD* thd, bool)
{
  if (thd->wsrep_cs().state() != wsrep::client_state::s_none  &&
      wsrep_thd_is_local(thd))
  {
    if (wsrep_check_pk(thd))
    {
      return 1;
    }
    else if (wsrep_streaming_enabled(thd))
    {
      return thd->wsrep_cs().after_row();
    }
  }
  return 0;
}

/*
  Helper method to determine whether commit time hooks
  should be run for the transaction.

  Commit hooks must be run in the following cases:
  - The transaction is local and has generated write set and is committing.
  - The transaction has been BF aborted
  - Is running in high priority mode and is ordered. This can be replayer,
    applier or storage access.
 */
static inline bool wsrep_run_commit_hook(THD* thd, bool all)
{
  DBUG_TRACE;

  /* skipping non-wsrep threads */
  if (!WSREP(thd)) return (false);

  DBUG_PRINT("wsrep", ("Is_active: %d is_real %d has_changes %d is_applying %d "
                       "is_ordered: %d",
                       wsrep_is_active(thd), wsrep_is_real(thd, all),
                       wsrep_has_changes(thd), wsrep_thd_is_applying(thd),
                       wsrep_is_ordered(thd)));

  /* skipping non-binlogging threads */
  if (!thd->variables.sql_log_bin)
    {
      WSREP_DEBUG("THD has sql_log_bin %d", thd->variables.sql_log_bin);
      return (false);
    }

  /* check if this transaction has already been flagged to run commit hooks */
  if (thd->get_transaction()->m_flags.wsrep_run_hooks)
  {
    WSREP_DEBUG("Transaction is flagged to run hooks");
    return (true);
  }

  /* check if caller has decided to skip this transaction */
  if (thd->get_transaction()->m_flags.wsrep_skip_hooks)
  {
    WSREP_DEBUG("Transaction is flagged to skip hooks");
    return (false);
  }

  /* Is MST commit or autocommit? */
  bool ret= wsrep_is_active(thd) && wsrep_is_real(thd, all);

  if (ret && !(wsrep_has_changes(thd) ||  /* Has generated write set */
               /* Is high priority (replay, applier, storage) and the
                  transaction is scheduled for commit ordering */
               (wsrep_thd_is_applying(thd) && wsrep_is_ordered(thd))))
  {
    mysql_mutex_lock(&thd->LOCK_thd_data);
    DBUG_PRINT("wsrep", ("state: %s",
                         wsrep::to_c_string(thd->wsrep_trx().state())));
    /* Transaction is local but has no changes, the commit hooks will
       be skipped and the wsrep transaction is terminated in
       wsrep_commit_empty() */
    if (thd->wsrep_trx().state() == wsrep::transaction::s_executing)
    {
      ret= false;
    }
    /* Transaction has been either aborted or it did not have any
       effect. */
    if (thd->wsrep_trx().state() == wsrep::transaction::s_aborted)
    {
      ret= false;
    }
    mysql_mutex_unlock(&thd->LOCK_thd_data);
  }

  /*  make our decision sticky for the entrire life time of this transaction */
  thd->get_transaction()->m_flags.wsrep_run_hooks= ret;
  WSREP_DEBUG("wsrep_run_commit_hook: %d", ret);
  DBUG_PRINT("wsrep", ("return: %d", ret));
  return (ret);
}

/*
  Called before the transaction is prepared.

  Return zero on succes, non-zero on failure.
 */
static inline int wsrep_before_prepare(THD* thd, bool all)
{
  DBUG_TRACE;
  WSREP_DEBUG("wsrep_before_prepare: %d", wsrep_is_real(thd, all));
  int ret= 0;
  if (wsrep_run_commit_hook(thd, all))
  {
    if ((ret= thd->wsrep_cs().before_prepare()) == 0)
    {
      assert(!thd->wsrep_trx().ws_meta().gtid().is_undefined());
      wsrep_xid_init(&thd->wsrep_xid,
                     thd->wsrep_trx().ws_meta().gtid(),
                     wsrep_local_gtid_manager.gtid());
    }
  }
  return (ret);
}

/*
  Called after the transaction has been prepared.

  Return zero on succes, non-zero on failure.
 */
static inline int wsrep_after_prepare(THD* thd, bool all)
{
  DBUG_TRACE;
  WSREP_DEBUG("wsrep_after_prepare: %d", wsrep_is_real(thd, all));
  int ret= (wsrep_run_commit_hook(thd, all) ?
            thd->wsrep_cs().after_prepare() : 0);
  assert(ret == 0 || thd->wsrep_cs().current_error() ||
              thd->wsrep_cs().transaction().state() == wsrep::transaction::s_must_replay);
  return (ret);
}


/*
  Called before the transaction is committed.

  This function must be called from both client and
  applier contexts before commit.

  Return zero on succes, non-zero on failure.
 */
static inline int wsrep_before_commit(THD* thd, bool all)
{
  DBUG_TRACE;
  WSREP_DEBUG("wsrep_before_commit: %d, %lld",
              wsrep_is_real(thd, all),
              (long long)wsrep_thd_trx_seqno(thd));
  int ret = 0;
  if (wsrep_run_commit_hook(thd, all)) {
    if ((ret = thd->wsrep_cs().before_commit()) == 0) {
      assert(!thd->wsrep_trx().ws_meta().gtid().is_undefined());
      /*
        Non-committing write-set should have Anonymous GTID type. So it doesn't
        participate in gtid_state.
       */
      if (!wsrep_thd_is_toi(thd) &&
          !(thd->wsrep_trx().ws_meta().flags() & wsrep::provider::flag::commit)) {
        thd->variables.gtid_next.set_anonymous();
        thd->owned_gtid.sidno = THD::OWNED_SIDNO_ANONYMOUS;
        thd->owned_gtid.gno = 0;
        global_sid_lock->rdlock();
        gtid_state->acquire_anonymous_ownership();
        global_sid_lock->unlock();
      }
      if (thd->variables.gtid_next.type == AUTOMATIC_GTID &&
          (thd->wsrep_trx().ws_meta().flags() & wsrep::provider::flag::commit)) {
        if (wsrep_gtid_mode)
          thd->server_id = wsrep_local_gtid_manager.server_id;
        thd->wsrep_current_gtid_seqno = wsrep_local_gtid_manager.seqno_inc();
      }
      wsrep_xid_init(&thd->wsrep_xid,
                     thd->wsrep_trx().ws_meta().gtid(),
                     wsrep_local_gtid_manager.gtid());
    }
  }
  return (ret);
}

/*
  Called after the transaction has been ordered for commit.

  This function must be called from both client and
  applier contexts after the commit has been ordered.

  @param thd Pointer to THD
  @param all 
  @param err Error buffer in case of applying error

  Return zero on succes, non-zero on failure.
 */
static inline int wsrep_ordered_commit(THD* thd, bool all)
{
  DBUG_TRACE;
  WSREP_DEBUG("wsrep_ordered_commit: %d", wsrep_is_real(thd, all));
  return (wsrep_run_commit_hook(thd, all) ?
              thd->wsrep_cs().ordered_commit() : 0);
}

/*
  Called after the transaction has been committed.

  Return zero on succes, non-zero on failure.
 */
static inline int wsrep_after_commit(THD* thd, bool all)
{
  DBUG_TRACE;
  WSREP_DEBUG("wsrep_after_commit: %d, %d, %lld, %d",
              wsrep_is_real(thd, all),
              wsrep_is_active(thd),
              (long long)wsrep_thd_trx_seqno(thd),
              wsrep_has_changes(thd));

  if(wsrep_run_commit_hook(thd, all))
  {
    /*  as transaction finished, reset the sticky commit hook flag */
    thd->get_transaction()->m_flags.wsrep_run_hooks= false;

    /*
      Non-committing write set was changed to Anonymous type,
      so at this point we have to restore value for owned_gtid.
     */
    if (!wsrep_thd_is_toi(thd) &&
        !(thd->wsrep_trx().ws_meta().flags() & wsrep::provider::flag::commit)) {
      thd->owned_gtid.sidno = 0;
      thd->owned_gtid.gno = 0;
    }

    /*
      Transaction has been committed, so we now can signal all waiters and
      update `wsrep_last_written_gtid_seqno` to be use with `WSREP_LAST_WRITTEN_GTID`.
    */
    if ((thd->wsrep_trx().state() == wsrep::transaction::s_committing) ||
        (thd->wsrep_trx().state() == wsrep::transaction::s_ordered_commit)) {
      wsrep_local_gtid_manager.signal_waiters(thd->wsrep_current_gtid_seqno, false);
      if (wsrep_thd_is_local(thd))
        thd->wsrep_last_written_gtid_seqno = thd->wsrep_current_gtid_seqno;
    }

    return ((thd->wsrep_trx().state() == wsrep::transaction::s_committing
                 ? thd->wsrep_cs().ordered_commit() : 0) ||
                (thd->wsrep_xid.reset(),
                 thd->wsrep_cs().after_commit()));
  }

  /*  as transaction finished, reset the sticky commit hook flag */
  thd->get_transaction()->m_flags.wsrep_run_hooks= false;
  return (0);
}

/*
  Called before the transaction is rolled back.

  Return zero on succes, non-zero on failure.
 */
static inline int wsrep_before_rollback(THD* thd, bool all)
{
  DBUG_TRACE;
  int ret= 0;
  if (wsrep_is_active(thd))
  {
    if (!all && thd->in_active_multi_stmt_transaction() &&
        !thd->wsrep_applier && thd->wsrep_trx().is_streaming() &&
        (wsrep_fragments_certified_for_stmt(thd) > 0))
    {
      /* Non-safe statement rollback during SR multi statement
         transaction. A statement rollback is considered unsafe, if
         the same statement has already replicated one or more fragments.
         Self abort the transaction, the actual rollback and error
         handling will be done in after statement phase. */
      WSREP_DEBUG("statement rollback is not safe for streaming replication");
      wsrep_thd_self_abort(thd);
      ret= 0;
    }
    else if (wsrep_is_real(thd, all) &&
             thd->wsrep_trx().state() != wsrep::transaction::s_aborted)
    {
      /* Real transaction rolling back and wsrep abort not completed
         yet */
      /* Reset XID so that it does not trigger writing serialization
         history in InnoDB. This needs to be avoided because rollback
         may happen out of order and replay may follow. */
      thd->wsrep_xid.reset();
      ret= thd->wsrep_cs().before_rollback();
    }
  }
  return (ret);
}

/*
  Called after the transaction has been rolled back.

  Return zero on succes, non-zero on failure.
 */
static inline int wsrep_after_rollback(THD* thd, bool all MY_ATTRIBUTE((unused)))
{
  DBUG_TRACE;
  /*  as transaction finished, reset the sticky commit hook flag */
  thd->get_transaction()->m_flags.wsrep_run_hooks= false;

  /* resetting aborter thread ID after full rollback */
  if (wsrep_is_real(thd, all)) thd->wsrep_aborter = 0;

  return ((wsrep_is_real(thd, all) && wsrep_is_active(thd) &&
               thd->wsrep_cs().transaction().state() !=
               wsrep::transaction::s_aborted) ?
              thd->wsrep_cs().after_rollback() : 0);
}

static inline int wsrep_before_statement(THD* thd)
{
  return (thd->wsrep_cs().state() != wsrep::client_state::s_none ?
	  thd->wsrep_cs().before_statement() : 0);
}

static inline
int wsrep_after_statement(THD* thd)
{
  DBUG_TRACE;
  return (thd->wsrep_cs().state() != wsrep::client_state::s_none ?
              thd->wsrep_cs().after_statement() : 0);
}

static inline void wsrep_after_apply(THD* thd)
{
  assert(wsrep_thd_is_applying(thd));
  WSREP_DEBUG("wsrep_after_apply %u", thd->thread_id());
  thd->wsrep_cs().after_applying();
}

static inline void wsrep_open(THD* thd)
{
  DBUG_TRACE;
  if (global_system_variables.wsrep_on)
  {
    thd->wsrep_cs().open(wsrep::client_id(thd->thread_id()));
    thd->wsrep_cs().debug_log_level(wsrep_debug);
    if (!thd->wsrep_applier && thd->variables.wsrep_trx_fragment_size)
    {
      thd->wsrep_cs().enable_streaming(
        wsrep_fragment_unit(thd->variables.wsrep_trx_fragment_unit),
        size_t(thd->variables.wsrep_trx_fragment_size));
    }
  }
  return;
}

static inline void wsrep_close(THD* thd)
{
  DBUG_TRACE;
  if (thd->wsrep_cs().state() != wsrep::client_state::s_none)
    thd->wsrep_cs().close();
  return;
}

static inline void wsrep_cleanup(THD* thd)
{
  DBUG_TRACE;
  if (thd->wsrep_cs().state() != wsrep::client_state::s_none)
  {
    thd->wsrep_cs().cleanup();
  }
  return;
}

static inline void
wsrep_wait_rollback_complete_and_acquire_ownership(THD *thd)
{
  DBUG_TRACE;
  if (thd->wsrep_cs().state() != wsrep::client_state::s_none)
  {
    thd->wsrep_cs().wait_rollback_complete_and_acquire_ownership();
  }
  return;
}

static inline int wsrep_before_command(THD* thd, bool keep_command_error)
{
  return (thd->wsrep_cs().state() != wsrep::client_state::s_none ?
	  thd->wsrep_cs().before_command(keep_command_error) : 0);
}

static inline int wsrep_before_command(THD* thd)
{
  return wsrep_before_command(thd, false);
}

/*
  Called after each command.

  Return zero on success, non-zero on failure.
*/
static inline void wsrep_after_command_before_result(THD* thd)
{
  if (thd->wsrep_cs().state() != wsrep::client_state::s_none)
  {
    thd->wsrep_cs().after_command_before_result();
  }
}

static inline void wsrep_after_command_after_result(THD* thd)
{
  if (thd->wsrep_cs().state() != wsrep::client_state::s_none)
  {
    thd->wsrep_cs().after_command_after_result();
  }
}

static inline void wsrep_after_command_ignore_result(THD* thd)
{
  wsrep_after_command_before_result(thd);
  assert(!thd->wsrep_cs().current_error());
  wsrep_after_command_after_result(thd);
}

static inline enum wsrep::client_error wsrep_current_error(THD* thd)
{
  return thd->wsrep_cs().current_error();
}

static inline enum wsrep::provider::status
wsrep_current_error_status(THD* thd)
{
  return thd->wsrep_cs().current_error_status();
}

/*
  Commit an empty transaction.

  If the transaction is real and the wsrep transaction is still active,
  the transaction did not generate any rows or keys and is committed
  as empty. Here the wsrep transaction is rolled back and after statement
  step is performed to leave the wsrep transaction in the state as it
  never existed.
*/
static inline void wsrep_commit_empty(THD* thd, bool all)
{
  DBUG_TRACE;
  WSREP_DEBUG(
      "empty commit real %d local %d active %d trx %d changes %d have error %d SQL %s",
      wsrep_is_real(thd, all),
      wsrep_thd_is_local(thd),
      thd->wsrep_trx().active(),
      thd->wsrep_trx().state(),
      wsrep_has_changes(thd),
      wsrep_current_error(thd), thd->query().str);

  if (wsrep_is_real(thd, all) &&
      wsrep_thd_is_local(thd) &&
      thd->wsrep_trx().active() &&
      thd->wsrep_trx().state() != wsrep::transaction::s_committed)
  {
    /* @todo CTAS with STATEMENT binlog format and empty result set
       seems to be committing empty. Figure out why and try to fix
       elsewhere. */
    assert(!wsrep_has_changes(thd) ||
		!thd->variables.sql_log_bin ||
                (thd->lex->sql_command == SQLCOM_CREATE_TABLE));
    bool have_error= wsrep_current_error(thd);

    int ret= wsrep_before_rollback(thd, all) ||
      wsrep_after_rollback(thd, all) ||
      wsrep_after_statement(thd);

    /* The committing transaction was empty but it held some locks and
       got BF aborted. As there were no certified changes in the
       data, we ignore the deadlock error and rely on error reporting
       by storage engine/server. */
    if (!ret && !have_error && wsrep_current_error(thd))
    {
      assert(wsrep_current_error(thd) == wsrep::e_deadlock_error);
      thd->wsrep_cs().reset_error();
    }
    if (ret)
    {
      WSREP_DEBUG("wsrep_commit_empty failed: %d", wsrep_current_error(thd));
    }
  }
  return;
}
#endif /* WSREP_TRANS_OBSERVER */
