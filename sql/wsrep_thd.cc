
/* Copyright (C) 2013-2019 Codership Oy <info@codership.com>

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

#include <wsrep.h>
#include "transaction.h"
#include "rpl_rli.h"
#include "log_event.h"
#include "sql_parse.h"
#include "sql_base.h" // close_thread_tables()
#include "mysqld.h"   //
#include "debug_sync.h"
#include "rpl_replica.h"
#include "rpl_rli.h"
#include "rpl_mi.h"
#include "mysql/plugin.h" // thd_proc_info
#include "sql/conn_handler/connection_handler_manager.h"

#include "wsrep_server_state.h"
#include "wsrep_thd.h"
#include "wsrep_trans_observer.h"
#include "wsrep_high_priority_service.h"
#include "wsrep_storage_service.h"
#include "mysql/service_wsrep.h"
#include "wsrep_mysqld.h" // start_wsrep_THD();
#include "wsrep_utils.h" // wsp::auto_lock

#include <deque>
class Wsrep_thd_queue
{
public:
  Wsrep_thd_queue(THD* t) : thd(t)
  {
    mysql_mutex_init(key_LOCK_wsrep_thd_queue,
                     &LOCK_wsrep_thd_queue,
                     MY_MUTEX_INIT_FAST);
    mysql_cond_init(key_COND_wsrep_thd_queue, &COND_wsrep_thd_queue);
  }
  ~Wsrep_thd_queue()
  {
    mysql_mutex_destroy(&LOCK_wsrep_thd_queue);
    mysql_cond_destroy(&COND_wsrep_thd_queue);
  }
  bool push_back(THD* thd)
  {
    assert(thd);
    wsp::auto_lock lock(&LOCK_wsrep_thd_queue);
    std::deque<THD*>::iterator it = queue.begin();
    while (it != queue.end())
    {
      if (*it == thd)
      {
        return true;
      }
      it++;
    }
    queue.push_back(thd);
    mysql_cond_signal(&COND_wsrep_thd_queue);
    return false;
  }
  THD* pop_front()
  {
    mysql_mutex_lock(&thd->LOCK_current_cond);
    thd->current_mutex= &LOCK_wsrep_thd_queue;
    thd->current_cond=  &COND_wsrep_thd_queue;
    mysql_mutex_unlock(&thd->LOCK_current_cond);

    THD *ret = nullptr;
    {
      wsp::auto_lock lock(&LOCK_wsrep_thd_queue);
      while (queue.empty())
      {
        if (thd->killed != THD::NOT_KILLED)
          goto out;
        mysql_cond_wait(&COND_wsrep_thd_queue, &LOCK_wsrep_thd_queue);
      }
      ret = queue.front();
      queue.pop_front();
    }
out:
    mysql_mutex_lock(&thd->LOCK_current_cond);
    thd->current_mutex= 0;
    thd->current_cond=  0;
    mysql_mutex_unlock(&thd->LOCK_current_cond);

    return ret;
  }
private:
  THD*             thd;
  std::deque<THD*> queue;
  mysql_mutex_t    LOCK_wsrep_thd_queue;
  mysql_cond_t     COND_wsrep_thd_queue;
};

static Wsrep_thd_queue* wsrep_rollback_queue= 0;


static long long wsrep_bf_aborts_counter = 0;

int wsrep_show_bf_aborts (THD* , SHOW_VAR *var, char *buff MY_ATTRIBUTE((unused)),
                          enum enum_var_type)
{
    wsrep_local_bf_aborts = wsrep_bf_aborts_counter;
    var->type = SHOW_LONGLONG;
    var->value = (char*)&wsrep_local_bf_aborts;
    return 0;
}


static void wsrep_replication_process(THD *thd,
                                      void* arg __attribute__((unused)))
{
  DBUG_ENTER("wsrep_replication_process");

  Wsrep_applier_service applier_service(thd);

  WSREP_INFO("Starting applier thread %u", thd->thread_id());
  thd_proc_info(thd, "wsrep applier idle");
  enum wsrep::provider::status
    ret= Wsrep_server_state::get_provider().run_applier(&applier_service);

  WSREP_INFO("Applier thread exiting ret: %d thd: %u", ret, thd->thread_id());
  wsrep_close_applier(thd);

  TABLE *tmp;
  while ((tmp = thd->temporary_tables))
  {
    WSREP_WARN("Applier %u, has temporary tables at exit: %s.%s",
               thd->thread_id(), 
               (tmp->s) ? tmp->s->db.str : "void",
               (tmp->s) ? tmp->s->table_name.str : "void");
  }
  DBUG_VOID_RETURN;
}

static bool create_wsrep_THD(Wsrep_thd_args* args)
{
  long old_wsrep_running_threads= wsrep_running_threads;
 
  my_thread_handle hThread;
  bool res= mysql_thread_create(
                            key_thread_handle_wsrep, 
                            &hThread, &connection_attrib,
                            start_wsrep_THD, args);
  /*
    if starting a thread on server startup, wait until the this thread's THD
    is fully initialized (otherwise a THD initialization code might
    try to access a partially initialized server data structure - MDEV-8208).
  */
  mysql_mutex_lock(&LOCK_wsrep_slave_threads);
  if (!mysqld_server_initialized)
    while (old_wsrep_running_threads == wsrep_running_threads)
      mysql_cond_wait(&COND_wsrep_slave_threads, &LOCK_wsrep_slave_threads);
  mysql_mutex_unlock(&LOCK_wsrep_slave_threads);
  return res;
}

void wsrep_create_appliers(long threads)
{
  /*  Dont' start slave threads if wsrep-provider or wsrep-cluster-address
      is not set.
  */
  if (!WSREP_PROVIDER_EXISTS) 
  {
    return; 
  }

  if (!wsrep_cluster_address || wsrep_cluster_address[0]== 0)
  {
    WSREP_DEBUG("wsrep_create_appliers exit due to empty address");
    return;
  }

  long wsrep_threads=0;
  
  while (wsrep_threads++ < threads)
  {
    Wsrep_thd_args* args(new Wsrep_thd_args(wsrep_replication_process, 0));
    if (create_wsrep_THD(args))
    {
      WSREP_WARN("Can't create thread to manage wsrep replication");
    }
  }
}

static void wsrep_remove_streaming_fragments(THD* thd, const char* ctx)
{
  wsrep::transaction_id transaction_id(thd->wsrep_trx().id());
  Wsrep_storage_service* storage_service= wsrep_create_storage_service(thd, ctx);
  storage_service->store_globals();
  storage_service->adopt_transaction(thd->wsrep_trx());
  storage_service->remove_fragments();
  storage_service->commit(wsrep::ws_handle(transaction_id, 0),
                          wsrep::ws_meta());
  Wsrep_server_state::instance().server_service()
    .release_storage_service(storage_service);
  wsrep_store_threadvars(thd);
}

static void wsrep_rollback_high_priority(THD *thd, THD *rollbacker)
{
  WSREP_DEBUG("Rollbacker aborting SR applier thd (%u %lu)",
              thd->thread_id(), thd->real_id);
  const char* orig_thread_stack= thd->thread_stack;
  thd->thread_stack= rollbacker->thread_stack;
  assert(thd->wsrep_cs().mode() == Wsrep_client_state::m_high_priority);
  /* Must be streaming and must have been removed from the
     server state streaming appliers map. */
  assert(thd->wsrep_trx().is_streaming());
  assert(!Wsrep_server_state::instance().find_streaming_applier(
                thd->wsrep_trx().server_id(),
                thd->wsrep_trx().id()));
  assert(thd->wsrep_applier_service);

  /* Fragment removal should happen before rollback to make
     the transaction non-observable in SR table after the rollback
     completes. For correctness the order does not matter here,
     but currently it is mandated by checks in some MTR tests. */
  wsrep_remove_streaming_fragments(thd, "high priority");
  thd->wsrep_applier_service->rollback(wsrep::ws_handle(),
                                       wsrep::ws_meta());
  thd->wsrep_applier_service->after_apply();
  thd->thread_stack= orig_thread_stack;
  WSREP_DEBUG("rollbacker aborted thd: (%u %lu)",
              thd->thread_id(), thd->real_id);
  /* Will free THD */
  Wsrep_server_state::instance().server_service()
    .release_high_priority_service(thd->wsrep_applier_service);
}

static void wsrep_rollback_local(THD *thd, THD *rollbacker)
{
  WSREP_DEBUG("Rollbacker aborting local thd (%u %lu)",
              thd->thread_id(), thd->real_id);
  const char* orig_thread_stack= thd->thread_stack;
  thd->thread_stack= rollbacker->thread_stack;
  if (thd->wsrep_trx().is_streaming())
  {
    wsrep_remove_streaming_fragments(thd, "local");
  }
  /* Set thd->scheduler.data temporarily to NULL to avoid
     callbacks to threadpool wait_begin() during rollback. */
  auto saved_esd= thd->scheduler.data;
  thd->scheduler.data= 0;
  mysql_mutex_lock(&thd->LOCK_thd_data);
  /* prepare THD for rollback processing */
  thd->reset_for_next_command();
  thd->lex->sql_command= SQLCOM_ROLLBACK;
  mysql_mutex_unlock(&thd->LOCK_thd_data);
  /* Perform a client rollback, restore globals and signal
     the victim only when all the resources have been
     released */
  thd->wsrep_cs().client_service().bf_rollback();
  wsrep_reset_threadvars(thd);
  /* Assign saved scheduler.data back before letting
     client to continue. */
  thd->scheduler.data= saved_esd;
  thd->thread_stack= orig_thread_stack;
  thd->wsrep_cs().sync_rollback_complete();
  WSREP_DEBUG("rollbacker aborted thd: (%u %lu)",
              thd->thread_id(), thd->real_id);
}

static void wsrep_rollback_process(THD *rollbacker,
                                   void *arg __attribute__((unused)))
{
  DBUG_ENTER("wsrep_rollback_process");

  THD* thd= NULL;
  assert(!wsrep_rollback_queue);
  wsrep_rollback_queue= new Wsrep_thd_queue(rollbacker);
  WSREP_INFO("Starting rollbacker thread %u", rollbacker->thread_id());
  rollbacker->wsrep_rollbacker= true;
  
  thd_proc_info(rollbacker, "wsrep aborter idle");
  while ((thd= wsrep_rollback_queue->pop_front()) != NULL)
  {
    mysql_mutex_lock(&thd->LOCK_thd_data);
    wsrep::client_state& cs(thd->wsrep_cs());
    const wsrep::transaction& tx(cs.transaction());
    if (tx.state() == wsrep::transaction::s_aborted)
    {
      WSREP_DEBUG("rollbacker thd already aborted: %llu state: %d",
                  (long long)thd->real_id,
                  tx.state());
      mysql_mutex_unlock(&thd->LOCK_thd_data);
      continue;
    }
    mysql_mutex_unlock(&thd->LOCK_thd_data);

    wsrep_reset_threadvars(rollbacker);
    wsrep_store_threadvars(thd);
    thd->wsrep_cs().acquire_ownership();

    thd_proc_info(rollbacker, "wsrep aborter active");

    /* Rollback methods below may free thd pointer. Do not try
       to access it after method returns */
    if (wsrep_thd_is_applying(thd))
    {
      wsrep_rollback_high_priority(thd, rollbacker);
    }
    else
    {
      wsrep_rollback_local(thd, rollbacker);
    }
    wsrep_store_threadvars(rollbacker);
    thd_proc_info(rollbacker, "wsrep aborter idle");
  }

  delete wsrep_rollback_queue;
  wsrep_rollback_queue= NULL;

  WSREP_INFO("rollbacker thread exiting %u", rollbacker->thread_id());

  assert(rollbacker->killed != THD::NOT_KILLED);

  DBUG_PRINT("wsrep",("wsrep rollbacker thread exiting"));
  DBUG_VOID_RETURN;
}

void wsrep_create_rollbacker()
{
  if (wsrep_cluster_address && wsrep_cluster_address[0] != 0)
  {
    Wsrep_thd_args* args= new Wsrep_thd_args(wsrep_rollback_process, 0);
    /* create rollbacker */
    if (create_wsrep_THD(args))
      WSREP_WARN("Can't create thread to manage wsrep rollback");
  }
}

/*
  Start async rollback process

  Asserts thd->LOCK_thd_data ownership
 */
void wsrep_fire_rollbacker(THD *thd)
{
  assert(thd->wsrep_trx().state() == wsrep::transaction::s_aborting);
  DBUG_PRINT("wsrep",("enqueuing trx abort for %u", thd->thread_id()));
  WSREP_DEBUG("enqueuing trx abort for (%u)", thd->thread_id());
  if (wsrep_rollback_queue->push_back(thd))
  {
    WSREP_WARN("duplicate thd %u for rollbacker",
               thd->thread_id());
  }
}

int wsrep_abort_thd(THD *bf_thd, THD *victim_thd, bool signal)
{
  DBUG_ENTER("wsrep_abort_thd");
  mysql_mutex_lock(&victim_thd->LOCK_thd_data);
  if ( (WSREP(bf_thd) ||
         ( (WSREP_ON || bf_thd->variables.wsrep_OSU_method == WSREP_OSU_RSU) &&
           wsrep_thd_is_toi(bf_thd)) )                         &&
       victim_thd &&
       !wsrep_thd_is_aborting(victim_thd))
  {
      WSREP_DEBUG("wsrep_abort_thd, by: %llu, victim: %llu", (bf_thd) ?
                  (long long)bf_thd->real_id : 0, (long long)victim_thd->real_id);
      mysql_mutex_unlock(&victim_thd->LOCK_thd_data);
      ha_wsrep_abort_transaction(bf_thd, victim_thd, signal);
      mysql_mutex_lock(&victim_thd->LOCK_thd_data);
  }
  else
  {
    WSREP_DEBUG("wsrep_abort_thd not effective: %p %p", bf_thd, victim_thd);
  }
  mysql_mutex_unlock(&victim_thd->LOCK_thd_data);
  DBUG_RETURN(1);
}

bool wsrep_bf_abort(THD* bf_thd, THD* victim_thd)
{
  WSREP_LOG_THD(bf_thd, "BF aborter before");

  wsrep_lock_thd_query(victim_thd, true);
  WSREP_LOG_THD(victim_thd, "victim before");
  wsrep_lock_thd_query(victim_thd, false);

  DBUG_EXECUTE_IF("sync.wsrep_bf_abort",
                  {
                    const char act[]=
                      "now "
                      "SIGNAL sync.wsrep_bf_abort_reached "
                      "WAIT_FOR signal.wsrep_bf_abort";
                    assert(!debug_sync_set_action(bf_thd,
                                                       STRING_WITH_LEN(act)));
                  };);
  
  wsrep::seqno bf_seqno(bf_thd->wsrep_trx().ws_meta().seqno());

  if (WSREP(victim_thd) && !victim_thd->wsrep_trx().active())
  {
    WSREP_DEBUG("wsrep_bf_abort, BF abort for non active transaction");
    switch (victim_thd->wsrep_trx().state()) {
    case wsrep::transaction::s_aborting: /* fall through */
    case wsrep::transaction::s_aborted:
      WSREP_DEBUG("victim is aborting ot has aborted");
      return false;
    default: break;
    }
 
    wsrep_start_transaction(victim_thd, victim_thd->wsrep_next_trx_id());
  }

  bool ret;
  if (wsrep_thd_is_toi(bf_thd))
  {
    ret= victim_thd->wsrep_cs().total_order_bf_abort(bf_seqno);
  }
  else
  {
    ret= victim_thd->wsrep_cs().bf_abort(bf_seqno);
  }
  if (ret)
  {
    wsrep_bf_aborts_counter++;
  }
  return ret;
}

int wsrep_create_threadvars()
{
  int ret= 0;
  if (Connection_handler_manager::thread_handling ==
      Connection_handler_manager::SCHEDULER_TYPES_COUNT)
  {
    /* Caller should have called wsrep_reset_threadvars() before this
       method. */
    assert(!my_thread_var_id());
#ifndef NDEBUG
    set_my_thread_var_id(0);
#endif /* NDEBUG */
    ret= my_thread_init();
  }
  return ret;
}

void wsrep_delete_threadvars()
{
  if (Connection_handler_manager::thread_handling ==
      Connection_handler_manager::SCHEDULER_TYPES_COUNT)
  {
    /* The caller should have called wsrep_store_threadvars() before
       this method. */
    assert(my_thread_var_id());
    my_thread_end();
#ifndef NDEBUG
    set_my_thread_var_id(0);
#endif /* NDEBUG */
  }
}

void wsrep_assign_from_threadvars(THD *)
{
  /* In MySQL this is no-op */
}

Wsrep_threadvars wsrep_save_threadvars()
{
  return Wsrep_threadvars
         {
           current_thd
#ifndef NDEBUG
           , my_thread_var_id()
#endif /* NDEBUG */
         };
}

void wsrep_restore_threadvars(const Wsrep_threadvars& globals)
{
  current_thd = globals.cur_thd;
#ifndef NDEBUG
  set_my_thread_var_id(globals.cur_id);
#endif /* NDEBUG */
}

int wsrep_store_threadvars(THD *thd)
{
  if (Connection_handler_manager::thread_handling ==
      Connection_handler_manager::SCHEDULER_TYPES_COUNT)
  {
#ifndef NDEBUG
    set_my_thread_var_id(thd->thread_id());
#endif /* NDEBUG */
  }
  thd->store_globals();
  return 0;
}

void wsrep_reset_threadvars(THD *thd)
{
  if (Connection_handler_manager::thread_handling ==
      Connection_handler_manager::SCHEDULER_TYPES_COUNT)
  {
#ifndef NDEBUG
    set_my_thread_var_id(0);
#endif /* NDEBUG */
  }
  else
  {
    thd->restore_globals();
  }
}
