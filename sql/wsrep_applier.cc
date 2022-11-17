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
#include "rpl_rli.h"   // class Relay_log_info
#include "log_event.h" // class THD, EVENT_LEN_OFFSET, etc.
#include "debug_sync.h"
#include "sql/binlog_reader.h"

#include "mysql/service_wsrep.h"
#include "wsrep_applier.h"

#include "wsrep_priv.h"
#include "wsrep_binlog.h" // wsrep_dump_rbr_buf()
#include "wsrep_xid.h"
#include "wsrep_thd.h"
#include "wsrep_trans_observer.h"

/*
  read the first event from (*buf). The size of the (*buf) is (*buf_len).
  At the end (*buf) is shitfed to point to the following event or NULL and
  (*buf_len) will be changed to account just being read bytes of the 1st event.
*/
static Log_event* wsrep_read_log_event(
    const char **arg_buf, size_t *arg_buf_len,
    const Format_description_log_event *description_event)
{
  DBUG_ENTER("wsrep_read_log_event");
  const char *head= (*arg_buf);

  uint data_len= uint4korr(head + EVENT_LEN_OFFSET);
  const char *buf= (*arg_buf);
  Log_event *res=  0;

  Binlog_read_error binlog_error =
    binlog_event_deserialize((const uchar*)(buf), data_len,
                               description_event, false, &res);
  if (binlog_error.has_error()) {
      WSREP_WARN(
          "Error in reading binlog event: '%s', data_len: %d, event_type: %d",
          binlog_error.get_str(), data_len, head[EVENT_TYPE_OFFSET]);
  }

  (*arg_buf)+= data_len;
  (*arg_buf_len)-= data_len;
  DBUG_RETURN(res);
}

#include "transaction.h" // trans_commit(), trans_rollback()

void wsrep_set_apply_format(THD* thd, Format_description_log_event* ev)
{
  if (thd->wsrep_apply_format)
  {
    delete thd->wsrep_apply_format;
  }
  thd->wsrep_apply_format= ev;
}

Format_description_log_event*
wsrep_get_apply_format(THD* thd)
{
  if (thd->wsrep_apply_format)
  {
    return thd->wsrep_apply_format;
  }

  assert(thd->wsrep_rli);

  return thd->wsrep_rli->get_rli_description_event();
}

void wsrep_store_error(const THD* const thd, wsrep::mutable_buffer& dst)
{
  Diagnostics_area::Sql_condition_iterator it=
    thd->get_stmt_da()->sql_conditions();
  const Sql_condition* cond;

  static size_t const max_len= 2*MAX_SLAVE_ERRMSG; // 2x so that we have enough

  dst.resize(max_len);

  char* slider= dst.data();
  const char* const buf_end= slider + max_len - 1; // -1: leave space for \0

  for (cond= it++; cond && slider < buf_end; cond= it++)
  {
    uint const err_code= cond->mysql_errno();
    const char* const err_str= cond->message_text();

    slider+= snprintf(slider, buf_end - slider, " %s, Error_code: %d;",
                         err_str, err_code);
  }

  if (slider != dst.data())
  {
    *slider= '\0';
    slider++;
  }

  dst.resize(slider - dst.data());

  WSREP_DEBUG("Error buffer for thd %u seqno %lld, %zu bytes: '%s'",
              thd->thread_id(), (long long)wsrep_thd_trx_seqno(thd),
              dst.size(), dst.size() ? dst.data() : "(null)");
}

int wsrep_apply_events(THD*            thd,
                       Relay_log_info* rli MY_ATTRIBUTE((unused)),
                       const void*     events_buf,
                       size_t          buf_len)
{
  const char *buf = (const char *)events_buf;
  int rcode = 0;
  int event = 1;

  DBUG_ENTER("wsrep_apply_events");
  if (!buf_len) WSREP_DEBUG("empty rbr buffer to apply: %lld",
                            (long long) wsrep_thd_trx_seqno(thd));

  /*
    Events that are replicated from cluster node, which is used in master-slave
    arhitecture, contains GTID_LOG_EVENT if gtid-mode=[ON, ON_PRESERVE].
    This event updates gtid_next variable, so we should "reset" it in case
    that following events doesn't contain GTID value.
   */
  thd->variables.gtid_next.set_automatic();

  while (buf_len) {
    int exec_res;
    Log_event* ev = wsrep_read_log_event(&buf, &buf_len,
                                         wsrep_get_apply_format(thd));
    if (!ev) {
      WSREP_ERROR("applier could not read binlog event, seqno: %lld, len: %zu",
                  (long long)wsrep_thd_trx_seqno(thd), buf_len);
      rcode = WSREP_ERR_BAD_EVENT;
      goto error;
    }

    switch (ev->get_type_code()) {
      case binary_log::FORMAT_DESCRIPTION_EVENT:
        wsrep_set_apply_format(thd, (Format_description_log_event*)ev);
        continue;
      case binary_log::GTID_LOG_EVENT: {
        /*
          gtid_pre_statement_checks will fail on the subsequent statement
          if the bits below are set. So we don't mark the thd to run in
          transaction mode yet, and assume there will be such a "BEGIN"
          log event that will set those appropriately.
        */
        thd->variables.option_bits &= ~OPTION_BEGIN;
        thd->server_status &= ~SERVER_STATUS_IN_TRANS;
        assert(event == 1);
        break;
      }
      default:
        break;
    }

    /*
      Cluster originated TOI under wsrep_gtid_mode doesn't advance seqno,
      so we should do it here.
     */
    if (wsrep_gtid_mode &&
        (thd->variables.gtid_next.type == AUTOMATIC_GTID) &&
        wsrep_thd_is_toi(thd) &&
        (ev->get_type_code() == binary_log::QUERY_EVENT)) {
      thd->wsrep_current_gtid_seqno = wsrep_local_gtid_manager.seqno_inc();
    }

    thd->server_id = ev->server_id; // use the original sver id for logging
    thd->unmasked_server_id = ev->common_header->unmasked_server_id;
    thd->set_time();                // time the query

    thd->lex->set_current_query_block(nullptr);
    if (!ev->common_header->when.tv_sec)
      my_micro_time_to_timeval(my_micro_time(), &ev->common_header->when);
    ev->thd = thd; // because up to this point, ev->thd == 0

    set_timespec_nsec(&thd->wsrep_rli->ts_exec[0], 0);
    thd->wsrep_rli->stats_read_time += diff_timespec(&thd->wsrep_rli->ts_exec[0],
                                                     &thd->wsrep_rli->ts_exec[1]);

    /* MySQL slave "Sleeps if needed, and unlocks rli->data_lock."
     * at this point. But this does not apply for wsrep, we just do the unlock part
     * of sql_delay_event()
     *
     * if (sql_delay_event(ev, thd, rli))
     */
    //mysql_mutex_assert_owner(&rli->data_lock);
    //mysql_mutex_unlock(&rli->data_lock);

    exec_res = ev->apply_event(thd->wsrep_rli);
    DBUG_PRINT("info", ("exec_event result: %d", exec_res));

    if (exec_res) {
      WSREP_WARN("Event %d %s apply failed: %d, seqno %lld",
                 event, ev->get_type_str(), exec_res,
                 (long long) wsrep_thd_trx_seqno(thd));
      rcode = exec_res;
      /* stop processing for the first error */
      delete ev;
      goto error;
    }

    set_timespec_nsec(&thd->wsrep_rli->ts_exec[1], 0);
    thd->wsrep_rli->stats_exec_time += diff_timespec(&thd->wsrep_rli->ts_exec[1],
                                          &thd->wsrep_rli->ts_exec[0]);

    DBUG_PRINT("info", ("wsrep apply_event error = %d", exec_res));
    event++;

    switch (ev->get_type_code()) {
      case binary_log::ROWS_QUERY_LOG_EVENT:
        /*
          Keeping Rows_log event, it will be needed still, and will be deleted later
          in rli->cleanup_context()
          Also FORMAT_DESCRIPTION_EVENT is needed further, but it skipped from this loop
          by 'continue' above, and thus  avoids the following 'delete ev'
        */
        continue;
      default:
        delete ev;
        break;
    }
  }

error:
  if (thd->killed == THD::KILL_CONNECTION)
    WSREP_INFO("applier aborted: %lld", (long long)wsrep_thd_trx_seqno(thd));

  if (rcode)
    DBUG_RETURN(WSREP_CB_FAILURE);

  DBUG_RETURN(WSREP_CB_SUCCESS);
}

