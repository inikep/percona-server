/* Copyright (C) 2013 Codership Oy <info@codership.com>

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
#include "log.h"
#include "log_event.h"
#include "binlog_ostream.h"
#include "transaction.h"
#include "binlog.h"
#include "mysql/psi/mysql_file.h"
#include "mysql/plugin.h" // thd_get_ha_data
#include "my_io.h"
#include "basic_ostream.h"

#include "mysql/service_wsrep.h"
#include "wsrep_binlog.h"
#include "wsrep_priv.h"
#include "wsrep_applier.h"

class Binlog_event_writer;
extern bool opt_log_replica_updates;

int wsrep_write_cache_buf_from_iocache(
       IO_CACHE_binlog_cache_storage *cache, uchar **buf, size_t *buf_len)
{
  my_off_t const saved_pos(cache->length());

  unsigned char* read_pos = NULL;
  my_off_t read_len = 0;

  if (cache->begin(&read_pos, &read_len)) {
    WSREP_ERROR("Failed to initialize io-cache");
    return ER_ERROR_ON_WRITE;
  }

  if (read_len == 0 && cache->next(&read_pos, &read_len)) {
    WSREP_ERROR("Failed to read from io-cache");
    return ER_ERROR_ON_WRITE;
  }

  size_t total_length = *buf_len;

  while (read_len > 0) {
    total_length += read_len;

    /*
      Bail out if buffer grows too large.
      A temporary fix to avoid allocating indefinitely large buffer,
      not a real limit on a writeset size which includes other things
      like header and keys.
    */
    if (total_length > wsrep_max_ws_size) {
      WSREP_WARN("Transaction/Write-set size limit (%lu) exceeded: %zu",
                 wsrep_max_ws_size, total_length);
      goto error;
    }

    uchar *tmp =
        (uchar *)my_realloc(key_memory_wsrep, *buf, total_length, MYF(0));
    if (!tmp) {
      WSREP_ERROR(
          "Fail to allocate/reallocate memory to hold"
          " write-set for replication."
          " Existing Size: %zu, Requested Size: %lu",
          *buf_len, (long unsigned)total_length);
      goto error;
    }
    *buf = tmp;

    memcpy(*buf + *buf_len, read_pos, read_len);
    *buf_len = total_length;
    cache->next(&read_pos, &read_len);
  }

  if (cache->truncate(saved_pos)) {
    WSREP_WARN("Failed to reinitialize io-cache");
    goto cleanup;
  }

  return 0;

error:

  if (cache->truncate(saved_pos)) {
    WSREP_WARN("Failed to reinitialize io-cache");
  }

cleanup:
  my_free(*buf);
  *buf = NULL;
  *buf_len = 0;
  return ER_ERROR_ON_WRITE;
}

#define STACK_SIZE 4096 /* 4K - for buffer preallocated on the stack:
                         * many transactions would fit in there
                         * so there is no need to reach for the heap */

/* Returns minimum multiple of HEAP_PAGE_SIZE that is >= length */
static inline size_t
heap_size(size_t length)
{
    return (length + HEAP_PAGE_SIZE - 1)/HEAP_PAGE_SIZE*HEAP_PAGE_SIZE;
}
/* append data to writeset */
static inline wsrep_status_t
wsrep_append_data(wsrep_t*           const wsrep,
                  wsrep_ws_handle_t* const ws,
                  const void*        const data,
                  size_t             const len)
{
    struct wsrep_buf const buff = { data, len };
    wsrep_status_t const rc(wsrep->append_data(wsrep, ws, &buff, 1,
                                               WSREP_DATA_ORDERED, true));
    if (rc != WSREP_OK)
    {
        WSREP_WARN("append_data() returned %d", rc);
    }

    return rc;
}

void wsrep_dump_rbr_buf(THD *thd, const void* rbr_buf, size_t buf_len)
{
  int len= snprintf(NULL, 0, "%s/GRA_%u_%lld.log",
                    wsrep_data_home_dir, thd->thread_id(),
                    (longlong) wsrep_thd_trx_seqno(thd));
  if (len < 0)
    {
      WSREP_ERROR("snprintf error: %d, skipping dump.", len);
      return;
    }
  /*
    len doesn't count the \0 end-of-string. Use len+1 below
    to alloc and pass as an argument to snprintf.
  */
  
  char *filename= (char *)malloc(len+1);
  int len1= snprintf(filename, len+1, "%s/GRA_%u_%lld.log",
		     wsrep_data_home_dir, thd->thread_id(),
                    (long long)wsrep_thd_trx_seqno(thd));

  if (len > len1)
  {
    WSREP_ERROR("RBR dump path truncated: %d, skipping dump.", len);
    free(filename);
    return;
  }

  FILE *of= fopen(filename, "wb");

  if (of)
  {
    if (fwrite(rbr_buf, buf_len, 1, of) == 0)
       WSREP_ERROR("Failed to write buffer of length %llu to '%s'",
                   (unsigned long long)buf_len, filename);

    fclose(of);
  }
  else
  {
    WSREP_ERROR("Failed to open file '%s': %d (%s)",
                filename, errno, strerror(errno));
  }
  free(filename);
}

void wsrep_dump_rbr_direct(THD* thd, IO_CACHE* cache)
{
  char filename[PATH_MAX]= {0};
  int len= snprintf(filename, PATH_MAX, "%s/GRA_%u_%lld.log",
                    wsrep_data_home_dir, thd->thread_id(),
                    (long long)wsrep_thd_trx_seqno(thd));
  size_t bytes_in_cache = 0;
  // check path
  if (len >= PATH_MAX)
  {
    WSREP_ERROR("RBR dump path too long: %d, skipping dump.", len);
    return ;
  }
  // init cache
  my_off_t const saved_pos(my_b_tell(cache));
  if (reinit_io_cache(cache, READ_CACHE, 0, 0, 0))
  {
    WSREP_ERROR("failed to initialize io-cache");
    return ;
  }
  // open file
  FILE* of = fopen(filename, "wb");
  if (!of)
  {
    WSREP_ERROR("Failed to open file '%s': %d (%s)",
                filename, errno, strerror(errno));
    goto cleanup;
  }
  // ready to write
  bytes_in_cache= my_b_bytes_in_cache(cache);
  if (unlikely(bytes_in_cache == 0)) bytes_in_cache = my_b_fill(cache);
  if (likely(bytes_in_cache > 0)) do
  {
    if (my_fwrite(of, cache->read_pos, bytes_in_cache,
                  MYF(MY_WME | MY_NABP)) == (size_t) -1)
    {
      WSREP_ERROR("Failed to write file '%s'", filename);
      goto cleanup;
    }
    cache->read_pos= cache->read_end;
  } while ((cache->file >= 0) && (bytes_in_cache= my_b_fill(cache)));
  if(cache->error == -1)
  {
    WSREP_ERROR("RBR inconsistent");
    goto cleanup;
  }
cleanup:
  // init back
  if (reinit_io_cache(cache, WRITE_CACHE, saved_pos, 0, 0))
  {
    WSREP_ERROR("failed to reinitialize io-cache");
  }
  // close file
  if (of) fclose(of);
}

/* Dump replication buffer along with header to a file. */
void wsrep_dump_rbr_buf_with_header(THD *thd, const void *rbr_buf,
                                    size_t buf_len)
{
  DBUG_ENTER("wsrep_dump_rbr_buf_with_header");

  File file;
  IO_CACHE cache;
  Format_description_log_event *ev= 0;

  longlong thd_trx_seqno= (long long)wsrep_thd_trx_seqno(thd);
  int len= snprintf(NULL, 0, "%s/GRA_%u_%lld_v2.log",
                    wsrep_data_home_dir, thd->thread_id(),
                    thd_trx_seqno);
  /*
    len doesn't count the \0 end-of-string. Use len+1 below
    to alloc and pass as an argument to snprintf.
  */
  char *filename;
  if (len < 0 || !(filename= (char*)malloc(len+1)))
  {
    WSREP_ERROR("snprintf error: %d, skipping dump.", len);
    DBUG_VOID_RETURN;
  }

  int len1= snprintf(filename, len+1, "%s/GRA_%u_%lld_v2.log",
                     wsrep_data_home_dir, thd->thread_id(),
                     thd_trx_seqno);

  if (len > len1)
  {
    WSREP_ERROR("RBR dump path truncated: %d, skipping dump.", len);
    free(filename);
    DBUG_VOID_RETURN;
  }

  if ((file= mysql_file_open(key_file_wsrep_gra_log, filename,
                             O_RDWR | O_CREAT | MY_FOPEN_BINARY, MYF(MY_WME))) < 0)
  {
    WSREP_ERROR("Failed to open file '%s' : %d (%s)",
                filename, errno, strerror(errno));
    goto cleanup1;
  }

  if (init_io_cache(&cache, file, 0, WRITE_CACHE, 0, 0, MYF(MY_WME | MY_NABP)))
  {
    mysql_file_close(file, MYF(MY_WME));
    goto cleanup2;
  }

  if (my_b_safe_write(&cache, (const uchar*)(BINLOG_MAGIC), BIN_LOG_HEADER_SIZE))
  {
    goto cleanup2;
  }

  /*
    Instantiate an FDLE object for non-wsrep threads (to be written
    to the dump file).
  */
  ev= (thd->wsrep_applier) ? wsrep_get_apply_format(thd) :
    (new Format_description_log_event());

#ifdef WSREP_TODO  
  //if (binary_event_serialize(ev, mysql_bin_log.get_binlog_file()))
  if (ev->write(mysql_bin_log.get_binlog_file()))
  {
    WSREP_ERROR("Failed to write to '%s'.", filename);
    goto cleanup2;
  }
#endif
  if (my_b_write(&cache, (const uchar*)(rbr_buf), buf_len) ||
      flush_io_cache(&cache))
  {
    WSREP_ERROR("Failed to write to '%s'.", filename);
    goto cleanup2;
  }

cleanup2:
  end_io_cache(&cache);

cleanup1:
  free(filename);
  mysql_file_close(file, MYF(MY_WME));

  if (!thd->wsrep_applier) delete ev;

  DBUG_VOID_RETURN;
}

bool wsrep_commit_will_write_binlog(THD *thd)
{
  return (!wsrep_emulate_bin_log && /* binlog enabled*/
          (wsrep_thd_is_local(thd) || /* local thd*/
           (thd->wsrep_applier_service && /* applier and log-replica-updates */
            opt_log_replica_updates)));
}

#ifdef NOT_IMPLEMENTED
/*
  The last THD/commit_for_wait registered for group commit.
*/
static wait_for_commit *commit_order_tail= NULL;

void wsrep_register_for_group_commit(THD *thd)
{
  DBUG_ENTER("wsrep_register_for_group_commit");
  if (wsrep_emulate_bin_log)
  {
    /* Binlog is off, no need to maintain group commit queue */
    DBUG_VOID_RETURN;
  }

  assert(thd->wsrep_trx().state() == wsrep::transaction::s_committing);

  wait_for_commit *wfc= thd->wait_for_commit_ptr= &thd->wsrep_wfc;

  mysql_mutex_lock(&LOCK_wsrep_group_commit);
  if (commit_order_tail)
  {
    wfc->register_wait_for_prior_commit(commit_order_tail);
  }
  commit_order_tail= thd->wait_for_commit_ptr;
  mysql_mutex_unlock(&LOCK_wsrep_group_commit);

  /*
    Now we have queued for group commit. If the commit will go
    through TC log_and_order(), the commit ordering is done
    by TC group commit. Otherwise the wait for prior
    commits to complete is done in ha_commit_one_phase().
  */
  DBUG_VOID_RETURN;
}

void wsrep_unregister_from_group_commit(THD *thd)
{
  assert(thd->wsrep_trx().state() == wsrep::transaction::s_ordered_commit||
              // ordered_commit() failure results in s_aborting state
              thd->wsrep_trx().state() == wsrep::transaction::s_aborting);
  wait_for_commit *wfc= thd->wait_for_commit_ptr;

  if (wfc)
  {
    mysql_mutex_lock(&LOCK_wsrep_group_commit);
    wfc->unregister_wait_for_prior_commit();
    //thd->wakeup_subsequent_commits(0);

    /* The last one queued for group commit has completed commit, it is
       safe to set tail to NULL. */
    if (wfc == commit_order_tail)
      commit_order_tail= NULL;
    mysql_mutex_unlock(&LOCK_wsrep_group_commit);
    thd->wait_for_commit_ptr= NULL;
  }
}
#endif

void wsrep_set_gtid_log_event(THD *thd, Log_event* ev)
{
  assert(!thd->wsrep_applier);
  assert(ev->get_type_code() == binary_log::GTID_LOG_EVENT);
  if (thd->wsrep_gtid_event_buf)
  {
    WSREP_WARN("MySQL GTID event pending");
    my_free((uchar*)thd->wsrep_gtid_event_buf);
    thd->wsrep_gtid_event_buf     = NULL;
    thd->wsrep_gtid_event_buf_len = 0;
  }
  ulong len= thd->wsrep_gtid_event_buf_len=
    uint4korr(ev->temp_buf + EVENT_LEN_OFFSET);
  thd->wsrep_gtid_event_buf= (void*)my_realloc(
    key_memory_wsrep,
    thd->wsrep_gtid_event_buf,
    thd->wsrep_gtid_event_buf_len,
    MYF(0));
  if (!thd->wsrep_gtid_event_buf)
  {
    WSREP_WARN("GTID event allocation for slave failed");
    thd->wsrep_gtid_event_buf_len= 0;
  }
  else
  {
    memcpy(thd->wsrep_gtid_event_buf, ev->temp_buf, len);
  }
}
