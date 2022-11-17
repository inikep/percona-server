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

#ifndef WSREP_BINLOG_H
#define WSREP_BINLOG_H

#include "wsrep.h" // WSREP_WARN
#include "wsrep_mysqld.h" // wsrep_max_ws_size
#include "sql_class.h" // THD, IO_CACHE
#include "binlog.h" // Binlog_cache_storage
#include "binlog_ostream.h"

#define HEAP_PAGE_SIZE 65536 /* 64K */
#define WSREP_MAX_WS_SIZE 2147483647 /* 2GB */

class Log_event;

class Wsrep_ws_size_limited_ostream : public Basic_ostream {
  size_t m_total_length;
  Basic_ostream &m_ostream;

 public:
  Wsrep_ws_size_limited_ostream(Basic_ostream &ostream)
      : m_total_length(0), m_ostream(ostream) {}
  bool write(const unsigned char *buffer, my_off_t length) override {
    m_total_length += length;
    if (m_total_length > wsrep_max_ws_size) {
      WSREP_WARN("transaction size limit (%lu) exceeded: %lu",
                 wsrep_max_ws_size, m_total_length);
      return true;
    }
    return m_ostream.write(buffer, length);
  }
};

class Wsrep_gtid_writer_ostream : public Basic_ostream {
  THD *m_thd;
  Basic_ostream &m_ostream;

 public:
  Wsrep_gtid_writer_ostream(THD *thd, Basic_ostream &ostream)
      : m_thd(thd), m_ostream(ostream) {}

  bool write(const unsigned char *buffer, my_off_t length) override {
    if (length > 0 && m_thd->wsrep_gtid_event_buf && write_gtid_event()) {
      return true;
    }
    return m_ostream.write(buffer, length);
  }

 private:
  bool write_gtid_event() {
    assert(m_thd->wsrep_gtid_event_buf);
    bool ret = m_ostream.write(
        reinterpret_cast<const unsigned char *>(m_thd->wsrep_gtid_event_buf),
        m_thd->wsrep_gtid_event_buf_len);
    my_free(m_thd->wsrep_gtid_event_buf);
    m_thd->wsrep_gtid_event_buf_len = 0;
    m_thd->wsrep_gtid_event_buf = NULL;
    return ret;
  }
};

class Wsrep_mutable_buffer_ostream : public Basic_ostream {
  wsrep::mutable_buffer &m_buffer;

 public:
  Wsrep_mutable_buffer_ostream(wsrep::mutable_buffer &buffer)
      : m_buffer(buffer) {}
  bool write(const unsigned char *buffer, my_off_t length) override {
    try {
      m_buffer.push_back(reinterpret_cast<const char *>(buffer),
                         reinterpret_cast<const char *>(buffer + length));
      return false;
    } catch (...) {
      return true;
    }
  }
};

class Wsrep_data_appender_ostream : public Basic_ostream {
  THD *m_thd;
  size_t m_size;

 public:
  Wsrep_data_appender_ostream(THD *thd) : m_thd(thd), m_size(0) {}
  size_t get_size() const { return m_size; }
  bool write(const unsigned char *buffer, my_off_t length) override {
    if (!m_thd->wsrep_cs().append_data(wsrep::const_buffer(buffer, length))) {
      m_size += length;
      return false;
    }
    return true;
  }
};

int wsrep_write_cache_buf_from_iocache(
       IO_CACHE_binlog_cache_storage *cache, uchar **buf, size_t *buf_len);

/* Dump replication buffer to disk */
void wsrep_dump_rbr_buf(THD *thd, const void* rbr_buf, size_t buf_len);

/* Dump replication buffer along with header to a file */
void wsrep_dump_rbr_buf_with_header(THD *thd, const void *rbr_buf,
                                    size_t buf_len);


void wsrep_register_binlog_handler(THD *thd, bool trx);
void wsrep_thd_binlog_trx_reset(THD * thd);

/**
   Return true if committing THD will write to binlog during commit.
   This is the case for:
   - Local THD, binlog is open
   - Replaying THD, binlog is open
   - Applier THD, log-replica-updates is enabled
*/
bool wsrep_commit_will_write_binlog(THD *thd);

/**
   Register THD for group commit. The wsrep_trx must be in committing state,
   i.e. the call must be done after wsrep_before_commit() but before
   commit order is released.

   This call will release commit order critical section if it is
   determined that the commit will go through binlog group commit.
 */
void wsrep_register_for_group_commit(THD *thd);

/**
   Deregister THD from group commit. The wsrep_trx must be in committing state,
   as for wsrep_register_for_group_commit() above.

   This call must be used only for THDs which will not go through
   binlog group commit.
*/
void wsrep_unregister_from_group_commit(THD *thd);

/**
   Copy the given Log_event ev to thd->wsrep_gtid_event_buf.
   This will be consumed by Wsrep_gtid_writer_ostream during
   replication.
 */
void wsrep_set_gtid_log_event(THD *thd, Log_event* ev);

#endif /* WSREP_BINLOG_H */
