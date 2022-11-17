/* Copyright 2014 Codership Oy <http://www.codership.com> & SkySQL Ab

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02111-1301 USA */

#ifndef WSREP_INCLUDED
#define WSREP_INCLUDED

#include <my_config.h>
#include <errno.h>
#include "sql/log.h"
#include "mysql/components/services/log_builtins.h"

#ifdef WITH_WSREP

#define IF_WSREP(A,B) A
#define assert_IF_WSREP(A) assert(A)

/* Base wsrep logging macro */
extern ulong wsrep_debug; // wsrep_mysqld.cc

#define WSREP_LOG(severity, fmt, ...)                              \
  do {                                                             \
    LogEvent()                                                     \
        .prio(severity)                                            \
        .subsys("WSREP")                                           \
        .component("WSREP")                                        \
        .source_line(__LINE__)                                     \
        .source_file(MY_BASENAME)                                  \
        .function(__FUNCTION__)                                    \
        .message(fmt, ##__VA_ARGS__);                              \
  } while (0)

/* When you want to log with a prefix (and some voodoo to avoid extra
 * space when prefix in null) */
#define WSREP_LOG_PFX(level, pfx, fmt, ...) \
  WSREP_LOG(level, "%s%.*s" fmt, \
            pfx ? pfx : "", pfx ? 1 : 0, " ", ##__VA_ARGS__)

/* Logging macros that take additional prefix string to tag the line */
#define WSREP_DEBUG_PFX(pfx, fmt, ...) \
  if (wsrep_debug) \
    WSREP_LOG_PFX(INFORMATION_LEVEL, pfx, "Debug:" fmt, ##__VA_ARGS__)
#define WSREP_INFO_PFX(pfx, fmt, ...) \
  WSREP_LOG_PFX(INFORMATION_LEVEL, pfx, fmt, ##__VA_ARGS__)
#define WSREP_WARN_PFX(pfx, fmt, ...) \
  WSREP_LOG_PFX(WARNING_LEVEL, pfx, fmt, ##__VA_ARGS__)
/* Flush error log after writing error log entry. This may mean
   that the provider will abort the process and we want to see all messages.*/
#define WSREP_ERROR_PFX(pfx, fmt, ...) do {                     \
        WSREP_LOG_PFX(ERROR_LEVEL, pfx, fmt, ##__VA_ARGS__);    \
        flush_error_log_messages();                             \
    } while (0)
#define WSREP_UNKNOWN_PFX(pfx, fmt, ...) \
  WSREP_ERROR_PFX(pfx, "UNKNOWN:" fmt, ##__VA_ARGS__)

/* Default empty prefix */
#define WSREP_DEBUG(fmt, ...)   WSREP_DEBUG_PFX(nullptr, fmt, ##__VA_ARGS__)
#define WSREP_INFO(fmt, ...)    WSREP_INFO_PFX(nullptr, fmt, ##__VA_ARGS__)
#define WSREP_WARN(fmt, ...)    WSREP_WARN_PFX(nullptr, fmt, ##__VA_ARGS__)
#define WSREP_ERROR(fmt, ...)   WSREP_ERROR_PFX(nullptr, fmt, ##__VA_ARGS__)
#define WSREP_UNKNOWN(fmt, ...) WSREP_UNKNOWN_PFX(nullptr, fmt, ##__VA_ARGS__)

#define WSREP_LOG_CONFLICT_THD(thd, role) do {                          \
  WSREP_LOG(INFORMATION_LEVEL,                                          \
            "%s:",                                                      \
            role                                                        \
            );                                                          \
  WSREP_LOG(INFORMATION_LEVEL,                                          \
            "  THD: %lu, mode: %s, state: %s, conflict: %s, seqno: %lld", \
            thd_get_thread_id(thd),                                     \
            wsrep_thd_client_mode_str(thd),                             \
            wsrep_thd_client_state_str(thd),                            \
            wsrep_thd_transaction_state_str(thd),                       \
            wsrep_thd_trx_seqno(thd)                                    \
            );                                                          \
  wsrep_lock_thd_query(thd, true);                                      \
  WSREP_LOG(INFORMATION_LEVEL, "  SQL: %s", wsrep_thd_query(thd));      \
  wsrep_lock_thd_query(thd, false);                                     \
  } while (0);

#define WSREP_LOG_CONFLICT(bf_thd, victim_thd, bf_abort)                \
  if (wsrep_debug || wsrep_log_conflicts) {                             \
    WSREP_LOG(INFORMATION_LEVEL,                                        \
              "cluster conflict due to %s for threads:",                \
              (bf_abort) ? "high priority abort" :                      \
                           "certification failure");                    \
    if (bf_thd) {                                                       \
      WSREP_LOG_CONFLICT_THD(bf_thd, "Winning thread");                 \
    }                                                                   \
    if (victim_thd) {                                                   \
      WSREP_LOG_CONFLICT_THD(victim_thd, "Victim thread");              \
    }                                                                   \
    WSREP_LOG(INFORMATION_LEVEL, "context: %s:%d", __FILE__, __LINE__); \
  }

#else /* !WITH_WSREP */

/* These macros are needed to compile MariaDB without WSREP support
 * (e.g. embedded) */

#define IF_WSREP(A,B) B
//#define assert_IF_WSREP(A)
#define WSREP_DEBUG(...)
#define WSREP_INFO(...)
#define WSREP_WARN(...)
#define WSREP_ERROR(...)
#endif /* WITH_WSREP */

#endif /* WSREP_INCLUDED */
