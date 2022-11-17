/* Copyright 2008-2019 Codership Oy <http://www.codership.com>

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

#ifndef WSREP_MYSQLD_H
#define WSREP_MYSQLD_H

#ifdef WITH_WSREP

#include "my_sqlcommand.h" // enum_sql_command
#include "rpl_gtid.h"

#include "mdl.h"

#include "mysql/psi/mysql_cond.h"
#include "mysql/psi/mysql_mutex.h"

#include "mysql/service_wsrep.h"
#include "wsrep_mysqld_c.h"
#include "wsrep/provider.hpp"
#include "wsrep/streaming_context.hpp"
#include "wsrep_api.h"
#include "wsrep_mutex.h"
#include "wsrep_condition_variable.h"

#include <atomic>
#include <map>

#define WSREP_UNDEFINED_TRX_ID ULLONG_MAX

extern bool opt_general_log_raw; // mysqld.cc

// Global wsrep parameters

// MySQL wsrep options
extern const char*        wsrep_provider_options;
extern const char*        wsrep_cluster_name;
extern const char*        wsrep_cluster_address;
extern const char*        wsrep_node_name;
extern const char*        wsrep_node_address;
extern const char*        wsrep_node_incoming_address;
extern const char*        wsrep_data_home_dir;
extern const char*        wsrep_dbug_option;
extern long               wsrep_slave_threads;
extern int                wsrep_slave_count_change;
extern ulong              wsrep_debug;
extern bool               wsrep_convert_LOCK_to_trx;
extern ulong              wsrep_retry_autocommit;
extern bool               wsrep_auto_increment_control;
extern bool               wsrep_drupal_282555_workaround;
extern bool               wsrep_incremental_data_collection;
extern const char*        wsrep_start_position;
extern ulong              wsrep_max_ws_size;
extern ulong              wsrep_max_ws_rows;
extern const char*        wsrep_notify_cmd;
extern const char*        wsrep_status_file;
extern bool               wsrep_certify_nonPK;
extern long int           wsrep_protocol_version;
extern bool               wsrep_desync;
extern ulong              wsrep_reject_queries;
extern bool               wsrep_recovery;
extern bool               wsrep_replicate_myisam;
extern bool               wsrep_log_conflicts;
extern ulong              wsrep_mysql_replication_bundle;
extern bool               wsrep_load_data_splitting;
extern bool               wsrep_restart_slave;
extern bool               wsrep_restart_slave_activated;
extern bool               wsrep_slave_FK_checks;
extern bool               wsrep_slave_UK_checks;
extern ulong              wsrep_trx_fragment_unit;
extern ulong              wsrep_SR_store_type;
extern uint               wsrep_ignore_apply_errors;
extern ulonglong   wsrep_mode;
extern std::atomic<long>  wsrep_running_threads;
extern bool               wsrep_new_cluster;
extern bool               wsrep_gtid_mode;
extern uint32             wsrep_gtid_domain_id;

enum enum_wsrep_reject_types {
  WSREP_REJECT_NONE,    /* nothing rejected */
  WSREP_REJECT_ALL,     /* reject all queries, with UNKNOWN_COMMAND error */
  WSREP_REJECT_ALL_KILL /* kill existing connections and reject all queries*/
};

enum enum_wsrep_OSU_method {
    WSREP_OSU_TOI,
    WSREP_OSU_RSU,
    WSREP_OSU_NONE,
};

enum enum_wsrep_sync_wait {
    WSREP_SYNC_WAIT_NONE= 0x0,
    // select, begin
    WSREP_SYNC_WAIT_BEFORE_READ= 0x1,
    WSREP_SYNC_WAIT_BEFORE_UPDATE_DELETE= 0x2,
    WSREP_SYNC_WAIT_BEFORE_INSERT_REPLACE= 0x4,
    WSREP_SYNC_WAIT_BEFORE_SHOW= 0x8,
    WSREP_SYNC_WAIT_MAX= 0xF
};

enum enum_wsrep_ignore_apply_error {
    WSREP_IGNORE_ERRORS_NONE= 0x0,
    WSREP_IGNORE_ERRORS_ON_RECONCILING_DDL= 0x1,
    WSREP_IGNORE_ERRORS_ON_RECONCILING_DML= 0x2,
    WSREP_IGNORE_ERRORS_ON_DDL= 0x4,
    WSREP_IGNORE_ERRORS_MAX= 0x7
};

// Streaming Replication
#define WSREP_FRAG_BYTES      0
#define WSREP_FRAG_ROWS       1
#define WSREP_FRAG_STATEMENTS 2

#define WSREP_SR_STORE_NONE   0
#define WSREP_SR_STORE_TABLE  1

extern const char *wsrep_fragment_units[];
extern const char *wsrep_SR_store_types[];
enum enum_wsrep_mode {
    WSREP_MODE_IGNORE_NATIVE_REPLICATION_FILTER_RULES = 0,
    WSREP_MODE_IGNORE_CASCADING_FK_DELETE_MISSING_ROW_ERROR = 1,
};

// MySQL status variables
extern bool        wsrep_connected;
extern bool        wsrep_ready;
extern const char* wsrep_cluster_state_uuid;
extern long long   wsrep_cluster_conf_id;
extern const char* wsrep_cluster_status;
extern long        wsrep_cluster_size;
extern long        wsrep_local_index;
extern long long   wsrep_local_bf_aborts;
extern const char* wsrep_provider_name;
extern const char* wsrep_provider_version;
extern const char* wsrep_provider_vendor;
extern char*       wsrep_provider_capabilities;
extern char*       wsrep_cluster_capabilities;
extern char*       wsrep_local_gtid;

int  wsrep_show_status(THD *thd, struct SHOW_VAR *var, char *buff);
int  wsrep_show_ready(THD *thd, struct SHOW_VAR *var, char *buff);
void wsrep_free_status(THD *thd);
void wsrep_update_cluster_state_uuid(const char* str);

/* Filters out --wsrep-new-cluster oprtion from argv[]
 * should be called in the very beginning of main() */
void wsrep_filter_new_cluster (int* argc, char* argv[]);
void wsrep_sst_set_defaults   (int argc, char* argv[]);

int  wsrep_init();
void wsrep_deinit(bool free_options);

/* Initialize wsrep thread LOCKs and CONDs */
void wsrep_thr_init();
/* Destroy wsrep thread LOCKs and CONDs */
void wsrep_thr_deinit();

void wsrep_recover();

bool wsrep_before_SE(); // initialize wsrep before storage
                        // engines (true) or after (false)
/* wsrep initialization sequence at startup
 * @param before wsrep_before_SE() value */
void wsrep_init_startup(bool before);

/* Recover streaming transactions from fragment storage */
void wsrep_recover_sr_from_storage(THD *);

// Other wsrep global variables
extern bool wsrep_inited; // whether wsrep is initialized

extern "C" void wsrep_fire_rollbacker(THD *thd);
extern "C" uint32 wsrep_thd_wsrep_rand(THD *thd);
extern void wsrep_close_client_connections(bool is_server_shutdown);
extern "C" query_id_t wsrep_thd_wsrep_last_query_id(THD *thd);
extern "C" void wsrep_thd_set_wsrep_last_query_id(THD *thd, query_id_t id);
extern "C" void wsrep_thd_awake(THD *thd, bool signal);

extern int  wsrep_wait_committing_connections_close(int wait_time);
extern void wsrep_close_applier(THD *thd);
extern void wsrep_wait_appliers_close(THD *thd);
extern void wsrep_close_applier_threads(int count);
extern void wsrep_kill_mysql(THD *thd);


/* new defines */
extern void wsrep_stop_replication(THD *thd);
extern bool wsrep_start_replication();
extern void wsrep_shutdown_replication();
extern bool wsrep_is_show_query(enum enum_sql_command command);
extern bool wsrep_must_sync_wait (THD* thd, uint mask= WSREP_SYNC_WAIT_BEFORE_READ);
extern bool wsrep_sync_wait (THD* thd, uint mask= WSREP_SYNC_WAIT_BEFORE_READ);
extern bool wsrep_sync_wait(THD* thd, enum enum_sql_command command);
extern bool wsrep_check_mode(uint mask);
extern enum wsrep::provider::status
wsrep_sync_wait_upto (THD* thd, wsrep_gtid_t* upto, int timeout);
#ifdef NOT_IMPLEMENTED
extern int  wsrep_check_opts();
#endif
extern int  wsrep_check_opts (int argc, char* const* argv);
extern void wsrep_prepend_PATH (const char* path);
void wsrep_append_fk_parent_table(THD* thd, TABLE_LIST* table, wsrep::key_array* keys);


#define WSREP_SYNC_WAIT(thd_, before_)                                  \
    { if (WSREP_CLIENT(thd_) &&                                         \
          wsrep_sync_wait(thd_, before_)) goto wsrep_error_label; }

#define WSREP_MYSQL_DB (const char *)"mysql"

#define WSREP_TO_ISOLATION_BEGIN_IF(db_, table_, table_list_)                 \
  if (WSREP(thd) && thd->wsrep_cs().state() != wsrep::client_state::s_none && \
      wsrep_to_isolation_begin(thd, db_, table_, table_list_))


#define WSREP_TO_ISOLATION_BEGIN(db_, table_, table_list_)                    \
  if (WSREP(thd) && thd->wsrep_cs().state() != wsrep::client_state::s_none && \
      wsrep_to_isolation_begin(thd, db_, table_, table_list_))                \
    goto wsrep_error_label;

#define WSREP_TO_ISOLATION_BEGIN_ALTER(db_, table_, table_list_, alter_info_, \
                                       fk_tables_)                            \
  if (WSREP(thd) && thd->wsrep_cs().state() != wsrep::client_state::s_none && \
      wsrep_thd_is_local(thd) &&                                              \
      wsrep_to_isolation_begin(thd, db_, table_, table_list_, alter_info_,    \
                               fk_tables_))

/*
  Checks if lex->no_write_to_binlog is set for statements that use LOCAL or
  NO_WRITE_TO_BINLOG.
*/
#define WSREP_TO_ISOLATION_BEGIN_WRTCHK(db_, table_, table_list_)             \
  if (WSREP(thd) && thd->wsrep_cs().state() != wsrep::client_state::s_none && \
      !thd->lex->no_write_to_binlog &&                                        \
      wsrep_to_isolation_begin(thd, db_, table_, table_list_))                \
    goto wsrep_error_label;

#define WSREP_TO_ISOLATION_END                                          \
  if ((WSREP(thd) && wsrep_thd_is_local_toi(thd)) ||                    \
      wsrep_thd_is_in_rsu(thd))                                         \
    wsrep_to_isolation_end(thd);

#define WSREP_TO_ISOLATION_BEGIN_FK_TABLES(db_, table_, table_list_, fk_tables) \
  if (WSREP(thd) && thd->wsrep_cs().state() != wsrep::client_state::s_none &&   \
      !thd->lex->no_write_to_binlog                                             \
      && wsrep_to_isolation_begin(thd, db_, table_, table_list_, NULL,          \
                                  fk_tables))

#define WSREP_PROVIDER_EXISTS \
  (wsrep_provider && strncasecmp(wsrep_provider, WSREP_NONE, FN_REFLEN))

#define WSREP_QUERY(thd)                                \
  ((!opt_general_log_raw) && thd->rewritten_query().length()    \
   ? thd->rewritten_query().ptr() : thd->query().str)

extern bool wsrep_ready_get();
extern void wsrep_ready_wait();

extern mysql_mutex_t LOCK_wsrep_ready;
extern mysql_cond_t  COND_wsrep_ready;
extern mysql_mutex_t LOCK_wsrep_sst;
extern mysql_cond_t  COND_wsrep_sst;
extern mysql_mutex_t LOCK_wsrep_sst_init;
extern mysql_cond_t  COND_wsrep_sst_init;
extern mysql_mutex_t LOCK_wsrep_replaying;
extern mysql_cond_t  COND_wsrep_replaying;
extern mysql_mutex_t LOCK_wsrep_slave_threads;
extern mysql_cond_t  COND_wsrep_slave_threads;
extern mysql_mutex_t LOCK_wsrep_gtid_wait_upto;
extern mysql_mutex_t LOCK_wsrep_cluster_config;
extern mysql_mutex_t LOCK_wsrep_desync;
extern mysql_mutex_t LOCK_wsrep_config_state;

extern int           wsrep_replaying;
extern bool          wsrep_emulate_bin_log;
extern int           wsrep_to_isolation;
extern bool          wsrep_preordered_opt;

#ifdef HAVE_PSI_INTERFACE

extern PSI_cond_key  key_COND_wsrep_thd;

extern PSI_mutex_key key_LOCK_wsrep_ready;
extern PSI_mutex_key key_COND_wsrep_ready;
extern PSI_mutex_key key_LOCK_wsrep_sst;
extern PSI_cond_key  key_COND_wsrep_sst;
extern PSI_mutex_key key_LOCK_wsrep_sst_init;
extern PSI_cond_key  key_COND_wsrep_sst_init;
extern PSI_mutex_key key_LOCK_wsrep_sst_thread;
extern PSI_cond_key  key_COND_wsrep_sst_thread;
extern PSI_mutex_key key_LOCK_wsrep_replaying;
extern PSI_cond_key  key_COND_wsrep_replaying;
extern PSI_mutex_key key_LOCK_wsrep_slave_threads;
extern PSI_cond_key  key_COND_wsrep_slave_threads;
extern PSI_mutex_key key_LOCK_wsrep_gtid_wait_upto;
extern PSI_cond_key  key_COND_wsrep_gtid_wait_upto;
extern PSI_mutex_key key_LOCK_wsrep_cluster_config;
extern PSI_mutex_key key_LOCK_wsrep_desync;
extern PSI_mutex_key key_LOCK_wsrep_global_seqno;
extern PSI_mutex_key key_LOCK_wsrep_thd_queue;
extern PSI_cond_key  key_COND_wsrep_thd_queue;

extern PSI_file_key key_file_wsrep_gra_log;
#endif /* HAVE_PSI_INTERFACE */
struct TABLE_LIST;
class Alter_info;
int wsrep_to_isolation_begin(THD *thd, const char *db_, const char *table_,
                             const TABLE_LIST* table_list,
                             Alter_info* alter_info= NULL, wsrep::key_array *fk_tables=NULL);

void wsrep_to_isolation_end(THD *thd);

int wsrep_to_buf_helper(THD* thd, const char *query, uint query_len, 
                        uchar** buf, size_t* buf_len);
int wsrep_create_trigger_query(THD *thd, uchar** buf, size_t* buf_len);
int wsrep_create_event_query(THD *thd, uchar** buf, size_t* buf_len);

bool wsrep_node_is_donor();
bool wsrep_node_is_synced();

void wsrep_init_SR();
void wsrep_verify_SE_checkpoint(const wsrep_uuid_t& uuid, wsrep_seqno_t seqno);
int wsrep_replay_from_SR_store(THD*, const wsrep_trx_meta_t&);

class Log_event;
struct TABLE;
int wsrep_ignored_error_code(Log_event* ev, int error, TABLE* table);
int wsrep_must_ignore_error(THD* thd);
void wsrep_setup_fk_checks(THD* thd);
void wsrep_setup_uk_checks(THD* thd);

struct wsrep_local_gtid_t
{
  wsrep_uuid_t uuid;
  uint32 server_id;
  uint64 seqno;
};
class Wsrep_local_gtid_manager
{
public:
  wsrep_uuid_t uuid;
  uint32 server_id;
  Wsrep_local_gtid_manager()
    : m_force_signal(false)
    , m_sidno(0)
    , m_seqno(0)
    , m_committed_seqno(0)
  { }
  void gtid(const wsrep_local_gtid_t& gtid) {
    memcpy(uuid.data, gtid.uuid.data, sizeof(uuid.data));
    server_id = gtid.server_id;
    m_seqno = gtid.seqno;
  }
  wsrep_local_gtid_t gtid() {
    wsrep_local_gtid_t gtid;
    memcpy(gtid.uuid.data, uuid.data, sizeof(uuid.data));
    gtid.seqno = m_seqno;
    gtid.server_id = server_id;
    return gtid;
  }
  void sidno(const uint32 sidno) { m_sidno = sidno; }
  uint32 sidno() const { return m_sidno; }
  void seqno(const uint64 seqno) { m_seqno = seqno; }
  uint64 seqno() const { return m_seqno; }
  uint64 seqno_committed() const { return m_committed_seqno; }
  uint64 seqno_inc() {
    m_seqno++;
    return m_seqno;
  }
  const wsrep_local_gtid_t undefined() {
    wsrep_local_gtid_t undefined;
    undefined.uuid = WSREP_UUID_UNDEFINED;
    undefined.seqno = 0;
    return undefined;
  }
  int wait_gtid_upto(const uint64_t seqno, uint timeout) {
    int wait_result = 0;
    struct timespec wait_time;
    int ret = 0;
    mysql_cond_t wait_cond;
    mysql_cond_init(key_COND_wsrep_gtid_wait_upto, &wait_cond);
    set_timespec(&wait_time, timeout);
    mysql_mutex_lock(&LOCK_wsrep_gtid_wait_upto);
    if (seqno > m_committed_seqno) {
      std::multimap<uint64, mysql_cond_t*>::iterator it;
      try {
        it = m_wait_map.insert(std::make_pair(seqno, &wait_cond));
      } 
      catch (std::bad_alloc& e) {
        ret = ENOMEM;
      }
      while (!ret && (m_committed_seqno < seqno) && !m_force_signal) {
        wait_result = mysql_cond_timedwait(&wait_cond,
                                           &LOCK_wsrep_gtid_wait_upto,
                                           &wait_time);
        if (wait_result == ETIMEDOUT || wait_result == ETIME) {
          ret = wait_result;
          break;
        }
      }
      if (ret != ENOMEM) {
        m_wait_map.erase(it);
      }
    }
    mysql_mutex_unlock(&LOCK_wsrep_gtid_wait_upto);
    mysql_cond_destroy(&wait_cond);
    return ret;
  }
  void signal_waiters(uint64 seqno, bool signal_all) {
    mysql_mutex_lock(&LOCK_wsrep_gtid_wait_upto);
    if (!signal_all && (m_committed_seqno >= seqno)) {
      mysql_mutex_unlock(&LOCK_wsrep_gtid_wait_upto);
      return;
    }
    m_force_signal = true;
    std::multimap<uint64, mysql_cond_t*>::iterator it_end;
    std::multimap<uint64, mysql_cond_t*>::iterator it_begin;
    if (signal_all) {
      it_end = m_wait_map.end();
    }
    else {
      it_end = m_wait_map.upper_bound(seqno);
    }
    if (m_committed_seqno < seqno) {
      m_committed_seqno = seqno;
    }
    for (it_begin = m_wait_map.begin(); it_begin != it_end; ++it_begin) {
      mysql_cond_signal(it_begin->second);
    }
    m_force_signal = false;
    mysql_mutex_unlock(&LOCK_wsrep_gtid_wait_upto);
  }
private:
  std::multimap<uint64, mysql_cond_t*> m_wait_map;
  bool m_force_signal;
  uint32              m_sidno;
  std::atomic<uint64> m_seqno;
  std::atomic<uint64> m_committed_seqno;
};
extern Wsrep_local_gtid_manager wsrep_local_gtid_manager;
void wsrep_setup_local_gtid(const wsrep_local_gtid_t&);

typedef struct wsrep_key_arr
{
    wsrep_key_t* keys;
    size_t       keys_len;
} wsrep_key_arr_t;
bool wsrep_prepare_keys_for_isolation(THD*              thd,
                                      const char*       db,
                                      const char*       table,
                                      const TABLE_LIST* table_list,
                                      wsrep_key_arr_t*  ka);
void wsrep_keys_free(wsrep_key_arr_t* key_arr);

/**
   Append a table level key for certification,
   with given db and table, and for the given type.
*/
int wsrep_append_table_level_key(THD* thd,
                                 const char* db,
                                 const char* table,
                                 enum wsrep::key::type type);

extern void
wsrep_handle_mdl_conflict(const MDL_context *requestor_ctx,
                          MDL_ticket *ticket,
                          const MDL_key *key);

typedef void (*wsrep_thd_processor_fun)(THD*, void *);
class Wsrep_thd_args
{
 public:
 Wsrep_thd_args(wsrep_thd_processor_fun fun, void* args)
   :
  fun_ (fun),
  args_(args)
  { }

  wsrep_thd_processor_fun fun() { return fun_; }

  void* args() { return args_; }

 private:

  Wsrep_thd_args(const Wsrep_thd_args&);
  Wsrep_thd_args& operator=(const Wsrep_thd_args&);

  wsrep_thd_processor_fun fun_;
  void*                    args_;
};

void* start_wsrep_THD(void*);

void wsrep_close_threads(THD *thd);
void wsrep_replay_transaction(THD *thd);
struct HA_CREATE_INFO;
bool wsrep_create_like_table(THD* thd, TABLE_LIST* table,
                             TABLE_LIST* src_table,
                             HA_CREATE_INFO *create_info);
bool wsrep_node_is_donor();
bool wsrep_node_is_synced();

/**
 * Check if the wsrep provider (ie the Galera library) is capable of
 * doing streaming replication.
 * @return true if SR capable
 */
bool wsrep_provider_is_SR_capable();

/**
 * Initialize WSREP server instance.
 *
 * @return Zero on success, non-zero on error.
 */
int wsrep_init_server();

/**
 * Initialize WSREP globals. This should be done after server initialization
 * is complete and the server has joined to the cluster.
 *
 */
void wsrep_init_globals();

/**
 * Deinit and release WSREP resources.
 */
void wsrep_deinit_server();

/**
 * Convert streaming fragment unit (WSREP_FRAG_BYTES, WSREP_FRAG_ROWS...)
 * to corresponding wsrep-lib fragment_unit
 */
enum wsrep::streaming_context::fragment_unit wsrep_fragment_unit(ulong unit);

#else /* !WITH_WSREP */

/* These macros are needed to compile MariaDB without WSREP support
 * (e.g. embedded) */

#define WSREP_PROVIDER_EXISTS (0)
#define wsrep_emulate_bin_log (0)
#define wsrep_to_isolation (0)
#define wsrep_before_SE() (0)
#define wsrep_init_startup(X)
#define wsrep_check_opts() (0)
#define wsrep_thr_init() do {} while(0)
#define wsrep_thr_deinit() do {} while(0)
#define wsrep_init_globals() do {} while(0)
#define wsrep_create_appliers(X) do {} while(0)
#define WSREP_MYSQL_DB (0)
#define WSREP_TO_ISOLATION_BEGIN_IF(db_, table_, table_list_)
#define WSREP_TO_ISOLATION_BEGIN(db_, table_, table_list_) do { } while(0)
#define WSREP_TO_ISOLATION_BEGIN_ALTER(db_, table_, table_list_, alter_info_)
#define WSREP_TO_ISOLATION_BEGIN_FK_TABLES(db_, table_, table_list_, fk_tables_)
#define WSREP_TO_ISOLATION_END
#define WSREP_TO_ISOLATION_BEGIN_WRTCHK(db_, table_, table_list_)
#define WSREP_SYNC_WAIT(thd_, before_)

#endif /* WITH_WSREP */

#endif /* WSREP_MYSQLD_H */
