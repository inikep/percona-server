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

#include <wsrep.h>
#include "sql_plugin.h"                         /* wsrep_plugins_pre_init() */
#include "mysqld.h"
#include "sql_base.h"
#include "sql_class.h"
#include "sql_parse.h"
#include "sql_show.h"
#include "mysql/plugin.h" // thd_proc_info
#include "binlog_ostream.h"
#include "protocol.h" // class Protool
#include "protocol_classic.h" // class Protool_classic

#include <cstdio>
#include <cstdlib>
#include <string>
#include "log_event.h"
#include "rpl_replica.h"
#include "rpl_msr.h"            // channel_map
#include "mysql/psi/mysql_file.h"
#include "transaction.h"
#include "sp_head.h"
#include "mysqld_thd_manager.h"              // Global_THD_manager
#include "sql/conn_handler/connection_handler_manager.h"      // Connection_handler_manager
#include "sql/conn_handler/connection_handler_impl.h"
#include "sql/dd/types/table.h"                           // dd::Table
#include "sql/dd/cache/dictionary_client.h"               // dd::cache::Dictionary_client
#include "sql_callback.h"              // MYSQL_CALLBACK
#include "debug_sync.h"

#include "wsrep_server_state.h"
#include "wsrep_event_service.h"
#include "wsrep_status.h"
#include "wsrep_mysqld.h"
#include "wsrep_thd.h"
#include "wsrep_sst.h"
#include "wsrep_utils.h"
#include "wsrep_var.h"
#include "wsrep_binlog.h"
#include "wsrep_applier.h"
#include "wsrep_schema.h"
#include "wsrep_xid.h"
#include "wsrep_trans_observer.h"
#include "mysql/service_wsrep.h"

bool wsrep_emulate_bin_log = false; // activating parts of binlog interface
bool wsrep_preordered_opt  = false;

/* Streaming Replication */
const char *wsrep_fragment_units[]= { "bytes", "rows", "statements", NullS };

/*
 * Begin configuration options
 */

extern bool dynamic_plugins_are_initialized;
extern uint kill_cached_threads;
extern mysql_cond_t COND_thread_cache;

/* System variables. */
const char *wsrep_provider;
const char *wsrep_provider_options;
const char *wsrep_cluster_address;
const char *wsrep_cluster_name;
const char *wsrep_node_name;
const char *wsrep_node_address;
const char *wsrep_node_incoming_address;
const char *wsrep_start_position;
const char *wsrep_data_home_dir;
const char *wsrep_dbug_option;
const char *wsrep_notify_cmd;
const char *wsrep_status_file;

ulong   wsrep_debug;                            // Debug level logging
bool wsrep_convert_LOCK_to_trx;              // Convert locking sessions to trx
bool wsrep_auto_increment_control;           // Control auto increment variables
bool wsrep_drupal_282555_workaround;         // Retry autoinc insert after dupkey
bool wsrep_certify_nonPK;                    // Certify, even when no primary key
ulong   wsrep_certification_rules      = WSREP_CERTIFICATION_RULES_OPTIMIZED;
bool wsrep_recovery;                         // Recovery
bool wsrep_replicate_myisam;                 // Enable MyISAM replication
bool wsrep_log_conflicts;
bool wsrep_load_data_splitting= 0;           // Commit load data every 10K intervals
bool wsrep_slave_UK_checks;                  // Slave thread does UK checks
bool wsrep_slave_FK_checks;                  // Slave thread does FK checks
ulonglong wsrep_mode                   = 0;
bool wsrep_restart_slave;                    // Should mysql slave thread be
                                                // restarted, when node joins back?
bool wsrep_desync;                           // De(re)synchronize the node from the
                                                // cluster
long wsrep_slave_threads;                       // No. of slave appliers threads
ulong wsrep_retry_autocommit;                   // Retry aborted autocommit trx
ulong wsrep_max_ws_size;                        // Max allowed ws (RBR buffer) size
ulong wsrep_max_ws_rows;                        // Max number of rows in ws
ulong wsrep_forced_binlog_format;
ulong wsrep_mysql_replication_bundle;

bool                     wsrep_gtid_mode;      // Enable WSREP native GTID support
Wsrep_local_gtid_manager wsrep_local_gtid_manager;

uint32 wsrep_gtid_domain_id;                    // gtid_domain_id for galera
                                                // transactions

/* Other configuration variables and their default values. */
bool wsrep_incremental_data_collection= 0;   // Incremental data collection
bool wsrep_restart_slave_activated= 0;       // Node has dropped, and slave
                                                // restart will be needed
bool wsrep_new_cluster= false;                  // Bootstrap the cluster?
int wsrep_slave_count_change= 0;                // No. of appliers to stop/start
int wsrep_to_isolation= 0;                      // No. of active TO isolation threads
long wsrep_max_protocol_version= 4;             // Maximum protocol version to use
long int  wsrep_protocol_version= wsrep_max_protocol_version;
ulong wsrep_trx_fragment_unit= WSREP_FRAG_BYTES;
                                                // unit for fragment size
uint  wsrep_ignore_apply_errors= 0;


/*
 * End configuration options
 */

/*
 * Other wsrep global variables.
 */

mysql_mutex_t LOCK_wsrep_ready;
mysql_cond_t  COND_wsrep_ready;
mysql_mutex_t LOCK_wsrep_sst;
mysql_cond_t  COND_wsrep_sst;
mysql_mutex_t LOCK_wsrep_sst_init;
mysql_cond_t  COND_wsrep_sst_init;
mysql_mutex_t LOCK_wsrep_replaying;
mysql_cond_t  COND_wsrep_replaying;
mysql_mutex_t LOCK_wsrep_slave_threads;
mysql_cond_t  COND_wsrep_slave_threads;
mysql_mutex_t LOCK_wsrep_gtid_wait_upto;
mysql_mutex_t LOCK_wsrep_cluster_config;
mysql_mutex_t LOCK_wsrep_desync;
mysql_mutex_t LOCK_wsrep_config_state;

int wsrep_replaying= 0;
std::atomic<long> wsrep_running_threads{0};
ulong  my_bind_addr;

#ifdef HAVE_PSI_INTERFACE
PSI_mutex_key 
  key_LOCK_wsrep_replaying, key_LOCK_wsrep_ready, key_LOCK_wsrep_sst,
  key_LOCK_wsrep_sst_thread, key_LOCK_wsrep_sst_init,
  key_LOCK_wsrep_slave_threads, key_LOCK_wsrep_gtid_wait_upto,
  key_LOCK_wsrep_desync,
  key_LOCK_wsrep_config_state, key_LOCK_wsrep_cluster_config,
  key_LOCK_wsrep_thd,
  key_LOCK_wsrep_thd_queue;

PSI_cond_key key_COND_wsrep_thd,
  key_COND_wsrep_replaying, key_COND_wsrep_ready, key_COND_wsrep_sst,
  key_COND_wsrep_sst_init, key_COND_wsrep_sst_thread,
  key_COND_wsrep_thd_queue, key_COND_wsrep_slave_threads, key_COND_wsrep_gtid_wait_upto;
  

PSI_file_key key_file_wsrep_gra_log;

static PSI_mutex_info wsrep_mutexes[]=
{
  { &key_LOCK_wsrep_ready, "LOCK_wsrep_ready", 0, 0, PSI_DOCUMENT_ME},
  { &key_LOCK_wsrep_sst, "LOCK_wsrep_sst", 0, 0, PSI_DOCUMENT_ME},
  { &key_LOCK_wsrep_sst_thread, "wsrep_sst_thread", 0, 0, PSI_DOCUMENT_ME},
  { &key_LOCK_wsrep_sst_init, "LOCK_wsrep_sst_init", 0, 0, PSI_DOCUMENT_ME},
  { &key_LOCK_wsrep_sst, "LOCK_wsrep_sst", 0, 0, PSI_DOCUMENT_ME},
  { &key_LOCK_wsrep_replaying, "LOCK_wsrep_replaying", 0, 0, PSI_DOCUMENT_ME},
  { &key_LOCK_wsrep_slave_threads, "LOCK_wsrep_slave_threads", 0, 0, PSI_DOCUMENT_ME},
  { &key_LOCK_wsrep_gtid_wait_upto, "LOCK_wsrep_gtid_wait_upto", 0, 0, PSI_DOCUMENT_ME},
  { &key_LOCK_wsrep_cluster_config, "LOCK_wsrep_cluster_config", 0, 0, PSI_DOCUMENT_ME},
  { &key_LOCK_wsrep_desync, "LOCK_wsrep_desync", 0, 0, PSI_DOCUMENT_ME},
  { &key_LOCK_wsrep_config_state, "LOCK_wsrep_config_state", 0, 0, PSI_DOCUMENT_ME},
  { &key_LOCK_wsrep_thd, "LOCK_wsrep_thd", 0, 0, PSI_DOCUMENT_ME}
};

static PSI_cond_info wsrep_conds[]=
{
  { &key_COND_wsrep_ready, "COND_wsrep_ready", 0, 0, PSI_DOCUMENT_ME},
  { &key_COND_wsrep_sst, "COND_wsrep_sst", 0, 0, PSI_DOCUMENT_ME},
  { &key_COND_wsrep_sst_init, "COND_wsrep_sst_init", 0, 0, PSI_DOCUMENT_ME},
  { &key_COND_wsrep_sst_thread, "wsrep_sst_thread", 0, 0, PSI_DOCUMENT_ME},
  { &key_COND_wsrep_thd, "THD::COND_wsrep_thd", 0, 0, PSI_DOCUMENT_ME},
  { &key_COND_wsrep_replaying, "COND_wsrep_replaying", 0, 0, PSI_DOCUMENT_ME},
  { &key_COND_wsrep_slave_threads, "COND_wsrep_wsrep_slave_threads", 0, 0, PSI_DOCUMENT_ME},
  { &key_COND_wsrep_gtid_wait_upto, "COND_wsrep_gtid_wait_upto", 0, 0, PSI_DOCUMENT_ME}
};

static PSI_file_info wsrep_files[]=
{
  { &key_file_wsrep_gra_log, "wsrep_gra_log", 0, 0, PSI_DOCUMENT_ME}
};
#endif

bool wsrep_inited= 0; // initialized ?

static wsrep_uuid_t node_uuid= WSREP_UUID_UNDEFINED;
static char         cluster_uuid_str[40]= { 0, };

static char provider_name[256]= { 0, };
static char provider_version[256]= { 0, };
static char provider_vendor[256]= { 0, };

/*
 * Wsrep status variables. LOCK_status must be locked When modifying
 * these variables,
 */
bool     wsrep_connected         = false;
bool     wsrep_ready             = false;
const char* wsrep_cluster_state_uuid= cluster_uuid_str;
long long   wsrep_cluster_conf_id   = WSREP_SEQNO_UNDEFINED;
const char* wsrep_cluster_status    = "Disconnected";
long        wsrep_cluster_size      = 0;
long        wsrep_local_index       = -1;
long long   wsrep_local_bf_aborts   = 0;
const char* wsrep_provider_name     = provider_name;
const char* wsrep_provider_version  = provider_version;
const char* wsrep_provider_vendor   = provider_vendor;
char* wsrep_provider_capabilities   = NULL;
char* wsrep_cluster_capabilities    = NULL;
char* wsrep_local_gtid              = NULL;
/* End wsrep status variables */

wsp::Config_state *wsrep_config_state;


wsrep_uuid_t               local_uuid       = WSREP_UUID_UNDEFINED;
wsrep_seqno_t              local_seqno      = WSREP_SEQNO_UNDEFINED;
wsp::node_status           local_status;

/*
 */
Wsrep_schema *wsrep_schema= 0;

static void wsrep_log_cb(wsrep::log::level level,
                         const char*       pfx,
                         const char*       msg)
{
  /*
    Silence all wsrep related logging from lib and provider if
    wsrep is not enabled.
  */
  if (!WSREP_ON) return;

  switch (level) {
  case wsrep::log::info:
    WSREP_INFO_PFX(pfx, "%s", msg);
    break;
  case wsrep::log::warning:
    WSREP_WARN_PFX(pfx, "%s", msg);
    break;
  case wsrep::log::error:
    WSREP_ERROR_PFX(pfx, "%s", msg);
    break;
  case wsrep::log::debug:
    WSREP_DEBUG_PFX(pfx, "%s", msg);
    break;
  case wsrep::log::unknown:
    WSREP_UNKNOWN_PFX(pfx, "%s", msg);
    break;
  }
}

static void wsrep_init_local_gtid(const wsrep::id& uuid)
{
  wsrep_local_gtid_t gtid = wsrep_get_SE_checkpoint<wsrep_local_gtid_t>();
  if (wsrep_new_cluster ||
      wsrep_uuid_compare(&gtid.uuid, &WSREP_UUID_UNDEFINED) == 0) {
    memcpy(gtid.uuid.data, uuid.data(), sizeof(gtid.uuid.data));
    gtid.seqno = 0;
    gtid.server_id = server_id;
  }
  wsrep_setup_local_gtid(gtid);
}

void wsrep_setup_local_gtid(const wsrep_local_gtid_t& gtid)
{
  uint32 wsrep_current_sidno;
  global_sid_lock->wrlock();
  binary_log::Uuid uuid;
  uuid.copy_from(gtid.uuid.data);
  wsrep_current_sidno = global_sid_map->add_sid(uuid);
  global_sid_lock->unlock();

  char uuid_str[40] = {0, };
  wsrep_uuid_print((const wsrep_uuid_t*)&gtid.uuid, uuid_str, sizeof(uuid_str));
  WSREP_INFO("Local GTID starting from: %s:%lu", uuid_str, gtid.seqno);

  wsrep_local_gtid_manager.sidno(wsrep_current_sidno);
  wsrep_local_gtid_manager.gtid(gtid);
}

static void wsrep_init_schema()
{
  assert(!wsrep_schema);

  WSREP_INFO("wsrep_init_schema_and_SR %p", wsrep_schema);
  if (!wsrep_schema)
  {
    wsrep_schema= new Wsrep_schema();
    if (wsrep_schema->init())
    {
      WSREP_ERROR("Failed to init wsrep schema");
      unireg_abort(1);
    }
  }
}

static void wsrep_deinit_schema()
{
  delete wsrep_schema;
  wsrep_schema= 0;
}

void wsrep_recover_sr_from_storage(THD *orig_thd)
{
  if (!wsrep_schema) {
    WSREP_ERROR(
        "Wsrep schema not initialized when trying to recover "
        "streaming transactions");
    unireg_abort(1);
  }
  if (wsrep_schema->recover_sr_transactions(orig_thd)) {
    WSREP_ERROR("Failed to recover SR transactions from schema");
    unireg_abort(1);
  }
}

/** Export the WSREP provider's capabilities as a human readable string.
 * The result is saved in a dynamically allocated string of the form:
 * :cap1:cap2:cap3:
 */
static void wsrep_capabilities_export(wsrep_cap_t const cap, char** str)
{
  static const char* names[] =
  {
    /* Keep in sync with wsrep/wsrep_api.h WSREP_CAP_* macros. */
    "MULTI_PRIMARY",
    "CERTIFICATION",
    "PARALLEL_APPLYING",
    "TRX_REPLAY",
    "ISOLATION",
    "PAUSE",
    "CAUSAL_READS",
    "CAUSAL_TRX",
    "INCREMENTAL_WRITESET",
    "SESSION_LOCKS",
    "DISTRIBUTED_LOCKS",
    "CONSISTENCY_CHECK",
    "UNORDERED",
    "ANNOTATION",
    "PREORDERED",
    "STREAMING",
    "SNAPSHOT",
    "NBO",
  };

  std::string s;
  for (size_t i= 0; i < sizeof(names) / sizeof(names[0]); ++i)
  {
    if (cap & (1ULL << i))
    {
      if (s.empty())
      {
        s= ":";
      }
      s += names[i];
      s += ":";
    }
  }

  /* A read from the string pointed to by *str may be started at any time,
   * so it must never point to free(3)d memory or non '\0' terminated string. */

  char* const previous= *str;

  *str= strdup(s.c_str());

  if (previous != NULL)
  {
    free(previous);
  }
}

/* Verifies that SE position is consistent with the group position
 * and initializes other variables */
void wsrep_verify_SE_checkpoint(const wsrep_uuid_t& uuid MY_ATTRIBUTE((unused)),
                                wsrep_seqno_t const seqno MY_ATTRIBUTE((unused)))
{
}

/*
  Wsrep is considered ready if
  1) Provider is not loaded (native mode)
  2) Server has reached synced state
  3) Server is in joiner mode and mysqldump SST method has been
     specified
  See Wsrep_server_service::log_state_change() for further details.
 */
bool wsrep_ready_get (void)
{
  if (mysql_mutex_lock (&LOCK_wsrep_ready)) abort();
  bool ret= wsrep_ready;
  mysql_mutex_unlock (&LOCK_wsrep_ready);
  return ret;
}

int wsrep_show_ready(THD *thd MY_ATTRIBUTE((unused)), SHOW_VAR *var, char *buff)
{
  var->type= SHOW_BOOL;
  var->value= buff;
  *((bool *)buff)= wsrep_ready_get();
  return 0;
}

void wsrep_update_cluster_state_uuid(const char* uuid)
{
  strncpy(cluster_uuid_str, uuid, sizeof(cluster_uuid_str) - 1);
}

/****************************************************************************
                         Helpers for wsrep_init()
 ****************************************************************************/
static std::string wsrep_server_name()
{
  std::string ret(wsrep_node_name ? wsrep_node_name : "");
  return ret;
}

static std::string wsrep_server_id()
{
  /* using empty server_id, which enables view change handler to
     set final server_id later on
  */
  std::string ret("");
  return ret;
}

static std::string wsrep_server_node_address()
{

  std::string ret;
  if (!wsrep_data_home_dir || strlen(wsrep_data_home_dir) == 0)
    wsrep_data_home_dir= mysql_real_data_home;

  /* Initialize node address */
  if (!wsrep_node_address || !strcmp(wsrep_node_address, ""))
  {
    char node_addr[512]= {0, };
    const size_t node_addr_max= sizeof(node_addr) - 1;
    size_t guess_ip_ret= wsrep_guess_ip(node_addr, node_addr_max);
    if (!(guess_ip_ret > 0 && guess_ip_ret < node_addr_max))
    {
      WSREP_WARN("Failed to guess base node address. Set it explicitly via "
                 "wsrep_node_address.");
    }
    else
    {
      ret= node_addr;
    }
  }
  else
  {
    ret= wsrep_node_address;
  }
  return ret;
}

static std::string wsrep_server_incoming_address()
{
  std::string ret;
  const std::string node_addr(wsrep_server_node_address());
  char inc_addr[512]= { 0, };
  size_t const inc_addr_max= sizeof (inc_addr);

  /*
    In case wsrep_node_incoming_address is either not set or set to AUTO,
    we need to use mysqld's my_bind_addr_str:mysqld_port, lastly fallback
    to wsrep_node_address' value if mysqld's bind-address is not set either.
  */
  if ((!wsrep_node_incoming_address ||
       !strcmp (wsrep_node_incoming_address, WSREP_NODE_INCOMING_AUTO)))
  {
    bool is_ipv6= false;
    unsigned int my_bind_ip= INADDR_ANY; // default if not set
    std::string bind_addr;
    if (my_bind_addr_str)
    {
      bind_addr.append(my_bind_addr_str);
    }
    if (bind_addr.size() > 0 && bind_addr != "*")
    {
      // As of version 8.13, bind_address accepts
      // a list of comma separated addresses.
      // We just condider the first address here.
      size_t separator= bind_addr.find_first_of(',');
      if (separator != std::string::npos)
      {
        bind_addr.erase(separator);
      }
      my_bind_ip= wsrep_check_ip(bind_addr.c_str(), &is_ipv6);
    }

    if (INADDR_ANY != my_bind_ip)
    {
      /*
        If its a not a valid address, leave inc_addr as empty string. mysqld
        is not listening for client connections on network interfaces.
      */
      if (INADDR_NONE != my_bind_ip && INADDR_LOOPBACK != my_bind_ip)
      {
        const char *fmt= (is_ipv6) ? "[%s]:%u" : "%s:%u";
        snprintf(inc_addr, inc_addr_max, fmt, bind_addr.c_str(), mysqld_port);
      }
    }
    else /* mysqld binds to 0.0.0.0, try taking IP from wsrep_node_address. */
    {
      if (node_addr.size())
      {
        size_t const ip_len= wsrep_host_len(node_addr.c_str(), node_addr.size());
        if (ip_len + 7 /* :55555\0 */ < inc_addr_max)
        {
          memcpy (inc_addr, node_addr.c_str(), ip_len);
          snprintf(inc_addr + ip_len, inc_addr_max - ip_len, ":%u",
                   (int)mysqld_port);
        }
        else
        {
          WSREP_WARN("Guessing address for incoming client connections: "
                     "address too long.");
          inc_addr[0]= '\0';
        }
      }

      if (!strlen(inc_addr))
      {
        WSREP_WARN("Guessing address for incoming client connections failed. "
                   "Try setting wsrep_node_incoming_address explicitly.");
        WSREP_INFO("Node addr: %s", node_addr.c_str());
      }
    }
  }
  else
  {
    wsp::Address addr(wsrep_node_incoming_address);

    if (!addr.is_valid())
    {
      WSREP_WARN("Could not parse wsrep_node_incoming_address : %s",
                 wsrep_node_incoming_address);
      goto done;
    }

    /*
      In case port is not specified in wsrep_node_incoming_address, we use
      mysqld_port.
    */
    int port= (addr.get_port() > 0) ? addr.get_port() : (int) mysqld_port;
    const char *fmt= (addr.is_ipv6()) ? "[%s]:%u" : "%s:%u";

    snprintf(inc_addr, inc_addr_max, fmt, addr.get_address(), port);
  }
  
 done:
  ret= wsrep_node_incoming_address;
  return ret;
}

static std::string wsrep_server_working_dir()
{
  std::string ret;
  if (!wsrep_data_home_dir || strlen(wsrep_data_home_dir) == 0)
  {
    ret= mysql_real_data_home;
  }
  else
  {
    ret= wsrep_data_home_dir;
  }
  return ret;
}

static wsrep::gtid wsrep_server_initial_position()
{
  wsrep::gtid ret;
  WSREP_DEBUG("Server initial position: %s", wsrep_start_position);
  std::istringstream is(wsrep_start_position);
  is >> ret;
  return ret;
}

/*
  Intitialize provider specific status variables
 */
static void wsrep_init_provider_status_variables()
{
  const wsrep::provider& provider=
    Wsrep_server_state::instance().provider();
  strncpy(provider_name,
          provider.name().c_str(),    sizeof(provider_name) - 1);
  strncpy(provider_version,
          provider.version().c_str(), sizeof(provider_version) - 1);
  strncpy(provider_vendor,
          provider.vendor().c_str(),  sizeof(provider_vendor) - 1);
}

int wsrep_init_server()
{
  wsrep::log::logger_fn(wsrep_log_cb);
  try
  {
    Wsrep_status::init_once(wsrep_status_file);

    std::string server_name;
    std::string server_id;
    std::string node_address;
    std::string incoming_address;
    std::string working_dir;
    wsrep::gtid initial_position;

    server_name= wsrep_server_name();
    server_id= wsrep_server_id();
    node_address= wsrep_server_node_address();
    incoming_address= wsrep_server_incoming_address();
    working_dir= wsrep_server_working_dir();
    initial_position= wsrep_server_initial_position();

    Wsrep_server_state::init_once(server_name,
                                  incoming_address,
                                  node_address,
                                  working_dir,
                                  initial_position,
                                  wsrep_max_protocol_version);
    Wsrep_server_state::instance().debug_log_level(wsrep_debug);
  }
  catch (const wsrep::runtime_error& e)
  {
    WSREP_ERROR("Failed to init wsrep server %s", e.what());
    return 1;
  }
  catch (const std::exception& e)
  {
    WSREP_ERROR("Failed to init wsrep server %s", e.what());
  }
  return 0;
}

void wsrep_init_globals()
{
  if (WSREP_ON)
  {
    wsrep_init_local_gtid(Wsrep_server_state::instance().connected_gtid().id());
    wsrep_init_schema();
    Wsrep_server_state::instance().initialized();
  }

  /*
    We have initialized new cluster GTID if `wsrep_new_cluster` is provided
    so it's safe to set `wsrep_new_cluster` to false, as we don't need it.
  */
  wsrep_new_cluster = false;
}

void wsrep_deinit_server()
{
  wsrep_deinit_schema();
  Wsrep_server_state::destroy();
  Wsrep_status::destroy();
}

int wsrep_init()
{
  assert(wsrep_provider);

  if (strlen(wsrep_provider)== 0 ||
      !strcmp(wsrep_provider, WSREP_NONE))
  {
    // enable normal operation in case no provider is specified
    global_system_variables.wsrep_on= 0;
    int err= Wsrep_server_state::instance().load_provider
      (wsrep_provider, wsrep_provider_options ? wsrep_provider_options : "");
    if (err)
    {
      DBUG_PRINT("wsrep",("wsrep::init() failed: %d", err));
      WSREP_ERROR("wsrep::init() failed: %d, must shutdown", err);
    }
    else
    {
      wsrep_init_provider_status_variables();
    }
    return err;
  }

  global_system_variables.wsrep_on= 1;

  if (!wsrep_data_home_dir || strlen(wsrep_data_home_dir) == 0)
    wsrep_data_home_dir= mysql_real_data_home;

  wsrep::provider::services const services
    (0, 0, 0, Wsrep_event_service::instance());

  if (Wsrep_server_state::instance().load_provider(wsrep_provider,
                                                   wsrep_provider_options,
                                                   services))
  {
    WSREP_ERROR("Failed to load provider");
    return 1;
  }

  global_sid_lock->rdlock();
  if ((global_gtid_mode.get() == Gtid_mode::ON) &&
      global_system_variables.wsrep_trx_fragment_size > 0) {
    global_sid_lock->unlock();
    WSREP_ERROR("`wsrep-gtid-mode = ON` do not work with SR enabled. "
                "Gtid mode `ON_PERMISSIVE` should be used instead.");
    return 1;
  }
  global_sid_lock->unlock();

  if (!wsrep_provider_is_SR_capable() &&
      global_system_variables.wsrep_trx_fragment_size > 0)
  {
    WSREP_ERROR("The WSREP provider (%s) does not support streaming "
                "replication but wsrep_trx_fragment_size is set to a "
                "value other than 0 (%lu). Cannot continue. Either set "
                "wsrep_trx_fragment_size to 0 or use wsrep_provider that "
                "supports streaming replication.",
                wsrep_provider, global_system_variables.wsrep_trx_fragment_size);
    Wsrep_server_state::instance().unload_provider();
    return 1;
  }
  wsrep_inited= 1;

  wsrep_init_provider_status_variables();
  wsrep_capabilities_export(Wsrep_server_state::instance().provider().capabilities(),
                            &wsrep_provider_capabilities);

  return 0;
}

/* Initialize wsrep thread LOCKs and CONDs */
void wsrep_thr_init()
{
  DBUG_ENTER("wsrep_thr_init");
  wsrep_config_state= new wsp::Config_state;
#ifdef HAVE_PSI_INTERFACE
  mysql_mutex_register("sql", wsrep_mutexes, array_elements(wsrep_mutexes));
  mysql_cond_register("sql", wsrep_conds, array_elements(wsrep_conds));
  mysql_file_register("sql", wsrep_files, array_elements(wsrep_files));
#endif

  mysql_mutex_init(key_LOCK_wsrep_ready, &LOCK_wsrep_ready, MY_MUTEX_INIT_FAST);
  mysql_cond_init(key_COND_wsrep_ready, &COND_wsrep_ready);
  mysql_mutex_init(key_LOCK_wsrep_sst, &LOCK_wsrep_sst, MY_MUTEX_INIT_FAST);
  mysql_cond_init(key_COND_wsrep_sst, &COND_wsrep_sst);
  mysql_mutex_init(key_LOCK_wsrep_sst_init, &LOCK_wsrep_sst_init, MY_MUTEX_INIT_FAST);
  mysql_cond_init(key_COND_wsrep_sst_init, &COND_wsrep_sst_init);
  mysql_mutex_init(key_LOCK_wsrep_replaying, &LOCK_wsrep_replaying, MY_MUTEX_INIT_FAST);
  mysql_cond_init(key_COND_wsrep_replaying, &COND_wsrep_replaying);
  mysql_mutex_init(key_LOCK_wsrep_slave_threads, &LOCK_wsrep_slave_threads, MY_MUTEX_INIT_FAST);
  mysql_cond_init(key_COND_wsrep_slave_threads, &COND_wsrep_slave_threads);
  mysql_mutex_init(key_LOCK_wsrep_gtid_wait_upto, &LOCK_wsrep_gtid_wait_upto, MY_MUTEX_INIT_FAST);
  mysql_mutex_init(key_LOCK_wsrep_cluster_config, &LOCK_wsrep_cluster_config, MY_MUTEX_INIT_FAST);
  mysql_mutex_init(key_LOCK_wsrep_desync, &LOCK_wsrep_desync, MY_MUTEX_INIT_FAST);
  mysql_mutex_init(key_LOCK_wsrep_config_state, &LOCK_wsrep_config_state, MY_MUTEX_INIT_FAST);
  DBUG_VOID_RETURN;
}

void wsrep_init_startup (bool before)
{
  if (wsrep_init()) unireg_abort(1);
#ifdef OUT
  wsrep_thr_lock_init(wsrep_thd_is_BF, wsrep_thd_bf_abort,
                      wsrep_debug, wsrep_convert_LOCK_to_trx, wsrep_on);
#endif
  /*
    Pre-initialize global_system_variables.table_plugin with a dummy engine
    (placeholder) required during the initialization of wsrep threads (THDs).
    (see: plugin_thdvar_init())
    Note: This only needs to be done for rsync & mariabackup based SST methods.
    In case of mysqldump SST method, the wsrep threads are created after the
    server plugins & global system variables are initialized.
  */
  if (before)
    wsrep_plugins_pre_init();

  /* Skip replication start if dummy wsrep provider is loaded */
  if (!strcmp(wsrep_provider, WSREP_NONE))
    return;

  /* Skip replication start if no cluster address */
  if (!wsrep_cluster_address || wsrep_cluster_address[0] == 0)
    return;

  /*
    Read value of wsrep_new_cluster before wsrep_start_replication(),
    the value is reset to FALSE inside wsrep_start_replication.
  */
  if (!wsrep_start_replication()) 
    unireg_abort(1);

  wsrep_create_rollbacker();
  wsrep_create_appliers(1);

  Wsrep_server_state& server_state= Wsrep_server_state::instance();
  /*
    If the SST happens before server initialization, wait until the server
    state reaches initializing. This indicates that
    either SST was not necessary or SST has been delivered.

    With mysqldump SST (!sst_first) wait until the server reaches
    joiner state and procedd to accepting connections.
  */
  if (before)
    server_state.wait_until_state(Wsrep_server_state::s_initializing);
  else
    server_state.wait_until_state(Wsrep_server_state::s_joiner);
}


void wsrep_deinit(bool free_options)
{
  assert(wsrep_inited == 1);
  WSREP_DEBUG("wsrep_deinit");

  Wsrep_server_state::instance().unload_provider();
  provider_name[0]=    '\0';
  provider_version[0]= '\0';
  provider_vendor[0]=  '\0';

  wsrep_inited= 0;

  if (wsrep_provider_capabilities != NULL)
  {
    char* p= wsrep_provider_capabilities;
    wsrep_provider_capabilities= NULL;
    free(p);
  }

  if (free_options)
  {
    wsrep_sst_auth_free();
  }
}

/* Destroy wsrep thread LOCKs and CONDs */
void wsrep_thr_deinit()
{
  if (!wsrep_config_state)
    return;                                     // Never initialized
  WSREP_DEBUG("wsrep_thr_deinit");
  mysql_mutex_destroy(&LOCK_wsrep_ready);
  mysql_cond_destroy(&COND_wsrep_ready);
  mysql_mutex_destroy(&LOCK_wsrep_sst);
  mysql_cond_destroy(&COND_wsrep_sst);
  mysql_mutex_destroy(&LOCK_wsrep_sst_init);
  mysql_cond_destroy(&COND_wsrep_sst_init);
  mysql_mutex_destroy(&LOCK_wsrep_replaying);
  mysql_cond_destroy(&COND_wsrep_replaying);
  mysql_mutex_destroy(&LOCK_wsrep_slave_threads);
  mysql_cond_destroy(&COND_wsrep_slave_threads);
  mysql_mutex_destroy(&LOCK_wsrep_gtid_wait_upto);
  mysql_mutex_destroy(&LOCK_wsrep_cluster_config);
  mysql_mutex_destroy(&LOCK_wsrep_desync);
  mysql_mutex_destroy(&LOCK_wsrep_config_state);

  delete wsrep_config_state;
  wsrep_config_state = 0;                        // Safety

  if (wsrep_cluster_capabilities != NULL) {
    char* p= wsrep_cluster_capabilities;
    wsrep_cluster_capabilities = NULL;
    free(p);
  }
  if (wsrep_local_gtid != NULL) {
    my_free(wsrep_local_gtid);
    wsrep_local_gtid = NULL;
  }
}

void wsrep_recover()
{
  char uuid_str[40];

  if (wsrep_uuid_compare(&local_uuid, &WSREP_UUID_UNDEFINED) == 0 &&
      local_seqno == -2)
  {
    wsrep_uuid_print(&local_uuid, uuid_str, sizeof(uuid_str));
    WSREP_INFO("Position %s:%lld given at startup, skipping position recovery",
               uuid_str, (long long)local_seqno);
    return;
  }
  wsrep::gtid gtid = wsrep_get_SE_checkpoint<wsrep::gtid>();
  std::ostringstream oss;
  oss << gtid;
  /* Make sure it is logged at default log verbosity */
  WSREP_LOG(SYSTEM_LEVEL, "Recovered position: %s", oss.str().c_str());
}


void wsrep_stop_replication(THD *thd)
{
  WSREP_INFO("Stop replication by %llu", (thd) ? thd->wsrep_cs().id().get() : 0);
  if (Wsrep_server_state::instance().state() !=
      Wsrep_server_state::s_disconnected)
  {
    WSREP_DEBUG("Disconnect provider");
    Wsrep_server_state::instance().disconnect();
    Wsrep_server_state::instance().wait_until_state(Wsrep_server_state::s_disconnected);
  }

  /* my connection, should not terminate with wsrep_close_client_connection(),
     make transaction to rollback
  */
  if (thd && !thd->wsrep_applier) trans_rollback(thd);
  wsrep_close_client_connections(false);
 
  /* wait until appliers have stopped */
  wsrep_wait_appliers_close(thd);

  node_uuid= WSREP_UUID_UNDEFINED;
}

void wsrep_shutdown_replication()
{
  WSREP_INFO("Shutdown replication");
  if (Wsrep_server_state::instance().state() != wsrep::server_state::s_disconnected)
  {
    WSREP_DEBUG("Disconnect provider");
    Wsrep_server_state::instance().disconnect();
    Wsrep_server_state::instance().wait_until_state(Wsrep_server_state::s_disconnected);
  }

  wsrep_close_client_connections(true);

  /* wait until appliers have stopped */
  wsrep_wait_appliers_close(NULL);
  node_uuid= WSREP_UUID_UNDEFINED;

  /* Undocking the thread specific data. */
  THD* thd= current_thd;
  if (thd) thd->restore_globals();
}

#define WSREP_NEW_CLUSTER "--wsrep-new-cluster"
/* Finds and hides --wsrep-new-cluster from the arguments list
 * by moving it to the end of the list and decrementing argument count */
void wsrep_filter_new_cluster (int* argc, char* argv[])
{
  int i;
  for (i= *argc - 1; i > 0; i--)
  {
    /* make a copy of the argument to convert possible underscores to hyphens.
     * the copy need not to be longer than WSREP_NEW_CLUSTER option */
    char arg[sizeof(WSREP_NEW_CLUSTER) + 1]= { 0, };
    strncpy(arg, argv[i], sizeof(arg) - 1);
    char* underscore(arg);
    while (NULL != (underscore= strchr(underscore, '_'))) *underscore= '-';

    if (!strcmp(arg, WSREP_NEW_CLUSTER))
    {
      wsrep_new_cluster= true;
      *argc -= 1;
      /* preserve the order of remaining arguments AND
       * preserve the original argument pointers - just in case */
      char* wnc= argv[i];
      memmove(&argv[i], &argv[i + 1], (*argc - i)*sizeof(argv[i]));
      argv[*argc]= wnc; /* this will be invisible to the rest of the program */
    }
  }
}

bool wsrep_start_replication()
{
  int rcode;
  WSREP_DEBUG("wsrep_start_replication");

  /*
    if provider is trivial, don't even try to connect,
    but resume local node operation
  */
  if (!WSREP_PROVIDER_EXISTS)
  {
    // enable normal operation in case no provider is specified
    return true;
  }

  if (!wsrep_cluster_address || wsrep_cluster_address[0]== 0)
  {
    // if provider is non-trivial, but no address is specified, wait for address
    WSREP_DEBUG("wsrep_start_replication exit due to empty address");
    return true;
  }

  bool const bootstrap(true == wsrep_new_cluster);

  WSREP_INFO("Start replication");

  if ((rcode= Wsrep_server_state::instance().connect(
        wsrep_cluster_name,
        wsrep_cluster_address,
        wsrep_sst_donor,
        bootstrap)))
  {
    DBUG_PRINT("wsrep",("wsrep_ptr->connect(%s) failed: %d",
                        wsrep_cluster_address, rcode));
    WSREP_ERROR("wsrep::connect(%s) failed: %d",
                wsrep_cluster_address, rcode);
    return false;
  }
  else
  {
    try
    {
      std::string opts= Wsrep_server_state::instance().provider().options();
      wsrep_provider_options_init(opts.c_str());
    }
    catch (const wsrep::runtime_error&)
    {
      WSREP_WARN("Failed to get wsrep options");
    }
  }

  return true;
}

bool wsrep_check_mode (uint mask)
{
  return wsrep_mode & (1ULL << mask);
}

bool wsrep_must_sync_wait (THD* thd, uint mask)
{
  bool ret;
  mysql_mutex_lock(&thd->LOCK_thd_data);
  ret= (thd->variables.wsrep_sync_wait & mask) &&
    thd->wsrep_client_thread &&
    WSREP_ON && thd->variables.wsrep_on &&
    !(thd->variables.wsrep_dirty_reads &&
      !is_update_query(thd->lex->sql_command)) &&
    !thd->in_active_multi_stmt_transaction() &&
    thd->wsrep_trx().state() !=
    wsrep::transaction::s_replaying &&
    thd->wsrep_cs().sync_wait_gtid().is_undefined();
  mysql_mutex_unlock(&thd->LOCK_thd_data);
  return ret;
}

bool wsrep_sync_wait (THD* thd, uint mask)
{
  if (wsrep_must_sync_wait(thd, mask))
  {
    WSREP_DEBUG("wsrep_sync_wait: thd->variables.wsrep_sync_wait= %u, "
                "mask= %u, thd->variables.wsrep_on= %d",
                thd->variables.wsrep_sync_wait, mask,
                thd->variables.wsrep_on);
    /*
      This allows autocommit SELECTs and a first SELECT after SET AUTOCOMMIT=0
      TODO: modify to check if thd has locked any rows.
    */
    if (thd->wsrep_cs().sync_wait(-1))
    {
      const char* msg;
      int err;

      /*
        Possibly relevant error codes:
        ER_CHECKREAD, ER_ERROR_ON_READ, ER_INVALID_DEFAULT, ER_EMPTY_QUERY,
        ER_FUNCTION_NOT_DEFINED, ER_NOT_ALLOWED_COMMAND, ER_NOT_SUPPORTED_YET,
        ER_FEATURE_DISABLED, ER_QUERY_INTERRUPTED
      */

      switch (thd->wsrep_cs().current_error())
      {
      case wsrep::e_not_supported_error:
        msg= "synchronous reads by wsrep backend. "
          "Please unset wsrep_sync_wait variable.";
        err= ER_NOT_SUPPORTED_YET;
        break;
      default:
        msg= "Synchronous wait failed.";
        err= ER_LOCK_WAIT_TIMEOUT; // NOTE: the above msg won't be displayed
                                   //       with ER_LOCK_WAIT_TIMEOUT
      }
      my_error(err, MYF(0), msg);

      return true;
    }
  }

  return false;
}

bool wsrep_is_show_query(enum enum_sql_command command)
{
  assert(command >= 0 && command <= SQLCOM_END);
  return (sql_command_flags[command] & CF_STATUS_COMMAND) != 0;
}

static bool wsrep_is_diagnostic_query(enum enum_sql_command command)
{
  assert(command >= 0 && command <= SQLCOM_END);
  return (sql_command_flags[command] & CF_DIAGNOSTIC_STMT) != 0;
}

static enum enum_wsrep_sync_wait
wsrep_sync_wait_mask_for_command(enum enum_sql_command command)
{
  switch (command)
  {
  case SQLCOM_SELECT:
  case SQLCOM_CHECKSUM:
    return WSREP_SYNC_WAIT_BEFORE_READ;
  case SQLCOM_DELETE:
  case SQLCOM_DELETE_MULTI:
  case SQLCOM_UPDATE:
  case SQLCOM_UPDATE_MULTI:
    return WSREP_SYNC_WAIT_BEFORE_UPDATE_DELETE;
  case SQLCOM_REPLACE:
  case SQLCOM_INSERT:
  case SQLCOM_REPLACE_SELECT:
  case SQLCOM_INSERT_SELECT:
    return WSREP_SYNC_WAIT_BEFORE_INSERT_REPLACE;
  default:
    if (wsrep_is_diagnostic_query(command))
    {
      return WSREP_SYNC_WAIT_NONE;
    }
    if (wsrep_is_show_query(command))
    {
      switch (command)
      {
      case SQLCOM_SHOW_PROFILE:
      case SQLCOM_SHOW_PROFILES:
      case SQLCOM_SHOW_SLAVE_HOSTS:
      case SQLCOM_SHOW_RELAYLOG_EVENTS:
      case SQLCOM_SHOW_SLAVE_STAT:
      case SQLCOM_SHOW_MASTER_STAT:
      case SQLCOM_SHOW_ENGINE_STATUS:
      case SQLCOM_SHOW_ENGINE_MUTEX:
      case SQLCOM_SHOW_ENGINE_LOGS:
      case SQLCOM_SHOW_PROCESSLIST:
      case SQLCOM_SHOW_PRIVILEGES:
        return WSREP_SYNC_WAIT_NONE;
      default:
        return WSREP_SYNC_WAIT_BEFORE_SHOW;
      }
    }
  }
  return WSREP_SYNC_WAIT_NONE;
}

bool wsrep_sync_wait(THD* thd, enum enum_sql_command command)
{
  bool res = false;
  if (WSREP_CLIENT(thd) && thd->variables.wsrep_sync_wait)
    res = wsrep_sync_wait(thd, wsrep_sync_wait_mask_for_command(command));
  return res;
}

enum wsrep::provider::status
wsrep_sync_wait_upto (THD*          thd MY_ATTRIBUTE((unused)),
                      wsrep_gtid_t* upto,
                      int           timeout)
{
  assert(upto);
  enum wsrep::provider::status ret;
  if (upto)
  {
    wsrep::gtid upto_gtid(wsrep::id(upto->uuid.data, sizeof(upto->uuid.data)),
                          wsrep::seqno(upto->seqno));
    ret= Wsrep_server_state::instance().wait_for_gtid(upto_gtid, timeout);
  }
  else
  {
    ret= Wsrep_server_state::instance().causal_read(timeout).second;
  }
  WSREP_DEBUG("wsrep_sync_wait_upto: %d", ret);
  return ret;
}

void wsrep_keys_free(wsrep_key_arr_t* key_arr)
{
    for (size_t i= 0; i < key_arr->keys_len; ++i)
    {
      my_free(const_cast<wsrep_buf_t *>(key_arr->keys[i].key_parts));
    }
    my_free(key_arr->keys);
    key_arr->keys= 0;
    key_arr->keys_len= 0;
}

static wsrep::key wsrep_prepare_key_for_toi(const char* db, const char* table,
                                     enum wsrep::key::type type)
{
  wsrep::key ret(type);
  assert(db);
  ret.append_key_part(db, strlen(db));
  if (table) ret.append_key_part(table, strlen(table));
  return ret;
}

void wsrep_append_fk_parent_table( THD* thd,
                                   TABLE_LIST* tables,
                                   wsrep::key_array* keys)
{
  TABLE_LIST *table;

  thd->mdl_context.release_transactional_locks();
  MDL_savepoint mdl_savepoint= thd->mdl_context.mdl_savepoint();

  uint counter;
  if (open_temporary_tables(thd, tables) ||
      open_tables(thd, &tables, &counter, MYSQL_OPEN_FORCE_SHARED_HIGH_PRIO_MDL))
  {
    WSREP_DEBUG("unable to open table for FK checks for %s", thd->query().str);
  }
  for (table= tables; table; table= table->next_local) {
    if (!is_temporary_table(table) && table->table) {
      for (TABLE_SHARE_FOREIGN_KEY_INFO *fk = table->table->s->foreign_key;
           fk < table->table->s->foreign_key + table->table->s->foreign_keys;
           ++fk) {
        WSREP_DEBUG("appended fkey %s", fk->referenced_table_name.str);
        keys->push_back(wsrep_prepare_key_for_toi(fk->referenced_table_db.str,
                                                  fk->referenced_table_name.str,
                                                  wsrep::key::shared));
      }
    }
  }
  /* close the table and release MDL locks */
  close_thread_tables(thd);
  thd->mdl_context.rollback_to_savepoint(mdl_savepoint);
  for (table= tables; table; table= table->next_local)
  {
    table->table= NULL;
    table->mdl_request.ticket= NULL;
  }
}

/*!
 * @param db      Database string
 * @param table   Table string
 * @param key     Array of wsrep_key_t
 * @param key_len In: number of elements in key array, Out: number of
 *                elements populated
 *
 * @return true if preparation was successful, otherwise false.
 */

static bool wsrep_prepare_key_for_isolation(const char* db,
                                           const char* table,
                                           wsrep_buf_t* key,
                                           size_t* key_len)
{
  if (*key_len < 2) return false;

  switch (wsrep_protocol_version)
  {
  case 0:
    *key_len= 0;
    break;
  case 1:
  case 2:
  case 3:
  case 4:
  {
    *key_len= 0;
    if (db)
    {
      key[*key_len].ptr= db;
      key[*key_len].len= strlen(db);
      ++(*key_len);
      if (table)
      {
        key[*key_len].ptr= table;
        key[*key_len].len= strlen(table);
        ++(*key_len);
      }
    }
    break;
  }
  default:
    WSREP_ERROR("Unsupported protocol version: %ld", wsrep_protocol_version);
    unireg_abort(1);
    return false;
  }

    return true;
}

static bool wsrep_prepare_key_for_isolation(const char* db,
                                            const char* table,
                                            wsrep_key_arr_t* ka)
{
  wsrep_key_t* tmp;
  tmp= (wsrep_key_t*)my_realloc(key_memory_wsrep,
                                ka->keys,
                                (ka->keys_len + 1) * sizeof(wsrep_key_t),
                                MYF(MY_ALLOW_ZERO_PTR));
  if (!tmp)
  {
    WSREP_ERROR("Can't allocate memory for key_array");
    return false;
  }
  ka->keys= tmp;
  if (!(ka->keys[ka->keys_len].key_parts= (const wsrep_buf_t*)
        my_malloc(key_memory_wsrep, sizeof(wsrep_buf_t)*2, MYF(0))))
  {
    WSREP_ERROR("Can't allocate memory for key_parts");
    return false;
  }
  ka->keys[ka->keys_len].key_parts_num= 2;
  ++ka->keys_len;
  if (!wsrep_prepare_key_for_isolation(db, table,
        const_cast<wsrep_buf_t*>(ka->keys[ka->keys_len - 1].key_parts),
        &ka->keys[ka->keys_len - 1].key_parts_num))
  {
    WSREP_ERROR("Preparing keys for isolation failed");
    return false;
  }

  return true;
}

static bool wsrep_prepare_keys_for_alter_add_fk(const char* child_table_db,
                                                Alter_info* alter_info,
                                                wsrep_key_arr_t* ka)
{
  for (const Key_spec *key : alter_info->key_list) {
    if (key->type == KEYTYPE_FOREIGN) {
      const Foreign_key_spec *fk_key = down_cast<const Foreign_key_spec *>(key);
      const char *db_name= fk_key->ref_db.str;
      const char *table_name= fk_key->ref_table.str;
      if (!db_name)
      {
        db_name= child_table_db;
      }
      if (!wsrep_prepare_key_for_isolation(db_name, table_name, ka))
      {
        return false;
      }
    }
  }
  return true;
}

static bool wsrep_prepare_keys_for_isolation(THD*              thd MY_ATTRIBUTE((unused)),
                                             const char*       db,
                                             const char*       table,
                                             const TABLE_LIST* table_list,
                                             Alter_info*       alter_info,
                                             wsrep_key_arr_t*  ka)
{
  ka->keys= 0;
  ka->keys_len= 0;

  if (db || table)
  {
    TABLE_LIST tmp_table;

    tmp_table.table_name= table;
    tmp_table.db= db;
    MDL_REQUEST_INIT(&tmp_table.mdl_request, MDL_key::GLOBAL, (db) ? db :  "",
                     (table) ? table : "",
                     MDL_INTENTION_EXCLUSIVE, MDL_STATEMENT);

    if (!wsrep_prepare_key_for_isolation(db, table, ka))
      goto err;
  }

  for (const TABLE_LIST* tab = table_list; tab; tab= tab->next_global)
  {
    if (!wsrep_prepare_key_for_isolation(tab->db, tab->table_name, ka))
      goto err;
  }

  if (alter_info && (alter_info->flags & (Alter_info::ADD_FOREIGN_KEY)))
  {
    if (!wsrep_prepare_keys_for_alter_add_fk(table_list->db, alter_info, ka))
      goto err;
  }
  return false;

err:
    wsrep_keys_free(ka);
    return true;
}

/*
 * Prepare key list from db/table and table_list
 *
 * Return zero in case of success, 1 in case of failure.
 */

bool wsrep_prepare_keys_for_isolation(THD*              thd,
                                      const char*       db,
                                      const char*       table,
                                      const TABLE_LIST* table_list,
                                      wsrep_key_arr_t*  ka)
{
  return wsrep_prepare_keys_for_isolation(thd, db, table, table_list, NULL, ka);
}

bool wsrep_prepare_key(const uchar* cache_key, size_t cache_key_len,
                       const uchar* row_id, size_t row_id_len,
                       wsrep_buf_t* key, size_t* key_len)
{
    if (*key_len < 3) return false;

    *key_len= 0;
    switch (wsrep_protocol_version)
    {
    case 0:
    {
        key[0].ptr= cache_key;
        key[0].len= cache_key_len;

        *key_len= 1;
        break;
    }
    case 1:
    case 2:
    case 3:
    case 4:
    {
        key[0].ptr= cache_key;
        key[0].len= strlen( (const char*)(cache_key) );
        key[1].ptr= cache_key + strlen( (const char*)cache_key ) + 1;
        key[1].len= strlen( (const char*)(key[1].ptr) );

        *key_len= 2;
        break;
    }
    default:
        return false;
    }

    key[*key_len].ptr= row_id;
    key[*key_len].len= row_id_len;
    ++(*key_len);

    return true;
}

bool wsrep_prepare_key_for_innodb(THD* thd MY_ATTRIBUTE((unused)),
                                  const uchar* cache_key,
                                  size_t cache_key_len,
                                  const uchar* row_id,
                                  size_t row_id_len,
                                  wsrep_buf_t* key,
                                  size_t* key_len)
{

  return wsrep_prepare_key(cache_key, cache_key_len, row_id, row_id_len, key, key_len);
}

int wsrep_append_table_level_key(THD *thd,
                                 const char* db,
                                 const char* table,
                                 enum wsrep::key::type type)
{
  // Make sure trx is started before appending table level key
  wsrep_start_trx_if_not_started(thd);
  wsrep::key key = wsrep_prepare_key_for_toi(db, table, type);
  return thd->wsrep_cs().append_key(key);
}

wsrep::key_array
wsrep_prepare_keys_for_alter_add_fk(const char* child_table_db,
                                    Alter_info* alter_info)

{
  wsrep::key_array ret;
  for (const Key_spec *key : alter_info->key_list) {
    if (key->type == KEYTYPE_FOREIGN) {
      const Foreign_key_spec *fk_key = down_cast<const Foreign_key_spec *>(key);

      const char *db_name= fk_key->ref_db.str;
      const char *table_name= fk_key->ref_table.str;
      if (!db_name)
      {
        db_name= child_table_db;
      }
      ret.push_back(wsrep_prepare_key_for_toi(db_name, table_name,
                                              wsrep::key::exclusive));
    }
  }
  return ret;
}

wsrep::key_array wsrep_prepare_keys_for_toi(const char* db,
                                            const char* table,
                                            const TABLE_LIST* table_list,
                                            Alter_info* alter_info,
                                            wsrep::key_array* fk_tables)
{
  wsrep::key_array ret;
  if (db || table)
  {
    ret.push_back(wsrep_prepare_key_for_toi(db, table, wsrep::key::exclusive));
  }
  for (const TABLE_LIST* tab = table_list; tab; tab = tab->next_global)
  {
    ret.push_back(wsrep_prepare_key_for_toi(tab->db, tab->table_name,
                                            wsrep::key::exclusive));
  }
  if (alter_info && (alter_info->flags & Alter_info::ADD_FOREIGN_KEY))
  {
    wsrep::key_array fk(wsrep_prepare_keys_for_alter_add_fk(table_list->db, alter_info));
    if (!fk.empty())
    {
      ret.insert(ret.end(), fk.begin(), fk.end());
    }
  }
  if (fk_tables && !fk_tables->empty())
  {
    ret.insert(ret.end(), fk_tables->begin(), fk_tables->end());
  }
  return ret;
}

/*
 * Construct Query_log_Event from thd query and serialize it
 * into buffer.
 *
 * Return 0 in case of success, 1 in case of error.
 */
int wsrep_to_buf_helper(
    THD* thd, const char *query, uint query_len, uchar** buf, size_t* buf_len)
{
  DBUG_ENTER("wsrep_to_buf_helper");
  int ret(0);

  IO_CACHE_binlog_cache_storage tmp_io_cache;
  if (tmp_io_cache.open(mysql_tmpdir, TEMP_PREFIX, 64 * 1024, 64 * 1024))
    DBUG_RETURN(true);

#ifdef OUT
  enum enum_binlog_checksum_alg current_binlog_check_alg =
    (enum_binlog_checksum_alg) binlog_checksum_options;
#endif
  Format_description_log_event *tmp_fde = new Format_description_log_event();

  /*
    Write FDE event and then commit (which will generate commit
    event and Gtid_log_event)
  */
  DBUG_PRINT("debug", ("Writing to trx_cache"));
  if (tmp_fde->write(&tmp_io_cache)) 
    ret = 1;

  delete tmp_fde;
  
#ifdef TODO
  if (thd->variables.gtid_next.type == GTID_GROUP) {
      Gtid_log_event gtid_ev(thd, false, &thd->variables.gtid_next);
      if (!gtid_ev.is_valid()) ret= 0;
      if (!ret && gtid_ev.write(&tmp_io_cache)) ret= 1;
  }
#endif

  /* if MySQL GTID event is set, we have to forward it in wsrep channel */
  if (!ret && thd->wsrep_gtid_event_buf) {
    *buf     = (uchar *)thd->wsrep_gtid_event_buf;
    *buf_len = thd->wsrep_gtid_event_buf_len;
  }

  /* if there is prepare query, add event for it */
  if (!ret && thd->wsrep_TOI_pre_query) {
    Query_log_event ev(thd, thd->wsrep_TOI_pre_query,
                       thd->wsrep_TOI_pre_query_len,
                       false, false, false, 0);
    DBUG_PRINT("debug", ("Writing to trx_cache"));
    if (ev.write(&tmp_io_cache))
      ret= 1;
  }

  /* continue to append the actual query */
  Query_log_event ev(thd, query, query_len, false, false, false, 0);
  if (wsrep_gtid_mode &&
      thd->variables.gtid_next.type == AUTOMATIC_GTID )
    ev.server_id = wsrep_local_gtid_manager.server_id;
  if (!ret && ev.write(&tmp_io_cache)) 
    ret = 1;
  if (!ret && wsrep_write_cache_buf_from_iocache(&tmp_io_cache, buf, buf_len)) 
    ret = 1;

  thd->wsrep_gtid_event_buf = *buf;
  thd->wsrep_gtid_event_buf_len = *buf_len;

  tmp_io_cache.close();

  DBUG_RETURN(ret);
}

static int
wsrep_alter_query_string(THD *thd, String *buf)
{
  /* Append the "ALTER" part of the query */
  if (buf->append(STRING_WITH_LEN("ALTER ")))
    return 1;
  /* Append definer */
  append_definer(thd, buf, thd->lex->definer->user, thd->lex->definer->host);
  /* Append the left part of thd->query after event name part */
  if (buf->append(thd->lex->stmt_definition_begin,
                  thd->lex->stmt_definition_end -
                  thd->lex->stmt_definition_begin))
    return 1;

  return 0;
}

static int wsrep_alter_event_query(THD *thd, uchar** buf, size_t* buf_len)
{
  String log_query;

  if (wsrep_alter_query_string(thd, &log_query))
  {
    WSREP_WARN("events alter string failed: schema: %s, query: %s",
               (thd->db().str ? thd->db().str : "(null)"), WSREP_QUERY(thd));
    return 1;
  }
  return wsrep_to_buf_helper(thd, log_query.ptr(), log_query.length(), buf, buf_len);
}

static int
create_view_query(THD *thd, uchar** buf, size_t* buf_len)
{
    LEX *lex= thd->lex;
    Query_block *query_block= lex->query_block;
    TABLE_LIST *first_table= query_block->get_table_list();
    TABLE_LIST *views= first_table;
    LEX_USER *definer;
    String buff;
    const LEX_CSTRING command[3]=
      {{ STRING_WITH_LEN("CREATE ") },
       { STRING_WITH_LEN("ALTER ") },
       { STRING_WITH_LEN("CREATE OR REPLACE ") }};

  buff.append(command[static_cast<int>(thd->lex->create_view_mode)].str,
              command[static_cast<int>(thd->lex->create_view_mode)].length);


    if (lex->definer)
      definer= get_current_user(thd, lex->definer);
    else
    {
      /*
        DEFINER-clause is missing; we have to create default definer in
        persistent arena to be PS/SP friendly.
        If this is an ALTER VIEW then the current user should be set as
        the definer.
      */
      definer= create_default_definer(thd);
    }

    if (definer)
    {
      views->definer.user= definer->user;
      views->definer.host= definer->host;
    } else {
      WSREP_ERROR("Failed to get DEFINER for VIEW.");
      return 1;
    }

    views->algorithm    = lex->create_view_algorithm;
    views->view_suid    = lex->create_view_suid;
    views->with_check   = lex->create_view_check;

    view_store_options(thd, views, &buff);
    buff.append(STRING_WITH_LEN("VIEW "));
    /* Test if user supplied a db (ie: we did not use thd->db) */
    if (views->db && views->db[0] &&
        (thd->db().str == NULL || strcmp(views->db, thd->db().str)))
    {
      append_identifier(thd, &buff, views->db,
			views->db_length);
      buff.append('.');
    }
    append_identifier(thd, &buff, views->table_name,
		      views->table_name_length);
    if (views->derived_column_names())
    {
      int i= 0;
      for (auto name : *views->derived_column_names())
      {
	buff.append(i++ ? ", " : "(");
	append_identifier(thd, &buff, name.str, name.length);
      }
      buff.append(')');
    }

    buff.append(STRING_WITH_LEN(" AS "));
    buff.append(thd->lex->create_view_query_block.str,
                thd->lex->create_view_query_block.length);
    return wsrep_to_buf_helper(thd, buff.ptr(), buff.length(), buf, buf_len);
}

/*
  Rewrite DROP TABLE for TOI. Temporary tables are eliminated from
  the query as they are visible only to client connection.

  TODO: See comments for sql_base.cc:drop_temporary_table() and refine
  the function to deal with transactional locked tables.
 */
static int wsrep_drop_table_query(THD* thd, uchar** buf, size_t* buf_len)
{

  LEX* lex= thd->lex;
  Query_block* query_block= lex->query_block;
  TABLE_LIST* first_table= query_block->get_table_list();
  String buff;

  assert(!lex->drop_temporary);

  bool found_temp_table= false;
  for (TABLE_LIST* table= first_table; table; table= table->next_global)
  {
    if (find_temporary_table(thd, table->db, table->table_name))
    {
      found_temp_table= true;
      break;
    }
  }

  if (found_temp_table)
  {
    buff.append("DROP TABLE ");
    if (lex->drop_if_exists)
      buff.append("IF EXISTS ");

    for (TABLE_LIST* table= first_table; table; table= table->next_global)
    {
      if (!find_temporary_table(thd, table->db, table->table_name))
      {
        append_identifier(thd, &buff, table->db, strlen(table->db),
                          system_charset_info, thd->charset());
        buff.append(".");

	append_identifier(thd, &buff, table->table_name,
                          strlen(table->table_name),
                          system_charset_info, thd->charset());
        buff.append(",");
      }
    }

    /* Chop the last comma */
    buff.chop();
    buff.append(" /* generated by wsrep */");

    WSREP_DEBUG("Rewrote '%s' as '%s'", thd->query().str, buff.ptr());

    return wsrep_to_buf_helper(thd, buff.ptr(), buff.length(), buf, buf_len);
  }
  else
  {
    return wsrep_to_buf_helper(thd, thd->query().str, thd->query().length,
                               buf, buf_len);
  }
}


/* Forward declarations. */
int wsrep_create_trigger_query(THD *thd, uchar** buf, size_t* buf_len);

/*
  Decide if statement should run in TOI.

  Look if table or table_list contain temporary tables. If the
  statement affects only temporary tables,   statement should not run
  in TOI. If the table list contains mix of regular and temporary tables
  (DROP TABLE, OPTIMIZE, ANALYZE), statement should be run in TOI but
  should be rewritten at later time for replication to contain only
  non-temporary tables.
 */
static bool wsrep_can_run_in_toi(THD *thd, const char *db, const char *table,
                                 const TABLE_LIST *table_list)
{
  assert(!table || db);
  assert(table_list || db);

  LEX* lex= thd->lex;
  Query_block* query_block= lex->query_block;
  TABLE_LIST* first_table= query_block->get_table_list();

  switch (lex->sql_command)
  {
  case SQLCOM_CREATE_TABLE:
    if (thd->lex->create_info->options & HA_LEX_CREATE_TMP_TABLE)
    {
      return false;
    }
    return true;

  case SQLCOM_CREATE_VIEW:

    assert(!table_list);
    assert(first_table); /* First table is view name */
    /*
      If any of the remaining tables refer to temporary table error
      is returned to client, so TOI can be skipped
    */
    for (TABLE_LIST* it= first_table->next_global; it; it= it->next_global)
    {
      if (find_temporary_table(thd, it))
      {
        return false;
      }
    }
    return true;

  case SQLCOM_CREATE_TRIGGER:
  case SQLCOM_DROP_TRIGGER:

    assert(table_list);

    if (find_temporary_table(thd, table_list))
    {
      return false;
    }
    return true;

  default:
    if (table && !find_temporary_table(thd, db, table))
    {
      return true;
    }

    if (table_list)
    {
      for (const TABLE_LIST* tab = table_list; tab; tab = tab->next_global)
      {
        if (!find_temporary_table(thd, tab->db, tab->table_name))
        {
          return true;
        }
      }
    }
    if (first_table)
    {
      for (TABLE_LIST* tab = first_table; tab; tab = tab->next_global)
      {
        if (!find_temporary_table(thd, tab->db, tab->table_name))
        {
          return true;
        }
      }
    }
    return !table && !first_table && !table_list;;
  }
}

#ifdef UNUSED /* 323f269d4099 (Jan Lindström     2018-07-19) */
static const char* wsrep_get_query_or_msg(const THD* thd)

{
  switch(thd->lex->sql_command)
  {
    case SQLCOM_CREATE_USER:
      return "CREATE USER";
    case SQLCOM_GRANT:
      return "GRANT";
    case SQLCOM_REVOKE:
      return "REVOKE";
    case SQLCOM_SET_OPTION:
      if (thd->lex->definer)
        return "SET PASSWORD";
      /* fallthrough */
    default:
      return thd->query();
   }
}
#endif //UNUSED
extern void sp_returns_type(THD *thd, String &result, sp_head *sp);
extern bool wsrep_create_string(THD *thd, String *buf,
                          enum_sp_type type,
                          const char *db, size_t dblen,
                          const char *name, size_t namelen,
                          const char *params, size_t paramslen,
                          const char *returns, size_t returnslen,
                          const char *body, size_t bodylen,
                          st_sp_chistics *chistics,
                          const LEX_CSTRING &definer_user,
                          const LEX_CSTRING &definer_host,
               	          sql_mode_t sql_mode);

int wsrep_create_sp(THD *thd, uchar** buf, size_t* buf_len)
{
  String log_query;
  sp_head *sp = thd->lex->sphead;
  ulong saved_mode= thd->variables.sql_mode;
  String retstr(64);
  retstr.set_charset(system_charset_info);

  log_query.set_charset(system_charset_info);

  if (sp->m_type == enum_sp_type::FUNCTION) sp->returns_type(thd, &retstr);

  if (!wsrep_create_string(thd, &log_query,
                     sp->m_type,
                     (sp->m_explicit_name ? sp->m_db.str : NULL), 
                     (sp->m_explicit_name ? sp->m_db.length : 0), 
                     sp->m_name.str, sp->m_name.length,
                     sp->m_params.str, sp->m_params.length,
                     retstr.c_ptr(), retstr.length(),
                     sp->m_body.str, sp->m_body.length,
                     sp->m_chistics, thd->lex->definer->user,
                     thd->lex->definer->host,
                     saved_mode))
  {
    WSREP_WARN("SP create string failed: schema: %s, query: %s",
               (thd->db().str ? thd->db().str : "(null)"), WSREP_QUERY(thd));
    return 1;
  }
  return wsrep_to_buf_helper(thd, log_query.ptr(), log_query.length(), buf, buf_len);
}

static int wsrep_TOI_event_buf(THD* thd, uchar** buf, size_t* buf_len)
{
  int err;
  switch (thd->lex->sql_command)
  {
  case SQLCOM_CREATE_VIEW:
    err= create_view_query(thd, buf, buf_len);
    break;
  case SQLCOM_CREATE_PROCEDURE:
  case SQLCOM_CREATE_SPFUNCTION:
    err= wsrep_create_sp(thd, buf, buf_len);
    break;
  case SQLCOM_CREATE_TRIGGER:
    err= wsrep_create_trigger_query(thd, buf, buf_len);
    break;
  case SQLCOM_CREATE_EVENT:
    err= wsrep_create_event_query(thd, buf, buf_len);
    break;
  case SQLCOM_ALTER_EVENT:
    err= wsrep_alter_event_query(thd, buf, buf_len);
    break;
  case SQLCOM_DROP_TABLE:
    err= wsrep_drop_table_query(thd, buf, buf_len);
    break;
#ifdef NOT_IMPLEMENTED
  case SQLCOM_CREATE_ROLE:
    if (sp_process_definer(thd))
    {
      WSREP_WARN("Failed to set CREATE ROLE definer for TOI.");
    }
    /* fallthrough */
#endif
  default:
    err= wsrep_to_buf_helper(thd, thd->query().str, thd->query().length, buf,
                             buf_len);
    break;
  }

  return err;
}

static void wsrep_TOI_begin_failed(THD* thd, const wsrep_buf_t* /* const err */)
{
  if (wsrep_thd_trx_seqno(thd) > 0) {
    if (wsrep_emulate_bin_log)
      wsrep_thd_binlog_trx_reset(thd);
    wsrep::client_state& cs(thd->wsrep_cs());
    std::string const err(wsrep::to_c_string(cs.current_error()));
    wsrep::mutable_buffer err_buf;
    err_buf.push_back(err);
    int const ret = cs.leave_toi_local(err_buf);
    if (ret) {
      WSREP_ERROR("Leaving critical section for failed TOI failed: thd: %lld, "
                  "schema: %s, SQL: %s, rcode: %d wsrep_error: %s",
                  (long long)thd->real_id, thd->db().str,
                  WSREP_QUERY(thd), ret, err.c_str());
      unireg_abort(1);
    }
  }
  return;
}


/*
  returns:
   0: statement was replicated as TOI
   1: TOI replication was skipped
  -1: TOI replication failed
 */
static int wsrep_TOI_begin(THD *thd, const char *db, const char *table,
                           const TABLE_LIST* table_list,
                           Alter_info* alter_info, wsrep::key_array* fk_tables)
{
  assert(thd->variables.wsrep_OSU_method == WSREP_OSU_TOI);

  WSREP_DEBUG("TOI Begin");
  if (wsrep_can_run_in_toi(thd, db, table, table_list) == false) {
    WSREP_DEBUG("No TOI for %s", WSREP_QUERY(thd));
    return 1;
  }

  uchar* buf = 0;
  size_t buf_len(0);
  int buf_err;
  int rc;

  buf_err = wsrep_TOI_event_buf(thd, &buf, &buf_len);
  if (buf_err) {
    WSREP_ERROR("Failed to create TOI event buf: %d", buf_err);
    my_message(ER_UNKNOWN_ERROR,
               "WSREP replication failed to prepare TOI event buffer. "
               "Check your query.",
               MYF(0));
    return -1;
  }
  struct wsrep_buf buff = { buf, buf_len };

  wsrep::key_array key_array =
    wsrep_prepare_keys_for_toi(db, table, table_list, alter_info, fk_tables);

#ifdef NOT_IMPLEMENTED
  if (thd->has_read_only_protection())
  {
    /* non replicated DDL, affecting temporary tables only */
    WSREP_DEBUG("TO isolation skipped, sql: %s."
                "Only temporary tables affected.",
                WSREP_QUERY(thd));
    if (buf) my_free(buf);
    return -1;
  }
#endif

  thd_proc_info(thd, "acquiring total order isolation");

  wsrep::client_state& cs(thd->wsrep_cs());
  int ret= cs.enter_toi_local(key_array,
                              wsrep::const_buffer(buff.ptr, buff.len));

  if (ret) {
    assert(cs.current_error());
    WSREP_DEBUG("to_execute_start() failed for %u: %s, seqno: %lld",
                thd->thread_id(), WSREP_QUERY(thd),
                (long long)wsrep_thd_trx_seqno(thd));

    /* jump to error handler in mysql_execute_command() */
    switch (cs.current_error()) {
      case wsrep::e_size_exceeded_error:
        WSREP_WARN("TO isolation failed for: %d, schema: %s, sql: %s. "
                  "Maximum size exceeded.",
                  ret,
                  (thd->db().str ? thd->db().str : "(null)"),
                  WSREP_QUERY(thd));
        my_error(ER_ERROR_DURING_COMMIT, MYF(0), WSREP_SIZE_EXCEEDED);
        break;
      default:
        WSREP_WARN("TO isolation failed for: %d, schema: %s, sql: %s. "
                  "Check wsrep connection state and retry the query.",
                  ret,
                  (thd->db().str ? thd->db().str : "(null)"),
                  WSREP_QUERY(thd));
        if (!thd->is_error()) {
          my_error(ER_LOCK_DEADLOCK, MYF(0), "WSREP replication failed. Check "
                  "your wsrep connection state and retry the query.");
        }
    }
    rc= -1;
  } else {
    if (thd->variables.gtid_next.type == AUTOMATIC_GTID) {
      if (wsrep_gtid_mode)
        thd->server_id = wsrep_local_gtid_manager.server_id;
      thd->wsrep_current_gtid_seqno = wsrep_local_gtid_manager.seqno_inc();
    }
    ++wsrep_to_isolation;
    rc = 0;
  }

  if (buf) my_free(buf);
  /* thd->wsrep_gtid_event_buf was free'ed above, just set to NULL */
  thd->wsrep_gtid_event_buf_len = 0;
  thd->wsrep_gtid_event_buf     = NULL;

  if (rc) wsrep_TOI_begin_failed(thd, NULL);

  return rc;
}

static void wsrep_TOI_end(THD *thd) {
  wsrep_to_isolation--;
  wsrep::client_state& client_state(thd->wsrep_cs());
  assert(wsrep_thd_is_local_toi(thd));
  WSREP_DEBUG("TO END: %lld: %s", client_state.toi_meta().seqno().get(),
              WSREP_QUERY(thd));

  wsrep_local_gtid_manager.signal_waiters(thd->wsrep_current_gtid_seqno, false);
  if (wsrep_thd_is_local_toi(thd)) {
    thd->wsrep_last_written_gtid_seqno = thd->wsrep_current_gtid_seqno;
    wsrep_set_SE_checkpoint(client_state.toi_meta().gtid(),
                            wsrep_local_gtid_manager.gtid(), true);
    wsrep::mutable_buffer err;
    if (thd->is_error() && !wsrep_must_ignore_error(thd))
        wsrep_store_error(thd, err);
    int const ret = client_state.leave_toi_local(err);
    if (!ret) {
      WSREP_DEBUG("TO END: %lld", client_state.toi_meta().seqno().get());
    }
    else {
      WSREP_WARN("TO isolation end failed for: %d, schema: %s, sql: %s",
                 ret, (thd->db().str ? thd->db().str : "(null)"),
                 WSREP_QUERY(thd));
    }
  }
}

static int wsrep_RSU_begin(THD *thd, const char *db_ MY_ATTRIBUTE(( unused )),
       	                   const char *table_ MY_ATTRIBUTE(( unused )))
{
  WSREP_DEBUG("RSU BEGIN: %lld, : %s", wsrep_thd_trx_seqno(thd),
              WSREP_QUERY(thd));

  if (thd->wsrep_cs().begin_rsu(5000)) {
    WSREP_WARN("RSU begin failed");
  }
  else {
    thd->variables.wsrep_on= 0;
  }

  return 0;
}

static void wsrep_RSU_end(THD *thd)
{
  WSREP_DEBUG("RSU END: %lld : %s", wsrep_thd_trx_seqno(thd),
              WSREP_QUERY(thd));

  if (thd->wsrep_cs().end_rsu())
    WSREP_WARN("Failed to end RSU, server may need to be restarted");

  thd->variables.wsrep_on= 1;
}

int wsrep_to_isolation_begin(THD *thd, const char *db_, const char *table_,
                             const TABLE_LIST* table_list,
                             Alter_info* alter_info, wsrep::key_array* fk_tables)
{
  /*
    No isolation for applier or replaying threads.
   */
  if (!wsrep_thd_is_local(thd)) 
    return 0;

  int ret = 0;

  mysql_mutex_lock(&thd->LOCK_thd_data);
  if (thd->wsrep_trx().state() == wsrep::transaction::s_must_abort) {
    WSREP_INFO("thread: %u, schema: %s, query: %s has been aborted due to cluster write conflict",
               thd->thread_id(),
               (thd->db().str ? thd->db().str : "(null)"),
               (thd->query().str) ? WSREP_QUERY(thd) : "void");
    mysql_mutex_unlock(&thd->LOCK_thd_data);
    return WSREP_TRX_FAIL;
  }
  mysql_mutex_unlock(&thd->LOCK_thd_data);

  assert(wsrep_thd_is_local(thd));
  assert(thd->wsrep_trx().ws_meta().seqno().is_undefined());

  if (thd->global_read_lock.is_acquired()) {
    WSREP_DEBUG("Aborting TOI: Global Read-Lock (FTWRL) in place: %s %u",
                WSREP_QUERY(thd), thd->thread_id());
    return -1;
  }

  if (wsrep_debug && thd->mdl_context.has_locks())
    WSREP_DEBUG("thread holds MDL locks at TI begin: %s %u",
                WSREP_QUERY(thd), thd->thread_id());

  /*
    It makes sense to set auto_increment_* to defaults in TOI operations.
    Must be done before wsrep_TOI_begin() since Query_log_event encapsulating
    TOI statement and auto inc variables for wsrep replication is constructed
    there. Variables are reset back in THD::reset_for_next_command() before
    processing of next command.
   */
  if (wsrep_auto_increment_control) {
    thd->variables.auto_increment_offset = 1;
    thd->variables.auto_increment_increment = 1;
  }

  if (thd->variables.wsrep_on && wsrep_thd_is_local(thd)) {
    switch (thd->variables.wsrep_OSU_method) {
      case WSREP_OSU_TOI:
        ret = wsrep_TOI_begin(thd, db_, table_, table_list, alter_info, fk_tables);
        DEBUG_SYNC(thd, "wsrep_after_toi_begin");
        break;
      case WSREP_OSU_RSU:
        ret = wsrep_RSU_begin(thd, db_, table_);
        break;
      default:
        WSREP_ERROR("Unsupported OSU method: %lu",
                    thd->variables.wsrep_OSU_method);
        ret = -1;
        break;
    }
    switch (ret) {
      case 0: /* wsrep_TOI_begin sould set toi mode */ break;
      case 1:
        /* TOI replication skipped, treat as success */
        ret = 0;
        break;
      case -1:
        /* TOI replication failed, treat as error */
        break;
    }
  }

  return ret;
}

void wsrep_to_isolation_end(THD *thd)
{
  assert(wsrep_thd_is_local_toi(thd) || wsrep_thd_is_in_rsu(thd));

  if (wsrep_thd_is_local_toi(thd)) {
    assert(thd->variables.wsrep_OSU_method == WSREP_OSU_TOI);
    wsrep_TOI_end(thd);
  } else if (wsrep_thd_is_in_rsu(thd)) {
    assert(thd->variables.wsrep_OSU_method == WSREP_OSU_RSU);
    wsrep_RSU_end(thd);
  } else {
    assert(0);
  }
  if (wsrep_emulate_bin_log)
    wsrep_thd_binlog_trx_reset(thd);
}

#define WSREP_MDL_LOG(severity, msg, schema, schema_len, req, gra)      \
  do {                                                                  \
    mysql_mutex_lock(&req->LOCK_thd_query);                             \
    std::string req_query= req->query().str ? req->query().str : "";    \
    mysql_mutex_unlock(&req->LOCK_thd_query);                           \
    mysql_mutex_lock(&gra->LOCK_thd_query);                             \
    std::string gra_query= gra->query().str ? gra->query().str : "";    \
    mysql_mutex_unlock(&gra->LOCK_thd_query);                           \
    WSREP_##severity(                                                   \
      "%s\n"                                                            \
      "schema:  %.*s\n"                                                 \
      "request: (%u \tseqno %lld \twsrep (%s, %s, %s) cmd %d %d \t%s)\n" \
      "granted: (%u \tseqno %lld \twsrep (%s, %s, %s) cmd %d %d \t%s)", \
      msg, schema_len, schema,                                          \
      req->thread_id(), (long long)wsrep_thd_trx_seqno(req),            \
      wsrep_thd_client_mode_str(req), wsrep_thd_client_state_str(req),  \
      wsrep_thd_transaction_state_str(req),                             \
      req->get_command(), req->lex->sql_command, req_query.c_str(),     \
      gra->thread_id(), (long long)wsrep_thd_trx_seqno(gra),            \
      wsrep_thd_client_mode_str(gra), wsrep_thd_client_state_str(gra),  \
      wsrep_thd_transaction_state_str(gra),                             \
      gra->get_command(), gra->lex->sql_command, gra_query.c_str());    \
  } while (0)

/**
  Check if request for the metadata lock should be granted to the requester.

  @param  requestor_ctx        The MDL context of the requestor
  @param  ticket               MDL ticket for the requested lock

*/

void wsrep_handle_mdl_conflict(const MDL_context *requestor_ctx,
                               MDL_ticket *ticket,
                               const MDL_key *key)
{
  /* Fallback to the non-wsrep behaviour */
  if (!WSREP_ON) return;

  THD *request_thd= requestor_ctx->wsrep_get_thd();
  THD *granted_thd= ticket->get_ctx()->wsrep_get_thd();

  const char* schema= key->db_name();
  int schema_len= key->db_name_length();

  mysql_mutex_lock(&request_thd->LOCK_thd_data);
  if (wsrep_thd_is_toi(request_thd) ||
      wsrep_thd_is_applying(request_thd)) {

    mysql_mutex_unlock(&request_thd->LOCK_thd_data);
    WSREP_MDL_LOG(DEBUG, "MDL conflict ", schema, schema_len,
                  request_thd, granted_thd);
    ticket->wsrep_report(wsrep_debug);

    mysql_mutex_lock(&granted_thd->LOCK_thd_data);
    if (wsrep_thd_is_toi(granted_thd) ||
        wsrep_thd_is_applying(granted_thd))
    {
      if (wsrep_thd_is_aborting(granted_thd))
      {
        WSREP_DEBUG("BF thread waiting for SR in aborting state");
        ticket->wsrep_report(wsrep_debug);
        mysql_mutex_unlock(&granted_thd->LOCK_thd_data);
      }
      else if (wsrep_thd_is_SR(granted_thd) && !wsrep_thd_is_SR(request_thd))
      {
        WSREP_MDL_LOG(INFO, "MDL conflict, DDL vs SR", 
                      schema, schema_len, request_thd, granted_thd);
        mysql_mutex_unlock(&granted_thd->LOCK_thd_data);
        wsrep_abort_thd(request_thd, granted_thd, 1);
      }
      else
      {
        WSREP_MDL_LOG(INFO, "MDL BF-BF conflict", schema, schema_len,
                      request_thd, granted_thd);
        ticket->wsrep_report(true);
        mysql_mutex_unlock(&granted_thd->LOCK_thd_data);
        unireg_abort(1);
      }
    }
    else if (ticket->get_key()->mdl_namespace() == MDL_key::BACKUP_LOCK)
    {
      WSREP_DEBUG("BF thread waiting for BACKUP");
      ticket->wsrep_report(wsrep_debug);
      mysql_mutex_unlock(&granted_thd->LOCK_thd_data);
    }
    else if (ticket->get_key()->mdl_namespace() == MDL_key::ACL_CACHE)
    {
      WSREP_DEBUG("BF thread waiting for ACL cache");
      ticket->wsrep_report(wsrep_debug);
      mysql_mutex_unlock(&granted_thd->LOCK_thd_data);
      wsrep_abort_thd(request_thd, granted_thd, 1);
    }
    else if (granted_thd->lex->sql_command == SQLCOM_FLUSH)
    {
      WSREP_DEBUG("BF thread waiting for FLUSH");
      ticket->wsrep_report(wsrep_debug);
      mysql_mutex_unlock(&granted_thd->LOCK_thd_data);
    }
    else if (ticket->get_ctx()->ticket_is_explicit(ticket))
    {
      WSREP_DEBUG("BF thread waiting for victim to release explicit locks");
      ticket->wsrep_report(wsrep_debug);
      mysql_mutex_unlock(&granted_thd->LOCK_thd_data);
    }
    else if (request_thd->lex->sql_command == SQLCOM_DROP_TABLE)
    {
      WSREP_DEBUG("DROP caused BF abort, conf %s",
                  wsrep_thd_transaction_state_str(granted_thd));
      ticket->wsrep_report(wsrep_debug);
      mysql_mutex_unlock(&granted_thd->LOCK_thd_data);
      wsrep_abort_thd(request_thd, granted_thd, 1);
    }
    else
    {
      WSREP_MDL_LOG(DEBUG, "MDL conflict-> BF abort", schema, schema_len,
                    request_thd, granted_thd);
      ticket->wsrep_report(wsrep_debug);
      if (granted_thd->wsrep_trx().active())
      {
        mysql_mutex_unlock(&granted_thd->LOCK_thd_data);
        wsrep_abort_thd(request_thd, granted_thd, 1);
      }
      else
      {
        /*
          Granted_thd is likely executing with wsrep_on=0. If the requesting
          thd is BF, BF abort and wait.
        */
        mysql_mutex_unlock(&granted_thd->LOCK_thd_data);
        if (wsrep_thd_is_BF(request_thd, false))
        {
          wsrep_abort_thd(request_thd, granted_thd, true);
        }
        else
        {
          WSREP_MDL_LOG(INFO, "MDL unknown BF-BF conflict", schema, schema_len,
                      request_thd, granted_thd);
          ticket->wsrep_report(true);
          unireg_abort(1);
        }
      }
    }
  }
  else
  {
    mysql_mutex_unlock(&request_thd->LOCK_thd_data);
  }
}

/**
========================================================================
   wsrep thread management
========================================================================
*/

/**
  Helper method to safely lock THD data, do an operation fun() under
  LOCK_thd_data and wake up by broadcasting current_cond.

  The template param F must be a callable with signature void fun(THD*).
*/

template <class F>
static void wsrep_lock_thd_do_and_awake(THD *thd, F fun)
{
  mysql_mutex_lock(&thd->LOCK_thd_data);
  fun(thd);
  mysql_mutex_unlock(&thd->LOCK_thd_data);

  mysql_mutex_lock(&thd->LOCK_current_cond);
  if (thd->current_cond)
  {
    mysql_mutex_lock(thd->current_mutex);
    mysql_cond_broadcast(thd->current_cond);
    mysql_mutex_unlock(thd->current_mutex);
  }
  mysql_mutex_unlock(&thd->LOCK_current_cond);
}

/**
  This class implements callback function used by close_connections()
  to set KILL_CONNECTION flag on all thds in thd list.
  If m_kill_dump_thread_flag is not set it kills all other threads
  except dump threads. If this flag is set, it kills dump threads.
*/
class Set_wsrep_kill_conn : public Do_THD_Impl
{
private:
  int m_dump_thread_count;
  bool m_kill_dump_threads_flag;
public:
  Set_wsrep_kill_conn()
    : m_dump_thread_count(0),
      m_kill_dump_threads_flag(false)
  {}

  void set_dump_thread_flag()
  {
    m_kill_dump_threads_flag= true;
  }

  int get_dump_thread_count() const
  {
    return m_dump_thread_count;
  }

  virtual void operator()(THD *killing_thd) override
  {
    DBUG_PRINT("quit",("Informing thread %u that it's time to die",
                       killing_thd->thread_id()));
    if (!m_kill_dump_threads_flag)
    {
      // We skip slave threads & scheduler on this first loop through.
      if (killing_thd->slave_thread)
        return;
    /* skip wsrep system threads as well */
      if (WSREP(killing_thd) && 
          (!wsrep_thd_is_local(killing_thd) || killing_thd->wsrep_applier))
        return;

      if (killing_thd->get_command() == COM_BINLOG_DUMP ||
          killing_thd->get_command() == COM_BINLOG_DUMP_GTID)
      {
        ++m_dump_thread_count;
        return;
      }
      DBUG_EXECUTE_IF("Check_dump_thread_is_alive",
                      {
                        assert(killing_thd->get_command() != COM_BINLOG_DUMP &&
                                    killing_thd->get_command() != COM_BINLOG_DUMP_GTID);
                      };);
    }
    wsrep_lock_thd_do_and_awake(killing_thd,
                                [](THD *killee) {
                                  killee->killed= THD::KILL_CONNECTION;
                                  MYSQL_CALLBACK(Connection_handler_manager::event_functions,
                                                 post_kill_notification, (killee));
                                });
  }
};

/**
  This class implements callback function used by close_connections()
  to close vio connection for all thds in thd list
*/
class Call_wsrep_close_client_conn : public Do_THD_Impl
{
public:
  Call_wsrep_close_client_conn(bool is_server_shutdown)
    : m_is_server_shutdown(is_server_shutdown)
  {}

  virtual void operator()(THD *closing_thd) override
  {
    if (closing_thd->get_protocol()->connection_alive() &&
        (WSREP(closing_thd) || wsrep_thd_is_local(closing_thd)) &&
        closing_thd != current_thd)
    {
      LEX_CSTRING main_sctx_user= closing_thd->m_main_security_ctx.user();
      sql_print_warning(ER_DEFAULT(ER_FORCING_CLOSE),my_progname,
                        closing_thd->thread_id(),
                        (main_sctx_user.length ? main_sctx_user.str : ""));
      close_connection(closing_thd, 0, m_is_server_shutdown, false);
    }
  }
private:
  bool m_is_server_shutdown;
};

/**
  This class implements callback function used by close_connections()
  to close all wsrep system threads
*/
class Call_wsrep_close_wsrep_threads : public Do_THD_Impl
{
public:
  Call_wsrep_close_wsrep_threads()
  {}

  virtual void operator()(THD *thd_in) override
  {
    if (thd_in->wsrep_applier)
    {
      WSREP_DEBUG("Closing applier thread %u", thd_in->thread_id());
      wsrep_lock_thd_do_and_awake(thd_in, [](THD *thd) {
          thd->killed = THD::KILL_CONNECTION;
          thd->wsrep_applier_closing = true;
        });
    }
  }
};
/**
  This class implements callback function used by close_connections()
  to wait for committing transactions
*/
class Find_thd_committing: public Find_THD_Impl
{
public:
  Find_thd_committing()
  {}

  virtual bool operator()(THD *thd) override
  {
    if (WSREP(thd) && wsrep_thd_is_local(thd) &&
        thd->wsrep_trx().state() == wsrep::transaction::s_committing)
    {
      return true;
    }
    return false;
  }
};

enum wsrep_thd_type   {APPLIER, ROLLBACKER, COMMITTING};

class Find_wsrep_thd: public Find_THD_Impl
{
public:
  Find_wsrep_thd(enum wsrep_thd_type type): m_type(type) {}
  virtual bool operator()(THD *thd) override
  {
    if (WSREP(thd))
    { 
      switch (m_type) 
        {
        case APPLIER:    return thd->wsrep_applier && !thd->wsrep_rollbacker;
        case ROLLBACKER: return thd->wsrep_applier;
        case COMMITTING: return thd->wsrep_trx().state() == wsrep::transaction::s_committing;
        }
    }
    return false;
  }
private:
  enum wsrep_thd_type m_type;
};

#ifdef NOT_USED
/**/
static bool abort_replicated(THD *thd)
{
  bool ret_code= false;
  if (thd->wsrep_trx().state() == wsrep::transaction::s_committing)
  {
    WSREP_DEBUG("aborting replicated trx: %llu", (ulonglong)(thd->real_id));

    (void)wsrep_abort_thd(thd, thd, true);
    ret_code= true;
  }
  return ret_code;
}
#endif
/**/
static inline bool is_client_connection(THD *thd)
{
  return (thd->wsrep_client_thread && thd->variables.wsrep_on);
}

static inline bool is_replaying_connection(THD *thd)
{
  bool ret;

  mysql_mutex_lock(&thd->LOCK_thd_data);
  ret=  (thd->wsrep_trx().state() == wsrep::transaction::s_replaying) ? true : false;
  mysql_mutex_unlock(&thd->LOCK_thd_data);

  return ret;
}

static inline bool is_committing_connection(THD *thd)
{
  bool ret;

  mysql_mutex_lock(&thd->LOCK_thd_data);
  ret=  (thd->wsrep_trx().state() == wsrep::transaction::s_committing) ? true : false;
  mysql_mutex_unlock(&thd->LOCK_thd_data);

  return ret;
}

#ifdef NOT_USED
static bool have_client_connections(THD *thd, void* arg MY_ATTRIBUTE(( unused )))
{
  //THD *except_thd = arg
  DBUG_PRINT("quit",("Informing thread %u that it's time to die",
                     thd->thread_id()));
  if (is_client_connection(thd) && thd->killed == THD::KILL_CONNECTION)
  {
    (void)abort_replicated(thd);
    return true;
  }
  return false;
}
#endif
void wsrep_close_client_connections(bool is_server_shutdown)
{
  /*
    First signal all threads that it's time to die
  */

  /* Kill blocked pthreads */
  Per_thread_connection_handler::kill_blocked_pthreads();

  Global_THD_manager *thd_manager= Global_THD_manager::get_instance();
  /*
    First signal all threads that it's time to die
    This will give the threads some time to gracefully abort their
    statements and inform their clients that the server is about to die.
  */

  sql_print_information("Giving %d client threads a chance to die gracefully",
                        static_cast<int>(thd_manager->get_thd_count()));

  Call_wsrep_close_client_conn call_wsrep_close_client_conn(is_server_shutdown);
  thd_manager->do_for_all_thd(&call_wsrep_close_client_conn);

  if (thd_manager->get_thd_count() > 0)
    sleep(2);         // Give threads time to die

  /* All client connection threads have now been aborted */
}

/*
   returns the number of wsrep appliers running.
   However, the caller (thd parameter) is not taken in account
 */
static bool have_wsrep_appliers(THD *thd MY_ATTRIBUTE(( unused )))
{
  Find_wsrep_thd find_wsrep_thd(APPLIER);

  auto tmp= Global_THD_manager::get_instance()->find_thd(&find_wsrep_thd);
  if (tmp) return true;
  return false;
}

static bool have_wsrep_rollbackers(THD *thd MY_ATTRIBUTE(( unused )))
{
  Find_wsrep_thd find_wsrep_thd(ROLLBACKER);

  auto tmp= Global_THD_manager::get_instance()->find_thd(&find_wsrep_thd);
  if (tmp) return true;
  return false;
}

static void wsrep_close_thread(THD *thd_in)
{
  wsrep_lock_thd_do_and_awake(thd_in,
                              [](THD *thd) {
                                thd->killed = THD::KILL_CONNECTION;
                              });
}

static bool have_committing_connections()
{
  //Find_thd_committing find_thd_committing();
  Find_wsrep_thd find_thd_committing(COMMITTING);

  auto tmp= Global_THD_manager::get_instance()->find_thd(&find_thd_committing);
  if (tmp) return true;
  return false;
}

int wsrep_wait_committing_connections_close(int wait_time)
{
  int sleep_time= 100;

  while (have_committing_connections() && wait_time > 0)
  {
    WSREP_DEBUG("wait for committing transaction to close: %d", wait_time);
    my_sleep(sleep_time);
    wait_time -= sleep_time;
  }
  if (have_committing_connections())
  {
    return 1;
  }
  return 0;
}

void wsrep_close_applier(THD *thd)
{
  WSREP_DEBUG("closing applier %u", thd->thread_id());
  wsrep_close_thread(thd);
}

void wsrep_close_threads(THD *thd MY_ATTRIBUTE((unused)))
{
  Global_THD_manager *thd_manager= Global_THD_manager::get_instance();

  Call_wsrep_close_wsrep_threads call_wsrep_close_wsrep_threads;
  thd_manager->do_for_all_thd(&call_wsrep_close_wsrep_threads);
}

void wsrep_close_applier_threads(int count MY_ATTRIBUTE((unused)))
{
  Global_THD_manager *thd_manager= Global_THD_manager::get_instance();

  Call_wsrep_close_wsrep_threads call_wsrep_close_wsrep_threads;
  thd_manager->do_for_all_thd(&call_wsrep_close_wsrep_threads);
}

void wsrep_wait_appliers_close(THD *thd)
{
  /* Wait for wsrep appliers to gracefully exit */
  while (have_wsrep_appliers(thd))
  // Rollbacker and post rollbacker threads need to be killed explicitly.
  // This gotta be fixed in a more elegant manner if we gonna have arbitrary
  // number of non-applier wsrep threads.
  {
  }
  /* Now kill remaining wsrep threads: rollbacker */
  wsrep_close_threads (thd);
  /* and wait for them to die */

  int round=0;
  while (have_wsrep_rollbackers(thd) && round < 5)
  {
    WSREP_INFO("active appliers remaining");
    wsrep_close_threads (thd);
    sleep(1);
    round++;
  }
  /* All wsrep applier threads have now been aborted. However, if this thread
     is also applier, we are still running...
  */
}

void wsrep_kill_mysql(THD *thd MY_ATTRIBUTE((unused)))
{
  if (mysqld_server_started)
  {
    WSREP_INFO("starting shutdown");
    kill_mysql();
  }
  else
  {
    unireg_abort(1);
  }
}

int wsrep_must_ignore_error(THD* thd)
{
  const int error= thd->get_stmt_da()->mysql_errno();
  const uint flags= sql_command_flags[thd->lex->sql_command];

  assert(error);
  assert(wsrep_thd_is_toi(thd));

  if ((wsrep_ignore_apply_errors & WSREP_IGNORE_ERRORS_ON_DDL))
    goto ignore_error;

  if ((flags & CF_WSREP_MAY_IGNORE_ERRORS) &&
      (wsrep_ignore_apply_errors & WSREP_IGNORE_ERRORS_ON_RECONCILING_DDL))
  {
    switch (error)
    {
    case ER_DB_DROP_EXISTS:
    case ER_BAD_TABLE_ERROR:
    case ER_CANT_DROP_FIELD_OR_KEY:
      goto ignore_error;
    }
  }

  return 0;

ignore_error:
  WSREP_WARN("Ignoring error '%s' on query. "
             "Default database: '%s'. Query: '%s', Error_code: %d",
             thd->get_stmt_da()->message_text(),
             print_slave_db_safe(thd->db().str),
             WSREP_QUERY(thd),
             error);
  return 1;
}

int wsrep_ignored_error_code(Log_event* ev, int error, TABLE* table)
{
  const THD* thd= ev->thd;

  assert(error);
  assert(wsrep_thd_is_applying(thd) &&
              !wsrep_thd_is_local_toi(thd));

  if ((wsrep_ignore_apply_errors & WSREP_IGNORE_ERRORS_ON_RECONCILING_DML))
  {
    const int ev_type= ev->get_type_code();
    if ((ev_type == binary_log::DELETE_ROWS_EVENT || ev_type == binary_log::DELETE_ROWS_EVENT_V1)
        && error == ER_KEY_NOT_FOUND)
      goto ignore_error;
  }

  /*
   * Work around mysql Bug#80821. With cascading deletes, when both parent and
   * child tables are involved in a multi table query, a delete rows event for
   * the child may be added to the binlog after the parent (cascading) delete.
   * This will cause the delete to fail on slaves. A new wsrep_mode value has
   * been added for ignoring such delete errors.
   */
  if (wsrep_check_mode(WSREP_MODE_IGNORE_CASCADING_FK_DELETE_MISSING_ROW_ERROR) &&
      error == ER_KEY_NOT_FOUND &&
      (ev->get_type_code() == binary_log::DELETE_ROWS_EVENT ||
       ev->get_type_code() == binary_log::DELETE_ROWS_EVENT_V1)) {
    assert(table);
    dd::cache::Dictionary_client::Auto_releaser releaser(thd->dd_client());
    const dd::Table *dd_table = nullptr;
    if (!thd->dd_client()->acquire(table->s->db.str, table->s->table_name.str, &dd_table)) {
      for (const dd::Foreign_key *fk : dd_table->foreign_keys()) {
        if (fk->delete_rule() == dd::Foreign_key::RULE_CASCADE) {
          goto ignore_error;
        }
      }
    }
  }

  return 0;

ignore_error:
  WSREP_WARN("Ignoring error '%s' on %s event. Error_code: %d",
             thd->get_stmt_da()->message_text(),
             ev->get_type_str(),
             error);
  return 1;
}

bool wsrep_provider_is_SR_capable()
{
  return Wsrep_server_state::has_capability(wsrep::provider::capability::streaming);
}

int wsrep_thd_retry_counter(const THD *thd)
{
  return thd->wsrep_retry_counter;
}

extern  bool wsrep_thd_ignore_table(THD *thd)
{
  return thd->wsrep_ignore_table;
}

bool wsrep_create_like_table(THD* thd, TABLE_LIST* table,
                             TABLE_LIST* src_table,
                             HA_CREATE_INFO *create_info)
{
  if (create_info->options & HA_LEX_CREATE_TMP_TABLE)
  {
    /* CREATE TEMPORARY TABLE LIKE must be skipped from replication */
    WSREP_DEBUG("CREATE TEMPORARY TABLE LIKE... skipped replication\n %s", 
                WSREP_QUERY(thd));
  }
  else if (!(find_temporary_table(thd, src_table)))
  {
    /* this is straight CREATE TABLE LIKE... with no tmp tables */
    WSREP_TO_ISOLATION_BEGIN(table->db, table->table_name, NULL);
  }
  else
  {
    /* here we have CREATE TABLE LIKE <temporary table> 
       the temporary table definition will be needed in slaves to
       enable the create to succeed
     */
    TABLE_LIST tbl;
    memset((void*) &tbl, 0, sizeof(tbl));
    tbl.db= src_table->db;
    tbl.table_name= tbl.alias= src_table->table_name;
    tbl.table= src_table->table;
    char buf[2048];
    String query(buf, sizeof(buf), system_charset_info);
    query.length(0);  // Have to zero it since constructor doesn't

#ifdef NOT_IMPLEMENTED
    (void)  show_create_table(thd, &tbl, &query, NULL, WITH_DB_NAME);
#endif
    WSREP_DEBUG("TMP TABLE: %s", query.ptr());

    thd->wsrep_TOI_pre_query=     query.ptr();
    thd->wsrep_TOI_pre_query_len= query.length();

    WSREP_TO_ISOLATION_BEGIN(table->db, table->table_name, NULL);

    thd->wsrep_TOI_pre_query=      NULL;
    thd->wsrep_TOI_pre_query_len= 0;
  }

  return(false);
#ifdef WITH_WSREP
wsrep_error_label:
  thd->wsrep_TOI_pre_query= NULL;
  return (true);
#endif
}

int wsrep_create_trigger_query(THD *thd, uchar** buf, size_t* buf_len)
{
  LEX *lex= thd->lex;
  String stmt_query;

  if (!lex->definer)
  {
    if (!thd->slave_thread)
    {
      if (!(lex->definer= create_default_definer(thd)))
        return 1;
    }
  }

  LEX_CSTRING definer_user= lex->definer->user;
  LEX_CSTRING definer_host= lex->definer->host;

  if (!lex->definer)
  {
    /* non-SUID trigger. */

    definer_user.str= 0;
    definer_user.length= 0;

    definer_host.str= 0;
    definer_host.length= 0;
  }

  stmt_query.append(STRING_WITH_LEN("CREATE "));

  append_definer(thd, &stmt_query, definer_user, definer_host);

  LEX_STRING stmt_definition;
  stmt_definition.str=  const_cast<char*>(thd->lex->stmt_definition_begin);
  stmt_definition.length= thd->lex->stmt_definition_end
    - thd->lex->stmt_definition_begin;
  trim_whitespace(thd->charset(), & stmt_definition);

  stmt_query.append(stmt_definition.str, stmt_definition.length);

  return wsrep_to_buf_helper(thd, stmt_query.c_ptr(), stmt_query.length(), 
                             buf, buf_len);
}

int init_wsrep_thread(THD* thd)
{
  DBUG_ENTER("init_wsrep_thread");

  thd->system_thread = SYSTEM_THREAD_SLAVE_WORKER;

  thd->security_context()->skip_grants();
  thd->get_protocol_classic()->init_net(0);
  thd->slave_thread = 1;
  thd->enable_slow_log= opt_log_slow_replica_statements;
  set_slave_thread_options(thd);
  thd->get_protocol_classic()->set_client_capabilities(
      CLIENT_LOCAL_FILES);

  thd->thd_tx_priority = 1;
  thd->tx_priority = 1;
  /*
    Replication threads are:
    - background threads in the server, not user sessions,
    - yet still assigned a PROCESSLIST_ID,
      for historical reasons (displayed in SHOW PROCESSLIST).
  */
  thd->set_new_thread_id();

#ifdef HAVE_PSI_INTERFACE
  /*
    Populate the PROCESSLIST_ID in the instrumentation.
  */
  struct PSI_thread *psi= PSI_THREAD_CALL(get_thread)();
  PSI_THREAD_CALL(set_thread_id)(psi, thd->thread_id());
#endif /* HAVE_PSI_INTERFACE */

  DBUG_EXECUTE_IF("simulate_wsrep_slave_error_on_init",
                  DBUG_RETURN(-1););

  thd->set_time();
  /* Do not use user-supplied timeout value for system threads. */
  thd->variables.lock_wait_timeout= LONG_TIMEOUT;
  DBUG_RETURN(0);
}

void deinit_wsrep_thread(THD* thd)
{
  thd->get_protocol_classic()->end_net();
  thd->release_resources();
}

void* start_wsrep_THD(void *arg)
{
  Global_THD_manager *thd_manager= Global_THD_manager::get_instance();
  THD *thd;
  bool thd_added= false;

  Wsrep_thd_args* thd_args= (Wsrep_thd_args*) arg;

  if (my_thread_init())
  {
    WSREP_ERROR("Could not initialize thread");
    return(NULL);
  }

  thd = new THD(false, true); // note that contructor of THD uses DBUG_ !
                              // (not enablin plugins, is applier)
  thd->thread_stack = (char*)&thd; // remember where our stack is

  wsrep_assign_from_threadvars(thd);
  if (wsrep_store_threadvars(thd))
  {
    goto error;
  }

  if (init_wsrep_thread(thd))
  {
    WSREP_WARN("Failed during wsrep thread initialization");
    goto error;
  }
  thd->real_id=pthread_self(); // Keep purify happy

  DBUG_PRINT("wsrep",(("creating thread %u"), thd->thread_id()));

  /* from bootstrap()... */
  thd->get_protocol_classic()->set_max_packet_size(
    replica_max_allowed_packet + MAX_LOG_EVENT_HEADER);

  /* handle_one_connection() again... */
  thd->set_proc_info(nullptr);
  thd->set_command(COM_SLEEP);
  thd->init_query_mem_roots();

  /* wsrep_running_threads counter is managed in thd_manager */
  thd_manager->add_thd(thd);
  thd_added= true;

#ifdef SKIP_INNODB_HP
  /* set priority */
  thd->thd_tx_priority = 1;
#endif
 

  WSREP_DEBUG("wsrep system thread %u, %p starting",
              thd->thread_id(), thd);
  thd_args->fun()(thd, thd_args->args());

 error:
  
  WSREP_DEBUG("wsrep system thread: %u, %p closing",
              thd->thread_id(), thd);

  /* Wsrep may reset globals during thread context switches, store globals
     before cleanup. */
  wsrep_store_threadvars(thd);

  delete thd_args;

  mysql_mutex_lock(&LOCK_wsrep_slave_threads);
  WSREP_DEBUG("wsrep running threads now: %lu", wsrep_running_threads.load());
  mysql_cond_broadcast(&COND_wsrep_slave_threads);
  mysql_mutex_unlock(&LOCK_wsrep_slave_threads);


  thd->clear_error();
  thd->set_catalog(NULL_CSTR);
  thd->reset_query();
  thd->reset_db(NULL_CSTR);
  close_connection(thd, 0, true, false);

  thd->temporary_tables = 0; // remove tempation from destructor to close them

  // Note: We can't call THD destructor without crashing
  // if plugins have not been initialized. However, in most of the
  // cases this means that pre SE initialization SST failed and
  // we are going to exit anyway.
  if (dynamic_plugins_are_initialized)
  {
    deinit_wsrep_thread(thd);
    THD_CHECK_SENTRY(thd);
    if (thd_added)
      thd_manager->remove_thd(thd);
  }
  else
  {
    // TODO: lightweight cleanup to get rid of:
    // 'Error in my_thread_global_end(): 2 threads didn't exit'
    // at server shutdown

    if (thd_added)
    {
      deinit_wsrep_thread(thd);
      thd_manager->remove_thd(thd);
    }
  }

  /*
    The thd can only be destructed after indirect references
    through mi->rli->info_thd are cleared: mi->rli->info_thd= NULL.

    For instance, user thread might be issuing show_slave_status
    and attempting to read mi->rli->info_thd->get_proc_info().
    Therefore thd must only be deleted after info_thd is set
    to NULL.
  */
  delete thd;

  my_thread_end();
#if OPENSSL_VERSION_NUMBER < 0x10100000L
  ERR_remove_thread_state(0);
#endif /* OPENSSL_VERSION_NUMBER < 0x10100000L */
  my_thread_exit(0);

  /* Abort if its the first applier/rollbacker thread. */
  if (!mysqld_server_initialized)
    unireg_abort(1);

  return NULL;
}

enum wsrep::streaming_context::fragment_unit wsrep_fragment_unit(ulong unit)
{
  switch (unit)
  {
  case WSREP_FRAG_BYTES: return wsrep::streaming_context::bytes;
  case WSREP_FRAG_ROWS: return wsrep::streaming_context::row;
  case WSREP_FRAG_STATEMENTS: return wsrep::streaming_context::statement;
  default:
    assert(0);
    return wsrep::streaming_context::bytes;
  }
}

/***** callbacks for wsrep service ************/

bool get_wsrep_recovery()
{
  return wsrep_recovery;
}

bool wsrep_consistency_check(THD *thd)
{
  return thd->wsrep_consistency_check == CONSISTENCY_CHECK_RUNNING;
}
