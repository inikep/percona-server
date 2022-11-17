/* Copyright 2008-2017 Codership Oy <http://www.codership.com>

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

#include "mysql_version.h"
#include <wsrep.h>
#include "mysqld.h"
#include "wsrep_sst.h"
#include "sql_class.h"
#include "sql_lex.h"
#include "set_var.h"
#include "sql_reload.h"
#include "sql_parse.h"
#include "sql_plugin.h"
#include "wsrep_server_state.h"
#include "wsrep_status.h"
#include "wsrep_priv.h"
#include "wsrep_utils.h"
#include "wsrep_xid.h"
#include "wsrep_thd.h"

#include <cstdio>
#include <cstdlib>
#include <cctype>
#include "debug_sync.h"
#include "my_default.h"
#include "rpl_replica.h"
#include "debug_sync.h"

static char wsrep_defaults_file[FN_REFLEN * 2 + 10 + 30 +
                                sizeof(WSREP_SST_OPT_CONF) +
                                sizeof(WSREP_SST_OPT_CONF_SUFFIX) +
                                sizeof(WSREP_SST_OPT_CONF_EXTRA)]= {0};

const char* wsrep_sst_method          = WSREP_SST_DEFAULT;
const char* wsrep_sst_receive_address = WSREP_SST_ADDRESS_AUTO;
const char* wsrep_sst_donor           = "";
const char* wsrep_sst_auth            = NULL;

// container for real auth string
static const char* sst_auth_real     = NULL;
bool wsrep_sst_donor_rejects_queries= false;

static const char* defaults_file   = NULL;
static const char* defaults_extra  = NULL;
static const char* defaults_suffix = NULL;

void wsrep_sst_set_defaults(int argc, char* argv[])
{
  // See: ../mysys/my_default.cc:get_defaults_options()
  bool found_no_defaults = false;
  int  default_option_count = 0;
  int  prev_argc = 0;

  while (argc >= 2 && argc != prev_argc) {
    /* Skip program name or previously handled argument */
    argv++;
    prev_argc = argc; /* To check if we found any we're looking for options */
    /* --no-defaults is always the first option. */
    if (is_prefix(*argv, "--no-defaults") && !default_option_count) {
      argc--;
      found_no_defaults = true;
      default_option_count++;
      continue;
    }
    if (!defaults_file && is_prefix(*argv, "--defaults-file=") &&
        !found_no_defaults) {
      defaults_file = *argv + sizeof("--defaults-file=") - 1;
      argc--;
      default_option_count++;
      continue;
    }
    if (!defaults_extra && is_prefix(*argv, "--defaults-extra-file=") &&
        !found_no_defaults) {
      defaults_extra = *argv + sizeof("--defaults-extra-file=") - 1;
      argc--;
      default_option_count++;
      continue;
    }
    if (!defaults_suffix && is_prefix(*argv, "--defaults-group-suffix=")) {
      defaults_suffix = *argv + sizeof("--defaults-group-suffix=") -1;
      argc--;
      default_option_count++;
      continue;
    }
  }
}

bool wsrep_sst_method_check (sys_var *self MY_ATTRIBUTE((unused)),
                             THD* thd MY_ATTRIBUTE((unused)), set_var* var)
{
  if ((! var->save_result.string_value.str) ||
      (var->save_result.string_value.length == 0 ))
  {
    my_error(ER_WRONG_VALUE_FOR_VAR, MYF(0), var->var->name.str,
             var->save_result.string_value.str ?
             var->save_result.string_value.str : "NULL");
    return 1;
  }

  return 0;
}

static const char* data_home_dir;

void wsrep_set_data_home_dir(const char *data_dir)
{
  data_home_dir= (data_dir && *data_dir) ? data_dir : NULL;
}

static void make_wsrep_defaults_file()
{
  if (!wsrep_defaults_file[0])
  {
    char *ptr= wsrep_defaults_file;
    char *end= ptr + sizeof(wsrep_defaults_file);
    if (defaults_file)
      ptr= strxnmov(ptr, end - ptr,
                    WSREP_SST_OPT_CONF, " '", defaults_file, "' ", NULL);

    if (defaults_extra)
      ptr= strxnmov(ptr, end - ptr,
                    WSREP_SST_OPT_CONF_EXTRA, " '", defaults_extra, "' ",NULL);

    if (defaults_suffix)
      ptr= strxnmov(ptr, end - ptr,
                    WSREP_SST_OPT_CONF_SUFFIX, " '", defaults_suffix,"' ",NULL);
  }
}


bool wsrep_sst_receive_address_check (sys_var *self MY_ATTRIBUTE((unused)),
                                      THD* thd MY_ATTRIBUTE((unused)),
                                      set_var* var)
{
  if ((! var->save_result.string_value.str) ||
      (var->save_result.string_value.length > (FN_REFLEN - 1))) // safety
  {
    goto err;
  }

  return 0;

err:
  my_error(ER_WRONG_VALUE_FOR_VAR, MYF(0), var->var->name.str,
           var->save_result.string_value.str ?
           var->save_result.string_value.str : "NULL");
  return 1;
}

bool wsrep_sst_receive_address_update (sys_var *self MY_ATTRIBUTE((unused)),
                                       THD* thd MY_ATTRIBUTE((unused)),
                                       enum_var_type type MY_ATTRIBUTE((unused)))
{
    return 0;
}

bool wsrep_sst_auth_check (sys_var *self MY_ATTRIBUTE((unused)),
                           THD* thd MY_ATTRIBUTE((unused)),
                           set_var* var MY_ATTRIBUTE((unused)))
{
    return 0;
}

static bool sst_auth_real_set (const char* value)
{
  const char* v= NULL;

  if (value)
  {
    v= my_strdup(key_memory_wsrep, value, MYF(0));
  }
  else                                          // its NULL
  {
    wsrep_sst_auth_free();
    return 0;
  }

  if (v)
  {
    // set sst_auth_real
    if (sst_auth_real) { my_free(const_cast<char *>(sst_auth_real)); }
    sst_auth_real= v;

    // mask wsrep_sst_auth
    if (strlen(sst_auth_real))
    {
      if (wsrep_sst_auth)
      {
        my_free (const_cast<char*>(wsrep_sst_auth));
        wsrep_sst_auth = my_strdup(key_memory_wsrep,
                                   WSREP_SST_AUTH_MASK, MYF(0));
      }
      else
      {
        wsrep_sst_auth = my_strdup (key_memory_wsrep,
                                    WSREP_SST_AUTH_MASK, MYF(0));
      }
    }
    return 0;
  }
  return 1;
}

void wsrep_sst_auth_free()
{
  if (wsrep_sst_auth) { my_free(const_cast<char *>(wsrep_sst_auth)); }
  if (sst_auth_real) { my_free(const_cast<char *>(sst_auth_real)); }
  wsrep_sst_auth= NULL;
  sst_auth_real= NULL;
}

bool wsrep_sst_auth_update (sys_var *self MY_ATTRIBUTE((unused)),
                            THD* thd MY_ATTRIBUTE((unused)),
                            enum_var_type type MY_ATTRIBUTE((unused)))
{
  return sst_auth_real_set (wsrep_sst_auth);
}

void wsrep_sst_auth_init ()
{
  sst_auth_real_set(wsrep_sst_auth);
}

bool  wsrep_sst_donor_check (sys_var *self MY_ATTRIBUTE((unused)),
                             THD* thd MY_ATTRIBUTE((unused))
                             , set_var* var MY_ATTRIBUTE((unused)))
{
  return 0;
}

bool wsrep_sst_donor_update (sys_var *self MY_ATTRIBUTE((unused)),
                             THD* thd MY_ATTRIBUTE((unused)),
                             enum_var_type type MY_ATTRIBUTE((unused)))
{
  return 0;
}

bool wsrep_before_SE()
{
  return (wsrep_provider != NULL
          && strcmp (wsrep_provider,   WSREP_NONE)
          && strcmp (wsrep_sst_method, WSREP_SST_SKIP)
          && strcmp (wsrep_sst_method, WSREP_SST_MYSQLDUMP));
}

// Signal end of SST
static void wsrep_sst_complete (THD*                thd,
                                int const           rcode)
{
  Wsrep_client_service client_service(thd, thd->wsrep_cs());
  Wsrep_server_state::instance().sst_received(client_service, rcode);
}

  /*
  If wsrep provider is loaded, inform that the new state snapshot
  has been received. Also update the local checkpoint.

  @param thd       [IN]
  @param uuid      [IN]               Initial state UUID
  @param seqno     [IN]               Initial state sequence number
  @param state     [IN]               Always NULL, also ignored by wsrep provider (?)
  @param state_len [IN]               Always 0, also ignored by wsrep provider (?)
*/
void wsrep_sst_received (THD*                thd,
                         const wsrep_uuid_t& uuid,
                         wsrep_seqno_t const seqno,
                         const void* const   state MY_ATTRIBUTE((unused)),
                         size_t const        state_len MY_ATTRIBUTE((unused)))
{
  /*
    To keep track of whether the local uuid:seqno should be updated. Also, note
    that local state (uuid:seqno) is updated/checkpointed only after we get an
    OK from wsrep provider. By doing so, the values remain consistent across
    the server & wsrep provider.
  */
    /*
      TODO: Handle backwards compatibility. WSREP API v25 does not have
      wsrep schema.
    */
    /*
      Logical SST methods (mysqldump etc) don't update InnoDB sys header.
      Reset the SE checkpoint before recovering view in order to avoid
      sanity check failure.
     */
    wsrep::gtid const sst_gtid(wsrep::id(uuid.data, sizeof(uuid.data)),
                               wsrep::seqno(seqno));

    if (!wsrep_before_SE()) {
      wsrep_set_SE_checkpoint(wsrep::gtid::undefined(),
                              wsrep_local_gtid_manager.undefined(), true);
      wsrep_set_SE_checkpoint(sst_gtid, wsrep_local_gtid_manager.gtid(),
                              true);
    }
    wsrep_verify_SE_checkpoint(uuid, seqno);

    /*
      Both wsrep_init_SR() and wsrep_recover_view() may use
      wsrep thread pool. Restore original thd context before returning.
    */
    if (thd) {
      wsrep_store_threadvars(thd);
    }

    if (WSREP_ON)
    {
      int const rcode(seqno < 0 ? seqno : 0);
      wsrep_sst_complete(thd,rcode);
    }
}

struct sst_thread_arg
{
  const char*     cmd;
  char**          env;
  char*           ret_str;
  int             err;
  mysql_mutex_t   lock;
  mysql_cond_t    cond;

  sst_thread_arg (const char* c, char** e)
    : cmd(c), env(e), ret_str(0), err(-1)
  {
    mysql_mutex_init(key_LOCK_wsrep_sst_thread, &lock, MY_MUTEX_INIT_FAST);
    mysql_cond_init(key_COND_wsrep_sst_thread, &cond);
  }

  ~sst_thread_arg()
  {
    mysql_cond_destroy  (&cond);
    mysql_mutex_unlock  (&lock);
    mysql_mutex_destroy (&lock);
  }
};

static int sst_scan_uuid_seqno(const char* str, uint32* offset,
                               wsrep_uuid_t* uuid, wsrep_seqno_t* seqno)
{
  const char* space_position = strchr(str, ' ');
  *offset = space_position ? space_position - str : 0;
  int offt = wsrep_uuid_scan(str, *offset ? *offset : strlen(str), uuid);
  if ((offt > 0) &&
      ((*offset ? *offset : strlen(str)) > (unsigned int)offt) &&
      (':' == str[offt])) {
    *seqno = strtoll (str + offt + 1, NULL, 10);
    if (*seqno != LLONG_MAX)
      return 0;
  }
  WSREP_ERROR("Failed to parse uuid:seqno pair: '%s'", str);
  return -EINVAL;
}

// get rid of trailing \n
static char* my_fgets (char* buf, size_t buf_len, FILE* stream)
{
   char* ret= fgets (buf, buf_len, stream);
   if (ret) {
    size_t len= strlen(ret);
    if (len > 0 && ret[len - 1] == '\n') ret[len - 1]= '\0';
   }
   return ret;
}

/*
  Generate opt_binlog_opt_val for sst_donate_other(), sst_prepare_other().

  Returns zero on success, negative error code otherwise.

  String containing binlog name is stored in param ret if binlog is enabled
  and GTID mode is on, otherwise empty string. Returned string should be
  freed with my_free().
 */
static int generate_binlog_opt_val(char** ret)
{
  assert(ret);
  *ret= NULL;
  if (opt_bin_log && global_gtid_mode.get() == Gtid_mode::ON) {
    assert(opt_bin_logname);
    *ret = my_strdup(key_memory_wsrep, opt_bin_logname, MYF(0));
  } else {
    *ret = my_strdup(key_memory_wsrep, "", MYF(0));
  }
  if (!*ret) return -ENOMEM;
  return 0;
}

static int generate_binlog_index_opt_val(char** ret)
{
  assert(ret);
  *ret = NULL;
  if (opt_binlog_index_name) {
    *ret = strcmp(opt_binlog_index_name, "0") ?
      my_strdup(key_memory_wsrep, opt_binlog_index_name, MYF(0)) :
      my_strdup(key_memory_wsrep, "", MYF(0));
  } else {
    *ret = my_strdup(key_memory_wsrep, "", MYF(0));
  }
  if (!*ret) return -ENOMEM;
  return 0;
}

static int generate_local_uuid(char** ret)
{
  assert(ret);
  *ret = NULL;
  char uuid[WSREP_UUID_STR_LEN + 1] = {0, };
  wsrep_uuid_print((const wsrep_uuid_t*)&wsrep_local_gtid_manager.uuid,
                   uuid, sizeof(uuid));
  *ret = my_strdup(key_memory_wsrep, uuid, MYF(0));
  if (!*ret) 
    return -ENOMEM;
  return 0;
}

// report progress event
static void sst_report_progress(int const       from,
                                long long const total_prev,
                                long long const total,
                                long long const complete)
{
  static char buf[128] = { '\0', };
  static size_t const buf_len= sizeof(buf) - 1;
  snprintf(buf, buf_len,
           "{ \"from\": %d, \"to\": %d, \"total\": %lld, \"done\": %lld, "
           "\"indefinite\": -1 }",
           from, WSREP_MEMBER_JOINED, total_prev + total, total_prev +complete);
  Wsrep_status::report_progress(buf);
  WSREP_DEBUG("REPORTING SST PROGRESS: '%s'", buf);
}

// process "complete" event from SST script feedback
static void sst_handle_complete(const char* const input,
                                long long const   total_prev,
                                long long*        total,
                                long long*        complete,
                                int const         from)
{
  long long x;
  int n= sscanf(input, " %lld", &x);
  if (n > 0 && x > *complete)
  {
    *complete= x;
    if (*complete > *total) *total= *complete;
    sst_report_progress(from, total_prev, *total, *complete);
  }
}

// process "total" event from SST script feedback
static void sst_handle_total(const char* const input,
                             long long*        total_prev,
                             long long*        total,
                             long long*        complete,
                             int const         from)
{
  long long x;
  int n= sscanf(input, " %lld", &x);
  if (n > 0)
  {
    // new stage starts, update total_prev
    *total_prev+= *total;
    *total= x;
    *complete= 0;
    sst_report_progress(from, *total_prev, *total, *complete);
  }
}

static void* sst_joiner_thread (void* a)
{
  sst_thread_arg* arg = (sst_thread_arg*) a;
  int err = 1;
  {
    THD* thd;
    static const char magic[] = "ready";
    static const size_t magic_len = sizeof(magic) - 1;
    const size_t out_len = 512;
    char out[out_len];

    WSREP_INFO("Running: '%s'", arg->cmd);

    wsp::process proc (arg->cmd, "r", arg->env);

    if (proc.pipe() && !proc.error()) {
      const char* tmp = my_fgets (out, out_len, proc.pipe());

      if (!tmp || strlen(tmp) < (magic_len + 2) ||
          strncasecmp (tmp, magic, magic_len)) {
        WSREP_ERROR("Failed to read '%s <addr>' from: %s\n\tRead: '%s'",
                    magic, arg->cmd, tmp);
        proc.wait();
        if (proc.error()) err = proc.error();
      } else {
        err = 0;
      }
    } else {
      err = proc.error();
      WSREP_ERROR("Failed to execute: %s : %d (%s)",
                   arg->cmd, err, strerror(err));
    }

    /*
      signal sst_prepare thread with ret code,
      it will go on sending SST request
    */
    mysql_mutex_lock (&arg->lock);
    if (!err) {
      arg->ret_str = strdup (out + magic_len + 1);
      if (!arg->ret_str) err = ENOMEM;
    }
    arg->err = -err;
    mysql_cond_signal  (&arg->cond);
    mysql_mutex_unlock (&arg->lock); //! @note arg is unusable after that.

    if (err) return NULL; /* lp:808417 - return immediately, don't signal
                           * initializer thread to ensure single thread of
                           * shutdown. */

    // current stage progress
    long long total= 0;
    long long complete= 0;
    // previous stages cumulative progress
    long long total_prev= 0;

    // in case of successful receiver start, wait for SST completion/end
    const char* tmp= NULL;
    err= EINVAL;

  wait_signal:
    tmp= my_fgets (out, out_len, proc.pipe());

    if (tmp)
    {
      static const char magic_total[]= "total";
      static const size_t total_len=strlen(magic_total);
      static const char magic_complete[]= "complete";
      static const size_t complete_len=strlen(magic_complete);
      static const int from= WSREP_MEMBER_JOINER;

      if (!strncasecmp (tmp, magic_complete, complete_len))
      {
        sst_handle_complete(tmp + complete_len, total_prev, &total, &complete,
                            from);
        goto wait_signal;
      }
      else if (!strncasecmp (tmp, magic_total, total_len))
      {
        sst_handle_total(tmp + total_len, &total_prev, &total, &complete, from);
        goto wait_signal;
      }
    }
    else
    {
      WSREP_ERROR("Failed to read uuid:seqno and wsrep_gtid_domain_id from "
                  "joiner script.");
      proc.wait();
      if (proc.error()) err= proc.error();
    }

    // this should be the final script output with GTID
    if (tmp)
    {
      proc.wait();

      uint32 next_delimiter_offset = 0;
      const char *pos = out;
      wsrep_uuid_t  ret_wsrep_uuid = WSREP_UUID_UNDEFINED;
      wsrep_seqno_t ret_wsrep_seqno = WSREP_SEQNO_UNDEFINED;

      // Read state ID (UUID:SEQNO)
      if ((err = sst_scan_uuid_seqno(out, &next_delimiter_offset,
                                    &ret_wsrep_uuid, &ret_wsrep_seqno)))
        goto done;

      if (next_delimiter_offset) {
        // We are expecting local WSREP UUID:SEQNO followed by server id
        wsrep_uuid_t  ret_local_wsrep_uuid = WSREP_UUID_UNDEFINED;
        wsrep_seqno_t ret_local_wsrep_seqno = WSREP_SEQNO_UNDEFINED;

        pos += next_delimiter_offset + 1;
        if ((err = sst_scan_uuid_seqno(pos, &next_delimiter_offset,
                                      &ret_local_wsrep_uuid, &ret_local_wsrep_seqno)))
          goto done;

        pos += next_delimiter_offset + 1;
        uint32 received_server_id = strtoul(pos, NULL, 10);

        wsrep_local_gtid_t sst_received_local_gtid;
        memcpy(sst_received_local_gtid.uuid.data, ret_local_wsrep_uuid.data, 16);
        sst_received_local_gtid.seqno = ret_local_wsrep_seqno;
        sst_received_local_gtid.server_id = received_server_id;
        wsrep_setup_local_gtid(sst_received_local_gtid);
      }
    }

done:
    /*
      Tell initializer thread that SST is complete
      For that initialize THD object
    */
    if (my_thread_init()) {
      WSREP_ERROR("my_thread_init() failed, can't signal end of SST. "
                  "Aborting.");
      unireg_abort(1);
    }

    thd = new THD(false, true);

    if (!thd) {
      WSREP_ERROR("Failed to allocate THD to restore view from local state, "
                  "can't signal end of SST. Aborting.");
      unireg_abort(1);
    }

    thd->thread_stack= (char*) &thd;
    thd->security_context()->skip_grants();
    thd->system_thread = SYSTEM_THREAD_SLAVE_SQL;
    thd->real_id= pthread_self();
    thd->set_new_thread_id();
    wsrep_assign_from_threadvars(thd);
    wsrep_store_threadvars(thd);

    /* */
    thd->variables.wsrep_on = 0;
    /* No binlogging */
    thd->variables.sql_log_bin = 0;
    thd->variables.option_bits &= ~OPTION_BIN_LOG;
    /* No general log */
    thd->variables.option_bits |= OPTION_LOG_OFF;
    /* Read committed isolation to avoid gap locking */
    thd->variables.transaction_isolation = ISO_READ_COMMITTED;

    wsrep_sst_complete (thd, -err);

    delete thd;
    my_thread_end();
  }
  return NULL;
}

#define WSREP_SST_AUTH_ENV        "WSREP_SST_OPT_AUTH"
#define WSREP_SST_REMOTE_AUTH_ENV "WSREP_SST_OPT_REMOTE_AUTH"
#define DATA_HOME_DIR_ENV         "INNODB_DATA_HOME_DIR"

static int sst_append_env_var(wsp::env&   env,
                              const char* const var,
                              const char* const val)
{
  int const env_str_size= strlen(var) + 1 /* = */
                          + (val ? strlen(val) : 0) + 1 /* \0 */;

  wsp::string env_str(env_str_size); // for automatic cleanup on return
  if (!env_str()) return -ENOMEM;

  int ret= snprintf(env_str(), env_str_size, "%s=%s", var, val ? val : "");

  if (ret < 0 || ret >= env_str_size)
  {
    WSREP_ERROR("sst_append_env_var(): snprintf(%s=%s) failed: %d",
                var, val, ret);
    return (ret < 0 ? ret : -EMSGSIZE);
  }

  env.append(env_str());
  return -env.error();
}

static ssize_t sst_prepare_other (const char*  method,
                                  const char*  sst_auth,
                                  const char*  addr_in,
                                  const char** addr_out)
{
  int const cmd_len= 4096;
  wsp::string cmd_str(cmd_len);

  if (!cmd_str())
  {
    WSREP_ERROR("sst_prepare_other(): could not allocate cmd buffer of %d bytes",
                cmd_len);
    return -ENOMEM;
  }

  const char* binlog_opt= "";
  const char* binlog_index_opt= "";
  char* binlog_opt_val= NULL;
  char* binlog_index_opt_val= NULL;

  int ret;
  if ((ret= generate_binlog_opt_val(&binlog_opt_val)))
  {
    WSREP_ERROR("sst_prepare_other(): generate_binlog_opt_val() failed: %d",
                ret);
    return ret;
  }

  if ((ret= generate_binlog_index_opt_val(&binlog_index_opt_val)))
  {
    WSREP_ERROR("sst_prepare_other(): generate_binlog_index_opt_val() failed %d",
                ret);
  }

  if (strlen(binlog_opt_val)) binlog_opt= WSREP_SST_OPT_BINLOG;
  if (strlen(binlog_index_opt_val)) binlog_index_opt= WSREP_SST_OPT_BINLOG_INDEX;

  make_wsrep_defaults_file();

  ret= snprintf (cmd_str(), cmd_len,
                 "wsrep_sst_%s "
                 WSREP_SST_OPT_ROLE " 'joiner' "
                 WSREP_SST_OPT_ADDR " '%s' "
                 WSREP_SST_OPT_DATA " '%s' "
                 " %s "
                 WSREP_SST_OPT_PARENT " '%d' "
                 WSREP_SST_OPT_VERSION " '%s' "
                 WSREP_SST_OPT_PLUGIN_DIR " '%s' "
                 " %s '%s'"
                 " %s '%s'",
                 method, addr_in, mysql_real_data_home,
                 wsrep_defaults_file,
                 (int)getpid(),
                 MYSQL_SERVER_VERSION MYSQL_SERVER_SUFFIX_DEF,
                 opt_plugin_dir ? opt_plugin_dir : "",
                 binlog_opt, binlog_opt_val,
                 binlog_index_opt, binlog_index_opt_val);
  my_free(binlog_opt_val);
  my_free(binlog_index_opt_val);

  if (ret < 0 || ret >= cmd_len)
  {
    WSREP_ERROR("sst_prepare_other(): snprintf() failed: %d", ret);
    return (ret < 0 ? ret : -EMSGSIZE);
  }

  wsp::env env(NULL);
  if (env.error())
  {
    WSREP_ERROR("sst_prepare_other(): env. var ctor failed: %d", -env.error());
    return -env.error();
  }

  if ((ret= sst_append_env_var(env, WSREP_SST_AUTH_ENV, sst_auth)))
  {
    WSREP_ERROR("sst_prepare_other(): appending auth failed: %d", ret);
    return ret;
  }

  if (data_home_dir)
  {
    if ((ret= sst_append_env_var(env, DATA_HOME_DIR_ENV, data_home_dir)))
    {
      WSREP_ERROR("sst_prepare_other(): appending data "
                  "directory failed: %d", ret);
      return ret;
    }
  }

  pthread_t tmp;
  sst_thread_arg arg(cmd_str(), env());
  mysql_mutex_lock (&arg.lock);
  ret= pthread_create (&tmp, NULL, sst_joiner_thread, &arg);
  if (ret)
  {
    WSREP_ERROR("sst_prepare_other(): pthread_create() failed: %d (%s)",
                ret, strerror(ret));
    return -ret;
  }
  mysql_cond_wait (&arg.cond, &arg.lock);

  *addr_out= arg.ret_str;

  if (!arg.err)
    ret= strlen(*addr_out);
  else
  {
    assert (arg.err < 0);
    ret= arg.err;
  }

  pthread_detach (tmp);

  return ret;
}

extern uint  mysqld_port;

/*! Just tells donor where to send mysqldump */
static ssize_t sst_prepare_mysqldump (const char*  addr_in,
                                      const char** addr_out)
{
  ssize_t ret= strlen (addr_in);

  if (!strrchr(addr_in, ':'))
  {
    ssize_t s= ret + 7;
    char* tmp= (char*) malloc (s);

    if (tmp)
    {
      ret= snprintf (tmp, s, "%s:%u", addr_in, mysqld_port);

      if (ret > 0 && ret < s)
      {
        *addr_out= tmp;
        return ret;
      }
      if (ret > 0) /* buffer too short */ ret= -EMSGSIZE;
      free (tmp);
    }
    else {
      ret= -ENOMEM;
    }

    WSREP_ERROR ("Could not prepare state transfer request: "
                 "adding default port failed: %zd.", ret);
  }
  else {
    *addr_out= addr_in;
  }

  return ret;
}

static bool SE_initialized = false;

//std::string wsrep_sst_prepare(void** msg, THD *thd)
std::string wsrep_sst_prepare()
{
  const ssize_t ip_max= 256;
  char ip_buf[ip_max];
  const char* addr_in=  NULL;
  const char* addr_out= NULL;
  const char* method;

  if (!strcmp(wsrep_sst_method, WSREP_SST_SKIP))
  {
    return WSREP_STATE_TRANSFER_TRIVIAL;
  }

  /*
    Figure out SST receive address. Common for all SST methods.
  */
  // Attempt 1: wsrep_sst_receive_address
  if (wsrep_sst_receive_address &&
      strcmp (wsrep_sst_receive_address, WSREP_SST_ADDRESS_AUTO))
  {
    addr_in= wsrep_sst_receive_address;
  }

  //Attempt 2: wsrep_node_address
  else if (wsrep_node_address && strlen(wsrep_node_address))
  {
    wsp::Address addr(wsrep_node_address);

    if (!addr.is_valid())
    {
      WSREP_ERROR("Could not parse wsrep_node_address : %s",
                  wsrep_node_address);
      throw wsrep::runtime_error("Failed to prepare for SST. Unrecoverable");
    }
    memcpy(ip_buf, addr.get_address(), addr.get_address_len());
    addr_in= ip_buf;
  }
  // Attempt 3: Try to get the IP from the list of available interfaces.
  else
  {
    ssize_t ret= wsrep_guess_ip (ip_buf, ip_max);

    if (ret && ret < ip_max)
    {
      addr_in= ip_buf;
    }
    else
    {
      WSREP_ERROR("Failed to guess address to accept state transfer. "
                  "wsrep_sst_receive_address must be set manually.");
      //if (thd) delete thd;
      throw wsrep::runtime_error("Could not prepare state transfer request");
      unireg_abort(1);
    }
  }

  ssize_t addr_len= -ENOSYS;
  method = wsrep_sst_method;
  if (!strcmp(method, WSREP_SST_MYSQLDUMP))
  {
    addr_len= sst_prepare_mysqldump (addr_in, &addr_out);
    if (addr_len < 0)
    {
      throw wsrep::runtime_error("Could not prepare mysqldimp address");
    }
  }
  else
  {
    /*! A heuristic workaround until we learn how to stop and start engines */
    if (Wsrep_server_state::instance().is_initialized() &&
        Wsrep_server_state::instance().state() == Wsrep_server_state::s_joiner)
    {
      // we already did SST at initializaiton, now engines are running
      // sql_print_information() is here because the message is too long
      // for WSREP_INFO.
      sql_print_information ("WSREP: "
                 "You have configured '%s' state snapshot transfer method "
                 "which cannot be performed on a running server. "
                 "Wsrep provider won't be able to fall back to it "
                 "if other means of state transfer are unavailable. "
                 "In that case you will need to restart the server.",
                 method);
      return "";
    }

    addr_len = sst_prepare_other (method, sst_auth_real,
                                  addr_in, &addr_out);
    if (addr_len < 0)
    {
      WSREP_ERROR("Failed to prepare for '%s' SST. Unrecoverable.",
                   method);
      throw wsrep::runtime_error("Failed to prepare for SST. Unrecoverable");
    }
  }

  std::string ret;
  ret += method;
  ret.push_back('\0');
  ret += addr_out;

  const char* method_ptr(ret.data());
  const char* addr_ptr(ret.data() + strlen(method_ptr) + 1);
  WSREP_DEBUG("Prepared SST request: %s|%s", method_ptr, addr_ptr);

  if (addr_out != addr_in) /* malloc'ed */ free (const_cast<char*>(addr_out));

  return ret;
}

// helper method for donors
static int sst_run_shell (const char* cmd_str, char** env, int max_tries)
{
  int ret= 0;

  for (int tries=1; tries <= max_tries; tries++)
  {
    wsp::process proc (cmd_str, "r", env);

    if (NULL != proc.pipe())
    {
      proc.wait();
    }

    if ((ret= proc.error()))
    {
      WSREP_ERROR("Try %d/%d: '%s' failed: %d (%s)",
                  tries, max_tries, proc.cmd(), ret, strerror(ret));
      sleep (1);
    }
    else
    {
      WSREP_DEBUG("SST script successfully completed.");
      break;
    }
  }

  return -ret;
}

static void sst_reject_queries(bool close_conn)
{
  WSREP_INFO("Rejecting client queries for the duration of SST.");
  if (true == close_conn) wsrep_close_client_connections(false);
}

static int sst_donate_mysqldump (const char*         addr,
                                 const wsrep::gtid&  gtid,
                                 bool                bypass,
                                 char**              env) // carries auth info
{
  int ret;
  char host[256];
  wsp::Address address(addr);

  if (!address.is_valid()) {
    WSREP_ERROR("Could not parse SST address : %s", addr);
    return 0;
  }

  memcpy(host, address.get_address(), address.get_address_len());
  int port = address.get_port();
  int const cmd_len = 4096;
  wsp::string cmd_str(cmd_len);

  if (!cmd_str()) {
    WSREP_ERROR("sst_donate_mysqldump(): "
                "could not allocate cmd buffer of %d bytes", cmd_len);
    return -ENOMEM;
  }

  char* local_gtid_val = NULL;
  if ((ret = generate_local_uuid(&local_gtid_val))) {
    WSREP_ERROR("sst_donate_mysqldump(): generate_local_uuid() failed %d",
                ret);
  }

  /*
    we enable new client connections so that mysqldump donation can connect in,
    but we reject local connections from modifyingcdata during SST, to keep
    data intact
  */
  if (!bypass && wsrep_sst_donor_rejects_queries) sst_reject_queries(true);

  make_wsrep_defaults_file();

  std::ostringstream uuid_oss;
  uuid_oss << gtid.id();
  ret = snprintf (cmd_str(), cmd_len,
                  "wsrep_sst_mysqldump "
                  WSREP_SST_OPT_ADDR " '%s' "
                  WSREP_SST_OPT_PORT " '%u' "
                  WSREP_SST_OPT_LPORT " '%u' "
                  WSREP_SST_OPT_SOCKET " '%s' "
                  " %s "
                  WSREP_SST_OPT_GTID " '%s:%lld' "
                  WSREP_SST_OPT_LOCAL_GTID " '%s:%lu' "
                  WSREP_SST_OPT_SERVER_ID " %d "
                  "%s",
                  addr, port, mysqld_port, mysqld_unix_port,
                  wsrep_defaults_file,
                  uuid_oss.str().c_str(), gtid.seqno().get(),
                  local_gtid_val, wsrep_local_gtid_manager.seqno(),
                  wsrep_local_gtid_manager.server_id,
                  bypass ? " " WSREP_SST_OPT_BYPASS : "");
  my_free(local_gtid_val);

  if (ret < 0 || ret >= cmd_len) {
    WSREP_ERROR("sst_donate_mysqldump(): snprintf() failed: %d", ret);
    return (ret < 0 ? ret : -EMSGSIZE);
  }

  WSREP_INFO("Running: '%s'", cmd_str());

  ret = sst_run_shell (cmd_str(), env, 3);

  wsrep::gtid sst_sent_gtid(ret == 0 ?
                            gtid :
                            wsrep::gtid(gtid.id(),
                                        wsrep::seqno::undefined()));
  Wsrep_server_state::instance().sst_sent(sst_sent_gtid, ret);

  return ret;
}

/*
  Create a file under data directory.
*/
static int sst_create_file(const char *name, const char *content)
{
  int err= 0;
  char *real_name;
  char *tmp_name;
  ssize_t len;
  FILE *file;

  len= strlen(mysql_real_data_home) + strlen(name) + 2;
  real_name= (char *) alloca(len);

  snprintf(real_name, (size_t) len, "%s/%s", mysql_real_data_home, name);

  tmp_name= (char *) alloca(len + 4);
  snprintf(tmp_name, (size_t) len + 4, "%s.tmp", real_name);

  file= fopen(tmp_name, "w+");

  if (0 == file)
  {
    err= errno;
    WSREP_ERROR("Failed to open '%s': %d (%s)", tmp_name, err, strerror(err));
  }
  else
  {
    // Write the specified content into the file.
    if (content != NULL)
    {
      fprintf(file, "%s\n", content);
      fsync(fileno(file));
    }

    fclose(file);

    if (rename(tmp_name, real_name) == -1)
    {
      err= errno;
      WSREP_ERROR("Failed to rename '%s' to '%s': %d (%s)", tmp_name,
                  real_name, err, strerror(err));
    }
  }

  return err;
}

static int run_sql_command(THD *thd, const char *query)
{
  thd->set_query(const_cast<char*>(query), strlen(query));

  Parser_state ps;
  if (ps.init(thd, thd->query().str, thd->query().length))
  {
    WSREP_ERROR("SST query: %s failed", query);
    return -1;
  }

  dispatch_sql_command(thd, &ps);
  if (thd->is_error())
  {
    int const err= thd->get_stmt_da()->mysql_errno();
    WSREP_WARN ("error executing '%s': %d (%s)%s",
                query, err, thd->get_stmt_da()->message_text(),
                err == ER_UNKNOWN_SYSTEM_VARIABLE ?
                ". Was mysqld built with --with-innodb-disallow-writes ?" : "");
    thd->clear_error();
    return -1;
  }
  return 0;
}

static void sst_disallow_writes (THD* thd, bool yes)
{
  char query_str[64]= { 0, };
  ssize_t const query_max= sizeof(query_str) - 1;
  const CHARSET_INFO *current_charset;
  DBUG_TRACE;

  current_charset= thd->variables.character_set_client;

  if (!is_supported_parser_charset(current_charset))
  {
      /* Do not use non-supported parser character sets */
      WSREP_WARN("Current client character set is non-supported parser character set: %s", current_charset->csname);
      thd->variables.character_set_client= &my_charset_latin1;
      WSREP_WARN("For SST temporally setting character set to : %s",
                 my_charset_latin1.csname);
  }

  snprintf (query_str, query_max, "SET GLOBAL innodb_disallow_writes=%d",
            yes ? 1 : 0);

  if (run_sql_command(thd, query_str))
  {
    WSREP_ERROR("Failed to disallow InnoDB writes");
  }
  thd->variables.character_set_client= current_charset;
}

static int sst_flush_tables(THD* thd)
{
  WSREP_INFO("Flushing tables for SST...");

  int err = 0;
  int not_used;
  /*
    Files created to notify the SST script about the outcome of table flush
    operation.
  */
  const char *flush_success = "tables_flushed";
  const char *flush_error = "sst_error";

  const CHARSET_INFO *current_charset = thd->variables.character_set_client;

  if (!is_supported_parser_charset(current_charset)) {
      /* Do not use non-supported parser character sets */
      WSREP_WARN("Current client character set is non-supported parser character set: %s", current_charset->csname);
      thd->variables.character_set_client= &my_charset_latin1;
      WSREP_WARN("For SST temporally setting character set to : %s",
                 my_charset_latin1.csname);
  }

  if (run_sql_command(thd, "FLUSH TABLES WITH READ LOCK")) {
    err = -1;
  } else {
    /*
      Make sure logs are flushed after global read lock acquired. In case
      reload fails, we must also release the acquired FTWRL.
    */
    if (handle_reload_request(thd, REFRESH_ENGINE_LOG | REFRESH_BINARY_LOG,
                              (TABLE_LIST *)0, &not_used)) {
      thd->global_read_lock.unlock_global_read_lock(thd);
      err = -1;
    }
  }

  thd->variables.character_set_client= current_charset;

  if (err) {
    WSREP_ERROR("Failed to flush and lock tables");
    /*
      The SST must be aborted as the flush tables failed. Notify this to SST
      script by creating the error file.
    */
    int tmp;
    if ((tmp = sst_create_file(flush_error, NULL)))
      err = tmp;
  } else {
    WSREP_INFO("Tables flushed.");
    /* disable further disk IO */
    sst_disallow_writes(thd, true);

    /*
      Tables have been flushed. Create a file with cluster state ID,
      local GTID and cluster server id.
    */
    char content[100];
    Wsrep_server_state& server_state = Wsrep_server_state::instance();
    std::ostringstream uuid_oss;
    uuid_oss << server_state.current_view().state_id().id();

    char* local_gtid = NULL;
    if ((err = generate_local_uuid(&local_gtid)))
      WSREP_ERROR("sst_flush_tables(): generate_local_uuid() failed %d", err);

    snprintf(content, sizeof(content), "%s:%lld %s:%lu %d",
             uuid_oss.str().c_str(), server_state.pause_seqno().get(),
             local_gtid, wsrep_local_gtid_manager.seqno(),
             wsrep_local_gtid_manager.server_id);
    my_free(local_gtid);

    /* this will release SST script to continue */
    err= sst_create_file(flush_success, content);
  }

  return err;
}

static void* sst_donor_thread (void* a)
{
  sst_thread_arg* arg= (sst_thread_arg*)a;

  WSREP_INFO("Running: '%s'", arg->cmd);

  int  err= 1;
  bool locked= false;

  const char*  out= NULL;
  const size_t out_len= 128;
  char         out_buf[out_len];

  wsrep_uuid_t  ret_uuid= WSREP_UUID_UNDEFINED;
  wsrep_seqno_t ret_seqno= WSREP_SEQNO_UNDEFINED; // seqno of complete SST

  wsp::thd thd(false); // we turn off wsrep_on for this THD so that it can
                       // operate with wsrep_ready == OFF
  wsp::process proc(arg->cmd, "r", arg->env);

  err= -proc.error();

/* Inform server about SST script startup and release TO isolation */
  mysql_mutex_lock   (&arg->lock);
  arg->err= -err;
  mysql_cond_signal  (&arg->cond);
  mysql_mutex_unlock (&arg->lock); //! @note arg is unusable after that.

  if (proc.pipe() && !err)
  {
    long long total= 0;
    long long complete= 0;
    // total form previous stages
    long long total_prev= 0;

wait_signal:
    out= my_fgets (out_buf, out_len, proc.pipe());

    if (out)
    {
      static const char magic_flush[]= "flush tables";
      static const char magic_cont[]= "continue";
      static const char magic_done[]= "done";
      static const size_t done_len=strlen(magic_done);
      static const char magic_total[]= "total";
      static const size_t total_len=strlen(magic_total);
      static const char magic_complete[]= "complete";
      static const size_t complete_len=strlen(magic_complete);
      static const int from= WSREP_MEMBER_DONOR;

      if (!strncasecmp (out, magic_complete, complete_len))
      {
        sst_handle_complete(out + complete_len, total_prev, &total, &complete,
                            from);
        goto wait_signal;
      }
      else if (!strncasecmp (out, magic_total, total_len))
      {
        sst_handle_total(out + total_len, &total_prev, &total, &complete, from);
        goto wait_signal;
      }
      else if (!strcasecmp (out, magic_flush))
      {
        err= sst_flush_tables (thd.ptr);
        if (!err)
        {
          locked= true;
          DBUG_EXECUTE_IF("sync.wsrep_donor_state",
                  {
                    const char act[]=
                      "now "
                      "SIGNAL sync.wsrep_donor_state_reached "
                      "WAIT_FOR signal.wsrep_donor_state";
                    assert(!debug_sync_set_action(thd.ptr,
                                                  STRING_WITH_LEN(act)));
                  };);
          goto wait_signal;
        }
      }
      else if (!strcasecmp (out, magic_cont))
      {
        if (locked)
        {
          sst_disallow_writes (thd.ptr, false);
          thd.ptr->global_read_lock.unlock_global_read_lock(thd.ptr);
          locked= false;
        }
        err=  0;
        goto wait_signal;
      }
      else if (!strncasecmp (out, magic_done, done_len))
      {
        uint32 dummy_offset = 0;
        err= sst_scan_uuid_seqno (out + strlen(magic_done) + 1, &dummy_offset,
                                  &ret_uuid, &ret_seqno);
      }
      else
      {
        WSREP_WARN("Received unknown signal: '%s'", out);
        proc.wait();
      }
    }
    else
    {
      WSREP_ERROR("Failed to read from: %s", proc.cmd());
      proc.wait();
    }
    if (!err && proc.error()) err= -proc.error();
  }
  else
  {
    WSREP_ERROR("Failed to execute: %s : %d (%s)",
                proc.cmd(), err, strerror(err));
  }

  if (locked) // don't forget to unlock server before return
  {
    sst_disallow_writes (thd.ptr, false);
    thd.ptr->global_read_lock.unlock_global_read_lock(thd.ptr);
  }

  wsrep::gtid gtid(wsrep::id(ret_uuid.data, sizeof(ret_uuid.data)),
                   wsrep::seqno(err ? wsrep::seqno::undefined() :
                                wsrep::seqno(ret_seqno)));
  Wsrep_server_state::instance().sst_sent(gtid, err);
  proc.wait();

  return NULL;
}

static int sst_donate_other (const char*        method,
                             const char*        addr,
                             const wsrep::gtid& gtid,
                             bool               bypass,
                             char**             env) // carries auth info
{
  int const cmd_len = 4096;
  wsp::string cmd_str(cmd_len);

  if (!cmd_str()) {
    WSREP_ERROR("sst_donate_other(): "
                "could not allocate cmd buffer of %d bytes", cmd_len);
    return -ENOMEM;
  }

  const char* binlog_opt = "";
  const char* binlog_index_opt = "";
  char* binlog_opt_val = NULL;
  char* binlog_index_opt_val = NULL;
  char* local_gtid_val = NULL;

  int ret;
  if ((ret = generate_binlog_opt_val(&binlog_opt_val))) {
    WSREP_ERROR("sst_donate_other(): generate_binlog_opt_val() failed: %d",ret);
    return ret;
  }

  if ((ret = generate_binlog_index_opt_val(&binlog_index_opt_val))) {
    WSREP_ERROR("sst_prepare_other(): generate_binlog_index_opt_val() failed %d",
                ret);
  }

  if ((ret = generate_local_uuid(&local_gtid_val))) {
    WSREP_ERROR("sst_prepare_other(): generate_local_uuid() failed %d",
                ret);
  }

  if (strlen(binlog_opt_val)) binlog_opt = WSREP_SST_OPT_BINLOG;
  if (strlen(binlog_index_opt_val)) binlog_index_opt = WSREP_SST_OPT_BINLOG_INDEX;

  make_wsrep_defaults_file();

  std::ostringstream uuid_oss;
  uuid_oss << gtid.id();
  ret= snprintf (cmd_str(), cmd_len,
                 "wsrep_sst_%s "
                 WSREP_SST_OPT_ROLE " 'donor' "
                 WSREP_SST_OPT_ADDR " '%s' "
                 WSREP_SST_OPT_LPORT " '%u' "
                 WSREP_SST_OPT_SOCKET " '%s' "
                 WSREP_SST_OPT_DATA " '%s' "
                 " %s "
                 WSREP_SST_OPT_VERSION " '%s' "
                 WSREP_SST_OPT_PLUGIN_DIR " '%s' "
                 " %s '%s' "
                 " %s '%s' "
                 WSREP_SST_OPT_GTID " %s:%lld "
                 WSREP_SST_OPT_LOCAL_GTID " %s:%lu "
                 WSREP_SST_OPT_SERVER_ID " %d "
                 "%s",
                 method, addr, mysqld_port, mysqld_unix_port,
                 mysql_real_data_home,
                 wsrep_defaults_file,
                 MYSQL_SERVER_VERSION MYSQL_SERVER_SUFFIX_DEF,
                 opt_plugin_dir ? opt_plugin_dir : "",
                 binlog_opt, binlog_opt_val,
                 binlog_index_opt, binlog_index_opt_val,
                 uuid_oss.str().c_str(), gtid.seqno().get(),
                 local_gtid_val, wsrep_local_gtid_manager.seqno(),
                 wsrep_local_gtid_manager.server_id,
                 bypass ? " " WSREP_SST_OPT_BYPASS : "");
  my_free(local_gtid_val);
  my_free(binlog_opt_val);
  my_free(binlog_index_opt_val);

  if (ret < 0 || ret >= cmd_len)
  {
    WSREP_ERROR("sst_donate_other(): snprintf() failed: %d", ret);
    return (ret < 0 ? ret : -EMSGSIZE);
  }

  if (!bypass && wsrep_sst_donor_rejects_queries) sst_reject_queries(false);

  pthread_t tmp;
  sst_thread_arg arg(cmd_str(), env);
  mysql_mutex_lock (&arg.lock);
  ret= pthread_create (&tmp, NULL, sst_donor_thread, &arg);
  if (ret)
  {
    WSREP_ERROR("sst_donate_other(): pthread_create() failed: %d (%s)",
                ret, strerror(ret));
    return ret;
  }
  mysql_cond_wait (&arg.cond, &arg.lock);

  WSREP_INFO("sst_donor_thread signaled with %d", arg.err);
  return arg.err;
}

/* return true if character can be a part of a filename */
static bool filename_char(int const c)
{
  return isalnum(c) || (c == '-') || (c == '_') || (c == '.');
}

/* return true if character can be a part of an address string */
static bool address_char(int const c)
{
  return filename_char(c) ||
         (c == ':') || (c == '[') || (c == ']') || (c == '/') || (c == '@');
}

static bool check_request_str(const char* const str,
                              bool (*check) (int c))
{
  for (size_t i(0); str[i] != '\0'; ++i)
  {
    if (!check(str[i]))
    {
      WSREP_WARN("Illegal character in state transfer request: %i (%c).",
                 str[i], str[i]);
      return true;
    }
  }

  return false;
}

int wsrep_sst_donate(const std::string& msg,
                     const wsrep::gtid& current_gtid,
                     const bool         bypass)
{
  const char* method= msg.data();
  size_t method_len= strlen (method);

  if (check_request_str(method, filename_char))
  {
    WSREP_ERROR("Bad SST method name. SST canceled.");
    return WSREP_CB_FAILURE;
  }

  const char* data= method + method_len + 1;

  /* check for auth@addr separator */
  const char* addr= strrchr(data, '@');
  wsp::string remote_auth;
  if (addr)
  {
    remote_auth.set(strndup(data, addr - data));
    addr++;
  }
  else
  {
    // no auth part
    addr= data;
  }

  if (check_request_str(addr, address_char))
  {
    WSREP_ERROR("Bad SST address string. SST canceled.");
    return WSREP_CB_FAILURE;
  }

  wsp::env env(NULL);
  if (env.error())
  {
    WSREP_ERROR("wsrep_sst_donate_cb(): env var ctor failed: %d", -env.error());
    return WSREP_CB_FAILURE;
  }

  int ret;
  if ((ret= sst_append_env_var(env, WSREP_SST_AUTH_ENV, sst_auth_real)))
  {
    WSREP_ERROR("wsrep_sst_donate_cb(): appending auth env failed: %d", ret);
    return WSREP_CB_FAILURE;
  }

  if (remote_auth())
  {
    if ((ret= sst_append_env_var(env, WSREP_SST_REMOTE_AUTH_ENV,remote_auth())))
    {
      WSREP_ERROR("wsrep_sst_donate_cb(): appending remote auth env failed: "
                  "%d", ret);
      return WSREP_CB_FAILURE;
    }
  }

  if (data_home_dir)
  {
    if ((ret= sst_append_env_var(env, DATA_HOME_DIR_ENV, data_home_dir)))
    {
      WSREP_ERROR("wsrep_sst_donate_cb(): appending data "
                  "directory failed: %d", ret);
      return WSREP_CB_FAILURE;
    }
  }

  if (!strcmp (WSREP_SST_MYSQLDUMP, method))
  {
    ret= sst_donate_mysqldump(addr, current_gtid, bypass, env());
  }
  else
  {
    ret= sst_donate_other(method, addr, current_gtid, bypass, env());
  }

  return (ret >= 0 ? WSREP_CB_SUCCESS : WSREP_CB_FAILURE);
}

void wsrep_SE_init_grab()
{
  if (mysql_mutex_lock (&LOCK_wsrep_sst_init)) abort();
}

void wsrep_SE_init_wait(THD* thd)
{
  while (SE_initialized == false && thd->killed == THD::NOT_KILLED)
  {
    mysql_mutex_lock(&thd->LOCK_thd_data);
    thd->current_cond= &COND_wsrep_sst_init;
    thd->current_mutex= &LOCK_wsrep_sst_init;
    mysql_mutex_unlock(&thd->LOCK_thd_data);
    
    mysql_cond_wait (&COND_wsrep_sst_init, &LOCK_wsrep_sst_init);

    if (thd->killed != THD::NOT_KILLED)
    {
      WSREP_DEBUG("SE init waiting canceled");
      break;
    }
    mysql_mutex_lock(&thd->LOCK_thd_data);
    thd->current_cond= NULL;
    thd->current_mutex= NULL;
    mysql_mutex_unlock(&thd->LOCK_thd_data);
  }
  mysql_mutex_unlock (&LOCK_wsrep_sst_init);

  mysql_mutex_lock(&thd->LOCK_thd_data);
  thd->current_cond= NULL;
  thd->current_mutex= NULL;
  mysql_mutex_unlock(&thd->LOCK_thd_data);
}

