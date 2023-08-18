/* Copyright (c) 2018, 2021 Percona LLC and/or its affiliates. All rights
   reserved.

   This program is free software; you can redistribute it and/or
   modify it under the terms of the GNU General Public License
   as published by the Free Software Foundation; version 2 of
   the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA */

#include <curl/curl.h>
#include <my_rnd.h>
#include <mysql/components/services/log_builtins.h>
#include <boost/preprocessor/stringize.hpp>
#include <memory>
#include "mysql/plugin_keyring.h"
#include "plugin/keyring/common/keyring.h"
#include "sql/sql_class.h"
#include "sql/sys_vars_shared.h"
#include "vault_curl.h"
#include "vault_io.h"
#include "vault_keys_container.h"
#include "vault_parser_composer.h"

using keyring::IVault_curl;
using keyring::IVault_parser_composer;
using keyring::Keys_iterator;
using keyring::Logger;
using keyring::Vault_curl;
using keyring::Vault_io;
using keyring::Vault_keys_container;
using keyring::Vault_parser_composer;

REQUIRES_SERVICE_PLACEHOLDER(log_builtins);
REQUIRES_SERVICE_PLACEHOLDER(log_builtins_string);

mysql_rwlock_t LOCK_keyring;

static void handle_std_bad_alloc_exception(const std::string &message_prefix) {
  assert(0);
  const std::string error_message =
      message_prefix + " due to memory allocation failure";
  if (logger != nullptr) logger->log(MY_ERROR_LEVEL, error_message.c_str());
}

static void handle_unknown_exception(const std::string &message_prefix) {
  assert(0);
  const std::string error_message = message_prefix +
                                    " due to internal "
                                    "exception inside the keyring_vault plugin";
  if (logger != nullptr) logger->log(MY_ERROR_LEVEL, error_message.c_str());
}

static char *keyring_vault_config_file = nullptr;
static uint keyring_vault_timeout = 0;

SERVICE_TYPE(log_builtins) *log_bi = nullptr;
SERVICE_TYPE(log_builtins_string) *log_bs = nullptr;

/**
  logger services initialization method for Component used when
  loading the Component.

  @return Status of performed operation
  @retval false success
  @retval true failure
*/
bool log_service_init() {
  log_bi = mysql_service_log_builtins;
  log_bs = mysql_service_log_builtins_string;

  return false;
}

/**
  logger services de-initialization method for Component used when
  unloading the Component.

  @return Status of performed operation
  @retval false success
  @retval true failure
*/
bool log_service_deinit() { return false; }

static int __attribute__((unused))
keyring_vault_init(MYSQL_PLUGIN plugin_info [[maybe_unused]]) {
  if (log_service_init()) return 1;

  try {
#ifdef HAVE_PSI_INTERFACE
    keyring_init_psi_keys();
#endif
    if (init_keyring_locks()) return 1;

    if (curl_global_init(CURL_GLOBAL_ALL) != 0) return 1;

    logger.reset(new Logger());
    keys.reset(new Vault_keys_container(logger.get()));
    std::unique_ptr<IVault_parser_composer> vault_parser(
        new Vault_parser_composer(logger.get()));
    std::unique_ptr<IVault_curl> vault_curl(new Vault_curl(
        logger.get(), vault_parser.get(), keyring_vault_timeout));
    IKeyring_io *keyring_io =
        new Vault_io(logger.get(), vault_curl.get(), vault_parser.get());
    vault_curl.release();
    vault_parser.release();
    if (keys->init(keyring_io, keyring_vault_config_file)) {
      is_keys_container_initialized = false;
      logger->log(
          MY_ERROR_LEVEL,
          "keyring_vault initialization failure. Please check that"
          " the keyring_vault_config_file points to readable keyring_vault "
          "configuration"
          " file. Please also make sure Vault is running and accessible."
          " The keyring_vault will stay unusable until correct configuration "
          "file gets"
          " provided.");

      if (current_thd != nullptr)
        push_warning(current_thd, Sql_condition::SL_WARNING, 42000,
                     "keyring_vault initialization failure. Please check the "
                     "server log.");
      return 0;
    }
    is_keys_container_initialized = true;
    return 0;
  } catch (const std::bad_alloc &e) {
    handle_std_bad_alloc_exception("keyring_vault initialization failure");
    curl_global_cleanup();
    return 1;
  } catch (...) {
    handle_unknown_exception("keyring_vault initialization failure");
    curl_global_cleanup();
    return 1;
  }
}

int keyring_vault_deinit(void *arg [[maybe_unused]]) noexcept {
  keys.reset();
  logger.reset();
  delete_keyring_file_data();
  mysql_rwlock_destroy(&LOCK_keyring);
  log_service_deinit();

  curl_global_cleanup();
  return 0;
}

bool mysql_key_fetch(const char *key_id, char **key_type, const char *user_id,
                     void **key, size_t *key_len) {
  return mysql_key_fetch<keyring::Vault_key>(key_id, key_type, user_id, key,
                                             key_len, "keyring_vault");
}

bool mysql_key_store(const char *key_id, const char *key_type,
                     const char *user_id, const void *key, size_t key_len) {
  return mysql_key_store<keyring::Vault_key>(key_id, key_type, user_id, key,
                                             key_len, "keyring_vault");
}

bool mysql_key_remove(const char *key_id, const char *user_id) {
  return mysql_key_remove<keyring::Vault_key>(key_id, user_id, "keyring_vault");
}

bool mysql_key_generate(const char *key_id, const char *key_type,
                        const char *user_id, size_t key_len) noexcept {
  try {
    std::unique_ptr<IKey> key_candidate(
        new keyring::Vault_key(key_id, key_type, user_id, NULL, 0));

    std::unique_ptr<uchar[]> key(new uchar[key_len]);
    if (key.get() == nullptr) return true;
    memset(key.get(), 0, key_len);
    if (!is_keys_container_initialized ||
        check_key_for_writing(key_candidate.get(), "generating") ||
        my_rand_buffer(key.get(), key_len))
      return true;

    return mysql_key_store(key_id, key_type, user_id, key.get(), key_len);
  } catch (const std::bad_alloc &e) {
    handle_std_bad_alloc_exception("Failed to generate a key");
    return true;
  } catch (...) {
    // We want to make sure that no exception leaves keyring_vault plugin and
    // goes into the server. That is why there are try..catch blocks in all
    // keyring_vault service methods.
    handle_unknown_exception("Failed to generate a key");
    return true;
  }
}
