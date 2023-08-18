/* Copyright (c) 2023 Percona LLC and/or its affiliates. All rights
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

#include "component_vault_keyring.h"
#include <mysql/components/services/component_sys_var_service.h>
#include "vault_keyring.cc"

REQUIRES_SERVICE_PLACEHOLDER(component_sys_variable_register);
REQUIRES_SERVICE_PLACEHOLDER(component_sys_variable_unregister);

static bool reset_curl() noexcept {
  curl_global_cleanup();
  return curl_global_init(CURL_GLOBAL_ALL) != 0;
}

int check_keyring_file_data(MYSQL_THD thd [[maybe_unused]],
                            SYS_VAR *var [[maybe_unused]], void *save,
                            st_mysql_value *value) {
  char buff[FN_REFLEN + 1];
  const char *keyring_filename;
  int len = sizeof(buff);
  std::unique_ptr<IKeys_container> new_keys(
      new Vault_keys_container(logger.get()));

  (*(const char **)save) = nullptr;
  keyring_filename = value->val_str(value, buff, &len);
  if (keyring_filename == nullptr) return 1;
  PolyLock_rwlock keyring_rwlock(&LOCK_keyring);
  AutoWLock keyring_auto_wlokc(&keyring_rwlock);

  try {
    if (reset_curl()) {
      logger->log(MY_ERROR_LEVEL, "Cannot set keyring_vault_config_file");
      return 1;
    }
    std::unique_ptr<IVault_parser_composer> vault_parser(
        new Vault_parser_composer(logger.get()));
    std::unique_ptr<IVault_curl> vault_curl(new Vault_curl(
        logger.get(), vault_parser.get(), keyring_vault_timeout));
    IKeyring_io *keyring_io(
        new Vault_io(logger.get(), vault_curl.get(), vault_parser.get()));
    vault_curl.release();
    vault_parser.release();
    if (new_keys->init(keyring_io, keyring_filename)) return 1;
    *reinterpret_cast<IKeys_container **>(save) = new_keys.release();
  } catch (const std::bad_alloc &e) {
    handle_std_bad_alloc_exception("Cannot set keyring_vault_config_file");
    return 1;
  } catch (...) {
    handle_unknown_exception("Cannot set keyring_vault_config_file");
    return 1;
  }
  return (0);
}

static void update_keyring_vault_timeout(MYSQL_THD thd [[maybe_unused]],
                                         SYS_VAR *var [[maybe_unused]],
                                         void *ptr, const void *val) noexcept {
  assert(dynamic_cast<Vault_keys_container *>(keys.get()) != nullptr);
  *reinterpret_cast<uint *>(ptr) = *reinterpret_cast<const uint *>(val);
  dynamic_cast<Vault_keys_container *>(keys.get())
      ->set_curl_timeout(*static_cast<const uint *>(val));
}

int register_system_variables() {
  INTEGRAL_CHECK_ARG(int) timeout;
  timeout.def_val = 15;
  timeout.min_val = 0;
  timeout.max_val = 86400; /* 24h  */
  timeout.blk_sz = 0;
  if (mysql_service_component_sys_variable_register->register_variable(
          CURRENT_COMPONENT_NAME_STR, "timeout",
          PLUGIN_VAR_INT | PLUGIN_VAR_UNSIGNED | PLUGIN_VAR_OPCMDARG,
          "The keyring_vault - Vault server connection timeout", nullptr,
          update_keyring_vault_timeout, (void *)&timeout,
          (void *)&keyring_vault_timeout)) {
    LogEvent()
        .type(LOG_TYPE_ERROR)
        .prio(ERROR_LEVEL)
        .lookup(ER_VALIDATE_PWD_VARIABLE_REGISTRATION_FAILED,
                CURRENT_COMPONENT_NAME_STR ".timeout");
    return 1;
  }

  STR_CHECK_ARG(str) config;
  config.def_val = nullptr;
  if (mysql_service_component_sys_variable_register->register_variable(
          CURRENT_COMPONENT_NAME_STR, "config",
          PLUGIN_VAR_STR | PLUGIN_VAR_MEMALLOC | PLUGIN_VAR_RQCMDARG,
          "The path to the keyring_vault configuration file",
          check_keyring_file_data, update_keyring_file_data, (void *)&config,
          (void *)&keyring_vault_config_file)) {
    LogEvent()
        .type(LOG_TYPE_ERROR)
        .prio(ERROR_LEVEL)
        .lookup(ER_VALIDATE_PWD_VARIABLE_REGISTRATION_FAILED,
                CURRENT_COMPONENT_NAME_STR ".config");
    goto timeout_var;
  }
  return 0; /* All system variables registered successfully */
timeout_var:
  mysql_service_component_sys_variable_unregister->unregister_variable(
      CURRENT_COMPONENT_NAME_STR, "timeout");
  return 1; /* register_variable() api failed for one of the system variable */
}

int unregister_system_variables() {
  if (mysql_service_component_sys_variable_unregister->unregister_variable(
          CURRENT_COMPONENT_NAME_STR, "timeout")) {
    LogEvent()
        .type(LOG_TYPE_ERROR)
        .prio(ERROR_LEVEL)
        .lookup(ER_VALIDATE_PWD_VARIABLE_UNREGISTRATION_FAILED,
                CURRENT_COMPONENT_NAME_STR ".timeout");
  }
  if (mysql_service_component_sys_variable_unregister->unregister_variable(
          CURRENT_COMPONENT_NAME_STR, "config")) {
    LogEvent()
        .type(LOG_TYPE_ERROR)
        .prio(ERROR_LEVEL)
        .lookup(ER_VALIDATE_PWD_VARIABLE_UNREGISTRATION_FAILED,
                CURRENT_COMPONENT_NAME_STR ".config");
  }
  return 0;
}

static mysql_service_status_t component_keyring_vault_init() {
  if (keyring_vault_init(nullptr)) return 1;
  if (register_system_variables()) return 1;
  return 0;
}

static mysql_service_status_t component_keyring_vault_deinit() {
  unregister_system_variables();
  keyring_vault_deinit(nullptr);
  return 0;
}

static void mysql_key_iterator_init(void **key_iterator) {
  *key_iterator = new Keys_iterator(logger.get());
  mysql_key_iterator_init<keyring::Vault_key>(
      static_cast<Keys_iterator *>(*key_iterator), "keyring_vault");
}

static void mysql_key_iterator_deinit(void *key_iterator) {
  mysql_key_iterator_deinit<keyring::Vault_key>(
      static_cast<Keys_iterator *>(key_iterator), "keyring_vault");
  delete static_cast<Keys_iterator *>(key_iterator);
}

static bool mysql_key_iterator_get_key(void *key_iterator, char *key_id,
                                       char *user_id) {
  return mysql_key_iterator_get_key<keyring::Vault_key>(
      static_cast<Keys_iterator *>(key_iterator), key_id, user_id,
      "keyring_vault");
}

DEFINE_BOOL_METHOD(mysql_key_fetch_imp,
                   (const char *key_id, char **key_type, const char *user_id,
                    void **key, size_t *key_len)) {
  return mysql_key_fetch(key_id, key_type, user_id, key, key_len);
}

DEFINE_BOOL_METHOD(mysql_key_store_imp,
                   (const char *key_id, const char *key_type,
                    const char *user_id, const void *key, size_t key_len)) {
  return mysql_key_store(key_id, key_type, user_id, key, key_len);
}

DEFINE_BOOL_METHOD(mysql_key_remove_imp,
                   (const char *key_id, const char *user_id)) {
  return mysql_key_remove(key_id, user_id);
}

DEFINE_BOOL_METHOD(mysql_key_generate_imp,
                   (const char *key_id, const char *key_type,
                    const char *user_id, size_t key_len)) {
  return mysql_key_generate(key_id, key_type, user_id, key_len);
}

DEFINE_BOOL_METHOD(mysql_key_iterator_init_imp, (void **key_iterator)) {
  mysql_key_iterator_init(key_iterator);
  return false;
}

DEFINE_BOOL_METHOD(mysql_key_iterator_deinit_imp, (void *key_iterator)) {
  mysql_key_iterator_deinit(key_iterator);
  return false;
}

DEFINE_BOOL_METHOD(mysql_key_iterator_get_key_imp,
                   (void *key_iterator, char *key_id, char *user_id)) {
  return mysql_key_iterator_get_key(key_iterator, key_id, user_id);
}

// clang-format off
BEGIN_SERVICE_IMPLEMENTATION(CURRENT_COMPONENT_NAME, CURRENT_COMPONENT_NAME)
  mysql_key_fetch_imp,
  mysql_key_store_imp,
  mysql_key_remove_imp,
  mysql_key_generate_imp,
  mysql_key_iterator_init_imp,
  mysql_key_iterator_deinit_imp,
  mysql_key_iterator_get_key_imp
END_SERVICE_IMPLEMENTATION();


BEGIN_COMPONENT_PROVIDES(CURRENT_COMPONENT_NAME)
  PROVIDES_SERVICE(CURRENT_COMPONENT_NAME, CURRENT_COMPONENT_NAME),
END_COMPONENT_PROVIDES();

BEGIN_COMPONENT_REQUIRES(CURRENT_COMPONENT_NAME)
  REQUIRES_SERVICE(log_builtins),
  REQUIRES_SERVICE(log_builtins_string),
  REQUIRES_SERVICE(component_sys_variable_register),
  REQUIRES_SERVICE(component_sys_variable_unregister),
END_COMPONENT_REQUIRES();

BEGIN_COMPONENT_METADATA(CURRENT_COMPONENT_NAME)
  METADATA("mysql.author", "Percona Corporation"),
  METADATA("mysql.license", "GPL"),
END_COMPONENT_METADATA();

DECLARE_COMPONENT(CURRENT_COMPONENT_NAME, CURRENT_COMPONENT_NAME_STR)
  component_keyring_vault_init,
  component_keyring_vault_deinit,
END_DECLARE_COMPONENT();
// clang-format on

DECLARE_LIBRARY_COMPONENTS &COMPONENT_REF(CURRENT_COMPONENT_NAME)
    END_DECLARE_LIBRARY_COMPONENTS
