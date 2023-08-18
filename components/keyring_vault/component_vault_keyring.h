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

#ifndef VAULT_KEYRING_SERVICE_IMP_H
#define VAULT_KEYRING_SERVICE_IMP_H

#include <mysql/components/service_implementation.h>
#include <boost/preprocessor/stringize.hpp>

// defined as a macro because needed both raw and stringized
#define CURRENT_COMPONENT_NAME keyring_vault
#define CURRENT_COMPONENT_NAME_STR BOOST_PP_STRINGIZE(CURRENT_COMPONENT_NAME)

BEGIN_SERVICE_DEFINITION(CURRENT_COMPONENT_NAME)
DECLARE_BOOL_METHOD(mysql_key_fetch_imp,
                    (const char *key_id, char **key_type, const char *user_id,
                     void **key, size_t *key_len));
DECLARE_BOOL_METHOD(mysql_key_store,
                    (const char *key_id, const char *key_type,
                     const char *user_id, const void *key, size_t key_len));
DECLARE_BOOL_METHOD(mysql_key_remove,
                    (const char *key_id, const char *user_id));
DECLARE_BOOL_METHOD(mysql_key_generate,
                    (const char *key_id, const char *key_type,
                     const char *user_id, size_t key_len));
DECLARE_BOOL_METHOD(mysql_key_iterator_init, (void **key_iterator));
DECLARE_BOOL_METHOD(mysql_key_iterator_deinit, (void *key_iterator));
DECLARE_BOOL_METHOD(mysql_key_iterator_get_key,
                    (void *key_iterator, char *key_id, char *user_id));
END_SERVICE_DEFINITION(CURRENT_COMPONENT_NAME)

#endif /* VAULT_KEYRING_SERVICE_IMP_H */
