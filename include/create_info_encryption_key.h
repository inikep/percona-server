#ifndef create_info_encryption_key_h
#define create_info_encryption_key_h

#include <cstdint>

#include "my_inttypes.h"

static constexpr uint ENCRYPTION_KEY_VERSION_INVALID = (~(uint)0);
static constexpr uint FIL_DEFAULT_ENCRYPTION_KEY = 0;
static constexpr uint ENCRYPTION_KEY_VERSION_NOT_ENCRYPTED = 0;

struct CreateInfoEncryptionKeyId
{
  CreateInfoEncryptionKeyId(bool was_encryption_key_id_set,
                            uint encryption_key_id)
    : was_encryption_key_id_set(was_encryption_key_id_set)
    , encryption_key_id(encryption_key_id)
  {} 

  CreateInfoEncryptionKeyId()
    : was_encryption_key_id_set(false)
    , encryption_key_id(FIL_DEFAULT_ENCRYPTION_KEY)
  {}

  bool was_encryption_key_id_set;
  uint encryption_key_id;
};

#endif
