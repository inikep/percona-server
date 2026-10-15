/* Copyright (c) 2026 Percona LLC and/or its affiliates.
   This program is free software; you can redistribute it and/or modify it
   under the terms of the GNU General Public License, version 2. */
#pragma once

#include <cstdint>
#include <limits>

namespace myrocks {

inline uint64_t rdb_ttl_saturating_add(uint64_t base, uint64_t duration) {
  constexpr auto max = std::numeric_limits<uint64_t>::max();
  return base > max - duration ? max : base + duration;
}

// Debug adjustments must not manufacture either immortal representation.
inline uint64_t rdb_ttl_adjust_expiry(uint64_t expiry, int64_t delta) {
  constexpr auto max = std::numeric_limits<uint64_t>::max();
  if (expiry == 0 || expiry == max) return expiry;
  if (delta < 0) {
    const auto magnitude = static_cast<uint64_t>(-(delta + 1)) + 1;
    return expiry <= magnitude ? 1 : expiry - magnitude;
  }
  const auto result = rdb_ttl_saturating_add(expiry, delta);
  return result == max ? max - 1 : result;
}

inline bool rdb_ttl_expired(uint64_t stamp, uint64_t duration, bool absolute,
                            uint64_t cutoff) {
  if (stamp == std::numeric_limits<uint64_t>::max()) return false;
  if (absolute) return stamp != 0 && stamp <= cutoff;
  // Preserve legacy table TTL arithmetic and encoding.
  return stamp + duration <= cutoff;
}

}  // namespace myrocks
