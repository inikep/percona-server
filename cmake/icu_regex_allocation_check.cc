/* Copyright (c) 2026 Percona LLC and/or its affiliates. All rights reserved.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0. */

// Run each fault in a separate process: vulnerable ICU versions crash inside
// compilation. Check the actual audit wrapper, including cleanup and recovery.
#include "components/audit_log_filter/audit_regex.h"

#include <unicode/uclean.h>

#include <cstdlib>
#include <cstring>

namespace {
long countdown = -1;
long outstanding = 0;
bool fired = false;

bool fail() {
  if (countdown < 0) return false;
  if (countdown-- != 0) return false;
  fired = true;
  return true;
}

void *U_CALLCONV allocate(const void *, size_t size) {
  if (fail()) return nullptr;
  void *p = std::malloc(size);
  if (p != nullptr) ++outstanding;
  return p;
}

void *U_CALLCONV reallocate(const void *, void *p, size_t size) {
  if (fail()) return nullptr;
  const bool was_null = p == nullptr;
  void *result = std::realloc(p, size);
  if (was_null && result != nullptr) ++outstanding;
  return result;
}

void U_CALLCONV release(const void *, void *p) {
  if (p != nullptr) --outstanding;
  std::free(p);
}
}  // namespace

int main(int argc, char **argv) {
  using namespace audit_log_filter;
  if (argc != 3) return 1;
  UErrorCode status = U_ZERO_ERROR;
  u_setMemoryFunctions(nullptr, allocate, reallocate, release, &status);
  if (U_FAILURE(status)) return 2;

  constexpr auto pattern = "^(new_orders|orders|history)[0-9]+$";
  AuditRegex::Error error;
  auto warm = AuditRegex::compile(pattern, error);
  if (!warm || warm->match("orders1", error) != AuditRegex::Result::Match)
    return 3;
  const bool matching = std::strcmp(argv[1], "match") == 0;
  if (!matching) warm.reset();
  const long baseline = outstanding;
  countdown = std::strtol(argv[2], nullptr, 10);
  bool failed;
  if (matching) {
    const auto result = warm->match("orders1", error);
    if (result == AuditRegex::Result::NoMatch) return 6;
    failed = result == AuditRegex::Result::Error;
  } else {
    auto compiled = AuditRegex::compile(pattern, error);
    failed = compiled == nullptr;
    countdown = -1;
    if (compiled &&
        (compiled->match("orders1", error) != AuditRegex::Result::Match ||
         compiled->match("customer1", error) != AuditRegex::Result::NoMatch ||
         compiled->match("orders1x", error) != AuditRegex::Result::NoMatch))
      return 6;
  }
  countdown = -1;
  if ((failed && !fired) || outstanding != baseline) return 4;
  // A failure must not poison shared state.
  auto recovery = AuditRegex::compile(pattern, error);
  if (!recovery ||
      recovery->match("orders1", error) != AuditRegex::Result::Match)
    return 5;
  return fired ? 0 : 77;  // The caller stops after all allocations were tested.
}
