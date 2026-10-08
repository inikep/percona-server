/* Copyright (c) 2026 Percona LLC and/or its affiliates. All rights reserved.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA */

#include "components/audit_log_filter/audit_regex.h"

#include <unicode/regex.h>
#include <unicode/ustring.h>
#include <unicode/utext.h>

#include <limits>
#include <new>
#include <vector>

namespace audit_log_filter {
namespace {
AuditRegex::Error engine_error(UErrorCode status) noexcept {
  using Category = AuditRegex::Category;
  Category category = Category::Engine;
  switch (status) {
    case U_MEMORY_ALLOCATION_ERROR:
      category = Category::Allocation;
      break;
    case U_REGEX_TIME_OUT:
      category = Category::Timeout;
      break;
    case U_REGEX_STACK_OVERFLOW:
      category = Category::Stack;
      break;
    default:
      break;
  }
  return {category, static_cast<int32_t>(status)};
}
struct LocalText {
  UText text = UTEXT_INITIALIZER;
  ~LocalText() { utext_close(&text); }
};
}  // namespace

struct AuditRegex::Impl {
  std::unique_ptr<icu::RegexPattern> pattern;
};

AuditRegex::AuditRegex(std::unique_ptr<Impl> impl) noexcept
    : m_impl(std::move(impl)) {}
AuditRegex::~AuditRegex() = default;

const char *AuditRegex::Error::status_name() const noexcept {
  return u_errorName(static_cast<UErrorCode>(status));
}
const char *AuditRegex::Error::category_name() const noexcept {
  switch (category) {
    case Category::Size:
      return "size";
    case Category::Encoding:
      return "encoding";
    case Category::Allocation:
      return "allocation";
    case Category::Syntax:
      return "syntax";
    case Category::Timeout:
      return "timeout";
    case Category::Stack:
      return "stack";
    case Category::Engine:
      return "engine";
  }
  return "engine";
}
AuditRegex::Error AuditRegex::allocation_error() noexcept {
  return engine_error(U_MEMORY_ALLOCATION_ERROR);
}

std::unique_ptr<AuditRegex> AuditRegex::compile(std::string_view pattern,
                                                Error &error) noexcept {
  error = {};
  if (pattern.size() > kMaxPatternBytes) {
    error = {Category::Size, U_INDEX_OUTOFBOUNDS_ERROR};
    return nullptr;
  }
  try {
    UErrorCode status = U_ZERO_ERROR;
    icu::UnicodeString owned;
    if (!pattern.empty()) {
      int32_t length = 0;
      u_strFromUTF8(nullptr, 0, &length, pattern.data(),
                    static_cast<int32_t>(pattern.size()), &status);
      if (status != U_BUFFER_OVERFLOW_ERROR && U_FAILURE(status)) {
        error = {Category::Encoding, status};
        return nullptr;
      }
      if (length < 0 || length == std::numeric_limits<int32_t>::max()) {
        error = {Category::Size, U_INDEX_OUTOFBOUNDS_ERROR};
        return nullptr;
      }
      status = U_ZERO_ERROR;
      std::vector<UChar> buffer(static_cast<size_t>(length) + 1);
      u_strFromUTF8(buffer.data(), length + 1, &length, pattern.data(),
                    static_cast<int32_t>(pattern.size()), &status);
      if (U_FAILURE(status)) {
        error = {Category::Encoding, status};
        return nullptr;
      }
      // This constructor copies; never compile a borrowed UText pattern.
      owned = icu::UnicodeString(buffer.data(), length);
    }
    if (owned.isBogus()) {
      error = allocation_error();
      return nullptr;
    }
    UParseError position{};
    position.offset = -1;
    auto impl = std::make_unique<Impl>();
    impl->pattern.reset(icu::RegexPattern::compile(owned, 0, position, status));
    if (U_FAILURE(status) || !impl->pattern) {
      error =
          engine_error(U_FAILURE(status) ? status : U_MEMORY_ALLOCATION_ERROR);
      if (status >= U_REGEX_INTERNAL_ERROR && status < U_REGEX_ERROR_LIMIT &&
          error.category == Category::Engine && position.offset >= 0) {
        error.category = Category::Syntax;
      }
      if (position.offset >= 0) {
        error.line = position.line;
        error.offset = position.offset;
      }
      return nullptr;
    }
    return std::unique_ptr<AuditRegex>(new AuditRegex(std::move(impl)));
  } catch (const std::bad_alloc &) {
    error = allocation_error();
  } catch (...) {
    error = engine_error(U_INTERNAL_PROGRAM_ERROR);
  }
  return nullptr;
}

AuditRegex::Result AuditRegex::match(std::string_view subject,
                                     Error &error) const noexcept {
  return match(subject, error, Limits{});
}

AuditRegex::Result AuditRegex::match(std::string_view subject, Error &error,
                                     Limits limits) const noexcept {
  error = {};
  if (subject.size() >
      static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
    error = engine_error(U_INDEX_OUTOFBOUNDS_ERROR);
    return Result::Error;
  }
  try {
    UErrorCode status = U_ZERO_ERROR;
    // Declaration order ensures matcher destruction precedes UText closure.
    LocalText text;
    auto *opened =
        utext_openUTF8(&text.text, subject.empty() ? "" : subject.data(),
                       static_cast<int64_t>(subject.size()), &status);
    if (U_FAILURE(status) || !opened) {
      error =
          engine_error(U_FAILURE(status) ? status : U_MEMORY_ALLOCATION_ERROR);
      return Result::Error;
    }
    std::unique_ptr<icu::RegexMatcher> matcher(
        m_impl->pattern->matcher(status));
    if (U_FAILURE(status) || !matcher) {
      error =
          engine_error(U_FAILURE(status) ? status : U_MEMORY_ALLOCATION_ERROR);
      return Result::Error;
    }
    matcher->reset(opened);
    matcher->setTimeLimit(limits.time, status);
    matcher->setStackLimit(limits.stack, status);
    if (U_FAILURE(status)) {
      error = engine_error(status);
      return Result::Error;
    }
    const bool matched = matcher->find(status);
    if (U_FAILURE(status)) {
      error = engine_error(status);
      return Result::Error;
    }
    return matched ? Result::Match : Result::NoMatch;
  } catch (const std::bad_alloc &) {
    error = allocation_error();
  } catch (...) {
    error = engine_error(U_INTERNAL_PROGRAM_ERROR);
  }
  return Result::Error;
}
}  // namespace audit_log_filter
