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

#ifndef AUDIT_LOG_FILTER_AUDIT_REGEX_H_INCLUDED
#define AUDIT_LOG_FILTER_AUDIT_REGEX_H_INCLUDED

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string_view>

namespace audit_log_filter {

// No server services or ICU types cross this boundary.
class AuditRegex {
 public:
  enum class Category {
    Size,
    Encoding,
    Allocation,
    Syntax,
    Timeout,
    Stack,
    Engine
  };
  struct Error {
    Category category{Category::Engine};
    int32_t status{0};
    int32_t line{0};
    int32_t offset{-1};
    const char *status_name() const noexcept;
    const char *category_name() const noexcept;
  };
  enum class Result { Match, NoMatch, Error };
  struct Limits {
    int32_t time{32};
    int32_t stack{8000000};
  };
  static constexpr size_t kMaxPatternBytes = 16384;

  static std::unique_ptr<AuditRegex> compile(std::string_view pattern,
                                             Error &error) noexcept;
  ~AuditRegex();
  AuditRegex(const AuditRegex &) = delete;
  AuditRegex &operator=(const AuditRegex &) = delete;

  Result match(std::string_view subject, Error &error) const noexcept;
  Result match(std::string_view subject, Error &error,
               Limits limits) const noexcept;
  static Error allocation_error() noexcept;

 private:
  struct Impl;
  explicit AuditRegex(std::unique_ptr<Impl> impl) noexcept;
  std::unique_ptr<Impl> m_impl;
};

}  // namespace audit_log_filter
#endif  // AUDIT_LOG_FILTER_AUDIT_REGEX_H_INCLUDED
