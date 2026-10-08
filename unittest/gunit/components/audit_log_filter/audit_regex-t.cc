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

#include <gtest/gtest.h>
#include <unicode/utypes.h>

#include "components/audit_log_filter/audit_regex.h"

#include <array>
#include <condition_variable>
#include <cstdlib>
#include <mutex>
#include <new>
#include <thread>
#include <vector>

// Scope allocation failures to this thread and only the explicitly armed
// wrapper call. ICU's own allocator is not replaced by this C++ allocation
// harness; real engine-limit tests exercise its separate resource failures.
namespace {
thread_local int fail_allocation_after = -1;
}
void *operator new(std::size_t size) {
  if (fail_allocation_after >= 0 && fail_allocation_after-- == 0) {
    throw std::bad_alloc();
  }
  if (void *p = std::malloc(size ? size : 1)) return p;
  throw std::bad_alloc();
}
void operator delete(void *p) noexcept { std::free(p); }
void operator delete(void *p, std::size_t) noexcept { std::free(p); }
void *operator new[](std::size_t size) { return ::operator new(size); }
void operator delete[](void *p) noexcept { std::free(p); }
void operator delete[](void *p, std::size_t) noexcept { std::free(p); }
void *operator new(std::size_t size, const std::nothrow_t &) noexcept {
  try {
    return ::operator new(size);
  } catch (...) {
    return nullptr;
  }
}
void *operator new[](std::size_t size, const std::nothrow_t &) noexcept {
  try {
    return ::operator new[](size);
  } catch (...) {
    return nullptr;
  }
}
void operator delete(void *p, const std::nothrow_t &) noexcept { std::free(p); }
void operator delete[](void *p, const std::nothrow_t &) noexcept {
  std::free(p);
}

namespace audit_log_filter {
namespace {
using Result = AuditRegex::Result;
using Category = AuditRegex::Category;

TEST(AuditRegex, GuardedCppAllocations) {
  for (int failure = 0; failure < 3; ++failure) {
    AuditRegex::Error error;
    fail_allocation_after = failure;
    auto regex = AuditRegex::compile("orders[0-9]+", error);
    fail_allocation_after = -1;
    EXPECT_EQ(nullptr, regex);
    EXPECT_EQ(Category::Allocation, error.category);
    EXPECT_EQ(-1, error.offset);
    EXPECT_STREQ("U_MEMORY_ALLOCATION_ERROR", error.status_name());
    regex = AuditRegex::compile("orders[0-9]+", error);
    ASSERT_NE(nullptr, regex);
    EXPECT_EQ(Result::Match, regex->match("orders1", error));
  }
}

TEST(AuditRegex, OwningCompilationAndTotalInputs) {
  AuditRegex::Error error;
  auto regex = [] {
    AuditRegex::Error compile_error;
    std::string temporary = "a\\x{00}b";
    return AuditRegex::compile(temporary, compile_error);
  }();
  ASSERT_NE(nullptr, regex);
  EXPECT_EQ(Result::Match, regex->match(std::string_view("a\0b", 3), error));
  EXPECT_EQ(Result::NoMatch, regex->match("ab", error));
  regex = AuditRegex::compile(std::string_view("a\0b", 3), error);
  ASSERT_NE(nullptr, regex);
  EXPECT_EQ(Result::Match, regex->match(std::string_view("a\0b", 3), error));
  const char pattern[] = {'a', 'b'};
  const char subject[] = {'x', 'a', 'b', 'y'};
  regex = AuditRegex::compile({pattern, 2}, error);
  ASSERT_NE(nullptr, regex);
  EXPECT_EQ(Result::Match, regex->match({subject, 4}, error));
  regex = AuditRegex::compile({}, error);
  ASSERT_NE(nullptr, regex);
  EXPECT_EQ(Result::Match, regex->match({}, error));
  EXPECT_EQ(Result::Match, regex->match("anything", error));
}

TEST(AuditRegex, StrictConversionAndBounds) {
  AuditRegex::Error error;
  for (const std::string bad :
       {"\xff", "\xc0\xaf", "\xed\xa0\x80", "\xf0\x9f"}) {
    EXPECT_EQ(nullptr, AuditRegex::compile(bad, error));
    EXPECT_EQ(Category::Encoding, error.category);
    EXPECT_EQ(-1, error.offset);
  }
  EXPECT_NE(nullptr, AuditRegex::compile(std::string(16384, 'a'), error));
  EXPECT_EQ(nullptr, AuditRegex::compile(std::string(16385, 'a'), error));
  EXPECT_EQ(Category::Size, error.category);
  // Strict conversion must reset the preflight status and fill the buffer.
  auto regex = AuditRegex::compile("café😀", error);
  ASSERT_NE(nullptr, regex);
  EXPECT_EQ(Result::Match, regex->match("xxcafé😀yy", error));
  EXPECT_EQ(Result::NoMatch, regex->match("xxcafe😀yy", error));
}

TEST(AuditRegex, EnginePositionsAreCharacterPositions) {
  struct Case {
    const char *pattern;
    int32_t line;
    int32_t offset;
    const char *status;
  };
  for (const auto &c :
       std::array<Case, 5>{{{"^(orders", 1, 8, "U_REGEX_MISMATCHED_PAREN"},
                            {"a{2,1}", 1, 6, "U_REGEX_MAX_LT_MIN"},
                            {"[z-a]", 1, 4, "U_REGEX_INVALID_RANGE"},
                            {"😀(", 1, 2, "U_REGEX_MISMATCHED_PAREN"},
                            {"a\n😀(", 2, 2, "U_REGEX_MISMATCHED_PAREN"}}}) {
    AuditRegex::Error error;
    EXPECT_EQ(nullptr, AuditRegex::compile(c.pattern, error));
    EXPECT_EQ(Category::Syntax, error.category);
    EXPECT_EQ(c.line, error.line);
    EXPECT_EQ(c.offset, error.offset);
    EXPECT_STREQ(c.status, error.status_name());
  }
  auto error = AuditRegex::allocation_error();
  EXPECT_EQ(Category::Allocation, error.category);
  EXPECT_EQ(-1, error.offset);
  EXPECT_EQ(0, error.line);
}

TEST(AuditRegex, SearchFlagsAnchorsAndDecoding) {
  struct Case {
    const char *pattern;
    const char *subject;
    Result expected;
  };
  for (const auto &c :
       std::vector<Case>{{"orders", "new_orders1", Result::Match},
                         {"^orders$", "orders\n", Result::Match},
                         {"\\Aorders\\z", "orders\n", Result::NoMatch},
                         {"^$", "", Result::Match},
                         {"\\A\\z", "", Result::Match},
                         {"^$", "orders1", Result::NoMatch},
                         {"ORDERS", "orders1", Result::NoMatch},
                         {"(?i)ORDERS", "orders1", Result::Match},
                         {"^zam.wienia[0-9]+$", "zamówienia1", Result::Match},
                         {"café", "caf\xe9", Result::NoMatch},
                         {"caf\\x{FFFD}", "caf\xe9", Result::Match},
                         {"\\N{LATIN SMALL LETTER A}", "a", Result::Match}}) {
    AuditRegex::Error error;
    auto regex = AuditRegex::compile(c.pattern, error);
    ASSERT_NE(nullptr, regex) << error.status_name();
    EXPECT_EQ(c.expected, regex->match(c.subject, error)) << c.pattern;
  }
}

TEST(AuditRegex, RealLimitsAndRecovery) {
  AuditRegex::Error error;
  auto regex = AuditRegex::compile("(a+)+$", error);
  ASSERT_NE(nullptr, regex);
  const auto subject = "SELECT '" + std::string(40, 'a') + "'";
  ASSERT_EQ(49U, subject.size());
  EXPECT_EQ(Result::Error, regex->match(subject, error));
  EXPECT_EQ(Category::Timeout, error.category);
  EXPECT_STREQ("U_REGEX_TIME_OUT", error.status_name());
  EXPECT_EQ(Result::Match, regex->match("SELECT 1 AS aaaa", error));
  regex = AuditRegex::compile("^(a|b)*$", error);
  ASSERT_NE(nullptr, regex);
  EXPECT_EQ(Result::Error,
            regex->match(std::string(100000, 'a'), error, {0, 4096}));
  EXPECT_EQ(Category::Stack, error.category);
  EXPECT_STREQ("U_REGEX_STACK_OVERFLOW", error.status_name());
  EXPECT_EQ(Result::Match, regex->match("abba", error, {0, 4096}));
  EXPECT_EQ(Result::Match, regex->match(std::string(100000, 'a'), error));
}

TEST(AuditRegex, ConcurrentLocalUTextAndRetainedOwnership) {
  AuditRegex::Error error;
  std::shared_ptr<AuditRegex> published =
      AuditRegex::compile("^orders[0-9]+$", error);
  ASSERT_NE(nullptr, published);
  std::weak_ptr<AuditRegex> lifetime = published;
  std::mutex mutex;
  std::condition_variable cv;
  int ready = 0;
  bool resume = false;
  std::vector<std::thread> workers;
  for (int i = 0; i < 8; ++i) {
    workers.emplace_back([&, retained = published, i] {
      {
        std::unique_lock lock(mutex);
        ++ready;
        cv.notify_all();
        cv.wait(lock, [&] { return resume; });
      }
      for (int n = 0; n < 100; ++n) {
        AuditRegex::Error local_error;
        EXPECT_EQ(Result::Match,
                  retained->match("orders" + std::to_string(i), local_error));
        EXPECT_EQ(Result::NoMatch,
                  retained->match("orders_archive", local_error));
      }
    });
  }
  {
    std::unique_lock lock(mutex);
    cv.wait(lock, [&] { return ready == 8; });
    published.reset();
    EXPECT_FALSE(lifetime.expired());
    resume = true;
  }
  cv.notify_all();
  for (auto &worker : workers) worker.join();
  EXPECT_TRUE(lifetime.expired());
}
}  // namespace
}  // namespace audit_log_filter
