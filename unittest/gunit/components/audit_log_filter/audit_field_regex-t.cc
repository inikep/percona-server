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

#include "components/audit_log_filter/audit_record.h"
#include "components/audit_log_filter/event_field_condition/and.h"
#include "components/audit_log_filter/event_field_condition/bool.h"
#include "components/audit_log_filter/event_field_condition/field_regex.h"
#include "components/audit_log_filter/event_field_condition/not.h"
#include "components/audit_log_filter/event_field_condition/or.h"

#include <mysql/components/services/defs/event_tracking_general_defs.h>
#include <mysql/components/services/defs/event_tracking_table_access_defs.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace audit_log_filter {
namespace {
namespace cond = event_field_condition;

using clock = std::chrono::steady_clock;

struct CapturedWarning {
  std::string filter;
  std::string preview;
  std::string field;
  std::string category;
  std::string status;
};

struct WarningCapture {
  std::mutex mutex;
  std::vector<CapturedWarning> warnings;
};

void capture_warning(const cond::RegexRuntimeDiagnostic &diagnostic,
                     void *context) noexcept {
  auto *capture = static_cast<WarningCapture *>(context);
  std::lock_guard<std::mutex> guard(capture->mutex);
  capture->warnings.push_back(CapturedWarning{
      diagnostic.filter_name, diagnostic.pattern_preview, diagnostic.field_name,
      diagnostic.category, diagnostic.status_name});
}

std::shared_ptr<cond::EventFieldConditionRegex> make_regex(
    std::string field, std::string pattern, WarningCapture *capture,
    std::string filter = "filt") {
  return std::make_shared<cond::EventFieldConditionRegex>(
      field, AuditRegex{pattern}, cond::escape_diagnostic_text(filter),
      cond::escape_diagnostic_text(field),
      cond::escape_diagnostic_text(pattern), &capture_warning, capture);
}

TEST(AuditFieldRegex, DiagnosticTextEscapesAndTruncates) {
  EXPECT_EQ("plain", cond::escape_diagnostic_text("plain"));
  EXPECT_EQ("a\\u0000b", cond::escape_diagnostic_text(std::string("a\0b", 3)));
  EXPECT_EQ("\\u0027", cond::escape_diagnostic_text("'"));
  EXPECT_EQ("\\\\", cond::escape_diagnostic_text("\\"));
  EXPECT_EQ("\\u001F", cond::escape_diagnostic_text(std::string(1, '\x1F')));
  EXPECT_EQ("\\u007F", cond::escape_diagnostic_text(std::string(1, '\x7F')));
  EXPECT_EQ("😀", cond::escape_diagnostic_text("😀"));

  const std::string exact(cond::kDiagnosticTextMaxBytes, 'a');
  EXPECT_EQ(exact, cond::escape_diagnostic_text(exact));

  const std::string longer(cond::kDiagnosticTextMaxBytes + 1, 'b');
  const auto truncated = cond::escape_diagnostic_text(longer);
  EXPECT_EQ(cond::kDiagnosticTextMaxBytes, truncated.size());
  EXPECT_EQ("...", truncated.substr(truncated.size() - 3));
  EXPECT_EQ(std::string(93, 'b'), truncated.substr(0, 93));

  std::string multibyte(90, 'a');
  multibyte += "😀";
  multibyte.append(20, 'b');
  const auto cut = cond::escape_diagnostic_text(multibyte);
  EXPECT_LE(cut.size(), cond::kDiagnosticTextMaxBytes);
  EXPECT_EQ("...", cut.substr(cut.size() - 3));
  EXPECT_EQ(std::string(90, 'a'), cut.substr(0, 90));
  EXPECT_EQ(std::string::npos, cut.find("😀"));
}

TEST(AuditFieldRegex, LimiterEligibilityAndBoundary) {
  cond::RegexWarningLimiter limiter;
  const auto base = clock::time_point{};
  const auto first = base + std::chrono::milliseconds(100990);
  EXPECT_TRUE(limiter.try_acquire(first));
  EXPECT_FALSE(limiter.try_acquire(first + std::chrono::seconds(59)));
  EXPECT_FALSE(limiter.try_acquire(base + std::chrono::milliseconds(160010)));
  EXPECT_TRUE(limiter.try_acquire(base + std::chrono::milliseconds(160990)));

  cond::RegexWarningLimiter other;
  EXPECT_TRUE(other.try_acquire(first));
}

TEST(AuditFieldRegex, LimiterContentionAllowsOneWinner) {
  cond::RegexWarningLimiter limiter;
  const auto when = clock::time_point{} + std::chrono::seconds(5);
  constexpr int kThreads = 8;
  std::mutex mutex;
  std::condition_variable cv;
  int arrived = 0;
  bool go = false;
  std::atomic<int> wins{0};
  std::vector<std::thread> threads;
  for (int i = 0; i < kThreads; ++i) {
    threads.emplace_back([&] {
      std::unique_lock<std::mutex> lock(mutex);
      ++arrived;
      cv.notify_all();
      cv.wait(lock, [&] { return go; });
      lock.unlock();
      if (limiter.try_acquire(when)) {
        wins.fetch_add(1);
      }
    });
  }
  {
    std::unique_lock<std::mutex> lock(mutex);
    cv.wait(lock, [&] { return arrived == kThreads; });
    go = true;
  }
  cv.notify_all();
  for (auto &thread : threads) {
    thread.join();
  }
  EXPECT_EQ(1, wins.load());
  EXPECT_FALSE(limiter.try_acquire(when + std::chrono::seconds(59)));
}

TEST(AuditFieldRegex, ReportingCountsEveryErrorAndLimitsWarnings) {
  cond::RegexWarningLimiter limiter;
  WarningCapture capture;
  cond::RegexRuntimeDiagnostic diagnostic{"filt", "pat", "field", "timeout",
                                          "U_REGEX_TIME_OUT"};
  const auto before = cond::regex_match_error_count();
  const auto base = clock::time_point{} + std::chrono::hours(24);
  cond::note_regex_match_error(limiter, base, diagnostic, &capture_warning,
                               &capture);
  cond::note_regex_match_error(limiter, base + std::chrono::seconds(59),
                               diagnostic, &capture_warning, &capture);
  cond::note_regex_match_error(limiter, base + std::chrono::seconds(60),
                               diagnostic, &capture_warning, &capture);
  EXPECT_EQ(before + 3, cond::regex_match_error_count());
  ASSERT_EQ(2U, capture.warnings.size());
  EXPECT_EQ("timeout", capture.warnings[0].category);
  EXPECT_EQ("U_REGEX_TIME_OUT", capture.warnings[0].status);
  EXPECT_EQ("filt", capture.warnings[0].filter);
}

TEST(AuditFieldRegex, MissingAndNonStringFieldsAreNotErrors) {
  WarningCapture capture;
  const auto before = cond::regex_match_error_count();
  auto condition = make_regex("table_name.str", "orders[0-9]+", &capture);
  AuditRecordFieldsList fields;
  EXPECT_FALSE(condition->check_applies(fields));
  fields.emplace("table_name.str", static_cast<uint64_t>(1));
  EXPECT_FALSE(condition->check_applies(fields));
  fields["table_name.str"] = std::string("orders1");
  EXPECT_TRUE(condition->check_applies(fields));
  fields["table_name.str"] = std::string("customer");
  EXPECT_FALSE(condition->check_applies(fields));
  EXPECT_EQ(before, cond::regex_match_error_count());
  EXPECT_TRUE(capture.warnings.empty());
}

TEST(AuditFieldRegex, BooleanShortCircuitSkipsRegex) {
  WarningCapture capture;
  const auto before = cond::regex_match_error_count();
  auto regex = make_regex("general_query.str", "(a+)+$", &capture);
  auto negative = std::make_shared<cond::EventFieldConditionBool>(false);
  auto positive = std::make_shared<cond::EventFieldConditionBool>(true);
  cond::EventFieldConditionAnd and_cond({negative, regex});
  cond::EventFieldConditionOr or_cond({positive, regex});
  AuditRecordFieldsList fields;
  fields.emplace("general_query.str", std::string(64, 'a'));
  EXPECT_FALSE(and_cond.check_applies(fields));
  EXPECT_TRUE(or_cond.check_applies(fields));
  EXPECT_EQ(before, cond::regex_match_error_count());
  EXPECT_TRUE(capture.warnings.empty());
}

TEST(AuditFieldRegex, RealFieldMapsKeepRawQuery) {
  mysql_event_tracking_general_data general{};
  general.connection_id = 9;
  AuditRecordGeneral record{"general",
                            "status",
                            audit_event_class_t::AUDIT_GENERAL_CLASS,
                            &general,
                            {}};
  record.extended_info.query = std::nullopt;
  record.extended_info.query_output = QueryOutput{"converted", "rewritten"};
  record.extended_info.query_charset = "latin1";
  auto fields = get_audit_record_fields(record);
  EXPECT_EQ("", std::get<std::string>(fields.at("general_query.str")));
  EXPECT_EQ(0U, std::get<uint64_t>(fields.at("general_query.length")));

  record.extended_info.query = std::string("");
  fields = get_audit_record_fields(record);
  EXPECT_EQ("", std::get<std::string>(fields.at("general_query.str")));

  record.extended_info.query = std::string("a\0b", 3);
  fields = get_audit_record_fields(record);
  EXPECT_EQ(std::string("a\0b", 3),
            std::get<std::string>(fields.at("general_query.str")));
  EXPECT_EQ(3U, std::get<uint64_t>(fields.at("general_query.length")));

  WarningCapture capture;
  auto condition =
      make_regex("general_query.str", std::string("a\0b", 3), &capture);
  EXPECT_TRUE(condition->check_applies(fields));

  record.extended_info.query = std::string("caf\xE9", 4);
  fields = get_audit_record_fields(record);
  auto utf8_literal = make_regex("general_query.str", "café", &capture);
  EXPECT_FALSE(utf8_literal->check_applies(fields));
  auto marker = make_regex("general_query.str", "caf", &capture);
  EXPECT_TRUE(marker->check_applies(fields));

  mysql_event_tracking_table_access_data table{};
  table.connection_id = 4;
  table.table_database = mysql_cstring_with_length{"tpcc", 4};
  table.table_name = mysql_cstring_with_length{"orders1", 7};
  AuditRecordTableAccess access{"table_access",
                                "insert",
                                audit_event_class_t::AUDIT_TABLE_ACCESS_CLASS,
                                &table,
                                {}};
  access.extended_info.query = std::nullopt;
  access.extended_info.query_output = QueryOutput{"other", ""};
  auto table_fields = get_audit_record_fields(access);
  EXPECT_EQ("", std::get<std::string>(table_fields.at("query.str")));
  EXPECT_EQ(0U, std::get<uint64_t>(table_fields.at("query.length")));
  EXPECT_EQ("orders1",
            std::get<std::string>(table_fields.at("table_name.str")));
}

TEST(AuditFieldRegex, TimeoutOnFieldReportsOncePerInterval) {
  WarningCapture capture;
  auto condition = make_regex("general_query.str", "(a+)+$", &capture, "rx");
  std::string subject = "SELECT '";
  subject.append(40, 'a');
  subject.push_back('\'');
  AuditRecordFieldsList fields;
  fields.emplace("general_query.str", subject);
  const auto before = cond::regex_match_error_count();
  EXPECT_FALSE(condition->check_applies(fields));
  EXPECT_FALSE(condition->check_applies(fields));
  EXPECT_EQ(before + 2, cond::regex_match_error_count());
  ASSERT_EQ(1U, capture.warnings.size());
  EXPECT_EQ("timeout", capture.warnings[0].category);
  EXPECT_EQ("U_REGEX_TIME_OUT", capture.warnings[0].status);
  EXPECT_EQ("rx", capture.warnings[0].filter);
  EXPECT_EQ("(a+)+$", capture.warnings[0].preview);
  EXPECT_EQ("general_query.str", capture.warnings[0].field);

  fields["general_query.str"] = std::string("SELECT 1 AS aaaa");
  EXPECT_TRUE(condition->check_applies(fields));
  EXPECT_EQ(before + 2, cond::regex_match_error_count());
}

TEST(AuditFieldRegex, TwoConditionsWarnIndependently) {
  WarningCapture capture;
  auto first = make_regex("general_query.str", "(a+)+$", &capture, "one");
  auto second = make_regex("general_query.str", "(b+)+$", &capture, "two");
  std::string subject = "SELECT '";
  subject.append(40, 'a');
  subject.push_back('\'');
  AuditRecordFieldsList fields{{"general_query.str", subject}};
  EXPECT_FALSE(first->check_applies(fields));
  std::string other = "SELECT '";
  other.append(40, 'b');
  other.push_back('\'');
  fields["general_query.str"] = other;
  EXPECT_FALSE(second->check_applies(fields));
  ASSERT_EQ(2U, capture.warnings.size());
  EXPECT_EQ("one", capture.warnings[0].filter);
  EXPECT_EQ("two", capture.warnings[1].filter);
}

TEST(AuditFieldRegex, SharedConditionSurvivesPublisherReset) {
  WarningCapture capture;
  std::atomic<bool> destroyed{false};
  std::shared_ptr<cond::EventFieldConditionRegex> published(
      new cond::EventFieldConditionRegex(
          "table_name.str", AuditRegex{"^orders[0-9]+$"}, "filt",
          "table_name.str", "^orders[0-9]+$", &capture_warning, &capture),
      [&](cond::EventFieldConditionRegex *condition) {
        delete condition;
        destroyed.store(true);
      });

  constexpr int kThreads = 4;
  std::mutex mutex;
  std::condition_variable cv;
  int holding = 0;
  bool release_publisher = false;
  bool resume = false;
  std::atomic<int> matches{0};
  std::vector<std::shared_ptr<cond::EventFieldConditionRegex>> held(kThreads);
  std::vector<std::thread> threads;
  for (int i = 0; i < kThreads; ++i) {
    threads.emplace_back([&, i] {
      held[i] = published;
      std::unique_lock<std::mutex> lock(mutex);
      ++holding;
      cv.notify_all();
      cv.wait(lock, [&] { return resume; });
      lock.unlock();
      AuditRecordFieldsList fields;
      fields.emplace("table_name.str", std::string("orders1"));
      if (held[i]->check_applies(fields)) {
        matches.fetch_add(1);
      }
      held[i].reset();
    });
  }
  {
    std::unique_lock<std::mutex> lock(mutex);
    cv.wait(lock, [&] { return holding == kThreads; });
    release_publisher = true;
    published.reset();
    resume = true;
  }
  cv.notify_all();
  for (auto &thread : threads) {
    thread.join();
  }
  EXPECT_EQ(kThreads, matches.load());
  EXPECT_TRUE(destroyed.load());
  EXPECT_FALSE(release_publisher && published);
}

TEST(AuditFieldRegex, NotInvertsFailedLeafOnlyAfterEvaluation) {
  WarningCapture capture;
  auto regex = make_regex("table_name.str", "^orders[0-9]+$", &capture);
  cond::EventFieldConditionNot inverted{regex};
  AuditRecordFieldsList fields{{"table_name.str", std::string("orders1")}};
  EXPECT_FALSE(inverted.check_applies(fields));
  fields["table_name.str"] = std::string("customer");
  EXPECT_TRUE(inverted.check_applies(fields));
}

}  // namespace
}  // namespace audit_log_filter
