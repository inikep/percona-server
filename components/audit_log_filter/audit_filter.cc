/* Copyright (c) 2022 Percona LLC and/or its affiliates. All rights reserved.

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

#include "components/audit_log_filter/audit_filter.h"
#include "components/audit_log_filter/audit_error_log.h"
#include "components/audit_log_filter/audit_rule.h"

namespace audit_log_filter {

AuditAction AuditEventFilter::apply(
    const AuditRule *rule, AuditRecordVariant &audit_record,
    const AuditRule *&replacement_rule) noexcept {
  auto event_class_name = std::visit(
      [](const auto &rec) { return rec.event_class_name; }, audit_record);
  auto event_subclass_name = std::visit(
      [](const auto &rec) { return rec.event_subclass_name; }, audit_record);

  // Default action for unmatched events is taken from the session's rule
  const auto should_log_unmatched = rule->should_log_unmatched();

  // Actions are taken from the replacement rule while it is active. Use the
  // same rule for the whole event, the event may change replacement_rule.
  const auto *actions_rule =
      replacement_rule != nullptr ? replacement_rule : rule;

  if (!actions_rule->has_actions_for(event_class_name, event_subclass_name)) {
    return should_log_unmatched ? AuditAction::Log : AuditAction::Skip;
  }

  const auto event_fields =
      std::visit([](const auto &rec) { return get_audit_record_fields(rec); },
                 audit_record);

  const auto *block_action = actions_rule->get_action(
      EventActionType::Block, event_class_name, event_subclass_name);

  if (block_action != nullptr &&
      block_action->apply(event_fields, audit_record, replacement_rule)) {
    return AuditAction::Block;
  }

  const auto *replace_field_action = actions_rule->get_action(
      EventActionType::ReplaceField, event_class_name, event_subclass_name);
  const auto *replace_filter_action = actions_rule->get_action(
      EventActionType::ReplaceFilter, event_class_name, event_subclass_name);
  const auto *print_query_attrs_action = actions_rule->get_action(
      EventActionType::PrintQueryAttrs, event_class_name, event_subclass_name);
  const auto *print_service_comp_action = actions_rule->get_action(
      EventActionType::PrintServiceComp, event_class_name, event_subclass_name);

  if (replace_field_action != nullptr) {
    replace_field_action->apply(event_fields, audit_record, replacement_rule);
  }
  if (print_query_attrs_action != nullptr) {
    print_query_attrs_action->apply(event_fields, audit_record,
                                    replacement_rule);
  }
  if (print_service_comp_action != nullptr) {
    print_service_comp_action->apply(event_fields, audit_record,
                                     replacement_rule);
  }

  const auto *log_action = actions_rule->get_action(
      EventActionType::Log, event_class_name, event_subclass_name);

  AuditAction result = AuditAction::None;

  if (log_action == nullptr) {
    result = should_log_unmatched ? AuditAction::Log : AuditAction::Skip;
  } else {
    result = log_action->apply(event_fields, audit_record, replacement_rule)
                 ? AuditAction::Log
                 : AuditAction::Skip;
  }

  // Filter replacement if any should not affect current event processing
  if (replace_filter_action != nullptr) {
    replace_filter_action->apply(event_fields, audit_record, replacement_rule);
  }

  return result;
}

}  // namespace audit_log_filter
