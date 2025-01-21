/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "crash_tracker/limiter.h"

#include "base/seastarx.h"
#include "config/node_config.h"
#include "crash_tracker/logger.h"
#include "hashing/xx.h"
#include "utils/file_io.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/sleep.hh>

using namespace std::chrono_literals;

namespace crash_tracker {

// Crash tracking resets every 1h.
static constexpr model::timestamp_clock::duration crash_reset_duration{1h};

ss::future<> limiter::check_for_crash_loop(ss::abort_source& as) const {
    if (config::node().developer_mode()) {
        // crash loop tracking has value only in long running clusters
        // that can potentially accumulate state across restarts.
        co_return;
    }
    auto file_path = config::node().crash_loop_tracker_path();
    std::optional<crash_tracker_metadata> maybe_crash_md;
    if (
      // Tracking is reset every time the broker boots in recovery mode.
      !config::node().recovery_mode_enabled()
      && co_await ss::file_exists(file_path.string())) {
        // Ok to read the entire file, it contains a serialized uint32_t.
        auto buf = co_await read_fully(file_path);
        try {
            maybe_crash_md = serde::from_iobuf<crash_tracker_metadata>(
              std::move(buf));
        } catch (const serde::serde_exception&) {
            // A malformed log file, ignore and reset it later.
            // We truncate it below.
            vlog(ctlog.warn, "Ignorning malformed tracker file {}", file_path);
        }
    }

    // Compute the checksum of the current node configuration.
    auto current_config = co_await read_fully_to_string(
      config::node().get_cfg_file_path());
    auto checksum = xxhash_64(current_config.c_str(), current_config.length());

    if (maybe_crash_md) {
        auto& crash_md = maybe_crash_md.value();
        auto& limit = config::node().crash_loop_limit.value();

        // Check if it has been atleast 1h since last unsuccessful restart.
        // Tracking resets every 1h.
        auto time_since_last_start
          = model::duration_since_epoch(model::timestamp::now())
            - model::duration_since_epoch(crash_md.last_start_ts);

        auto crash_limit_ok = !limit || crash_md.crash_count <= limit.value();
        auto node_config_changed = crash_md.config_checksum != checksum;
        auto tracking_reset = time_since_last_start > crash_reset_duration;

        auto ok_to_proceed = crash_limit_ok || node_config_changed
                             || tracking_reset;

        if (!ok_to_proceed) {
            vlog(
              ctlog.error,
              "Crash loop detected. Too many consecutive crashes {}, exceeded "
              "{} configured value {}. To recover Redpanda from this state, "
              "manually remove file at path {}. Crash loop automatically "
              "resets 1h after last crash or with node configuration changes.",
              crash_md.crash_count,
              config::node().crash_loop_limit.name(),
              limit.value(),
              file_path);

            const auto crash_loop_sleep_val
              = config::node().crash_loop_sleep_sec.value();
            if (crash_loop_sleep_val) {
                vlog(
                  ctlog.info,
                  "Sleeping for {} seconds before terminating...",
                  *crash_loop_sleep_val / 1s);
                co_await ss::sleep_abortable(*crash_loop_sleep_val, as);
            }

            throw crash_loop_limit_reached();
        }

        vlog(
          ctlog.debug,
          "Consecutive crashes detected: {} node config changed: {} "
          "time based tracking reset: {}",
          crash_md.crash_count,
          node_config_changed,
          tracking_reset);

        if (node_config_changed || tracking_reset) {
            crash_md.crash_count = 0;
        }
    }

    // Truncate and bump the crash count. We consider a run to be unclean by
    // default unless the scheduled cleanup (that runs very late in shutdown)
    // resets the file. See schedule_crash_tracker_file_cleanup().
    auto new_crash_count = maybe_crash_md
                             ? maybe_crash_md.value().crash_count + 1
                             : 1;
    crash_tracker_metadata updated{
      .crash_count = new_crash_count,
      .config_checksum = checksum,
      .last_start_ts = model::timestamp::now()};
    co_await write_fully(file_path, serde::to_iobuf(updated));
    co_await ss::sync_directory(
      config::node().data_directory.value().as_sstring());
}

ss::future<> limiter::record_clean_shutdown() const {
    auto file = config::node().crash_loop_tracker_path().string();
    if (co_await ss::file_exists(file)) {
        co_await ss::remove_file(file);
        co_await ss::sync_directory(
          config::node().data_directory().as_sstring());
        vlog(ctlog.debug, "Deleted crash loop tracker file: {}", file);
    }
    co_return;
}

} // namespace crash_tracker
