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

#include "crash_tracker/recorder.h"

#include "config/node_config.h"
#include "crash_tracker/logger.h"
#include "model/timestamp.h"
#include "random/generators.h"

#include <seastar/core/file.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/sleep.hh>

#include <chrono>

using namespace std::chrono_literals;

namespace crash_tracker {

static constexpr std::string_view crash_report_suffix = ".crash";

recorder& get_recorder() {
    static recorder inst;
    return inst;
}

ss::future<> recorder::start() {
    // Ensure that the crash report directory exists
    auto crash_report_dir = config::node().crash_report_dir_path();
    if (!co_await ss::file_exists(crash_report_dir.string())) {
        vlog(
          ctlog.info,
          "Creating crash report directory {}",
          crash_report_dir.string());
        co_await ss::recursive_touch_directory(crash_report_dir.string());
        vlog(
          ctlog.debug,
          "Successfully created crash report directory {}",
          crash_report_dir.string());
    }

    // Loop a few times to avoid (very unlikely) collisions in the filename
    std::optional<std::filesystem::path> crash_file_name{};
    for (int i = 0; i < 10; ++i) {
        auto time_now = model::timestamp::now().value();
        auto random_int = random_generators::get_int(0, 10000);
        auto try_name = crash_report_dir
                        / fmt::format(
                          "{}_{}{}", time_now, random_int, crash_report_suffix);
        if (co_await ss::file_exists(try_name.string())) {
            // Try again in the rare case of a collision
            continue;
        }

        crash_file_name = try_name;
        break;
    }
    if (!crash_file_name) {
        // The anti-collision above should ensure that we never reach this
        throw std::runtime_error(
          "Failed to create a unique crash recorder file");
    }

    co_await _writer.initialize(*crash_file_name);
}

ss::future<> recorder::stop() {
    std::unique_lock<ss::util::spinlock> g(_writer_lock, std::try_to_lock);
    vassert(
      g.owns_lock(),
      "stop() runs after all previous calls have completed, so we must have "
      "been able to take the _writer_lock");
    co_await _writer.release();
}

} // namespace crash_tracker
