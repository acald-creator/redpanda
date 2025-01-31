/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "base/seastarx.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/scheduling.hh>

// manage cpu scheduling groups. scheduling groups are global, so one instance
// of this class can be created at the top level and passed down into any server
// and any shard that needs to schedule continuations into a given group.
class scheduling_groups final {
public:
    ss::future<> create_groups() {
        _admin = co_await ss::create_scheduling_group("admin", 100);
        _raft = co_await ss::create_scheduling_group("raft", 1000);
        _kafka = co_await ss::create_scheduling_group("kafka", 1000);
        _cluster = co_await ss::create_scheduling_group("cluster", 300);
        _cache_background_reclaim = co_await ss::create_scheduling_group(
          "cache_background_reclaim", 200);
        _compaction = co_await ss::create_scheduling_group(
          "log_compaction", 100);
        _raft_learner_recovery = co_await ss::create_scheduling_group(
          "raft_learner_recovery", 50);
        _archival_upload = co_await ss::create_scheduling_group(
          "archival_upload", 100);
        /**
         * Raft group to run the raft heartbeats in. This group has the highest
         * priority to make sure Raft failure detector is not starved even in
         * highly loaded clusters.
         */
        _raft_heartbeats = co_await ss::create_scheduling_group(
          "raft_hb", 1500);
        /**
         * Group used to schedule a self test.
         */
        _self_test = co_await ss::create_scheduling_group("self_test", 100);
        _fetch = co_await ss::create_scheduling_group("fetch", 1000);
        _transforms = co_await ss::create_scheduling_group("transforms", 100);
        _datalake = co_await ss::create_scheduling_group("datalake", 100);
        _produce = co_await ss::create_scheduling_group("produce", 1000);
    }

    ss::future<> destroy_groups() {
        co_await destroy_scheduling_group(_admin);
        co_await destroy_scheduling_group(_raft);
        co_await destroy_scheduling_group(_kafka);
        co_await destroy_scheduling_group(_cluster);
        co_await destroy_scheduling_group(_cache_background_reclaim);
        co_await destroy_scheduling_group(_compaction);
        co_await destroy_scheduling_group(_raft_learner_recovery);
        co_await destroy_scheduling_group(_archival_upload);
        co_await destroy_scheduling_group(_raft_heartbeats);
        co_await destroy_scheduling_group(_self_test);
        co_await destroy_scheduling_group(_fetch);
        co_await destroy_scheduling_group(_transforms);
        co_await destroy_scheduling_group(_datalake);
        co_await destroy_scheduling_group(_produce);
    }

    ss::scheduling_group admin_sg() { return _admin; }
    ss::scheduling_group raft_sg() { return _raft; }
    ss::scheduling_group kafka_sg() { return _kafka; }
    ss::scheduling_group cluster_sg() { return _cluster; }

    ss::scheduling_group cache_background_reclaim_sg() {
        return _cache_background_reclaim;
    }
    ss::scheduling_group compaction_sg() { return _compaction; }
    ss::scheduling_group raft_learner_recovery_sg() {
        return _raft_learner_recovery;
    }
    ss::scheduling_group archival_upload() { return _archival_upload; }
    ss::scheduling_group raft_heartbeats() { return _raft_heartbeats; }
    ss::scheduling_group self_test_sg() { return _self_test; }
    ss::scheduling_group transforms_sg() { return _transforms; }
    ss::scheduling_group datalake_sg() { return _datalake; }
    /**
     * @brief Scheduling group for fetch requests.
     *
     * This scheduling group is used for consumer fetch processing. We assign
     * it the same priority as the default group (where most other kafka
     * handling takes place), but by putting it into its own group we prevent
     * non-fetch requests from being significantly delayed when fetch requests
     * use all the CPU.
     */
    ss::scheduling_group fetch_sg() { return _fetch; }
    /**
     * Scheduling group for produce requests.
     *
     * We use separate scheduling group for produce requests to prevent them
     * from negatively impacting other work executed in the default scheduling
     * group.
     */
    ss::scheduling_group produce_sg() { return _produce; }

    std::vector<std::reference_wrapper<const ss::scheduling_group>>
    all_scheduling_groups() const {
        return {
          std::cref(_default),
          std::cref(_admin),
          std::cref(_raft),
          std::cref(_kafka),
          std::cref(_cluster),
          std::cref(_cache_background_reclaim),
          std::cref(_compaction),
          std::cref(_raft_learner_recovery),
          std::cref(_archival_upload),
          std::cref(_raft_heartbeats),
          std::cref(_self_test),
          std::cref(_fetch),
          std::cref(_transforms),
          std::cref(_datalake),
          std::cref(_produce)};
    }

private:
    ss::scheduling_group _default{
      seastar::default_scheduling_group()}; // created and managed by seastar
    ss::scheduling_group _admin;
    ss::scheduling_group _raft;
    ss::scheduling_group _kafka;
    ss::scheduling_group _cluster;
    ss::scheduling_group _cache_background_reclaim;
    ss::scheduling_group _compaction;
    ss::scheduling_group _raft_learner_recovery;
    ss::scheduling_group _archival_upload;
    ss::scheduling_group _raft_heartbeats;
    ss::scheduling_group _self_test;
    ss::scheduling_group _fetch;
    ss::scheduling_group _transforms;
    ss::scheduling_group _datalake;
    ss::scheduling_group _produce;
};
