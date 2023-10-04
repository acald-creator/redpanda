/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "utils/mutex.h"
#include "wasm/api.h"

#include <absl/container/btree_map.h>

namespace wasm {

struct cached_factory;

/**
 * A runtime that reuses factories and caches them per process as to share the
 * executable memory.
 *
 * To enable this, this runtime can only create factories on a single shard
 * (the same shard that it is created on, which is probably shard zero).
 * However, factories that are created by this runtime can be used to create
 * engines for any shard.
 *
 * Additionally, engines from this runtime's factories are reused within a
 * single shard. Ramifications of this is that failures to a single engine cause
 * the engine to be restarted and all users of a given engine must wait until
 * it's restarted to use the engine.
 */
class caching_runtime : public runtime {
public:
    explicit caching_runtime(std::unique_ptr<runtime>);
    caching_runtime(const caching_runtime&) = delete;
    caching_runtime(caching_runtime&&) = delete;
    caching_runtime& operator=(const caching_runtime&) = delete;
    caching_runtime& operator=(caching_runtime&&) = delete;
    ~caching_runtime() override;

    ss::future<> start() override;
    ss::future<> stop() override;

    /**
     * Create a factory, must be called only on a single shard.
     */
    ss::future<ss::shared_ptr<factory>>
    make_factory(model::transform_metadata, iobuf, ss::logger*) override;

private:
    /*
     * This map holds locks for creating factories.
     *
     * These mutexes are shortlived and should only live during the creation of
     * factories.
     */
    absl::btree_map<model::offset, std::unique_ptr<mutex>>
      _factory_creation_mu_map;
    std::unique_ptr<runtime> _underlying;
    absl::btree_map<model::offset, ss::shared_ptr<cached_factory>>
      _factory_cache;
};

} // namespace wasm
