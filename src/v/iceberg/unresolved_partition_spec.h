// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#pragma once

#include "container/fragmented_vector.h"
#include "iceberg/transform.h"

namespace iceberg {

struct unresolved_partition_spec {
    struct field {
        // Components of the nested source field name, in increasing depth
        // order.
        std::vector<ss::sstring> source_name;
        transform transform;
        ss::sstring name;

        friend std::ostream& operator<<(std::ostream&, const field&);
    };

    chunked_vector<field> fields;

    friend std::ostream&
    operator<<(std::ostream&, const unresolved_partition_spec&);
};

} // namespace iceberg
