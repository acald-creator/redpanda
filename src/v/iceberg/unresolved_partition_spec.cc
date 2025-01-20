// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "iceberg/unresolved_partition_spec.h"

namespace iceberg {

std::ostream&
operator<<(std::ostream& o, const unresolved_partition_spec::field& f) {
    fmt::print(
      o,
      "{{source_name: {}, transform: {}, name: {}}}",
      f.source_name,
      f.transform,
      f.name);
    return o;
}

std::ostream& operator<<(std::ostream& o, const unresolved_partition_spec& ps) {
    fmt::print(o, "{{fields: {}}}", ps.fields);
    return o;
}

} // namespace iceberg
