// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.kudu.client;

import com.google.common.base.Preconditions;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * This class represents a range partition schema with table-wide hash schema.
 *
 * See also RangePartitionWithCustomHashSchema.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
class RangePartition {
  final PartialRow lowerBound;
  final PartialRow upperBound;
  final RangePartitionBound lowerBoundType;
  final RangePartitionBound upperBoundType;

  public RangePartition(PartialRow lowerBound,
                        PartialRow upperBound,
                        RangePartitionBound lowerBoundType,
                        RangePartitionBound upperBoundType) {
    Preconditions.checkNotNull(lowerBound);
    Preconditions.checkNotNull(upperBound);
    Preconditions.checkArgument(
        lowerBound.getSchema().equals(upperBound.getSchema()));
    this.lowerBound = lowerBound;
    this.upperBound = upperBound;
    this.lowerBoundType = lowerBoundType;
    this.upperBoundType = upperBoundType;
  }

  public PartialRow getLowerBound() {
    return lowerBound;
  }

  public RangePartitionBound getLowerBoundType() {
    return lowerBoundType;
  }

  public PartialRow getUpperBound() {
    return upperBound;
  }

  public RangePartitionBound getUpperBoundType() {
    return upperBoundType;
  }
}
