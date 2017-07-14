/**
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */

package org.apache.kudu.mapreduce.tools;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.api.DelegatingReadSupport;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.example.GroupReadSupport;

/**
 * Read support for Apache Parquet.
 */
public final class ParquetReadSupport extends DelegatingReadSupport<Group> {

  public ParquetReadSupport() {
    super(new GroupReadSupport());
  }

  @Override
  public org.apache.parquet.hadoop.api.ReadSupport.ReadContext init(InitContext context) {
    return super.init(context);
  }
}
