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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.common.base.Predicates;
import com.google.common.collect.Iterators;
import com.stumbleupon.async.Deferred;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

import org.apache.kudu.Schema;

/**
 * A KuduTable represents a table on a particular cluster. It holds the current
 * schema of the table. Any given KuduTable instance belongs to a specific AsyncKuduClient
 * instance.
 *
 * Upon construction, the table is looked up in the catalog (or catalog cache),
 * and the schema fetched for introspection. The schema is not kept in sync with the master.
 *
 * This class is thread-safe.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class KuduTable {

  private final Schema schema;
  private final PartitionSchema partitionSchema;
  private final AsyncKuduClient client;
  private final String name;
  private final String tableId;
  private final int numReplicas;
  private final Map<String, String> extraConfig;
  private final String owner;
  private final String comment;

  /**
   * Package-private constructor, use {@link KuduClient#openTable(String)} to get an instance.
   * @param client the client this instance belongs to
   * @param name this table's name
   * @param tableId this table's UUID
   * @param schema this table's schema
   * @param partitionSchema this table's partition schema
   * @param numReplicas this table's replication factor
   * @param extraConfig this table's extra configuration properties
   * @param owner this table's owner
   * @param comment this table's comment
   */
  KuduTable(AsyncKuduClient client, String name, String tableId,
            Schema schema, PartitionSchema partitionSchema, int numReplicas,
            Map<String, String> extraConfig, String owner, String comment) {
    this.schema = schema;
    this.partitionSchema = partitionSchema;
    this.client = client;
    this.name = name;
    this.tableId = tableId;
    this.numReplicas = numReplicas;
    this.extraConfig = extraConfig;
    this.owner = owner;
    this.comment = comment;
  }

  /**
   * Get this table's schema, as of the moment this instance was created.
   * @return this table's schema
   */
  public Schema getSchema() {
    return this.schema;
  }

  /**
   * Gets the table's partition schema.
   *
   * This method is new, and not considered stable or suitable for public use.
   *
   * @return the table's partition schema.
   */
  @InterfaceAudience.LimitedPrivate("Impala")
  @InterfaceStability.Unstable
  public PartitionSchema getPartitionSchema() {
    return partitionSchema;
  }

  /**
   * Get this table's name.
   * @return this table's name
   */
  public String getName() {
    return this.name;
  }

  /**
   * Get this table's unique identifier.
   * @return this table's tableId
   */
  public String getTableId() {
    return tableId;
  }

  /**
   * Get this table's replication factor.
   * @return this table's replication factor
   */
  public int getNumReplicas() {
    return numReplicas;
  }

  /**
   * Get this table's extra configuration properties.
   * @return this table's extra configuration properties
   */
  public Map<String, String> getExtraConfig() {
    return extraConfig;
  }

  /**
   * Get this table's owner.
   * @return this table's owner or an empty string if the table was created without owner on a
   *  version of Kudu that didn't automatically assign an owner.
   */
  public String getOwner() {
    return owner;
  }

  /**
   * Get this table's comment.
   *
   * @return this table's comment.
   */
  public String getComment() {
    return comment;
  }

  /**
   * Get the async client that created this instance.
   * @return an async kudu client
   */
  public AsyncKuduClient getAsyncClient() {
    return this.client;
  }

  /**
   * Get a new insert configured with this table's schema. The returned object should not be reused.
   * @return an insert with this table's schema
   */
  public Insert newInsert() {
    return new Insert(this);
  }

  /**
   * Get a new update configured with this table's schema. The returned object should not be reused.
   * @return an update with this table's schema
   */
  public Update newUpdate() {
    return new Update(this);
  }

  /**
   * Get a new delete configured with this table's schema. The returned object should not be reused.
   * @return a delete with this table's schema
   */
  public Delete newDelete() {
    return new Delete(this);
  }

  /**
   * Get a new upsert configured with this table's schema. The returned object should not be reused.
   * @return an upsert with this table's schema
   * @throws UnsupportedOperationException if the table has auto-incrementing column
   */
  public Upsert newUpsert() {
    if (schema.hasAutoIncrementingColumn()) {
      throw new UnsupportedOperationException(
          "Tables with auto-incrementing column do not support UPSERT operations");
    }
    return new Upsert(this);
  }

  /**
   * Get a new upsert ignore configured with this table's schema. The operation ignores errors of
   * updating immutable cells in a row. This is useful when upserting rows in a table with immutable
   * columns.
   * @return an upsert with this table's schema
   * @throws UnsupportedOperationException if the table has auto-incrementing column
   */
  public UpsertIgnore newUpsertIgnore() {
    if (schema.hasAutoIncrementingColumn()) {
      throw new UnsupportedOperationException(
          "Tables with auto-incrementing column do not support UPSERT_IGNORE operations");
    }
    return new UpsertIgnore(this);
  }

  /**
   * Get a new insert ignore configured with this table's schema. An insert ignore will
   * ignore duplicate row errors. This is useful when the same insert may be sent multiple times.
   * The returned object should not be reused.
   * @return an insert ignore with this table's schema
   */
  public InsertIgnore newInsertIgnore() {
    return new InsertIgnore(this);
  }

  /**
   * Get a new update ignore configured with this table's schema. An update ignore will
   * ignore missing row errors and updating on immutable columns errors. This is useful to
   * update a row only if it exists, or update a row with immutable columns.
   * The returned object should not be reused.
   * @return an update ignore with this table's schema
   */
  public UpdateIgnore newUpdateIgnore() {
    return new UpdateIgnore(this);
  }

  /**
   * Get a new delete ignore configured with this table's schema. An delete ignore will
   * ignore missing row errors. This is useful to delete a row only if it exists.
   * The returned object should not be reused.
   * @return a delete ignore with this table's schema
   */
  public DeleteIgnore newDeleteIgnore() {
    return new DeleteIgnore(this);
  }

  /**
   * Asynchronously get all the tablets for this table.
   * @param deadline max time spent in milliseconds for the deferred result of this method to
   *         get called back, if deadline is reached, the deferred result will get erred back
   * @return a {@link Deferred} object that yields a list containing the metadata and
   * locations for each of the tablets in the table
   * @deprecated use the {@link KuduScanToken} API
   */
  @Deprecated
  public Deferred<List<LocatedTablet>> asyncGetTabletsLocations(long deadline) {
    return asyncGetTabletsLocations(null, null, deadline);
  }

  /**
   * Asynchronously get all or some tablets for this table.
   * @param startKey where to start in the table, pass null to start at the beginning
   * @param endKey where to stop in the table (exclusive), pass null to get all the tablets until
   *               the end of the table
   * @param deadline max time spent in milliseconds for the deferred result of this method to
   *         get called back, if deadline is reached, the deferred result will get erred back
   * @return a {@link Deferred} object that yields a list containing the metadata and locations
   *           for each of the tablets in the table
   * @deprecated use the {@link KuduScanToken} API
   */
  @Deprecated
  public Deferred<List<LocatedTablet>> asyncGetTabletsLocations(byte[] startKey,
                                                                byte[] endKey,
                                                                long deadline) {
    return client.locateTable(this, startKey, endKey,
                              AsyncKuduClient.FETCH_TABLETS_PER_RANGE_LOOKUP,
                              deadline);
  }

  /**
   * Get all the tablets for this table. This may query the master multiple times if there
   * are a lot of tablets.
   * @param deadline deadline in milliseconds for this method to finish
   * @return a list containing the metadata and locations for each of the tablets in the
   *         table
   * @throws Exception
   * @deprecated use the {@link KuduScanToken} API
   */
  @Deprecated
  public List<LocatedTablet> getTabletsLocations(long deadline) throws Exception {
    return getTabletsLocations(null, null, deadline);
  }

  /**
   * Get all or some tablets for this table. This may query the master multiple times if there
   * are a lot of tablets.
   * This method blocks until it gets all the tablets.
   * @param startKey where to start in the table, pass null to start at the beginning
   * @param endKey where to stop in the table (exclusive), pass null to get all the tablets until
   *               the end of the table
   * @param deadline deadline in milliseconds for this method to finish
   * @return a list containing the metadata and locations for each of the tablets in the
   *         table
   * @throws Exception
   * @deprecated use the {@link KuduScanToken} API
   */
  @Deprecated
  public List<LocatedTablet> getTabletsLocations(byte[] startKey,
                                                 byte[] endKey,
                                                 long deadline) throws Exception {
    return client.syncLocateTable(this, startKey, endKey,
                                  AsyncKuduClient.FETCH_TABLETS_PER_RANGE_LOOKUP,
                                  deadline);
  }

  /**
   * Retrieves a formatted representation of this table's range partitions. The
   * range partitions will be returned in sorted order by value, and will
   * contain no duplicates.
   *
   * @param timeout the timeout of the operation
   * @return a list of the formatted range partitions
   */
  @InterfaceAudience.LimitedPrivate("Impala")
  @InterfaceStability.Unstable
  public List<String> getFormattedRangePartitions(long timeout) throws Exception {
    List<Partition> rangePartitions = getRangePartitions(timeout);
    List<String> formattedPartitions = new ArrayList<>();
    for (Partition partition : rangePartitions) {
      formattedPartitions.add(partition.formatRangePartition(this, false));
    }
    return formattedPartitions;
  }

  /**
   * Retrieves a formatted representation of this table's range partitions along
   * with hash schema output for each range. The range partitions are returned
   * in sorted order by value and contain no duplicates.
   *
   * @param timeout the timeout of the operation
   * @return a list of the formatted range partitions with hash schema for each
   */
  @InterfaceAudience.LimitedPrivate("Impala")
  @InterfaceStability.Unstable
  public List<String> getFormattedRangePartitionsWithHashSchema(long timeout)
      throws Exception {
    List<Partition> rangePartitions = getRangePartitions(timeout);
    List<String> formattedPartitions = new ArrayList<>();
    for (Partition partition : rangePartitions) {
      formattedPartitions.add(partition.formatRangePartition(this, true));
    }
    return formattedPartitions;
  }

  /**
   * Retrieves this table's range partitions. The range partitions will be returned
   * in sorted order by value, and will contain no duplicates.
   *
   * @param timeout the timeout of the operation
   * @return a list of the formatted range partitions
   */
  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public List<Partition> getRangePartitions(long timeout) throws Exception {
    // TODO: This could be moved into the RangeSchemaPB returned from server
    // to avoid an extra call to get the range partitions.
    return getRangePartitionsHelper(timeout, false);
  }

  /**
   * Only retrieves this table's range partitions that contain the table wide hash schema. The
   * range partitions will be returned in sorted order by value, and will contain no duplicates.
   *
   * @param timeout the timeout of the operation
   * @return a list of the formatted range partitions
   */
  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public List<Partition> getRangePartitionsWithTableHashSchema(long timeout) throws Exception {
    return getRangePartitionsHelper(timeout, true);
  }

  /**
   * Helper method that retrieves the table's range partitions. If onlyTableHashSchema is evaluated
   * to true, then only range partitions that have the table wide hash schema will be returned. The
   * range partitions will be returned in sorted order by value and will contain no duplicates.
   * @param timeout the timeout of the operation
   * @param onlyTableHashSchema whether to filter out the partitions with custom hash schema
   * @return a list of the formatted range partitions
   */
  private List<Partition> getRangePartitionsHelper(long timeout,
                                                   boolean onlyTableHashSchema) throws Exception {
    List<Partition> rangePartitions = new ArrayList<>();
    List<KuduScanToken> scanTokens = new KuduScanToken.KuduScanTokenBuilder(client, this)
        .setTimeout(timeout)
        .build();
    for (KuduScanToken token : scanTokens) {
      Partition partition = token.getTablet().getPartition();
      // Filter duplicate range partitions by taking only the tablets whose hash
      // partitions are all 0s.
      if (!Iterators.all(partition.getHashBuckets().iterator(), Predicates.equalTo(0))) {
        continue;
      }
      // If onlyTableHashSchema is true, filter out any partitions
      // that are part of a range that contains a custom hash schema.
      if (onlyTableHashSchema && partitionSchema.getHashSchemaForRange(partition.rangeKeyStart) !=
          partitionSchema.getHashBucketSchemas()) {
        continue;
      }
      rangePartitions.add(partition);
    }
    return rangePartitions;
  }

  /**
   * Get this table's statistics.
   * @return this table's statistics
   */
  public KuduTableStatistics getTableStatistics() throws KuduException {
    return client.syncClient().getTableStatistics(name);
  }
}
