// Copyright (c) 2015, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
package org.kududb.client;

import org.kududb.Schema;
import org.kududb.annotations.InterfaceAudience;
import org.kududb.annotations.InterfaceStability;

import java.util.List;

/**
 * A partition schema describes how the rows of a table are distributed among
 * tablets.
 *
 * Primarily, a table's partition schema is responsible for translating the
 * primary key column values of a row into a partition key that can be used to
 * find the tablet containing the key.
 *
 * The partition schema is made up of zero or more hash bucket components,
 * followed by a single range component.
 *
 * Each hash bucket component includes one or more columns from the primary key
 * column set, with the restriction that an individual primary key column may
 * only be included in a single hash component.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
class PartitionSchema {

  private final RangeSchema rangeSchema;
  private final List<HashBucketSchema> hashBucketSchemas;
  private final boolean isSimple;

  /**
   * Creates a new partition schema from the range and hash bucket schemas.
   *
   * @param rangeSchema the range schema
   * @param hashBucketSchemas the hash bucket schemas
   * @param schema the table schema
   */
  PartitionSchema(RangeSchema rangeSchema,
                  List<HashBucketSchema> hashBucketSchemas,
                  Schema schema) {
    this.rangeSchema = rangeSchema;
    this.hashBucketSchemas = hashBucketSchemas;

    boolean isSimple = hashBucketSchemas.isEmpty()
        && rangeSchema.columns.size() == schema.getPrimaryKeyColumnCount();
    if (isSimple) {
      int i = 0;
      for (Integer id : rangeSchema.columns) {
        if (schema.getColumnIndex(id) != i++) {
          isSimple = false;
          break;
        }
      }
    }
    this.isSimple = isSimple;
  }

  /**
   * Returns the encoded partition key of the row.
   * @return a byte array containing the encoded partition key of the row
   */
  public byte[] encodePartitionKey(PartialRow row) {
    return new KeyEncoder().encodePartitionKey(row, this);
  }

  public RangeSchema getRangeSchema() {
    return rangeSchema;
  }

  public List<HashBucketSchema> getHashBucketSchemas() {
    return hashBucketSchemas;
  }

  /**
   * Returns true if the partition schema if the partition schema does not include any hash
   * components, and the range columns match the table's primary key columns.
   *
   * @return whether the partition schema is the default simple range partitioning.
   */
  boolean isSimpleRangePartitioning() {
    return isSimple;
  }

  public static class RangeSchema {
    private final List<Integer> columns;

    RangeSchema(List<Integer> columns) {
      this.columns = columns;
    }

    public List<Integer> getColumns() {
      return columns;
    }
  }

  public static class HashBucketSchema {
    private final List<Integer> columnIds;
    private int numBuckets;
    private int seed;

    HashBucketSchema(List<Integer> columnIds, int numBuckets, int seed) {
      this.columnIds = columnIds;
      this.numBuckets = numBuckets;
      this.seed = seed;
    }

    /**
     * Gets the column IDs of the columns in the hash partition.
     * @return the column IDs of the columns in the has partition
     */
    public List<Integer> getColumnIds() {
      return columnIds;
    }

    public int getNumBuckets() {
      return numBuckets;
    }

    public int getSeed() {
      return seed;
    }
  }
}
