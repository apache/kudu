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

package org.apache.kudu.hive.serde;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by bimal on 4/11/16.
 */

@SuppressWarnings("WeakerAccess")
public final class HiveKuduConstants {

    /** Table Properties
     * used in the hive table definition when creating a new table
     * */
    private static final String KUDU_PROPERTY_PREFIX = "kudu.";

    /** comma-separated list of "host:port" pairs of the masters */
    public static final String MASTER_ADDRESS_NAME = KUDU_PROPERTY_PREFIX + "master_addresses";
    /**name of the table in kudu */
    public static final String TABLE_NAME = KUDU_PROPERTY_PREFIX + "table_name";
    /**key columns */
    public static final String KEY_COLUMNS = KUDU_PROPERTY_PREFIX + "key_columns";


    /** MapReduce Properties */
    public static final String MR_PROPERTY_PREFIX = KUDU_PROPERTY_PREFIX + "mapreduce.";

    /** Job parameter that specifies the input table. */
    public static final String INPUT_TABLE_KEY = MR_PROPERTY_PREFIX + "input.table";
    /** Job parameter that specifies the input table. */
    public static final String OUTPUT_TABLE_KEY = MR_PROPERTY_PREFIX + "output.table";
    /** Job parameter that specifies where the masters are. */
    public static final String MASTER_ADDRESSES_KEY = MR_PROPERTY_PREFIX + "master.addresses";
    /** Job parameter that specifies where the masters are. */
    public static final String MASTER_ADDRESS_KEY = MR_PROPERTY_PREFIX + "master.address";
    /** Job parameter that specifies how long we wait for operations to complete (default: 10s). */
    public static final String OPERATION_TIMEOUT_MS_KEY = MR_PROPERTY_PREFIX + "operation.timeout.ms";
    /** Job parameter that specifies the encoded column predicates (may be empty). */
    public static final String ENCODED_PREDICATES_KEY = MR_PROPERTY_PREFIX + "encoded.predicates";

    public static final Map<String,String> KUDU_TO_MAPREDUCE_MAPPING = mapreduceToKuduProperties();

    private HiveKuduConstants() {
    }

    private static Map<String,String> mapreduceToKuduProperties() {
        Map<String,String> returner = new HashMap<>();
        returner.put(INPUT_TABLE_KEY, TABLE_NAME);
        returner.put(OUTPUT_TABLE_KEY, TABLE_NAME);
        returner.put(MASTER_ADDRESSES_KEY, MASTER_ADDRESS_NAME);
        returner.put(MASTER_ADDRESS_KEY, MASTER_ADDRESS_NAME);
        returner.put(MASTER_ADDRESS_NAME, MASTER_ADDRESS_NAME);
        returner.put(TABLE_NAME, TABLE_NAME);
        returner.put(KEY_COLUMNS, KEY_COLUMNS);
        return returner;
    }
}