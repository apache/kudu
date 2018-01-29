/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kudu.flink.connector;

import java.io.Serializable;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Type;

public class KuduColumnInfo implements Serializable {

    protected final String name;
    protected final Type type;
    protected final boolean key;
    protected final boolean rangeKey;
    protected final boolean nullable;
    protected final Object defaultValue;
    protected final int blockSize;
    protected final Encoding encoding;
    protected final Compression compression;

    private KuduColumnInfo(String name, Type type,
                           boolean key, boolean rangeKey, boolean nullable,
                           Object defaultValue, int blockSize,
                           Encoding encoding, Compression compression) {
        this.name = name;
        this.type = type;
        this.key = key;
        this.nullable = nullable;
        this.defaultValue = defaultValue;
        this.blockSize = blockSize;
        this.encoding = encoding;
        this.compression = compression;
        this.rangeKey = rangeKey;
    }

    protected ColumnSchema columnSchema() {
        return new ColumnSchema.ColumnSchemaBuilder(name, type)
                    .key(key)
                    .nullable(nullable)
                    .defaultValue(defaultValue)
                    .desiredBlockSize(blockSize)
                    .encoding(encoding.encode)
                    .compressionAlgorithm(compression.algorithm)
                    .build();
    }

    public static class Builder {
        private final String name;
        private final Type type;
        private boolean key = false;
        private boolean rangeKey = false;
        private boolean nullable = false;
        private Object defaultValue = null;
        private int blockSize = 0;
        private KuduColumnInfo.Encoding encoding = Encoding.AUTO;
        private KuduColumnInfo.Compression compression = Compression.DEFAULT;

        private Builder(String name, Type type) {
            this.name = name;
            this.type = type;
        }

        public static KuduColumnInfo.Builder create(String name, Type type) {
            return new Builder(name, type);
        }

        public KuduColumnInfo.Builder key(boolean key) {
            this.key = key;
            return this;
        }

        public KuduColumnInfo.Builder rangeKey(boolean rangeKey) {
            this.rangeKey = rangeKey;
            return this;
        }

        public KuduColumnInfo.Builder nullable(boolean nullable) {
            this.nullable = nullable;
            return this;
        }

        public KuduColumnInfo.Builder defaultValue(Object defaultValue) {
            this.defaultValue = defaultValue;
            return this;
        }

        public KuduColumnInfo.Builder desiredBlockSize(int blockSize) {
            this.blockSize = blockSize;
            return this;
        }

        public KuduColumnInfo.Builder encoding(KuduColumnInfo.Encoding encoding) {
            this.encoding = encoding;
            return this;
        }

        public KuduColumnInfo.Builder compressionAlgorithm(KuduColumnInfo.Compression compression) {
            this.compression = compression;
            return this;
        }

        public KuduColumnInfo build() {
            return new KuduColumnInfo(this.name, this.type, this.key, this.rangeKey, this.nullable, this.defaultValue, this.blockSize, this.encoding, this.compression);
        }
    }

    public enum Compression {
        UNKNOWN(ColumnSchema.CompressionAlgorithm.UNKNOWN),
        DEFAULT(ColumnSchema.CompressionAlgorithm.DEFAULT_COMPRESSION),
        WITHOUT(ColumnSchema.CompressionAlgorithm.NO_COMPRESSION),
        SNAPPY(ColumnSchema.CompressionAlgorithm.SNAPPY),
        LZ4(ColumnSchema.CompressionAlgorithm.LZ4),
        ZLIB(ColumnSchema.CompressionAlgorithm.ZLIB);

        final ColumnSchema.CompressionAlgorithm algorithm;

        Compression(ColumnSchema.CompressionAlgorithm algorithm) {
            this.algorithm = algorithm;
        }
    }

    public enum Encoding {
        UNKNOWN(ColumnSchema.Encoding.UNKNOWN),
        AUTO(ColumnSchema.Encoding.AUTO_ENCODING),
        PLAIN(ColumnSchema.Encoding.PLAIN_ENCODING),
        PREFIX(ColumnSchema.Encoding.PREFIX_ENCODING),
        GROUP_VARINT(ColumnSchema.Encoding.GROUP_VARINT),
        RLE(ColumnSchema.Encoding.RLE),
        DICT(ColumnSchema.Encoding.DICT_ENCODING),
        BIT_SHUFFLE(ColumnSchema.Encoding.BIT_SHUFFLE);

        final ColumnSchema.Encoding encode;

        Encoding(ColumnSchema.Encoding encode) {
            this.encode = encode;
        }
    }

}
