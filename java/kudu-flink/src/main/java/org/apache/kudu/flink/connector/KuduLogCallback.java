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

import com.stumbleupon.async.Callback;
import java.util.List;
import org.apache.kudu.client.OperationResponse;
import org.apache.kudu.client.RowError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class KuduLogCallback implements Callback<Boolean, List<OperationResponse>> {

    private static final Logger LOG = LoggerFactory.getLogger(KuduLogCallback.class);

    @Override
    public Boolean call(List<OperationResponse> operationResponses) {
        Boolean isOk = operationResponses.isEmpty();
        for(OperationResponse operationResponse : operationResponses) {
            logError(operationResponse.getRowError());
        }
        return isOk;
    }

    private void logError(RowError error) {
        LOG.error("Error {} on {}: {} ", error.getErrorStatus(), error.getOperation(), error.toString());
    }
}
