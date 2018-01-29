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
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.util.Preconditions;

public class KuduRow implements Serializable {

    private Map<String, Object> rowValues;

    public KuduRow(Map<String, Object> rowValues) {
        this.rowValues = Preconditions.checkNotNull(rowValues, "Row values cannot be null");
    }

    public KuduRow(Object object) {
        this.rowValues = new HashMap<>();
        for (Class<?> c = object.getClass(); c != null; c = c.getSuperclass()) {
            Field[] fields = c.getDeclaredFields();
            for (Field cField : fields) {
                try {
                    if (!Modifier.isStatic(cField.getModifiers())
                            && !Modifier.isTransient(cField.getModifiers())) {
                        cField.setAccessible(true);
                        rowValues.put(cField.getName(), cField.get(object));
                    }
                } catch (IllegalAccessException e) {
                    String error = String.format("Cannot get value for %s", cField.getName());
                    throw new IllegalArgumentException(error, e);
                }
            }
        }
    }

    public Map<String,Object> blindMap() {
        return new HashMap<>(rowValues);
    }

    public <P> P blind(Class<P> clazz) {
        P o = createInstance(clazz);

        for (Class<?> c = clazz; c != null; c = c.getSuperclass()) {
            Field[] fields = c.getDeclaredFields();
            for (Field cField : fields) {
                try {
                    if(rowValues.containsKey(cField.getName())
                            && !Modifier.isStatic(cField.getModifiers())
                            && !Modifier.isTransient(cField.getModifiers())) {

                        cField.setAccessible(true);
                        cField.set(o, rowValues.get(cField.getName()));
                    }
                } catch (IllegalAccessException e) {
                    String error = String.format("Cannot get value for %s", cField.getName());
                    throw new IllegalArgumentException(error, e);
                }
            }
        }

        return o;

    }

    protected Object getValue(String keyValue) {
        return rowValues.get(keyValue);
    }

    private <P> P createInstance(Class<P> clazz) {
        try {
            return clazz.getConstructor().newInstance();
        } catch (ReflectiveOperationException e) {
            String error = String.format("Cannot create instance for %s", clazz.getSimpleName());
            throw new IllegalArgumentException(error, e);
        }
    }

}
