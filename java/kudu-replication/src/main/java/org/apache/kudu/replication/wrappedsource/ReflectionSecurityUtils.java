// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.apache.kudu.replication.wrappedsource;

import java.lang.reflect.Field;

import org.apache.kudu.client.internals.SecurityManagerCompatibility;

/**
 * Utility class for performing reflection operations with proper SecurityManager compliance.
 * All reflection operations that may be restricted by a SecurityManager are wrapped using
 * our SecurityManagerCompatibility infrastructure, which handles both legacy and modern
 * Java versions where SecurityManager support has been deprecated/removed.
 */
public final class ReflectionSecurityUtils {

  private ReflectionSecurityUtils() {
  }

  /**
   * Gets the value of a private field in one operation.
   * Useful when you need to access a field value once or infrequently.
   *
   * @param obj the object containing the field
   * @param fieldName the name of the field to access
   * @param <T> the expected type of the field value
   * @return the field value cast to the expected type
   * @throws RuntimeException if field access fails
   */
  @SuppressWarnings("unchecked")
  public static <T> T getPrivateFieldValue(Object obj, String fieldName) {
    return SecurityManagerCompatibility.get().doPrivileged(() -> {
      try {
        Field field = obj.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return (T) field.get(obj);
      } catch (Exception e) {
        throw new RuntimeException(
            String.format("Failed to access field '%s'", fieldName), e);
      }
    });
  }

  /**
   * Gets a private field and makes it accessible for repeated use.
   * Useful when we need to access a field multiple times (e.g., for metrics).
   *
   * @param obj the object containing the field
   * @param fieldName the name of the field to access
   * @return the accessible Field object
   * @throws RuntimeException if field access fails
   */
  public static Field getAccessibleField(Object obj, String fieldName) {
    return SecurityManagerCompatibility.get().doPrivileged(() -> {
      try {
        Field field = obj.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return field;
      } catch (NoSuchFieldException e) {
        throw new RuntimeException(
            String.format("Failed to access private field '%s'", fieldName), e);
      } catch (Exception e) {
        throw new RuntimeException(
            String.format("Unexpected exception accessing private field '%s'", fieldName), e);
      }
    });
  }

  /**
   * Gets the value from an already-accessible field.
   * Use this with fields obtained from getAccessibleField() for performance
   * when accessing the same field repeatedly.
   *
   * @param field the accessible field
   * @param obj the object containing the field
   * @param <T> the expected type of the field value
   * @return the field value cast to the expected type
   * @throws RuntimeException if field access fails
   */
  @SuppressWarnings("unchecked")
  public static <T> T getFieldValue(Field field, Object obj) {
    return SecurityManagerCompatibility.get().doPrivileged(() -> {
      try {
        return (T) field.get(obj);
      } catch (Exception e) {
        throw new RuntimeException("Failed to read field value", e);
      }
    });
  }

  /**
   * Gets a long value from an already-accessible field.
   * Optimized version for primitive long fields to avoid boxing/unboxing.
   *
   * @param field the accessible field
   * @param obj the object containing the field
   * @return the long field value
   * @throws RuntimeException if field access fails
   */
  public static long getLongFieldValue(Field field, Object obj) {
    return SecurityManagerCompatibility.get().doPrivileged(() -> {
      try {
        return field.getLong(obj);
      } catch (Exception e) {
        throw new RuntimeException("Failed to read long field value", e);
      }
    });
  }
}