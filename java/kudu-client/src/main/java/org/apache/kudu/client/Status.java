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

import org.apache.kudu.WireProtocol;
import org.apache.kudu.annotations.InterfaceAudience;
import org.apache.kudu.annotations.InterfaceStability;
import org.apache.kudu.master.Master;
import org.apache.kudu.tserver.Tserver;

/**
 * Representation of an error code and message.
 * See also {@code src/kudu/util/status.h} in the C++ codebase.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class Status {

  // Keep a single OK status object else we'll end up instantiating tons of them.
  private static final Status STATIC_OK = new Status(WireProtocol.AppStatusPB.ErrorCode.OK);

  private final WireProtocol.AppStatusPB appStatusPB;

  private Status(WireProtocol.AppStatusPB appStatusPB) {
    this.appStatusPB = appStatusPB;
  }

  private Status(WireProtocol.AppStatusPB.ErrorCode code, String msg, int posixCode) {
    this.appStatusPB =
        WireProtocol.AppStatusPB.newBuilder()
            .setCode(code)
            .setMessage(msg)
            .setPosixCode(posixCode)
            .build();
  }

  private Status(WireProtocol.AppStatusPB.ErrorCode code, String msg) {
    this(code, msg, -1);
  }

  private Status(WireProtocol.AppStatusPB.ErrorCode code) {
    this(code, "", -1);
  }

  // Factory methods.

  /**
   * Create a status object from a master error.
   * @param masterErrorPB pb object received via RPC from the master
   * @return status object equivalent to the pb
   */
  static Status fromMasterErrorPB(Master.MasterErrorPB masterErrorPB) {
    if (masterErrorPB == Master.MasterErrorPB.getDefaultInstance()) {
      return Status.OK();
    } else {
      return new Status(masterErrorPB.getStatus());
    }
  }

  /**
   * Create a status object from a tablet server error.
   * @param tserverErrorPB pb object received via RPC from the TS
   * @return status object equivalent to the pb
   */
  static Status fromTabletServerErrorPB(Tserver.TabletServerErrorPB tserverErrorPB) {
    if (tserverErrorPB == Tserver.TabletServerErrorPB.getDefaultInstance()) {
      return Status.OK();
    } else {
      return new Status(tserverErrorPB.getStatus());
    }
  }

  /**
   * Create a Status object from a {@link WireProtocol.AppStatusPB} protobuf object.
   * Package-private because we shade Protobuf and this is not usable outside this package.
   */
  static Status fromPB(WireProtocol.AppStatusPB pb) {
    return new Status(pb);
  }
  // CHECKSTYLE:OFF
  public static Status OK() {
    return STATIC_OK;
  }

  public static Status NotFound(String msg) {
    return new Status(WireProtocol.AppStatusPB.ErrorCode.NOT_FOUND, msg);
  }
  public static Status NotFound(String msg, int posixCode) {
    return new Status(WireProtocol.AppStatusPB.ErrorCode.NOT_FOUND, msg, posixCode);
  }

  public static Status Corruption(String msg) {
    return new Status(WireProtocol.AppStatusPB.ErrorCode.CORRUPTION, msg);
  }
  public static Status Corruption(String msg, int posixCode) {
    return new Status(WireProtocol.AppStatusPB.ErrorCode.CORRUPTION, msg, posixCode);
  }

  public static Status NotSupported(String msg) {
    return new Status(WireProtocol.AppStatusPB.ErrorCode.NOT_SUPPORTED, msg);
  }
  public static Status NotSupported(String msg, int posixCode) {
    return new Status(WireProtocol.AppStatusPB.ErrorCode.NOT_SUPPORTED, msg, posixCode);
  }

  public static Status InvalidArgument(String msg) {
    return new Status(WireProtocol.AppStatusPB.ErrorCode.INVALID_ARGUMENT, msg);
  }
  public static Status InvalidArgument(String msg, int posixCode) {
    return new Status(WireProtocol.AppStatusPB.ErrorCode.INVALID_ARGUMENT, msg, posixCode);
  }

  public static Status IOError(String msg) {
    return new Status(WireProtocol.AppStatusPB.ErrorCode.IO_ERROR, msg);
  }
  public static Status IOError(String msg, int posixCode) {
    return new Status(WireProtocol.AppStatusPB.ErrorCode.IO_ERROR, msg, posixCode);
  }

  public static Status AlreadyPresent(String msg) {
    return new Status(WireProtocol.AppStatusPB.ErrorCode.ALREADY_PRESENT, msg);
  }
  public static Status AlreadyPresent(String msg, int posixCode) {
    return new Status(WireProtocol.AppStatusPB.ErrorCode.ALREADY_PRESENT, msg, posixCode);
  }

  public static Status RuntimeError(String msg) {
    return new Status(WireProtocol.AppStatusPB.ErrorCode.RUNTIME_ERROR, msg);
  }
  public static Status RuntimeError(String msg, int posixCode) {
    return new Status(WireProtocol.AppStatusPB.ErrorCode.RUNTIME_ERROR, msg, posixCode);
  }

  public static Status NetworkError(String msg) {
    return new Status(WireProtocol.AppStatusPB.ErrorCode.NETWORK_ERROR, msg);
  }
  public static Status NetworkError(String msg, int posixCode) {
    return new Status(WireProtocol.AppStatusPB.ErrorCode.NETWORK_ERROR, msg, posixCode);
  }

  public static Status IllegalState(String msg) {
    return new Status(WireProtocol.AppStatusPB.ErrorCode.ILLEGAL_STATE, msg);
  }
  public static Status IllegalState(String msg, int posixCode) {
    return new Status(WireProtocol.AppStatusPB.ErrorCode.ILLEGAL_STATE, msg, posixCode);
  }

  public static Status NotAuthorized(String msg) {
    return new Status(WireProtocol.AppStatusPB.ErrorCode.NOT_AUTHORIZED, msg);
  }
  public static Status NotAuthorized(String msg, int posixCode) {
    return new Status(WireProtocol.AppStatusPB.ErrorCode.NOT_AUTHORIZED, msg, posixCode);
  }

  public static Status Aborted(String msg) {
    return new Status(WireProtocol.AppStatusPB.ErrorCode.ABORTED, msg);
  }
  public static Status Aborted(String msg, int posixCode) {
    return new Status(WireProtocol.AppStatusPB.ErrorCode.ABORTED, msg, posixCode);
  }

  public static Status RemoteError(String msg) {
    return new Status(WireProtocol.AppStatusPB.ErrorCode.REMOTE_ERROR, msg);
  }
  public static Status RemoteError(String msg, int posixCode) {
    return new Status(WireProtocol.AppStatusPB.ErrorCode.REMOTE_ERROR, msg, posixCode);
  }

  public static Status ServiceUnavailable(String msg) {
    return new Status(WireProtocol.AppStatusPB.ErrorCode.SERVICE_UNAVAILABLE, msg);
  }
  public static Status ServiceUnavailable(String msg, int posixCode) {
    return new Status(WireProtocol.AppStatusPB.ErrorCode.SERVICE_UNAVAILABLE, msg, posixCode);
  }

  public static Status TimedOut(String msg) {
    return new Status(WireProtocol.AppStatusPB.ErrorCode.TIMED_OUT, msg);
  }
  public static Status TimedOut(String msg, int posixCode) {
    return new Status(WireProtocol.AppStatusPB.ErrorCode.TIMED_OUT, msg, posixCode);
  }

  public static Status Uninitialized(String msg) {
    return new Status(WireProtocol.AppStatusPB.ErrorCode.UNINITIALIZED, msg);
  }
  public static Status Uninitialized(String msg, int posixCode) {
    return new Status(WireProtocol.AppStatusPB.ErrorCode.UNINITIALIZED, msg, posixCode);
  }

  public static Status ConfigurationError(String msg) {
    return new Status(WireProtocol.AppStatusPB.ErrorCode.CONFIGURATION_ERROR, msg);
  }
  public static Status ConfigurationError(String msg, int posixCode) {
    return new Status(WireProtocol.AppStatusPB.ErrorCode.CONFIGURATION_ERROR, msg, posixCode);
  }

  public static Status Incomplete(String msg) {
    return new Status(WireProtocol.AppStatusPB.ErrorCode.INCOMPLETE, msg);
  }
  public static Status Incomplete(String msg, int posixCode) {
    return new Status(WireProtocol.AppStatusPB.ErrorCode.INCOMPLETE, msg, posixCode);
  }

  public static Status EndOfFile(String msg) {
    return new Status(WireProtocol.AppStatusPB.ErrorCode.END_OF_FILE, msg);
  }
  public static Status EndOfFile(String msg, int posixCode) {
    return new Status(WireProtocol.AppStatusPB.ErrorCode.END_OF_FILE, msg, posixCode);
  }
  // CHECKSTYLE:ON
  // Boolean status checks.

  public boolean ok() {
    return appStatusPB.getCode() == WireProtocol.AppStatusPB.ErrorCode.OK;
  }

  public boolean isCorruption() {
    return appStatusPB.getCode() == WireProtocol.AppStatusPB.ErrorCode.CORRUPTION;
  }

  public boolean isNotFound() {
    return appStatusPB.getCode() == WireProtocol.AppStatusPB.ErrorCode.NOT_FOUND;
  }

  public boolean isNotSupported() {
    return appStatusPB.getCode() == WireProtocol.AppStatusPB.ErrorCode.NOT_SUPPORTED;
  }

  public boolean isInvalidArgument() {
    return appStatusPB.getCode() == WireProtocol.AppStatusPB.ErrorCode.INVALID_ARGUMENT;
  }

  public boolean isIOError() {
    return appStatusPB.getCode() == WireProtocol.AppStatusPB.ErrorCode.IO_ERROR;
  }

  public boolean isAlreadyPresent() {
    return appStatusPB.getCode() == WireProtocol.AppStatusPB.ErrorCode.ALREADY_PRESENT;
  }

  public boolean isRuntimeError() {
    return appStatusPB.getCode() == WireProtocol.AppStatusPB.ErrorCode.RUNTIME_ERROR;
  }

  public boolean isNetworkError() {
    return appStatusPB.getCode() == WireProtocol.AppStatusPB.ErrorCode.NETWORK_ERROR;
  }

  public boolean isIllegalState() {
    return appStatusPB.getCode() == WireProtocol.AppStatusPB.ErrorCode.ILLEGAL_STATE;
  }

  public boolean isNotAuthorized() {
    return appStatusPB.getCode() == WireProtocol.AppStatusPB.ErrorCode.NOT_AUTHORIZED;
  }

  public boolean isAborted() {
    return appStatusPB.getCode() == WireProtocol.AppStatusPB.ErrorCode.ABORTED;
  }

  public boolean isRemoteError() {
    return appStatusPB.getCode() == WireProtocol.AppStatusPB.ErrorCode.REMOTE_ERROR;
  }

  public boolean isServiceUnavailable() {
    return appStatusPB.getCode() == WireProtocol.AppStatusPB.ErrorCode.SERVICE_UNAVAILABLE;
  }

  public boolean isTimedOut() {
    return appStatusPB.getCode() == WireProtocol.AppStatusPB.ErrorCode.TIMED_OUT;
  }

  public boolean isUninitialized() {
    return appStatusPB.getCode() == WireProtocol.AppStatusPB.ErrorCode.UNINITIALIZED;
  }

  public boolean isConfigurationError() {
    return appStatusPB.getCode() == WireProtocol.AppStatusPB.ErrorCode.CONFIGURATION_ERROR;
  }

  public boolean isIncomplete() {
    return appStatusPB.getCode() == WireProtocol.AppStatusPB.ErrorCode.INCOMPLETE;
  }

  public boolean isEndOfFile() {
    return appStatusPB.getCode() == WireProtocol.AppStatusPB.ErrorCode.END_OF_FILE;
  }

  /**
   * Return a human-readable version of the status code.
   * See also status.cc in the C++ codebase.
   */
  private String getCodeAsString() {
    switch (appStatusPB.getCode().getNumber()) {
      case WireProtocol.AppStatusPB.ErrorCode.OK_VALUE:
        return "OK";
      case WireProtocol.AppStatusPB.ErrorCode.NOT_FOUND_VALUE:
        return "Not found";
      case WireProtocol.AppStatusPB.ErrorCode.CORRUPTION_VALUE:
        return "Corruption";
      case WireProtocol.AppStatusPB.ErrorCode.NOT_SUPPORTED_VALUE:
        return "Not implemented";
      case WireProtocol.AppStatusPB.ErrorCode.INVALID_ARGUMENT_VALUE:
        return "Invalid argument";
      case WireProtocol.AppStatusPB.ErrorCode.IO_ERROR_VALUE:
        return "IO error";
      case WireProtocol.AppStatusPB.ErrorCode.ALREADY_PRESENT_VALUE:
        return "Already present";
      case WireProtocol.AppStatusPB.ErrorCode.RUNTIME_ERROR_VALUE:
        return "Runtime error";
      case WireProtocol.AppStatusPB.ErrorCode.NETWORK_ERROR_VALUE:
        return "Network error";
      case WireProtocol.AppStatusPB.ErrorCode.ILLEGAL_STATE_VALUE:
        return "Illegal state";
      case WireProtocol.AppStatusPB.ErrorCode.NOT_AUTHORIZED_VALUE:
        return "Not authorized";
      case WireProtocol.AppStatusPB.ErrorCode.ABORTED_VALUE:
        return "Aborted";
      case WireProtocol.AppStatusPB.ErrorCode.REMOTE_ERROR_VALUE:
        return "Remote error";
      case WireProtocol.AppStatusPB.ErrorCode.SERVICE_UNAVAILABLE_VALUE:
        return "Service unavailable";
      case WireProtocol.AppStatusPB.ErrorCode.TIMED_OUT_VALUE:
        return "Timed out";
      case WireProtocol.AppStatusPB.ErrorCode.UNINITIALIZED_VALUE:
        return "Uninitialized";
      case WireProtocol.AppStatusPB.ErrorCode.CONFIGURATION_ERROR_VALUE:
        return "Configuration error";
      case WireProtocol.AppStatusPB.ErrorCode.INCOMPLETE_VALUE:
        return "Incomplete";
      case WireProtocol.AppStatusPB.ErrorCode.END_OF_FILE_VALUE:
        return "End of file";
      default:
        return "Unknown error (" + appStatusPB.getCode().getNumber() + ")";
    }
  }

  /**
   * Get the posix code associated with the error.
   * @return {@code -1} if no posix code is set. Otherwise, returns the posix code.
   */
  public int getPosixCode() {
    return appStatusPB.getPosixCode();
  }

  /**
   * Get enum code name.
   * Intended for internal use only.
   */
  String getCodeName() {
    return appStatusPB.getCode().name();
  }

  /**
   * Returns string error message.
   * Intended for internal use only.
   */
  String getMessage() {
    return appStatusPB.getMessage();
  }

  /**
   * Get a human-readable version of the Status message fit for logging or display.
   */
  @Override
  public String toString() {
    String str = getCodeAsString();
    if (appStatusPB.getCode() == WireProtocol.AppStatusPB.ErrorCode.OK) {
      return str;
    }
    str = String.format("%s: %s", str, appStatusPB.getMessage());
    if (appStatusPB.getPosixCode() != -1) {
      str = String.format("%s (error %d)", str, appStatusPB.getPosixCode());
    }
    return str;
  }
}
