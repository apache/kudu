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

package org.apache.kudu.subprocess;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.BasicConfigurator;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Utility class that manages common configurations to run a subprocess.
 */
@InterfaceAudience.Private
public class SubprocessConfiguration {
  private int queueSize;
  private static final int QUEUE_SIZE_DEFAULT = 100;
  private int maxMsgParserThreads;
  private static final int MAX_MSG_PARSER_THREADS_DEFAULT = 3;
  private int maxMsgBytes;
  private String keytabFile;
  private static final String KEYTAB_FILE_DEFAULT = "";
  private String servicePrincipal;
  private static final String SERVICE_PRINCIPAL_DEFAULT = "";
  private OutputStream outputStream;

  @VisibleForTesting
  static final int MAX_MESSAGE_BYTES_DEFAULT = 1024 * 1024;

  public SubprocessConfiguration(String[] args) {
    parse(args);
  }

  /**
   * @return the output stream to output messages to.
   */
  OutputStream getOutputStream() {
    return outputStream;
  }

  /**
   * @return the size of the message queue, or the default value if not
   * provided
   */
  int getQueueSize() {
    return queueSize;
  }

  /**
   * @return the maximum number of threads in the message parser thread pool,
   * or the default value if not provided
   */
  int getMaxMsgParserThreads() {
    return maxMsgParserThreads;
  }

  /**
   * @return the maximum bytes of a message, or the default value if not
   * provided
   */
  int getMaxMessageBytes() {
    return maxMsgBytes;
  }

  /**
   * @return the path to the service keytab file
   */
  public String getKeytabFile() {
    return keytabFile;
  }

  /**
   * @return the principal name of the service to load from the keytab file
   */
  public String getServicePrincipal() {
    return servicePrincipal;
  }

  /**
   * Parses the arguments according to the specified options.
   *
   * @param args the subprocess arguments
   * @throws KuduSubprocessException if there are any problems encountered
   *                                 while parsing the command line interface.
   */
  private void parse(String[] args) throws KuduSubprocessException {
    Options options = new Options();

    final String queueSizeLongOpt = "queueSize";
    Option queueSizeOpt = new Option(
        "q", queueSizeLongOpt, /* hasArg= */true,
        "Maximum number of messages held by the message queue");
    queueSizeOpt.setRequired(false);
    options.addOption(queueSizeOpt);

    final String maxMsgParserThreadsLongOpt = "maxMsgParserThreads";
    Option maxThreadsOpt = new Option(
        "p", maxMsgParserThreadsLongOpt, /* hasArg= */true,
        "Maximum number of threads in the message parser thread pool for subprocess");
    maxThreadsOpt.setRequired(false);
    options.addOption(maxThreadsOpt);

    final String maxMsgBytesLongOpt = "maxMsgBytes";
    Option maxMsgOpt = new Option(
        "m", maxMsgBytesLongOpt, /* hasArg= */true,
        "Maximum bytes of a message for subprocess");
    maxMsgOpt.setRequired(false);
    options.addOption(maxMsgOpt);

    final String keytabFileLongOpt = "keytab";
    Option keytabOpt = new Option(
        "k", keytabFileLongOpt, /* hasArg= */true,
        "The path to the service keytab file");
    keytabOpt.setRequired(false);
    options.addOption(keytabOpt);

    final String principalLongOpt = "principal";
    Option principalOpt = new Option(
        "i", principalLongOpt, /* hasArg= */true,
        "The service principal name to load from the keytab file");
    principalOpt.setRequired(false);
    options.addOption(principalOpt);

    final String outputPipeLongOpt = "outputPipe";
    Option outputPipeOpt = new Option(
        "o", outputPipeLongOpt, /* hasArg= */ true,
        "The pipe to output messages to. If not set, outputs to stdout (this " +
        "is generally unsafe and should only be used in tests)");
    outputPipeOpt.setRequired(false);
    options.addOption(outputPipeOpt);

    CommandLineParser parser = new BasicParser();
    String outputPipePath;
    try {
      CommandLine cmd = parser.parse(options, args);
      String queueSize = cmd.getOptionValue(queueSizeLongOpt);
      this.queueSize = queueSize == null ?
          QUEUE_SIZE_DEFAULT : Integer.parseInt(queueSize);

      String maxParserThreads = cmd.getOptionValue(maxMsgParserThreadsLongOpt);
      this.maxMsgParserThreads = maxParserThreads == null ?
          MAX_MSG_PARSER_THREADS_DEFAULT : Integer.parseInt(maxParserThreads);

      String maxMsgBytes = cmd.getOptionValue(maxMsgBytesLongOpt);
      this.maxMsgBytes = maxMsgBytes == null ?
          MAX_MESSAGE_BYTES_DEFAULT : Integer.parseInt(maxMsgBytes);

      String keytab = cmd.getOptionValue(keytabFileLongOpt);
      this.keytabFile = keytab == null ?
          KEYTAB_FILE_DEFAULT : keytab;

      String principal = cmd.getOptionValue(principalLongOpt);
      this.servicePrincipal = principal == null ?
          SERVICE_PRINCIPAL_DEFAULT : principal;

      outputPipePath = cmd.getOptionValue(outputPipeLongOpt);
    } catch (ParseException e) {
      throw new KuduSubprocessException("Cannot parse the subprocess command line", e);
    }

    try {
      if (outputPipePath == null) {
        this.outputStream = new SubprocessOutputStream(System.out);
      } else {
        // If we're not sending messages to System.out, redirect our logs to it.
        BasicConfigurator.configure();
        RandomAccessFile outputFile = new RandomAccessFile(new File(outputPipePath), "rw");
        this.outputStream = new FileOutputStream(outputFile.getFD());
      }
    } catch (FileNotFoundException e) {
      throw new KuduSubprocessException("Output file not found", e);
    } catch (IOException e) {
      throw new KuduSubprocessException("IO error opening file", e);
    }
  }
}
