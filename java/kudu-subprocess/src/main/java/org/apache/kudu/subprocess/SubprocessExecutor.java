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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import com.google.common.annotations.VisibleForTesting;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link SubprocessExecutor} class,
 *    1. parses the command line to get the configuration,
 *    2. has a single reader thread that continuously reads protobuf-based
 *       messages from the standard input and puts the messages to a FIFO
 *       blocking queue,
 *    3. has multiple writer threads that continuously retrieve the messages
 *       from the queue, process them and write the responses to the
 *       standard output.
 */
@InterfaceAudience.Private
public class SubprocessExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(SubprocessExecutor.class);
  private final Function<Throwable, Object> errorHandler;
  private boolean injectInterrupt = false;

  public SubprocessExecutor() {
    errorHandler = (t) -> {
      // Exit the program with a nonzero status code if unexpected exception(s)
      // thrown by the reader or writer tasks.
      System.exit(1);
      return null;
    };
  }

  @VisibleForTesting
  public SubprocessExecutor(Function<Throwable, Object> errorHandler) {
    this.errorHandler = errorHandler;
  }

  /**
   * Executes the subprocess with the given arguments and protocol processor.
   *
   * @param args the subprocess arguments
   * @param protocolHandler the subprocess protocol handler
   * @param timeoutMs the maximum time to wait for subproces tasks to finish, -1 means
   *                  no time out and the tasks will continue execute until it finishes
   * @throws ExecutionException if any tasks of the subprocess completed exceptionally
   * @throws InterruptedException if the current thread was interrupted while waiting
   * @throws TimeoutException if the wait timed out
   */
  @VisibleForTesting
  public void run(String[] args, ProtocolHandler protocolHandler, long timeoutMs)
      throws InterruptedException, ExecutionException, TimeoutException {
    SubprocessConfiguration conf = new SubprocessConfiguration(args);
    int maxWriterThread = conf.getMaxWriterThreads();
    int queueSize = conf.getQueueSize();
    int maxMessageBytes = conf.getMaxMessageBytes();

    BlockingQueue<byte[]> inboundQueue = new ArrayBlockingQueue<>(queueSize, /* fair= */true);
    ExecutorService readerService = Executors.newSingleThreadExecutor();
    ExecutorService writerService = Executors.newFixedThreadPool(maxWriterThread);

    // Wrap the system output in a SubprocessOutputStream so IOExceptions
    // from system output are propagated up instead of being silently swallowed.
    // Note that the BufferedOutputStream is initiated with the maximum bytes of
    // a message to ensure the underlying buffer can hold the entire message before
    // flushing.
    try (BufferedInputStream in = new BufferedInputStream(System.in);
         BufferedOutputStream out = new BufferedOutputStream(
             new SubprocessOutputStream(System.out), maxMessageBytes)) {
      MessageIO messageIO = new MessageIO(maxMessageBytes, in, out);

      // Start a single reader thread and run the task asynchronously.
      MessageReader reader = new MessageReader(inboundQueue, messageIO, injectInterrupt);
      CompletableFuture readerFuture = CompletableFuture.runAsync(reader, readerService);
      readerFuture.exceptionally(errorHandler);

      // Start multiple writer threads and run the tasks asynchronously.
      MessageWriter writer = new MessageWriter(inboundQueue, messageIO, protocolHandler);
      CompletableFuture[] writerFutures = new CompletableFuture[maxWriterThread];
      for (int i = 0; i < maxWriterThread; i++) {
        CompletableFuture writerFuture = CompletableFuture.runAsync(writer, writerService);
        writerFuture.exceptionally(errorHandler);
        writerFutures[i] = writerFuture;
      }

      // Wait until the tasks finish execution. -1 means the reader (or writer) tasks
      // continue the execution until finish. In cases where we don't want the tasks
      // to run forever, e.g. in tests, wait for the specified timeout.
      if (timeoutMs == -1) {
        readerFuture.join();
        CompletableFuture.allOf(writerFutures).join();
      } else {
        readerFuture.get(timeoutMs, TimeUnit.MILLISECONDS);
        CompletableFuture.allOf(writerFutures)
                         .get(timeoutMs, TimeUnit.MILLISECONDS);
      }
    } catch (IOException e) {
      LOG.error("Unable to close the underlying stream", e);
    }
  }

  /**
   * Sets the interruption flag to true.
   */
  @VisibleForTesting
  public void interrupt() {
    injectInterrupt = true;
  }
}
