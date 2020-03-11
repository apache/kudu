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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link SubprocessExecutor} class,
 *    1. parses the command line to get the configuration,
 *    2. has a single reader thread that continuously reads protobuf-based
 *       messages from stdin and puts the message onto the inbound request
 *       queue,
 *    3. has multiple parser threads that continuously retrieve the messages
 *       from the inbound queue, process them, and put the responses onto the
 *       outbound response queue,
 *    4. has a single writer thread that continuously retrieves the responses
 *       from the outbound queue, and writes the responses to stdout.
 */
@InterfaceAudience.Private
public class SubprocessExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(SubprocessExecutor.class);
  private final Function<Throwable, Void> errorHandler;
  private boolean injectInterrupt = false;
  private long blockWriteMs = -1;
  private BlockingQueue<OutboundResponse> outboundQueue;
  private BlockingQueue<InboundRequest> inboundQueue;

  public SubprocessExecutor() {
    errorHandler = (t) -> {
      // If unexpected exception(s) are thrown by any of the tasks, this error
      // handler wraps the throwable in a runtime exception and rethrows,
      // causing the program to exit with a nonzero status code.
      throw new RuntimeException(t);
    };
  }

  @VisibleForTesting
  public SubprocessExecutor(Function<Throwable, Void> errorHandler) {
    this.errorHandler = errorHandler;
  }

  /**
   * Executes the subprocess with the given arguments and protocol processor.
   *
   * @param args the subprocess arguments
   * @param protocolHandler the subprocess protocol handler
   * @param timeoutMs the maximum time to wait for subprocess tasks to finish, -1 means
   *                  no time out and the tasks will continue execute until it finishes
   * @throws ExecutionException if any tasks of the subprocess completed exceptionally
   * @throws InterruptedException if the current thread was interrupted while waiting
   * @throws TimeoutException if the wait timed out
   */
  @VisibleForTesting
  public void run(String[] args, ProtocolHandler protocolHandler, long timeoutMs)
      throws InterruptedException, ExecutionException, TimeoutException {
    SubprocessConfiguration conf = new SubprocessConfiguration(args);
    int maxMsgParserThread = conf.getMaxMsgParserThreads();
    int queueSize = conf.getQueueSize();
    int maxMessageBytes = conf.getMaxMessageBytes();

    inboundQueue = new ArrayBlockingQueue<>(queueSize, /* fair= */true);
    outboundQueue = new ArrayBlockingQueue<>(queueSize, /* fair= */true);
    ExecutorService readerService = Executors.newSingleThreadExecutor();
    ExecutorService parserService = Executors.newFixedThreadPool(maxMsgParserThread);
    ExecutorService writerService = Executors.newSingleThreadExecutor();

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
      CompletableFuture<Void> readerFuture = CompletableFuture.runAsync(reader, readerService);
      readerFuture.exceptionally(errorHandler);

      // Start multiple message parser threads and run the tasks asynchronously.
      MessageParser parser = new MessageParser(inboundQueue, outboundQueue, protocolHandler);
      List<CompletableFuture<Void>> parserFutures = new ArrayList<>();
      for (int i = 0; i < maxMsgParserThread; i++) {
        CompletableFuture<Void> parserFuture = CompletableFuture.runAsync(parser, parserService);
        parserFuture.exceptionally(errorHandler);
        parserFutures.add(parserFuture);
      }

      // Start a single writer thread and run the task asynchronously.
      MessageWriter writer = new MessageWriter(outboundQueue, messageIO, blockWriteMs);
      CompletableFuture<Void> writerFuture = CompletableFuture.runAsync(writer, writerService);
      writerFuture.exceptionally(errorHandler);

      // Wait until the tasks finish execution. A timeout of -1 means the reader, parser,
      // and writer tasks should continue until finished. In cases where we don't want
      // the tasks to run forever, e.g. in tests, wait for the specified
      // timeout.
      if (timeoutMs == -1) {
        readerFuture.join();
        writerFuture.join();
        CompletableFuture.allOf(parserFutures.toArray(new CompletableFuture[0])).join();
      } else {
        readerFuture.get(timeoutMs, TimeUnit.MILLISECONDS);
        writerFuture.get(timeoutMs, TimeUnit.MILLISECONDS);
        CompletableFuture.allOf(parserFutures.toArray(new CompletableFuture[0]))
                         .get(timeoutMs, TimeUnit.MILLISECONDS);
      }
    } catch (IOException e) {
      LOG.error("Unable to close the underlying stream", e);
    }
  }

  /**
   * Returns the outbound message queue.
   */
  @VisibleForTesting
  public BlockingQueue<OutboundResponse> getOutboundQueue() {
    return outboundQueue;
  }

  /**
   * Sets the interruption flag to true.
   */
  @VisibleForTesting
  public void interrupt() {
    injectInterrupt = true;
  }

  /**
   * Blocks the message write for the given milliseconds.
   */
  @VisibleForTesting
  public void blockWriteMs(long blockWriteMs) {
    this.blockWriteMs = blockWriteMs;
  }

  /**
   * Wrapper around <code>run()</code> that runs until 'timeoutMs' elapses,
   * catches any timeout exceptions, and returns.
   *
   * Used in tests.
   * TODO(awong): it'd be nice if we had a nicer way to shut down the executor.
   */
  public void runUntilTimeout(String[] args, ProtocolHandler handler, long timeoutMs)
      throws ExecutionException, InterruptedException {
    Preconditions.checkArgument(timeoutMs != -1);
    try {
      run(args, handler, timeoutMs);
    } catch (TimeoutException e) {
      // no-op
    }
  }
}
