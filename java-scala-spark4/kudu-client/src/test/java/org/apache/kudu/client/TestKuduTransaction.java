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

import static org.apache.kudu.test.ClientTestUtil.countRowsInScan;
import static org.apache.kudu.test.ClientTestUtil.createBasicSchemaInsert;
import static org.apache.kudu.test.junit.AssertHelpers.assertEventuallyTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.function.ThrowingRunnable;

import org.apache.kudu.test.ClientTestUtil;
import org.apache.kudu.test.KuduTestHarness;
import org.apache.kudu.test.KuduTestHarness.MasterServerConfig;
import org.apache.kudu.test.KuduTestHarness.TabletServerConfig;
import org.apache.kudu.test.junit.AssertHelpers.BooleanExpression;
import org.apache.kudu.transactions.Transactions.TxnTokenPB;


public class TestKuduTransaction {
  private KuduClient client;
  private AsyncKuduClient asyncClient;

  @Rule
  public KuduTestHarness harness = new KuduTestHarness();

  @Before
  public void setUp() {
    client = harness.getClient();
    asyncClient = harness.getAsyncClient();
  }

  private KuduTransaction makeFakeTransaction(KuduTransaction txn) throws IOException {
    byte[] buf = txn.serialize();
    final TxnTokenPB pb = TxnTokenPB.parseFrom(CodedInputStream.newInstance(buf));
    assertTrue(pb.hasTxnId());
    final long txnId = pb.getTxnId();
    assertTrue(txnId > AsyncKuduClient.INVALID_TXN_ID);

    final long fakeTxnId = txnId + 123;

    TxnTokenPB.Builder b = TxnTokenPB.newBuilder();
    b.setTxnId(fakeTxnId);
    b.setEnableKeepalive(false);
    b.setKeepaliveMillis(0);
    TxnTokenPB message = b.build();
    byte[] fakeTxnBuf = new byte[message.getSerializedSize()];
    CodedOutputStream cos = CodedOutputStream.newInstance(fakeTxnBuf);
    message.writeTo(cos);
    cos.flush();
    return KuduTransaction.deserialize(fakeTxnBuf, asyncClient);
  }

  /**
   * Test scenario that starts a new transaction given an instance of
   * KuduClient. The purpose of this test is to make sure it's possible
   * to start a new transaction given a KuduClient object.
   */
  @Test(timeout = 100000)
  @MasterServerConfig(flags = {
      "--txn_manager_enabled",
  })
  @TabletServerConfig(flags = {
      "--enable_txn_system_client_init=true",
  })
  public void testNewTransaction() throws Exception {
    KuduTransaction txn = client.newTransaction();
    assertNotNull(txn);
    byte[] buf = txn.serialize();
    assertNotNull(buf);
    final TxnTokenPB pb = TxnTokenPB.parseFrom(CodedInputStream.newInstance(buf));
    assertTrue(pb.hasTxnId());
    assertTrue(pb.getTxnId() > AsyncKuduClient.INVALID_TXN_ID);
    assertTrue(pb.hasEnableKeepalive());
    // By default, keepalive is disabled for a serialized txn token.
    assertFalse(pb.getEnableKeepalive());
    assertTrue(pb.hasKeepaliveMillis());
    assertTrue(pb.getKeepaliveMillis() > 0);
  }

  /**
   * Test scenario that starts many new transaction given an instance of
   * KuduClient.
   */
  @Test(timeout = 100000)
  @MasterServerConfig(flags = {
      "--txn_manager_enabled",
  })
  @TabletServerConfig(flags = {
      "--enable_txn_system_client_init=true",
  })
  public void testStartManyTransactions() throws Exception {
    List<KuduTransaction> transactions = new ArrayList<>();
    for (int i = 0; i < 1000; ++i) {
      KuduTransaction txn = client.newTransaction();
      assertNotNull(txn);
      transactions.add(txn);
    }
    for (KuduTransaction txn : transactions) {
      txn.rollback();
    }
  }

  /**
   * Test scenario that starts a new transaction and rolls it back.
   */
  @Test(timeout = 100000)
  @MasterServerConfig(flags = {
      "--txn_manager_enabled",
  })
  @TabletServerConfig(flags = {
      "--enable_txn_system_client_init=true",
  })
  public void testRollbackAnEmptyTransaction() throws Exception {
    KuduTransaction txn = client.newTransaction();
    txn.rollback();
    // A duplicate call to rollback an aborted transaction using the same
    // handle should report an error.
    IllegalStateException ex = assertThrows(
        IllegalStateException.class, new ThrowingRunnable() {
          @Override
          public void run() throws Throwable {
            txn.rollback();
          }
        });
    assertEquals("transaction is not open for this handle", ex.getMessage());

    // Try to rollback the same transaction using another handle that has been
    // constructed using serialize/deserialize sequence: it should be fine
    // since aborting a transaction has idempotent semantics for the back-end.
    byte[] buf = txn.serialize();
    KuduTransaction serdesTxn = KuduTransaction.deserialize(buf, asyncClient);
    serdesTxn.rollback();
  }

  /**
   * Test scenario that starts a new transaction and commits it right away.
   */
  @Test(timeout = 100000)
  @MasterServerConfig(flags = {
      "--txn_manager_enabled",
  })
  @TabletServerConfig(flags = {
      "--txn_schedule_background_tasks=false",
      "--enable_txn_system_client_init=true",
  })
  public void testCommitAnEmptyTransaction() throws Exception {
    KuduTransaction txn = client.newTransaction();
    txn.startCommit();
    // A duplicate call to commit the transaction using the same handle
    // should fail.
    IllegalStateException ex = assertThrows(
        IllegalStateException.class, new ThrowingRunnable() {
          @Override
          public void run() throws Throwable {
            txn.startCommit();
          }
        });
    assertEquals("transaction is not open for this handle", ex.getMessage());

    // Try to commit the same transaction using another handle that has been
    // constructed using serialize/deserialize sequence: it should be fine
    // since committing a transaction has idempotent semantics for the back-end.
    byte[] buf = txn.serialize();
    KuduTransaction serdesTxn = KuduTransaction.deserialize(buf, asyncClient);
    serdesTxn.startCommit();
  }

  /**
   * Test scenario that tries to commit a non-existent transaction.
   */
  @Test(timeout = 100000)
  @MasterServerConfig(flags = {
      "--txn_manager_enabled",
  })
  @TabletServerConfig(flags = {
      "--enable_txn_system_client_init=true",
  })
  public void testCommitNonExistentTransaction() throws Exception {
    KuduTransaction txn = client.newTransaction();
    assertNotNull(txn);
    KuduTransaction fakeTxn = makeFakeTransaction(txn);
    try {
      // Try to commit the transaction in non-synchronous mode, i.e. just
      // initiate committing the transaction.
      fakeTxn.startCommit();
      fail("committing a non-existing transaction should have failed");
    } catch (NonRecoverableException e) {
      final String errmsg = e.getMessage();
      final Status status = e.getStatus();
      assertTrue(status.toString(), status.isInvalidArgument());
      assertTrue(errmsg, errmsg.matches(".*transaction ID .* not found.*"));
    } catch (Exception e) {
      fail("unexpected exception: " + e.toString());
    }

    try {
      // Try to commit the transaction in synchronous mode, i.e. initiate
      // committing the transaction and wait for the commit phase to finalize.
      fakeTxn.commit();
      fail("committing a non-existing transaction should have failed");
    } catch (NonRecoverableException e) {
      final String errmsg = e.getMessage();
      final Status status = e.getStatus();
      assertTrue(status.toString(), status.isInvalidArgument());
      assertTrue(errmsg, errmsg.matches(".*transaction ID .* not found.*"));
    } catch (Exception e) {
      fail("unexpected exception: " + e.toString());
    }
  }

  /**
   * Transactional sessions can be closed as regular ones.
   */
  @Test(timeout = 100000)
  @MasterServerConfig(flags = {
      "--txn_manager_enabled",
  })
  @TabletServerConfig(flags = {
      "--enable_txn_system_client_init=true",
  })
  public void testTxnSessionClose() throws Exception {
    final String TABLE_NAME = "txn_session_close";
    client.createTable(
        TABLE_NAME,
        ClientTestUtil.getBasicSchema(),
        new CreateTableOptions().addHashPartitions(ImmutableList.of("key"), 2));
    KuduTable table = client.openTable(TABLE_NAME);

    // Open and close an empty transaction session.
    {
      KuduTransaction txn = client.newTransaction();
      assertNotNull(txn);
      KuduSession session = txn.newKuduSession();
      assertNotNull(session);
      assertFalse(session.isClosed());
      session.close();
      assertTrue(session.isClosed());
    }

    // Open new transaction, insert one row for a session, close the session
    // and then rollback the transaction. No rows should be persisted.
    {
      KuduTransaction txn = client.newTransaction();
      assertNotNull(txn);
      KuduSession session = txn.newKuduSession();
      assertNotNull(session);
      session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);

      Insert insert = createBasicSchemaInsert(table, 1);
      session.apply(insert);
      session.close();

      txn.rollback();

      assertTrue(session.isClosed());
      assertEquals(0, session.countPendingErrors());

      KuduScanner scanner = new KuduScanner.KuduScannerBuilder(asyncClient, table)
          .readMode(AsyncKuduScanner.ReadMode.READ_YOUR_WRITES)
          .build();
      assertEquals(0, countRowsInScan(scanner));
    }
  }

  /**
   * Test scenario that starts a new transaction, initiates its commit phase,
   * and checks whether the commit is complete using the
   * KuduTransaction.isCommitComplete() method.
   */
  @Test(timeout = 100000)
  @MasterServerConfig(flags = {
      "--txn_manager_enabled",
  })
  @TabletServerConfig(flags = {
      "--txn_schedule_background_tasks=false",
      "--txn_status_manager_inject_latency_finalize_commit_ms=1000",
      "--enable_txn_system_client_init=true",
  })
  public void testIsCommitComplete() throws Exception {
    KuduTransaction txn = client.newTransaction();
    txn.startCommit();
    assertFalse(txn.isCommitComplete());
  }

  /**
   * Verify how KuduTransaction.isCommitComplete() works for a transaction handle
   * in a few special cases.
   */
  @Test(timeout = 100000)
  @MasterServerConfig(flags = {
      "--txn_manager_enabled",
  })
  @TabletServerConfig(flags = {
      "--txn_schedule_background_tasks=false",
      "--enable_txn_system_client_init=true",
  })
  public void testIsCommitCompleteSpecialCases() throws Exception {
    KuduTransaction txn = client.newTransaction();

    {
      NonRecoverableException ex = assertThrows(
          NonRecoverableException.class, new ThrowingRunnable() {
            @Override
            public void run() throws Throwable {
              txn.isCommitComplete();
            }
          });
      assertTrue(ex.getStatus().isIllegalState());
      assertEquals("transaction is still open", ex.getMessage());
    }

    // Rollback the transaction.
    txn.rollback();

    {
      NonRecoverableException ex = assertThrows(
          NonRecoverableException.class, new ThrowingRunnable() {
            @Override
            public void run() throws Throwable {
              txn.isCommitComplete();
            }
          });
      assertTrue(ex.getStatus().isAborted());
      assertEquals("transaction is being aborted", ex.getMessage());
    }

    // Try to call isCommitComplete() on a handle that isn't backed by any
    // transaction registered with the system.
    {
      KuduTransaction fakeTxn = makeFakeTransaction(txn);
      NonRecoverableException ex = assertThrows(
          NonRecoverableException.class, new ThrowingRunnable() {
            @Override
            public void run() throws Throwable {
              fakeTxn.isCommitComplete();
            }
          });
      final Status status = ex.getStatus();
      assertTrue(status.toString(), status.isInvalidArgument());
      final String errmsg = ex.getMessage();
      assertTrue(errmsg, errmsg.matches(".*transaction ID .* not found.*"));
    }
  }

  /**
   * Test scenario that starts a new empty transaction and commits it in a
   * synchronous way (i.e. waits for the transaction to be committed).
   *
   */
  @Test(timeout = 100000)
  @MasterServerConfig(flags = {
      "--txn_manager_enabled",
  })
  @TabletServerConfig(flags = {
      "--enable_txn_system_client_init=true",
  })
  public void testCommitAnEmptyTransactionWait() throws Exception {
    KuduTransaction txn = client.newTransaction();
    txn.commit();
    assertTrue(txn.isCommitComplete());
  }

  /**
   * Test scenario that tries to rollback a non-existent transaction.
   */
  @Test(timeout = 100000)
  @MasterServerConfig(flags = {
      "--txn_manager_enabled",
  })
  @TabletServerConfig(flags = {
      "--enable_txn_system_client_init=true",
  })
  public void testRollbackNonExistentTransaction() throws Exception {
    KuduTransaction txn = client.newTransaction();
    assertNotNull(txn);
    KuduTransaction fakeTxn = makeFakeTransaction(txn);
    try {
      fakeTxn.rollback();
      fail("rolling back non-existing transaction should have failed");
    } catch (NonRecoverableException e) {
      final String errmsg = e.getMessage();
      final Status status = e.getStatus();
      assertTrue(status.toString(), status.isInvalidArgument());
      assertTrue(errmsg, errmsg.matches(".*transaction ID .* not found.*"));
    } catch (Exception e) {
      fail("unexpected exception: " + e.toString());
    }
  }

  /**
   * Test scenario that starts a new transaction given an instance of
   * AsyncKuduClient. The purpose of this test is to make sure it's possible
   * to start a new transaction given an AsyncKuduClient object.
   */
  @Test(timeout = 100000)
  @MasterServerConfig(flags = {
      "--txn_manager_enabled",
  })
  @TabletServerConfig(flags = {
      "--enable_txn_system_client_init=true",
  })
  public void testNewTransactionAsyncClient() throws Exception {
    KuduTransaction txn = client.newTransaction();
    assertNotNull(txn);
    byte[] buf = txn.serialize();
    final TxnTokenPB pb = TxnTokenPB.parseFrom(CodedInputStream.newInstance(buf));
    assertTrue(pb.hasTxnId());
    assertTrue(pb.getTxnId() > AsyncKuduClient.INVALID_TXN_ID);
    assertTrue(pb.hasEnableKeepalive());
    // By default, keepalive is disabled for a serialized txn token.
    assertFalse(pb.getEnableKeepalive());
    assertTrue(pb.hasKeepaliveMillis());
    assertTrue(pb.getKeepaliveMillis() > 0);
  }

  /**
   * Test scenario that starts a transaction and creates a new transactional
   * KuduSession based on the newly started transaction.
   */
  @Test(timeout = 100000)
  @MasterServerConfig(flags = {
      "--txn_manager_enabled",
  })
  @TabletServerConfig(flags = {
      "--enable_txn_system_client_init=true",
  })
  public void testNewTransactionalSession() throws Exception {
    final String TABLE_NAME = "new_transactional_session";
    client.createTable(
        TABLE_NAME,
        ClientTestUtil.getBasicSchema(),
        new CreateTableOptions().addHashPartitions(ImmutableList.of("key"), 2));

    KuduTransaction txn = client.newTransaction();
    assertNotNull(txn);
    KuduSession session = txn.newKuduSession();
    assertNotNull(session);
    session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);

    KuduTable table = client.openTable(TABLE_NAME);
    Insert insert = createBasicSchemaInsert(table, 1);
    session.apply(insert);
    session.flush();

    // Rollback the transaction.
    txn.rollback();

    assertFalse(session.isClosed());
    assertEquals(0, session.countPendingErrors());

    KuduScanner scanner = new KuduScanner.KuduScannerBuilder(asyncClient, table)
        .readMode(AsyncKuduScanner.ReadMode.READ_YOUR_WRITES)
        .build();
    assertEquals(0, countRowsInScan(scanner));
  }

  /**
   * Test scenario that starts a transaction and creates a new transactional
   * AsyncKuduSession based on the newly started transaction. No rows are
   * inserted: it should be possible to rollback the empty transaction with
   * no errors reported.
   */
  @Test(timeout = 100000)
  @MasterServerConfig(flags = {
      "--txn_manager_enabled",
  })
  @TabletServerConfig(flags = {
      "--enable_txn_system_client_init=true",
  })
  public void testNewAsyncTransactionalSession() throws Exception {
    KuduTransaction txn = client.newTransaction();
    assertNotNull(txn);
    AsyncKuduSession session = txn.newAsyncKuduSession();
    assertNotNull(session);

    // Rollback the empty transaction.
    txn.rollback();

    assertFalse(session.isClosed());
    assertEquals(0, session.countPendingErrors());
  }

  /**
   * Try to start a transaction when the backend doesn't have the required
   * functionality (e.g. a backend which predates the introduction of the
   * txn-related functionality).
   */
  @Test(timeout = 100000)
  @MasterServerConfig(flags = {
      "--txn_manager_enabled=false",
  })
  @TabletServerConfig(flags = {
      "--enable_txn_system_client_init=true",
  })
  public void testTxnOpsWithoutTxnManager() throws Exception {
    try (KuduTransaction txn = client.newTransaction()) {
      fail("starting a new transaction without TxnManager should have failed");
    } catch (KuduException e) {
      final String errmsg = e.getMessage();
      final Status status = e.getStatus();
      assertTrue(status.toString(), status.isRemoteError());
      assertTrue(errmsg, errmsg.matches(".* Not found: .*"));
      assertTrue(errmsg, errmsg.matches(
          ".* kudu.transactions.TxnManagerService not registered on Master"));
    } catch (Exception e) {
      fail("unexpected exception: " + e.toString());
    }
  }

  /**
   * Test KuduTransaction to be used in auto-closable manner.
   */
  @Test(timeout = 100000)
  @MasterServerConfig(flags = {
      "--txn_manager_enabled",
  })
  @TabletServerConfig(flags = {
      "--txn_schedule_background_tasks=false",
      "--enable_txn_system_client_init=true",
  })
  public void testAutoclosableUsage() throws Exception {
    byte[] buf = null;

    try (KuduTransaction txn = client.newTransaction()) {
      buf = txn.serialize();
      assertNotNull(buf);
      txn.startCommit();
      txn.isCommitComplete();
    } catch (Exception e) {
      fail("unexpected exception: " + e.toString());
    }

    try (KuduTransaction txn = KuduTransaction.deserialize(buf, asyncClient)) {
      buf = txn.serialize();
      assertNotNull(buf);
      txn.rollback();
    } catch (Exception e) {
      fail("unexpected exception: " + e.toString());
    }

    // Do this once more time, just in case to verify that handles created by
    // the serialize/deserialize sequence behave as expected.
    try (KuduTransaction txn = KuduTransaction.deserialize(buf, asyncClient)) {
      buf = txn.serialize();
      assertNotNull(buf);
      txn.rollback();
    } catch (Exception e) {
      fail("unexpected exception: " + e.toString());
    }

    {
      KuduTransaction txn = client.newTransaction();
      // Explicitly call KuduTransaction.close() more than once time to make
      // sure it's possible to do so and the method's behavior is idempotent.
      txn.close();
      txn.close();
    }
  }

  /**
   * Verify that a transaction token created by the KuduClient.serialize()
   * method has keepalive enabled or disabled as specified by the
   * SerializationOptions.
   */
  @Test(timeout = 100000)
  @MasterServerConfig(flags = {
      "--txn_manager_enabled",
  })
  @TabletServerConfig(flags = {
      "--enable_txn_system_client_init=true",
  })
  public void testSerializationOptions() throws Exception {
    final KuduTransaction txn = client.newTransaction();

    // Check the keepalive settings when serializing/deserializing with default
    // settings for SerializationOptions.
    {
      byte[] buf = txn.serialize();
      TxnTokenPB pb = TxnTokenPB.parseFrom(CodedInputStream.newInstance(buf));
      assertTrue(pb.hasKeepaliveMillis());
      assertTrue(pb.getKeepaliveMillis() > 0);
      assertTrue(pb.hasEnableKeepalive());
      assertFalse(pb.getEnableKeepalive());
    }

    // Same as above, but supply an instance of SerializationOptions with
    // default settings created by the constructor.
    {
      KuduTransaction.SerializationOptions options =
          new KuduTransaction.SerializationOptions();
      byte[] buf = txn.serialize(options);
      TxnTokenPB pb = TxnTokenPB.parseFrom(CodedInputStream.newInstance(buf));
      assertTrue(pb.hasKeepaliveMillis());
      assertTrue(pb.getKeepaliveMillis() > 0);
      assertTrue(pb.hasEnableKeepalive());
      assertFalse(pb.getEnableKeepalive());
    }

    // Same as above, but explicitly disable keepalive for an instance of
    // SerializationOptions.
    {
      KuduTransaction.SerializationOptions options =
          new KuduTransaction.SerializationOptions();
      options.setEnableKeepalive(false);
      byte[] buf = txn.serialize(options);
      TxnTokenPB pb = TxnTokenPB.parseFrom(CodedInputStream.newInstance(buf));
      assertTrue(pb.hasKeepaliveMillis());
      assertTrue(pb.getKeepaliveMillis() > 0);
      assertTrue(pb.hasEnableKeepalive());
      assertFalse(pb.getEnableKeepalive());
    }

    // Explicitly enable keepalive with SerializationOptions.
    {
      KuduTransaction.SerializationOptions options =
          new KuduTransaction.SerializationOptions();
      options.setEnableKeepalive(true);
      byte[] buf = txn.serialize(options);
      TxnTokenPB pb = TxnTokenPB.parseFrom(CodedInputStream.newInstance(buf));
      assertTrue(pb.hasKeepaliveMillis());
      assertTrue(pb.getKeepaliveMillis() > 0);
      assertTrue(pb.hasEnableKeepalive());
      assertTrue(pb.getEnableKeepalive());
    }
  }

  /**
   * Test that a KuduTransaction handle created by KuduClient.newTransaction()
   * automatically sends keepalive messages.
   */
  @Test(timeout = 100000)
  @MasterServerConfig(flags = {
      "--txn_manager_enabled",
  })
  @TabletServerConfig(flags = {
      "--txn_keepalive_interval_ms=200",
      "--txn_staleness_tracker_interval_ms=50",
      "--enable_txn_system_client_init=true",
  })
  public void testKeepaliveBasic() throws Exception {
    try (KuduTransaction txn = client.newTransaction()) {
      final byte[] buf = txn.serialize();
      final TxnTokenPB pb = TxnTokenPB.parseFrom(CodedInputStream.newInstance(buf));
      assertTrue(pb.hasKeepaliveMillis());
      final long keepaliveMillis = pb.getKeepaliveMillis();
      assertTrue(keepaliveMillis > 0);
      Thread.sleep(3 * keepaliveMillis);
      // It should be possible to commit the transaction since it supposed to be
      // open at this point even after multiples of the inactivity timeout
      // interval.
      txn.startCommit();
    } catch (Exception e) {
      fail("unexpected exception: " + e.toString());
    }

    {
      KuduTransaction txn = client.newTransaction();
      final byte[] buf = txn.serialize();
      final TxnTokenPB pb = TxnTokenPB.parseFrom(CodedInputStream.newInstance(buf));
      assertTrue(pb.hasKeepaliveMillis());
      final long keepaliveMillis = pb.getKeepaliveMillis();
      assertTrue(keepaliveMillis > 0);
      // Call KuduTransaction.close() explicitly.
      txn.close();

      // Keep the handle around without any activity for longer than the
      // keepalive timeout interval.
      Thread.sleep(3 * keepaliveMillis);

      // At this point, the underlying transaction should be automatically
      // aborted by the backend. An attempt to commit the transaction should
      // fail because the transaction is assumed to be already aborted at this
      // point.
      NonRecoverableException ex = assertThrows(
          NonRecoverableException.class, new ThrowingRunnable() {
            @Override
            public void run() throws Throwable {
              txn.startCommit();
            }
          });
      final String errmsg = ex.getMessage();
      assertTrue(errmsg, errmsg.matches(
          ".* transaction ID .* is not open: state: ABORT.*"));

      // Verify that KuduTransaction.rollback() successfully runs on a transaction
      // handle if the underlying transaction is already aborted automatically
      // by the backend. Rolling back the transaction explicitly should succeed
      // since it's a pure no-op: rolling back a transaction has idempotent
      // semantics.
      txn.rollback();
    }
  }

  /**
   * Test that a KuduTransaction handle created by KuduClient.deserialize()
   * automatically sends or doesn't send keepalive heartbeat messages
   * depending on the SerializationOptions used while serializing the handle
   * into a transaction token.
   */
  @Test(timeout = 100000)
  @MasterServerConfig(flags = {
      "--txn_manager_enabled",
  })
  @TabletServerConfig(flags = {
      "--txn_keepalive_interval_ms=200",
      "--txn_schedule_background_tasks=false",
      "--txn_staleness_tracker_interval_ms=50",
      "--enable_txn_system_client_init=true",
  })
  public void testKeepaliveForDeserializedHandle() throws Exception {
    // Check the keepalive behavior when serializing/deserializing with default
    // settings for SerializationOptions.
    {
      KuduTransaction txn = client.newTransaction();
      final byte[] buf = txn.serialize();
      final TxnTokenPB pb = TxnTokenPB.parseFrom(CodedInputStream.newInstance(buf));
      assertTrue(pb.hasKeepaliveMillis());
      final long keepaliveMillis = pb.getKeepaliveMillis();
      assertTrue(keepaliveMillis > 0);

      KuduTransaction serdesTxn = KuduTransaction.deserialize(buf, asyncClient);

      // Call KuduTransaction.close() explicitly to stop sending automatic
      // keepalive messages from 'txn' handle.
      txn.close();

      // Keep the handle around without any activity for longer than the
      // keepalive timeout interval.
      Thread.sleep(3 * keepaliveMillis);

      // At this point, the underlying transaction should be automatically
      // aborted by the backend: the 'txn' handle should not send any heartbeats
      // anymore since it's closed, and the 'serdesTxn' handle should not be
      // sending any heartbeats.
      NonRecoverableException ex = assertThrows(
          NonRecoverableException.class, new ThrowingRunnable() {
            @Override
            public void run() throws Throwable {
              serdesTxn.startCommit();
            }
          });
      final String errmsg = ex.getMessage();
      assertTrue(errmsg, errmsg.matches(
          ".* transaction ID .* is not open: state: ABORT.*"));

      // Verify that KuduTransaction.rollback() successfully runs on both
      // transaction handles if the underlying transaction is already aborted
      // automatically by the backend.
      txn.rollback();
      serdesTxn.rollback();
    }

    // Check the keepalive behavior when serializing/deserializing when
    // keepalive heartbeating is enabled in SerializationOptions used
    // during the serialization of the original transaction handle.
    {
      final KuduTransaction.SerializationOptions options =
          new KuduTransaction.SerializationOptions();
      options.setEnableKeepalive(true);
      KuduTransaction txn = client.newTransaction();
      final byte[] buf = txn.serialize(options);
      final TxnTokenPB pb = TxnTokenPB.parseFrom(CodedInputStream.newInstance(buf));
      assertTrue(pb.hasKeepaliveMillis());
      final long keepaliveMillis = pb.getKeepaliveMillis();
      assertTrue(keepaliveMillis > 0);

      KuduTransaction serdesTxn = KuduTransaction.deserialize(buf, asyncClient);

      // Call KuduTransaction.close() explicitly to stop sending automatic
      // keepalive messages by the 'txn' handle.
      txn.close();

      // Keep the handle around without any activity for longer than the
      // keepalive timeout interval.
      Thread.sleep(3 * keepaliveMillis);

      // At this point, the underlying transaction should be kept open
      // because the 'serdesTxn' handle sends keepalive heartbeats even if the
      // original handle ceased to send those after calling 'close()' on it.
      // As an extra sanity check, call 'startCommit()' and 'isCommitComplete()'
      // on both handles to make sure no exception is thrown.
      serdesTxn.startCommit();
      serdesTxn.isCommitComplete();
      txn.startCommit();
      txn.isCommitComplete();
    }
  }

  /**
   * This scenario validates the propagation of the commit timestamp for a
   * multi-row transaction when committing the transaction synchronously via
   * {@link KuduTransaction#commit()} or calling
   * {@link KuduTransaction#isCommitComplete()} once the transaction's commit
   * has started to run asynchronously.
   */
  @Test(timeout = 100000)
  @MasterServerConfig(flags = {
      // TxnManager functionality is necessary for this scenario.
      "--txn_manager_enabled",
  })
  @TabletServerConfig(flags = {
      // Inject latency to have a chance spotting the transaction in the
      // FINALIZE_IN_PROGRESS state and make KuduTransaction.isCommitComplete()
      // to return 'false' at least once before returning 'true'.
      "--txn_status_manager_inject_latency_finalize_commit_ms=250",
      "--enable_txn_system_client_init=true",
  })
  public void testPropagateTxnCommitTimestamp() throws Exception {
    final String TABLE_NAME = "propagate_txn_commit_timestamp";
    client.createTable(
        TABLE_NAME,
        ClientTestUtil.getBasicSchema(),
        new CreateTableOptions().addHashPartitions(ImmutableList.of("key"), 8));

    KuduTable table = client.openTable(TABLE_NAME);

    // Make sure the commit timestamp for a transaction is propagated to the
    // client upon synchronously committing a transaction.
    {
      KuduTransaction txn = client.newTransaction();
      KuduSession session = txn.newKuduSession();
      session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);

      // Insert many rows: the goal is to get at least one row inserted into
      // every tablet of the hash-partitioned test table, so every tablet would
      // be a participant in the transaction, and most likely every tablet
      // server would be involved.
      for (int key = 0; key < 128; ++key) {
        session.apply(createBasicSchemaInsert(table, key));
      }
      session.flush();
      assertEquals(0, session.countPendingErrors());

      final long tsBeforeCommit = client.getLastPropagatedTimestamp();
      txn.commit();
      final long tsAfterCommit = client.getLastPropagatedTimestamp();
      assertTrue(tsAfterCommit > tsBeforeCommit);
    }

    // Make sure the commit timestamp for a transaction is propagated to the
    // client upon calling KuduTransaction.isCommitComplete().
    {
      KuduTransaction txn = client.newTransaction();
      KuduSession session = txn.newKuduSession();
      session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);

      // Insert many rows: the goal is to get at least one row inserted into
      // every tablet of the hash-partitioned test table, so every tablet would
      // be a participant in the transaction, and most likely every tablet
      // server would be involved.
      for (int key = 128; key < 256; ++key) {
        session.apply(createBasicSchemaInsert(table, key));
      }
      session.flush();
      assertEquals(0, session.countPendingErrors());

      final long tsBeforeCommit = client.getLastPropagatedTimestamp();
      txn.startCommit();
      assertEquals(tsBeforeCommit, client.getLastPropagatedTimestamp());

      assertEventuallyTrue("commit should eventually finalize",
          new BooleanExpression() {
            @Override
            public boolean get() throws Exception {
              return txn.isCommitComplete();
            }
          }, 30000/*timeoutMillis*/);
      long tsAfterCommitFinalized = client.getLastPropagatedTimestamp();
      assertTrue(tsAfterCommitFinalized > tsBeforeCommit);

      // A sanity check: calling isCommitComplete() again after the commit phase
      // has been finalized doesn't change last propagated timestamp at the
      // client side.
      for (int i = 0; i < 10; ++i) {
        assertTrue(txn.isCommitComplete());
        assertEquals(tsAfterCommitFinalized, client.getLastPropagatedTimestamp());
        Thread.sleep(10);
      }
    }

    // An empty transaction doesn't have a timestamp, so there is nothing to
    // propagate back to client when an empty transaction is committed, so the
    // timestamp propagated to the client side should stay unchanged.
    {
      KuduTransaction txn = client.newTransaction();
      final long tsBeforeCommit = client.getLastPropagatedTimestamp();
      txn.commit();

      // Just in case, linger a bit after commit has been finalized, checking
      // for the timestamp propagated to the client side.
      for (int i = 0; i < 10; ++i) {
        Thread.sleep(10);
        assertEquals(tsBeforeCommit, client.getLastPropagatedTimestamp());
      }
    }
  }

  /**
   * Test to verify that Kudu client is able to switch to TxnManager hosted by
   * other kudu-master process when the previously used one isn't available.
   */
  @Test(timeout = 100000)
  @MasterServerConfig(flags = {
      // TxnManager functionality is necessary for this scenario.
      "--txn_manager_enabled",

      // Set Raft heartbeat interval short for faster test runtime: speed up
      // leader failure detection and new leader election.
      "--raft_heartbeat_interval_ms=100",
  })
  @TabletServerConfig(flags = {
      "--enable_txn_system_client_init=true",
  })
  public void testSwitchToOtherTxnManager() throws Exception {
    final String TABLE_NAME = "txn_manager_ops_fallback";
    client.createTable(
        TABLE_NAME,
        ClientTestUtil.getBasicSchema(),
        new CreateTableOptions().addHashPartitions(ImmutableList.of("key"), 2));

    // Start a transaction, then restart every available TxnManager instance
    // before attempting any txn-related operation.
    {
      KuduTransaction txn = client.newTransaction();
      KuduSession session = txn.newKuduSession();

      KuduTable table = client.openTable(TABLE_NAME);

      Insert insert = createBasicSchemaInsert(table, 0);
      session.apply(insert);
      session.flush();

      harness.killAllMasterServers();
      harness.startAllMasterServers();

      // Querying the status of a transaction should be possible, as usual.
      // Since the transaction is still open, KuduTransaction.isCommitComplete()
      // should throw corresponding exception with Status.IllegalState.
      try {
        txn.isCommitComplete();
        fail("KuduTransaction.isCommitComplete should have thrown");
      } catch (NonRecoverableException e) {
        assertTrue(e.getStatus().toString(), e.getStatus().isIllegalState());
        assertEquals("transaction is still open", e.getMessage());
      }

      harness.killAllMasterServers();
      harness.startAllMasterServers();

      // It should be possible to commit the transaction.
      txn.commit();

      // An extra sanity check: read back the rows written into the table in the
      // context of the transaction.
      KuduScanner scanner = new KuduScanner.KuduScannerBuilder(asyncClient, table)
          .readMode(AsyncKuduScanner.ReadMode.READ_YOUR_WRITES)
          .replicaSelection(ReplicaSelection.LEADER_ONLY)
          .build();

      assertEquals(1, scanner.nextRows().getNumRows());
    }

    // Similar to the above, but run KuduTransaction.commit() when only 2 out
    // of 3 masters are running while the TxnManager which used to start the
    // transaction is no longer around.
    {
      KuduTransaction txn = client.newTransaction();
      KuduSession session = txn.newKuduSession();

      KuduTable table = client.openTable(TABLE_NAME);

      Insert insert = createBasicSchemaInsert(table, 1);
      session.apply(insert);
      session.flush();

      harness.killLeaderMasterServer();

      // It should be possible to commit the transaction: 2 out of 3 masters are
      // running and Raft should be able to establish a leader master. So,
      // txn-related operations routed through TxnManager should succeed.
      txn.commit();

      // An extra sanity check: read back the rows written into the table in the
      // context of the transaction.
      KuduScanner scanner = new KuduScanner.KuduScannerBuilder(asyncClient, table)
          .readMode(AsyncKuduScanner.ReadMode.READ_YOUR_WRITES)
          .replicaSelection(ReplicaSelection.LEADER_ONLY)
          .build();

      // It's an empty transaction, and 1 row should be there from the prior
      // sub-scenario.
      assertEquals(1, scanner.nextRows().getNumRows());
    }
  }

  /**
   * Test to verify that Kudu client is able to switch to TxnManager hosted by
   * other kudu-master process when the previously used one isn't available,
   * even if txn-related calls first are issued when no TxnManager was running.
   */
  @Test(timeout = 100000)
  @MasterServerConfig(flags = {
      // TxnManager functionality is necessary for this scenario.
      "--txn_manager_enabled",

      // Set Raft heartbeat interval short for faster test runtime: speed up
      // leader failure detection and new leader election.
      "--raft_heartbeat_interval_ms=100",
  })
  @TabletServerConfig(flags = {
      "--enable_txn_system_client_init=true",
  })
  public void testSwitchToOtherTxnManagerInFlightCalls() throws Exception {
    final String TABLE_NAME = "txn_manager_ops_fallback_inflight";
    client.createTable(
        TABLE_NAME,
        ClientTestUtil.getBasicSchema(),
        new CreateTableOptions().addHashPartitions(ImmutableList.of("key"), 2));

    KuduTransaction txn = client.newTransaction();
    KuduSession session = txn.newKuduSession();

    KuduTable table = client.openTable(TABLE_NAME);

    Insert insert = createBasicSchemaInsert(table, 0);
    session.apply(insert);
    session.flush();

    harness.killAllMasterServers();

    Thread t = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          // Sleep for some time to allow the KuduTransaction.commit() call
          // below issue RPCs to non-running TxnManangers.
          Thread.sleep(1000);
          harness.startAllMasterServers();
        } catch (Exception e) {
          fail("failed to start all masters: " + e);
        }
      }
    });
    t.start();

    // It should be possible to commit the transaction.
    txn.commit();

    // Just an extra sanity check: the thread should join pretty fast, otherwise
    // the call to KuduTransaction.commit() above could not succeed.
    t.join(250);

    // An extra sanity check: read back the rows written into the table in the
    // context of the transaction.
    KuduScanner scanner = new KuduScanner.KuduScannerBuilder(asyncClient, table)
        .readMode(AsyncKuduScanner.ReadMode.READ_YOUR_WRITES)
        .replicaSelection(ReplicaSelection.LEADER_ONLY)
        .build();

    assertEquals(1, countRowsInScan(scanner));
  }

  /**
   * Test to verify that Kudu client is able to switch to another TxnManager
   * instance when the kudu-master process which hosts currently used TxnManager
   * becomes temporarily unavailable (e.g. shut down, restarted, stopped, etc.).
   *
   * The essence of this scenario is to make sure that Kudu Java client connects
   * to a different TxnManager instance and starts sending txn keepalive
   * messages there in a timely manner, keeping the transaction alive even if
   * the originally used TxnManager instance isn't available.
   */
  @Test(timeout = 100000)
  @MasterServerConfig(flags = {
      // TxnManager functionality is necessary for this scenario.
      "--txn_manager_enabled",

      // Set Raft heartbeat interval short for faster test runtime: speed up
      // leader failure detection and new leader election.
      "--raft_heartbeat_interval_ms=100",
  })
  @TabletServerConfig(flags = {
      // The txn keepalive interval should be long enough to accommodate Raft
      // leader failure detection and election.
      "--txn_keepalive_interval_ms=1000",
      "--txn_staleness_tracker_interval_ms=250",
      "--enable_txn_system_client_init=true",
  })
  public void testTxnKeepaliveSwitchesToOtherTxnManager() throws Exception {
    final String TABLE_NAME = "txn_manager_fallback";
    client.createTable(
        TABLE_NAME,
        ClientTestUtil.getBasicSchema(),
        new CreateTableOptions().addHashPartitions(ImmutableList.of("key"), 2));

    KuduTransaction txn = client.newTransaction();
    KuduSession session = txn.newKuduSession();

    KuduTable table = client.openTable(TABLE_NAME);

    Insert insert = createBasicSchemaInsert(table, 0);
    session.apply(insert);
    session.flush();

    harness.killLeaderMasterServer();

    // Wait for two keepalive intervals to make sure the backend got a chance
    // to automatically abort the transaction if not receiving txn keepalive
    // messages.
    Thread.sleep(2 * 1000);

    // It should be possible to commit the transaction. This is to verify that
    //
    //   * the client eventually starts sending txn keepalive messages to other
    //     TxnManager instance (the original was hosted by former leader master
    //     which is no longer available), so the backend doesn't abort the
    //     transaction automatically due to not receiving keepalive messages
    //
    //   * the client switches to the new TxnManager for other txn-related
    //     operations as well
    txn.commit();

    // An extra sanity check: read back the rows written into the table in the
    // context of the transaction.
    KuduScanner scanner = new KuduScanner.KuduScannerBuilder(asyncClient, table)
        .readMode(AsyncKuduScanner.ReadMode.READ_YOUR_WRITES)
        .replicaSelection(ReplicaSelection.LEADER_ONLY)
        .build();
    assertEquals(1, countRowsInScan(scanner));
  }

  /**
   * Similar to the {@link #testTxnKeepaliveSwitchesToOtherTxnManager()} above,
   * but with additional twist of "rolling" unavailability of leader masters.
   * In addition, make sure the errors sent from TxnManager are processed
   * accordingly when TxnStatusManager is not around.
   */
  @Test(timeout = 100000)
  @MasterServerConfig(flags = {
      // TxnManager functionality is necessary for this scenario.
      "--txn_manager_enabled",

      // Set Raft heartbeat interval short for faster test runtime: speed up
      // leader failure detection and new leader election.
      "--raft_heartbeat_interval_ms=100",
  })
  @TabletServerConfig(flags = {
      // The txn keepalive interval should be long enough to accommodate Raft
      // leader failure detection and election.
      "--txn_keepalive_interval_ms=1000",
      "--txn_staleness_tracker_interval_ms=250",
      "--enable_txn_system_client_init=true",
  })
  public void testTxnKeepaliveRollingSwitchToOtherTxnManager() throws Exception {
    final String TABLE_NAME = "txn_manager_fallback_rolling";
    client.createTable(
        TABLE_NAME,
        ClientTestUtil.getBasicSchema(),
        new CreateTableOptions().addHashPartitions(ImmutableList.of("key"), 2));

    KuduTransaction txn = client.newTransaction();
    KuduSession session = txn.newKuduSession();

    KuduTable table = client.openTable(TABLE_NAME);

    // Cycle the leadership among masters, making sure the client successfully
    // switches to every newly elected leader master to send keepalive messages.
    final int numMasters = harness.getMasterServers().size();
    for (int i = 0; i < numMasters; ++i) {
      // Shutdown the leader master.
      final HostAndPort hp = harness.killLeaderMasterServer();

      // Wait for two keepalive intervals to give the backend a chance
      // to automatically abort the transaction if not receiving txn keepalive
      // messages.
      Thread.sleep(2 * 1000);

      // The transaction should be still alive.
      try {
        txn.isCommitComplete();
        fail("KuduTransaction.isCommitComplete should have thrown");
      } catch (NonRecoverableException e) {
        assertTrue(e.getStatus().toString(), e.getStatus().isIllegalState());
        assertEquals("transaction is still open", e.getMessage());
      }

      // In addition, it should be possible to insert rows in the context
      // of the transaction.
      session.apply(createBasicSchemaInsert(table, i));
      session.flush();

      // Start the master back.
      harness.startMaster(hp);
    }

    // Make sure Java client properly processes error responses sent back by
    // TxnManager when the TxnStatusManager isn't available. So, shutdown all
    // tablet servers: this is to make sure TxnStatusManager isn't there.
    harness.killAllTabletServers();

    Thread t = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          // Sleep for some time to allow the KuduTransaction.commit() call
          // below issue RPCs when TxnStatusManager is not yet around.
          Thread.sleep(2 * 1000);

          // Start all the tablet servers back so the TxnStatusManager is back.
          harness.startAllTabletServers();
        } catch (Exception e) {
          fail("failed to start all tablet servers back: " + e);
        }
      }
    });
    t.start();

    // The transaction should be still alive, and it should be possible to
    // commit it.
    txn.commit();

    t.join();

    // An extra sanity check: read back the rows written into the table in the
    // context of the transaction.
    KuduScanner scanner = new KuduScanner.KuduScannerBuilder(asyncClient, table)
        .readMode(AsyncKuduScanner.ReadMode.READ_YOUR_WRITES)
        .build();
    assertEquals(numMasters, countRowsInScan(scanner));
  }

  /**
   * Make sure {@link KuduTransaction#commit} flushes pending operations
   * for all sessions created off the {@link KuduTransaction} handle.
   */
  @Test(timeout = 100000)
  @MasterServerConfig(flags = {
      // TxnManager functionality is necessary for this scenario.
      "--txn_manager_enabled",
  })
  @TabletServerConfig(flags = {
      "--enable_txn_system_client_init=true",
  })
  public void testFlushSessionsOnCommit() throws Exception {
    final String TABLE_NAME = "flush_sessions_on_commit";
    client.createTable(
        TABLE_NAME,
        ClientTestUtil.getBasicSchema(),
        new CreateTableOptions().addHashPartitions(ImmutableList.of("key"), 2));
    KuduTable table = client.openTable(TABLE_NAME);
    int key = 0;

    // Regardless of the flush mode, a transactional session is automatically
    // flushed when the transaction is committed.
    {
      final SessionConfiguration.FlushMode[] kFlushModes = {
          SessionConfiguration.FlushMode.MANUAL_FLUSH,
          SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND,
          SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC,
      };

      for (SessionConfiguration.FlushMode mode : kFlushModes) {
        KuduTransaction txn = client.newTransaction();
        KuduSession session = txn.newKuduSession();
        session.setFlushMode(mode);
        Insert insert = createBasicSchemaInsert(table, key++);
        session.apply(insert);

        if (mode == SessionConfiguration.FlushMode.MANUAL_FLUSH) {
          assertTrue(session.hasPendingOperations());
        }

        txn.commit();

        assertFalse(session.hasPendingOperations());
        assertEquals(0, session.getPendingErrors().getRowErrors().length);
      }

      // Make sure all the applied rows have been persisted.
      KuduScanner scanner = new KuduScanner.KuduScannerBuilder(asyncClient, table)
          .readMode(AsyncKuduScanner.ReadMode.READ_YOUR_WRITES)
          .build();
      assertEquals(key, countRowsInScan(scanner));
    }

    // Make sure that all the transactional sessions are flushed upon committing
    // a transaction.
    {
      KuduTransaction txn = client.newTransaction();
      List<KuduSession> sessions = new ArrayList<>(10);
      for (int i = 0; i < 10; ++i) {
        KuduSession s = txn.newKuduSession();
        s.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
        Insert insert = createBasicSchemaInsert(table, key++);
        s.apply(insert);
        assertTrue(s.hasPendingOperations());
        sessions.add(s);
      }

      txn.commit();

      for (KuduSession session : sessions) {
        assertFalse(session.hasPendingOperations());
        assertEquals(0, session.getPendingErrors().getRowErrors().length);
      }

      // Make sure all the applied rows have been persisted.
      KuduScanner scanner = new KuduScanner.KuduScannerBuilder(asyncClient, table)
          .readMode(AsyncKuduScanner.ReadMode.READ_YOUR_WRITES)
          .build();
      assertEquals(key, countRowsInScan(scanner));
    }

    // Closing and flushing transactional sessions explicitly prior to commit
    // is totally fine as well.
    {
      KuduTransaction txn = client.newTransaction();
      {
        KuduSession s = txn.newKuduSession();
        s.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
        Insert insert = createBasicSchemaInsert(table, key++);
        s.apply(insert);
        s.close();
      }
      KuduSession session = txn.newKuduSession();
      session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
      Insert insert = createBasicSchemaInsert(table, key++);
      session.apply(insert);
      session.flush();

      txn.commit();

      assertFalse(session.hasPendingOperations());
      assertEquals(0, session.getPendingErrors().getRowErrors().length);

      // Make sure all the applied rows have been persisted.
      KuduScanner scanner = new KuduScanner.KuduScannerBuilder(asyncClient, table)
          .readMode(AsyncKuduScanner.ReadMode.READ_YOUR_WRITES)
          .build();
      assertEquals(key, countRowsInScan(scanner));
    }
  }

  /**
   * Make sure it's possible to recover from an error occurred while flushing
   * a transactional session: a transaction handle stays valid and it's possible
   * to retry calling {@link KuduTransaction#commit()} after handling session
   * flush errors.
   */
  @Test(timeout = 100000)
  @MasterServerConfig(flags = {
      // TxnManager functionality is necessary for this scenario.
      "--txn_manager_enabled",
  })
  @TabletServerConfig(flags = {
      "--enable_txn_system_client_init=true",
  })
  public void testRetryCommitAfterSessionFlushError() throws Exception {
    final String TABLE_NAME = "retry_commit_after_session_flush_error";
    client.createTable(
        TABLE_NAME,
        ClientTestUtil.getBasicSchema(),
        new CreateTableOptions().addHashPartitions(ImmutableList.of("key"), 2));
    KuduTable table = client.openTable(TABLE_NAME);
    int key = 0;

    KuduTransaction txn = client.newTransaction();
    KuduSession session = txn.newKuduSession();
    session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
    {
      Insert insert = createBasicSchemaInsert(table, key);
      session.apply(insert);
    }
    // Try to insert a row with a duplicate key.
    {
      Insert insert = createBasicSchemaInsert(table, key++);
      session.apply(insert);
    }

    try {
      txn.commit();
      fail("committing a transaction with duplicate row should have failed");
    } catch (NonRecoverableException e) {
      final String errmsg = e.getMessage();
      final Status status = e.getStatus();
      assertTrue(status.toString(), status.isIncomplete());
      assertTrue(errmsg, errmsg.matches(
          "failed to flush a transactional session: .*"));
    }

    // Insert one more row using the same session.
    {
      Insert insert = createBasicSchemaInsert(table, key++);
      session.apply(insert);
    }

    // Now, retry committing the transaction.
    txn.commit();

    assertEquals(0, session.getPendingErrors().getRowErrors().length);

    // Make sure all the applied rows have been persisted.
    KuduScanner scanner = new KuduScanner.KuduScannerBuilder(asyncClient, table)
        .readMode(AsyncKuduScanner.ReadMode.READ_YOUR_WRITES)
        .build();
    assertEquals(key, countRowsInScan(scanner));
  }

  /**
   * Make sure {@link KuduTransaction#startCommit} succeeds when called on
   * a transaction handle which has all of its transactional sessions flushed.
   */
  @Test(timeout = 100000)
  @MasterServerConfig(flags = {
      // TxnManager functionality is necessary for this scenario.
      "--txn_manager_enabled",
  })
  @TabletServerConfig(flags = {
      "--enable_txn_system_client_init=true",
  })
  public void testStartCommitWithFlushedSessions() throws Exception {
    final String TABLE_NAME = "start_commit_with_flushed_sessions";
    client.createTable(
        TABLE_NAME,
        ClientTestUtil.getBasicSchema(),
        new CreateTableOptions().addHashPartitions(ImmutableList.of("key"), 2));
    KuduTable table = client.openTable(TABLE_NAME);
    int key = 0;

    KuduTransaction txn = client.newTransaction();
    {
      KuduSession session = txn.newKuduSession();
      session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC);
      Insert insert = createBasicSchemaInsert(table, key++);
      session.apply(insert);
      assertFalse(session.hasPendingOperations());
      assertEquals(0, session.getPendingErrors().getRowErrors().length);
    }

    KuduSession session = txn.newKuduSession();
    session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
    Insert insert = createBasicSchemaInsert(table, key);
    session.apply(insert);
    assertTrue(session.hasPendingOperations());
    session.flush();
    assertFalse(session.hasPendingOperations());

    // KuduTransaction.startCommit() should succeed now.
    txn.startCommit();
  }

  /**
   * Check the behavior of {@link KuduTransaction#startCommit} when there are
   * non-flushed transactional sessions started off a transaction handle.
   */
  @Test(timeout = 100000)
  @MasterServerConfig(flags = {
      // TxnManager functionality is necessary for this scenario.
      "--txn_manager_enabled",
  })
  @TabletServerConfig(flags = {
      "--enable_txn_system_client_init=true",
  })
  public void testStartCommitWithNonFlushedSessions() throws Exception {
    final String TABLE_NAME = "non_flushed_sessions_on_start_commit";
    client.createTable(
        TABLE_NAME,
        ClientTestUtil.getBasicSchema(),
        new CreateTableOptions().addHashPartitions(ImmutableList.of("key"), 2));
    KuduTable table = client.openTable(TABLE_NAME);
    int key = 0;

    KuduTransaction txn = client.newTransaction();

    // Create one session which will have no pending operations upon
    // startCommit()
    {
      KuduSession session = txn.newKuduSession();
      session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC);
      Insert insert = createBasicSchemaInsert(table, key++);
      session.apply(insert);
      assertFalse(session.hasPendingOperations());
      assertEquals(0, session.getPendingErrors().getRowErrors().length);
    }

    KuduSession session = txn.newKuduSession();
    session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
    Insert insert = createBasicSchemaInsert(table, key);
    session.apply(insert);
    assertTrue(session.hasPendingOperations());

    try {
      txn.startCommit();
      fail("startCommit() should have failed when operations are pending");
    } catch (NonRecoverableException e) {
      final String errmsg = e.getMessage();
      final Status status = e.getStatus();
      assertTrue(status.toString(), status.isIllegalState());
      assertTrue(errmsg, errmsg.matches(
          ".* at least one transactional session has write operations pending"));
    }

    assertTrue(session.hasPendingOperations());
    assertEquals(0, session.getPendingErrors().getRowErrors().length);
  }

  /**
   * Verify the behavior of {@link KuduTransaction#newAsyncKuduSession} when the
   * commit process has already been started for the corresponding transaction.
   * This automatically verifies the behavior of
   * {@link KuduTransaction#newKuduSession} because it works via
   * {@link KuduTransaction#newAsyncKuduSession}.
   */
  @Test(timeout = 100000)
  @MasterServerConfig(flags = {
      // TxnManager functionality is necessary for this scenario.
      "--txn_manager_enabled",
  })
  @TabletServerConfig(flags = {
      "--enable_txn_system_client_init=true",
  })
  public void testNewSessionAfterCommit() throws Exception {
    final String TABLE_NAME = "new_session_after_commit";
    client.createTable(
        TABLE_NAME,
        ClientTestUtil.getBasicSchema(),
        new CreateTableOptions().addHashPartitions(ImmutableList.of("key"), 2));
    KuduTable table = client.openTable(TABLE_NAME);
    int key = 0;

    {
      KuduTransaction txn = client.newTransaction();
      KuduSession session = txn.newKuduSession();
      session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
      {
        Insert insert = createBasicSchemaInsert(table, key);
        session.apply(insert);
      }
      // Try to insert a row with a duplicate key.
      {
        Insert insert = createBasicSchemaInsert(table, key);
        session.apply(insert);
      }
      try {
        txn.commit();
        fail("committing a transaction with duplicate row should have failed");
      } catch (NonRecoverableException e) {
        final String errmsg = e.getMessage();
        final Status status = e.getStatus();
        assertTrue(status.toString(), status.isIncomplete());
        assertTrue(errmsg, errmsg.matches(
            "failed to flush a transactional session: .*"));
      }

      try {
        txn.newAsyncKuduSession();
        fail("newKuduSession() should throw when transaction already committed");
      } catch (IllegalStateException e) {
        final String errmsg = e.getMessage();
        assertTrue(errmsg, errmsg.matches("commit already started"));
      }
      txn.rollback();
    }

    {
      KuduTransaction txn = client.newTransaction();
      txn.commit();
      try {
        txn.newAsyncKuduSession();
        fail("newKuduSession() should throw when transaction already committed");
      } catch (IllegalStateException e) {
        final String errmsg = e.getMessage();
        assertTrue(errmsg, errmsg.matches(
            "transaction is not open for this handle"));
      }
    }
  }

  /**
   * This scenario is similar to the scenario above, but it calls
   * {@link KuduTransaction#startCommit} instead of
   * {@link KuduTransaction#commit}.
   */
  @Test(timeout = 100000)
  @MasterServerConfig(flags = {
      // TxnManager functionality is necessary for this scenario.
      "--txn_manager_enabled",
  })
  @TabletServerConfig(flags = {
      "--enable_txn_system_client_init=true",
  })
  public void testCreateSessionAfterStartCommit() throws Exception {
    KuduTransaction txn = client.newTransaction();
    txn.startCommit();
    try {
      txn.newAsyncKuduSession();
      fail("newKuduSession() should throw when transaction already committed");
    } catch (IllegalStateException e) {
      final String errmsg = e.getMessage();
      assertTrue(errmsg, errmsg.matches(
          "transaction is not open for this handle"));
    }
  }

  /**
   * A test scenario to verify the behavior of the client API when a write
   * operation submitted into a transaction session after the transaction
   * has already been committed.
   */
  @Test(timeout = 100000)
  @MasterServerConfig(flags = {
      // TxnManager functionality is necessary for this scenario.
      "--txn_manager_enabled",
  })
  @TabletServerConfig(flags = {
      "--enable_txn_system_client_init=true",
  })
  public void testSubmitWriteOpAfterCommit() throws Exception {
    final String TABLE_NAME = "submit_write_op_after_commit";
    client.createTable(
        TABLE_NAME,
        ClientTestUtil.getBasicSchema(),
        new CreateTableOptions().addHashPartitions(ImmutableList.of("key"), 2));
    KuduTable table = client.openTable(TABLE_NAME);
    int key = 0;

    KuduTransaction txn = client.newTransaction();
    KuduSession session = txn.newKuduSession();
    session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
    {
      Insert insert = createBasicSchemaInsert(table, key++);
      session.apply(insert);
    }

    txn.commit();

    {
      Insert insert = createBasicSchemaInsert(table, key);
      session.apply(insert);
    }
    List<OperationResponse> results = session.flush();
    assertEquals(1, results.size());
    OperationResponse rowResult = results.get(0);
    assertTrue(rowResult.hasRowError());
    String errmsg = rowResult.getRowError().toString();
    assertTrue(errmsg, errmsg.matches(
        ".* transaction ID .* not open: COMMITTED .*"));
  }

  // TODO(aserbin): when test harness allows for sending Kudu servers particular
  //                signals, add a test scenario to verify that timeout for
  //                TxnManager request is set low enough to detect 'frozen'
  //                TxnManager instance (e.g., sent SIGSTOP signal), and is able
  //                to switch to another TxnManager to send txn keepalive
  //                requests fast enough to keep the transaction alive.
}
