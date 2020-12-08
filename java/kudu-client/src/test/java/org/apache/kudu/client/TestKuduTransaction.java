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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.function.ThrowingRunnable;

import org.apache.kudu.test.KuduTestHarness;
import org.apache.kudu.test.KuduTestHarness.MasterServerConfig;
import org.apache.kudu.test.KuduTestHarness.TabletServerConfig;
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
  public void testCommitAnEmptyTransaction() throws Exception {
    KuduTransaction txn = client.newTransaction();
    txn.commit(false);
    // A duplicate call to commit the transaction using the same handle
    // should fail.
    IllegalStateException ex = assertThrows(
        IllegalStateException.class, new ThrowingRunnable() {
          @Override
          public void run() throws Throwable {
            txn.commit(false);
          }
        });
    assertEquals("transaction is not open for this handle", ex.getMessage());

    // Try to commit the same transaction using another handle that has been
    // constructed using serialize/deserialize sequence: it should be fine
    // since committing a transaction has idempotent semantics for the back-end.
    byte[] buf = txn.serialize();
    KuduTransaction serdesTxn = KuduTransaction.deserialize(buf, asyncClient);
    serdesTxn.commit(false);
  }

  /**
   * Test scenario that tries to commit a non-existent transaction.
   */
  @Test(timeout = 100000)
  @MasterServerConfig(flags = {
      "--txn_manager_enabled",
  })
  public void testCommitNonExistentTransaction() throws Exception {
    KuduTransaction txn = client.newTransaction();
    assertNotNull(txn);
    KuduTransaction fakeTxn = makeFakeTransaction(txn);
    try {
      // Try to commit the transaction in non-synchronous mode, i.e. just
      // initiate committing the transaction.
      fakeTxn.commit(false);
      fail("committing a non-existing transaction should have failed");
    } catch (NonRecoverableException e) {
      final String errmsg = e.getMessage();
      final Status status = e.getStatus();
      assertTrue(status.toString(), status.isNotFound());
      assertTrue(errmsg, errmsg.matches(".*transaction ID .* not found.*"));
    } catch (Exception e) {
      fail("unexpected exception: " + e.toString());
    }

    try {
      // Try to commit the transaction in synchronous mode, i.e. initiate
      // committing the transaction and wait for the commit phase to finalize.
      fakeTxn.commit(true);
      fail("committing a non-existing transaction should have failed");
    } catch (NonRecoverableException e) {
      final String errmsg = e.getMessage();
      final Status status = e.getStatus();
      assertTrue(status.toString(), status.isNotFound());
      assertTrue(errmsg, errmsg.matches(".*transaction ID .* not found.*"));
    } catch (Exception e) {
      fail("unexpected exception: " + e.toString());
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
  public void testIsCommitComplete() throws Exception {
    KuduTransaction txn = client.newTransaction();

    txn.commit(false);
    // TODO(aserbin): artificially delay the transaction's commit phase once
    //                the transaction commit orchestration is implemented
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
      assertEquals("transaction was aborted", ex.getMessage());
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
      assertTrue(status.toString(), status.isNotFound());
      final String errmsg = ex.getMessage();
      assertTrue(errmsg, errmsg.matches(".*transaction ID .* not found.*"));
    }
  }

  /**
   * Test scenario that starts a new transaction and commits it in a synchronous
   * way (i.e. waits for the transaction to be committed).
   *
   * TODO(aserbin): uncomment this once txn commit orchestration is ready
  @Test(timeout = 100000)
  @MasterServerConfig(flags = {
      "--txn_manager_enabled",
  })
  public void testCommitAnEmptyTransactionWait() throws Exception {
    KuduTransaction txn = client.newTransaction();
    txn.commit(true);
    assertTrue(txn.isCommitComplete());
  }
   */

  /**
   * A test scenario to start a new transaction and commit it in a synchronous
   * way (i.e. wait for the transaction to be committed) when the back-end is
   * running in the test-only mode to immediately finalize a transaction
   * right after transitioning its state to COMMIT_IN_PROGRESS.
   *
   * TODO(aserbin): remove this scenario once txn commit orchestration is ready
   */
  @Test(timeout = 100000)
  @MasterServerConfig(flags = {
      "--txn_manager_enabled",
  })
  @TabletServerConfig(flags = {
      "--txn_status_manager_finalize_commit_on_begin",
  })
  public void testCommitAnEmptyTransactionWaitFake2PCO() throws Exception {
    KuduTransaction txn = client.newTransaction();
    txn.commit(true);
    assertTrue(txn.isCommitComplete());
  }

  /**
   * Test scenario that tries to rollback a non-existent transaction.
   */
  @Test(timeout = 100000)
  @MasterServerConfig(flags = {
      "--txn_manager_enabled",
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
      assertTrue(status.toString(), status.isNotFound());
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
  public void testNewTransactionalSession() throws Exception {
    KuduTransaction txn = client.newTransaction();
    assertNotNull(txn);
    KuduSession session = txn.newKuduSession();
    assertNotNull(session);
    // TODO(aserbin): insert a few rows and rollback the transaction; run a
    //                table scan: the rows should not be there
    txn.rollback();
  }

  /**
   * Test scenario that starts a transaction and creates a new transactional
   * AsyncKuduSession based on the newly started transaction.
   */
  @Test(timeout = 100000)
  @MasterServerConfig(flags = {
      "--txn_manager_enabled",
  })
  public void testNewAsyncTransactionalSession() throws Exception {
    KuduTransaction txn = client.newTransaction();
    assertNotNull(txn);
    AsyncKuduSession session = txn.newAsyncKuduSession();
    assertNotNull(session);
    // TODO(aserbin): insert a few rows and rollback the transaction; run a
    //                table scan: the rows should not be there
    txn.rollback();
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
   *
   * TOOD(aserbin): update this once transaction handles send keepalive messages
   */
  @Test(timeout = 100000)
  @MasterServerConfig(flags = {
      "--txn_manager_enabled=true",
  })
  public void testAutoclosableUsage() throws Exception {
    try (KuduTransaction txn = client.newTransaction()) {
      byte[] buf = txn.serialize();
      assertNotNull(buf);
      txn.commit(false);
      txn.isCommitComplete();
    } catch (Exception e) {
      fail("unexpected exception: " + e.toString());
    }
  }
}
