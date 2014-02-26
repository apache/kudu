// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_CLIENT_CLIENT_H
#define KUDU_CLIENT_CLIENT_H

#include "common/partial_row.h"
#include "common/schema.h"
#include "gutil/gscoped_ptr.h"
#include "gutil/ref_counted.h"
#include "gutil/macros.h"
#include "util/async_util.h"
#include "util/locks.h"
#include "util/status.h"
#include "tserver/tserver_service.proxy.h" // TODO: move this to a protocol/ module

#include <gtest/gtest.h>
#include <string>
#include <tr1/memory>
#include <vector>

namespace kudu {

class DnsResolver;

namespace rpc {
class Messenger;
}

namespace master {
class AlterTableRequestPB;
class MasterServiceProxy;
}

namespace client {

class AlterTableBuilder;
class Insert;
class KuduTable;
class KuduSession;
class MetaCache;
class CreateTableOptions;

namespace internal {
class Batcher;
class ErrorCollector;
} // namespace internal

struct KuduClientOptions {
  KuduClientOptions();

  // The RPC address of the master.
  // When we have a replicated master, this will switch to a vector of addresses.
  std::string master_server_addr;

  // The messenger to use.
  std::tr1::shared_ptr<rpc::Messenger> messenger;
};

// The KuduClient represents a connection to a cluster. From the user
// perspective, they should only need to create one of these in their
// application, likely a singleton -- but it's not a singleton in Kudu in any
// way. Different Client objects do not interact with each other -- no
// connection pooling, etc. Each KuduClient instance is sandboxed with no
// global cross-client state.
//
// In the implementation, the client holds various pieces of common
// infrastructure which is not table-specific:
//
// - RPC messenger: reactor threads and RPC connections are pooled here
// - Authentication: the client is initialized with some credentials, and
//   all accesses through it share those credentials.
// - Caches: caches of table schemas, tablet locations, tablet server IP
//   addresses, etc are shared per-client.
//
// In order to actually access data on the cluster, callers must first
// create a KuduSession object using NewSession(). A KuduClient may
// have several associated sessions.
//
// TODO: Cluster administration functions are likely to be in this class
// as well.
//
// This class is thread-safe.
class KuduClient : public std::tr1::enable_shared_from_this<KuduClient> {
 public:
  static Status Create(const KuduClientOptions& options,
                       std::tr1::shared_ptr<KuduClient>* client);

  Status CreateTable(const std::string& table_name,
                     const Schema& schema);
  Status CreateTable(const std::string& table_name,
                     const Schema& schema,
                     const CreateTableOptions& opts);

  Status DeleteTable(const std::string& table_name);

  Status AlterTable(const std::string& table_name,
                    const AlterTableBuilder& alter);

  // set 'alter_in_progress' to true if an AlterTable operation is in-progress
  Status IsAlterTableInProgress(const std::string& table_name,
                                bool *alter_in_progress);

  Status GetTableSchema(const std::string& table_name,
                        Schema *schema);

  // Open the table with the given name. If the table has not been opened before
  // in this client, this will do an RPC to ensure that the table exists and
  // look up its schema.
  //
  // TODO: should we offer an async version of this as well?
  // TODO: probably should have a configurable timeout in KuduClientOptions?
  // TODO: we should fetch the Schema from the catalog, rather than making the
  // user pass it here. the schema arg is a temporary stand-in.
  Status OpenTable(const std::string& table_name,
                   const Schema& schema,
                   scoped_refptr<KuduTable>* table);

  // Advanced API: return an RPC proxy to the given tablet ID.
  // TODO: this is only here temporarily. Eventually, users of this API should
  // not ever be exposed to the actual RPC proxy directly.
  Status GetTabletProxy(const std::string& tablet_id,
                        std::tr1::shared_ptr<tserver::TabletServerServiceProxy>* proxy);

  // Create a new session for interacting with the cluster.
  // User is responsible for destroying the session object.
  // This is a fully local operation (no RPCs or blocking).
  std::tr1::shared_ptr<KuduSession> NewSession();

  // TODO: this should probably be private and exposed only to certain friend classes.
  const std::tr1::shared_ptr<rpc::Messenger>& messenger() const {
    return messenger_;
  }

  // Return a proxy to the current master.
  // TODO: in the future, the master might move around (switch leaders), etc.
  // So, this returns a copy of the shared_ptr instead of a reference, in case it
  // gets modified.
  std::tr1::shared_ptr<master::MasterServiceProxy> master_proxy() const {
    return master_proxy_;
  }

  const KuduClientOptions& options() const { return options_; }

  // TODO: this should probably be private and exposed only to certain friend classes.
  DnsResolver* dns_resolver() { return dns_resolver_.get(); }

 private:
  friend class KuduTable;
  friend class RemoteTablet;
  friend class internal::Batcher;

  explicit KuduClient(const KuduClientOptions& options);
  Status Init();

  bool initted_;
  KuduClientOptions options_;
  std::tr1::shared_ptr<rpc::Messenger> messenger_;

  gscoped_ptr<DnsResolver> dns_resolver_;
  gscoped_ptr<MetaCache> meta_cache_;

  // Proxy to the master.
  std::tr1::shared_ptr<master::MasterServiceProxy> master_proxy_;

  DISALLOW_COPY_AND_ASSIGN(KuduClient);
};

class CreateTableOptions {
 public:
  CreateTableOptions();
  ~CreateTableOptions();

  // Set keys on which to pre-split the table. The vector is
  // copied.
  CreateTableOptions& WithSplitKeys(const std::vector<std::string>& keys);

 private:
  friend class KuduClient;
  std::vector<std::string> split_keys_;
};

// A KuduTable represents a table on a particular cluster. It holds the current
// schema of the table. Any given KuduTable instance belongs to a specific KuduClient
// instance.
//
// Upon construction, the table is looked up in the catalog (or catalog cache),
// and the schema fetched for introspection.
//
// This class is thread-safe.
class KuduTable : public base::RefCountedThreadSafe<KuduTable> {
 public:
  const std::string& name() const { return name_; }

  const Schema& schema() const { return schema_; }

  // Create a new insertion for this table.
  gscoped_ptr<Insert> NewInsert();

  // Get a proxy to the tablet server hosting this table.
  // This is a temporary shim which will go away once tablets are splittable, etc.
  std::tr1::shared_ptr<tserver::TabletServerServiceProxy> proxy();

  // TODO: REMOVE ME (exposed because some code uses some direct stuff)
  // --------------------------------------------------------------------------
  const string& tablet_id() const { return tablet_id_; }
  // --------------------------------------------------------------------------

 private:
  friend class KuduClient;
  friend class KuduScanner;
  friend class Insert;
  friend class base::RefCountedThreadSafe<KuduTable>;

  KuduTable(const std::tr1::shared_ptr<KuduClient>& client,
            const std::string& name,
            const Schema& schema);
  ~KuduTable();

  Status Open();

  std::tr1::shared_ptr<KuduClient> client_;

  // TODO: this will eventually go away, since every request will potentially go
  // to a different server. Instead, each request should lookup the RemoteTablet,
  // instance, use that to get a RemoteTabletServer, and then use that to obtain
  // the proxy.
  std::tr1::shared_ptr<tserver::TabletServerServiceProxy> proxy_;

  // Lock protecting proxy_
  // Should not call any Batcher methods or Session methods while holding this
  // lock.
  mutable simple_spinlock lock_;

  std::string name_;
  string tablet_id_;

  // TODO: figure out how we deal with a schema change from the client perspective.
  // Do we make them call a RefreshSchema() method? Or maybe reopen the table and get
  // a new KuduTable instance (which would simplify the object lifecycle a little?)
  const Schema schema_;

  DISALLOW_COPY_AND_ASSIGN(KuduTable);
};

// A single row insert to be sent to the cluster.
// See PartialRow API for field setters, etc.
class Insert {
 public:
  virtual ~Insert();

  const KuduTable* table() const { return table_.get(); }

  PartialRow* mutable_row() { return &row_; }
  const PartialRow& row() const { return row_; }

  std::string ToString() const {
    return "INSERT " + row_.ToString();
  }

 private:
  friend class KuduTable;
  explicit Insert(KuduTable* table);
  scoped_refptr<KuduTable> const table_;
  PartialRow row_;

  DISALLOW_COPY_AND_ASSIGN(Insert);
};

// Alter Table helper
//   AlterTableBuilder builder;
//   builder.AddNullableColumn("col1", UINT32);
//   client->AlterTable("table-name", builder);
class AlterTableBuilder {
 public:
  AlterTableBuilder();
  ~AlterTableBuilder();

  void Reset();

  bool has_changes() const;

  Status RenameTable(const string& new_name);

  Status AddColumn(const std::string& name,
                   DataType type,
                   const void *default_value,
                   ColumnStorageAttributes attributes = ColumnStorageAttributes());

  Status AddNullableColumn(const std::string& name,
                           DataType type,
                           ColumnStorageAttributes attributes = ColumnStorageAttributes());

  Status DropColumn(const std::string& name);

  Status RenameColumn(const std::string& old_name,
                      const std::string& new_name);

  // TODO: Add Edit column

 private:
  friend class KuduClient;
  master::AlterTableRequestPB* alter_steps_;

  DISALLOW_COPY_AND_ASSIGN(AlterTableBuilder);
};

// An error which occurred in a given operation. This tracks the operation
// which caused the error, along with whatever the actual error was.
class Error {
 public:
  ~Error();

  // Return the actual error which occurred.
  const Status& status() const {
    return status_;
  }

  // Return the operation which failed.
  const Insert& failed_op() const {
    return *failed_op_;
  }

  // Release the operation that failed. The caller takes ownership. Must only
  // be called once.
  gscoped_ptr<Insert> release_failed_op() {
    CHECK_NOTNULL(failed_op_.get());
    return failed_op_.Pass();
  }

  // In some cases, it's possible that the server did receive and successfully
  // perform the requested operation, but the client can't tell whether or not
  // it was successful. For example, if the call times out, the server may still
  // succeed in processing at a later time.
  //
  // This function returns true if there is some chance that the server did
  // process the operation, and false if it can guarantee that the operation
  // did not succeed.
  bool was_possibly_successful() const {
    // TODO: implement me - right now be conservative.
    return true;
  }

 private:
  Error(gscoped_ptr<Insert> failed_op, const Status& error);
  friend class internal::Batcher;

  gscoped_ptr<Insert> failed_op_;
  Status status_;

  DISALLOW_COPY_AND_ASSIGN(Error);
};


// A KuduSession belongs to a specific KuduClient, and represents a context in
// which all read/write data access should take place. Within a session,
// multiple operations may be accumulated and batched together for better
// efficiency. Settings like timeouts, priorities, and trace IDs are also set
// per session.
//
// A KuduSession's main purpose is for grouping together multiple data-access
// operations together into batches or transactions. It is important to note
// the distinction between these two:
//
// * A batch is a set of operations which are grouped together in order to
//   amortize fixed costs such as RPC call overhead and round trip times.
//   A batch DOES NOT imply any ACID-like guarantees. Within a batch, some
//   operations may succeed while others fail, and concurrent readers may see
//   partial results. If the client crashes mid-batch, it is possible that some
//   of the operations will be made durable while others were lost.
//
// * In contrast, a transaction is a set of operations which are treated as an
//   indivisible semantic unit, per the usual definitions of database transactions
//   and isolation levels.
//
// NOTE: Kudu does not currently support transactions! They are only mentioned
// in the above documentation to clarify that batches are not transactional and
// should only be used for efficiency.
//
// KuduSession is separate from KuduTable because a given batch or transaction
// may span multiple tables. This is particularly important in the future when
// we add ACID support, but even in the context of batching, we may be able to
// coalesce writes to different tables hosted on the same server into the same
// RPC.
//
// KuduSession is separate from KuduClient because, in a multi-threaded
// application, different threads may need to concurrently execute
// transactions. Similar to a JDBC "session", transaction boundaries will be
// delineated on a per-session basis -- in between a "BeginTransaction" and
// "Commit" call on a given session, all operations will be part of the same
// transaction. Meanwhile another concurrent Session object can safely run
// non-transactional work or other transactions without interfering.
//
// Additionally, there is a guarantee that writes from different sessions do not
// get batched together into the same RPCs -- this means that latency-sensitive
// clients can run through the same KuduClient object as throughput-oriented
// clients, perhaps by setting the latency-sensitive session's timeouts low and
// priorities high. Without the separation of batches, a latency-sensitive
// single-row insert might get batched along with 10MB worth of inserts from the
// batch writer, thus delaying the response significantly.
//
// Though we currently do not have transactional support, users will be forced
// to use a KuduSession to instantiate reads as well as writes.  This will make
// it more straight-forward to add RW transactions in the future without
// significant modifications to the API.
//
// Users who are familiar with the Hibernate ORM framework should find this
// concept of a Session familiar.
//
// This class is not thread-safe except where otherwise specified.
class KuduSession : public std::tr1::enable_shared_from_this<KuduSession> {
 public:
  ~KuduSession();

  enum FlushMode {
    // Every write will be sent to the server in-band with the Apply()
    // call. No batching will occur. This is the default flush mode. In this
    // mode, the Flush() call never has any effect, since each Apply() call
    // has already flushed the buffer. This is the default flush mode.
    AUTO_FLUSH_SYNC,

    // Apply() calls will return immediately, but the writes will be sent in
    // the background, potentially batched together with other writes from
    // the same session. If there is not sufficient buffer space, then Apply()
    // may block for buffer space to be available.
    //
    // Because writes are applied in the background, any errors will be stored
    // in a session-local buffer. Call CountPendingErrors() or GetPendingErrors()
    // to retrieve them.
    // TODO: provide an API for the user to specify a callback to do their own
    // error reporting.
    // TODO: specify which threads the background activity runs on (probably the
    // messenger IO threads?)
    //
    // The Flush() call can be used to block until the buffer is empty.
    AUTO_FLUSH_BACKGROUND,

    // Apply() calls will return immediately, and the writes will not be
    // sent until the user calls Flush(). If the buffer runs past the
    // configured space limit, then Apply() will return an error.
    MANUAL_FLUSH
  };

  // Set the flush mode.
  // REQUIRES: there should be no pending writes -- call Flush() first to ensure.
  Status SetFlushMode(FlushMode m) WARN_UNUSED_RESULT;

  // Set the amount of buffer space used by this session for outbound writes.
  // The effect of the buffer size varies based on the flush mode of the
  // session:
  //
  // AUTO_FLUSH_SYNC:
  //   since no buffering is done, this has no effect
  // AUTO_FLUSH_BACKGROUND:
  //   if the buffer space is exhausted, then write calls will block until there
  //   is space available in the buffer.
  // MANUAL_FLUSH:
  //   if the buffer space is exhausted, then write calls will return an error.
  void SetMutationBufferSpace(size_t size);

  // Set the timeout for writes made in this session.
  //
  // TODO: need to more carefully handle timeouts so that they include the
  // time spent doing tablet lookups, etc.
  void SetTimeoutMillis(int millis);

  // Set priority for calls made from this session. Higher priority calls may skip
  // lower priority calls.
  // TODO: this is not yet implemented and needs further design work to know what
  // exactly it will mean in practice. The API is just here to show at what layer
  // call priorities will be exposed to the client.
  void SetPriority(int priority);

  // TODO: add "doAs" ability here for proxy servers to be able to act on behalf of
  // other users, assuming access rights.

  // Insert the given row.
  // TODO: will probably make Insert derive from some "WriteOperation" class,
  // and have Apply take any WriteOperation, rather than being insert-specific.
  // Just supporting inserts for now to keep reviews smaller.
  //
  // If this returns an OK status, then the session has taken ownership of the
  // pointer and the gscoped_ptr will be reset to NULL. On error, the gscoped_ptr
  // is left alone (eg so that the caller may log an error message with it).
  //
  // The behavior of this function depends on the current flush mode. Regardless
  // of flush mode, however, Apply may begin to perform processing in the background
  // for the call (e.g looking up the tablet, etc). Given that, an error may be
  // queued into the PendingErrors structure prior to flushing, even in MANUAL_FLUSH
  // mode.
  //
  // This is thread safe.
  Status Apply(gscoped_ptr<Insert>* insert) WARN_UNUSED_RESULT;

  // Similar to the above, except never blocks. Even in the flush modes that return
  // immediately, StatusCallback is triggered with the result. The callback may
  // be called by a reactor thread, or in some cases may be called inline by
  // the same thread which calls ApplyAsync().
  // TODO: not yet implemented.
  void ApplyAsync(gscoped_ptr<Insert>* insert, StatusCallback cb);

  // Flush any pending writes.
  //
  // Returns a bad status if there are any pending errors after the rows have
  // been flushed. Callers should then use GetPendingErrors to determine which
  // specific operations failed.
  //
  // In AUTO_FLUSH_SYNC mode, this has no effect, since every Apply() call flushes
  // itself inline.
  //
  // In the case that the async version of this method is used, then the callback
  // will be called upon completion of the operations which were buffered since the
  // last flush. In other words, in the following sequence:
  //
  //    session->Insert(a);
  //    session->FlushAsync(callback_1);
  //    session->Insert(b);
  //    session->FlushAsync(callback_2);
  //
  // ... 'callback_2' will be triggered once 'b' has been inserted, regardless of whether
  // 'a' has completed or not.
  //
  // Note that this also means that, if FlushAsync is called twice in succession, with
  // no intervening operationsthe second flush will return immediately. For example:
  //
  //    session->Insert(a);
  //    session->FlushAsync(callback_1); // called when 'a' is inserted
  //    session->FlushAsync(callback_2); // called immediately!
  //
  // Note that, as in all other async functions in Kudu, the callback may be called
  // either from an IO thread or the same thread which calls FlushAsync. The callback
  // should not block.
  //
  // This function is thread-safe.
  Status Flush() WARN_UNUSED_RESULT;
  void FlushAsync(const StatusCallback& cb);

  // Close the session.
  // Returns an error if there are unflushed or in-flight operations.
  Status Close() WARN_UNUSED_RESULT;

  // Return true if there are operations which have not yet been delivered to the
  // cluster. This may include buffered operations (i.e those that have not yet been
  // flushed) as well as in-flight operations (i.e those that are in the process of
  // being sent to the servers). After calling Flush()
  // TODO: maybe "incomplete" or "undelivered" is clearer?
  //
  // This function is thread-safe.
  bool HasPendingOperations() const;

  // Return the number of buffered operations. These are operations that have
  // not yet been flushed - i.e they are not en-route yet.
  //
  // Note that this is different than HasPendingOperations() above, which includes
  // operations which have been sent and not yet responded to.
  //
  // This is only relevant in MANUAL_FLUSH mode, where the result will not
  // decrease except for after a manual Flush, after which point it will be 0.
  // In the other flush modes, data is immediately put en-route to the destination,
  // so this will return 0.
  //
  // This function is thread-safe.
  int CountBufferedOperations() const;

  // Return the number of errors which are pending. Errors may accumulate when
  // using the AUTO_FLUSH_BACKGROUND mode.
  //
  // This function is thread-safe.
  int CountPendingErrors() const;

  // Return any errors from previous calls. If there were more errors
  // than could be held in the session's error storage, then sets *overflowed to true.
  //
  // Caller takes ownership of the returned errors.
  //
  // This function is thread-safe.
  void GetPendingErrors(std::vector<Error*>* errors, bool* overflowed);

  KuduClient* client() { return client_.get(); }

 private:
  friend class KuduClient;
  friend class internal::Batcher;
  explicit KuduSession(const std::tr1::shared_ptr<KuduClient>& client);

  // Must be called after construction, and after the KuduSession is inside
  // a shared_ptr.
  void Init();

  // Called by Batcher when a flush has finished.
  void FlushFinished(internal::Batcher* b);

  // Swap in a new Batcher instance, returning the old one in '*old_batcher', unless it is
  // NULL.
  void NewBatcher(scoped_refptr<internal::Batcher>* old_batcher);

  // The client that this session is associated with.
  const std::tr1::shared_ptr<KuduClient> client_;

  // Lock protecting internal state.
  // Note that this lock should not be taken if the thread is already holding
  // a Batcher lock. This must be acquired first.
  mutable simple_spinlock lock_;

  // Buffer for errors.
  scoped_refptr<internal::ErrorCollector> error_collector_;

  // The current batcher being prepared.
  scoped_refptr<internal::Batcher> batcher_;

  // Any batchers which have been flushed but not yet finished.
  //
  // Upon a batch finishing, it will call FlushFinished(), which removes the batcher from
  // this set. This set does not hold any reference count to the Batcher, since, while
  // the flush is active, the batcher manages its own refcount. The Batcher will always
  // call FlushFinished() before it destructs itself, so we're guaranteed that these
  // pointers stay valid.
  std::tr1::unordered_set<internal::Batcher*> flushed_batchers_;

  FlushMode flush_mode_;

  // Timeout for the next batch.
  int timeout_ms_;

  DISALLOW_COPY_AND_ASSIGN(KuduSession);
};


// A single scanner. This class is not thread-safe, though different
// scanners on different threads may share a single KuduTable object.
class KuduScanner {
 public:
  // Initialize the scanner. The given 'table' object must remain valid
  // for the lifetime of this scanner object.
  // TODO: should table be a const pointer?
  explicit KuduScanner(KuduTable* table);
  ~KuduScanner();

  // TODO: add an explicit close? use the dtor? would be good to
  // free the server-side resources.

  // Set the projection used for this scanner.
  Status SetProjection(const Schema& projection);

  // Add a predicate to this scanner.
  // The predicates act as conjunctions -- i.e, they all must pass for
  // a row to be returned.
  // TODO: currently, the predicates must refer to columns which are also
  // part of the projection.
  Status AddConjunctPredicate(const tserver::ColumnRangePredicatePB& pb);

  // Begin scanning.
  Status Open();

  // Close the scanner.
  // This releases resources on the server.
  //
  // This call does not block, and will not ever fail, even if the server
  // cannot be contacted.
  //
  // NOTE: the scanner is reset to its initial state by this function.
  // You'll have to re-add any projection, predicates, etc if you want
  // to reuse this Scanner object.
  void Close();

  // Return true if there are more rows to be fetched from this scanner.
  bool HasMoreRows() const;

  // Return the next batch of rows.
  // Each row is a pointer suitable for constructing a ConstContiguousRow.
  // TODO: this isn't a good API... need to fix this up while maintaining good
  // performance.
  Status NextBatch(std::vector<const uint8_t*>* rows);

  // Set the hint for the size of the next batch in bytes.
  // If setting to 0 before calling Open(), it means that the first call
  // to the tablet server won't return data.
  Status SetBatchSizeBytes(uint32_t batch_size);

 private:
  Status CheckForErrors();

  bool open_;

  bool data_in_open_;

  Schema projection_;

  // The next scan request to be sent. This is cached as a field
  // since most scan requests will share the scanner ID with the previous
  // request.
  tserver::ScanRequestPB next_req_;

  // The last response received from the server. Cached for buffer reuse.
  tserver::ScanResponsePB last_response_;

  // RPC controller for the last in-flight RPC.
  rpc::RpcController controller_;

  KuduTable* table_;

  DISALLOW_COPY_AND_ASSIGN(KuduScanner);
};

} // namespace client
} // namespace kudu
#endif
