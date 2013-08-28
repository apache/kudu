// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_CLIENT_CLIENT_H
#define KUDU_CLIENT_CLIENT_H

#include "common/schema.h"
#include "gutil/gscoped_ptr.h"
#include "gutil/macros.h"
#include "util/status.h"
#include "tserver/tserver.proxy.h" // TODO: move this to a protocol/ module
#include <string>
#include <tr1/memory>
#include <vector>

namespace kudu {

namespace rpc {
class Messenger;
}

namespace client {

class KuduTable;

struct KuduClientOptions {
  KuduClientOptions();

  // The RPC address of the tablet server to connect to.
  // When we are multi-node, this will switch to be whatever we use
  // to bootstrap the client -- perhaps a list of master addresses.
  std::string tablet_server_addr;
};

// A connection to a Kudu cluster.
// This class is thread-safe.
class KuduClient : public std::tr1::enable_shared_from_this<KuduClient> {
 public:
  static Status Create(const KuduClientOptions& options,
                       std::tr1::shared_ptr<KuduClient>* client);

  // Open the table with the given name.
  Status OpenTable(const std::string& table_name, std::tr1::shared_ptr<KuduTable>* table);

 private:
  friend class KuduTable;

  explicit KuduClient(const KuduClientOptions& options);
  Status Init();

  Status GetTabletProxy(const std::string& tablet_id,
                        std::tr1::shared_ptr<tserver::TabletServerServiceProxy>* proxy);

  bool initted_;
  KuduClientOptions options_;
  std::tr1::shared_ptr<rpc::Messenger> messenger_;

  // The proxy to the single tablet server. Eventually this will be some
  // kind of map from server ID to proxy.
  std::tr1::shared_ptr<tserver::TabletServerServiceProxy> proxy_;

  DISALLOW_COPY_AND_ASSIGN(KuduClient);
};

// A table in a Kudu cluster.
// This class is thread-safe.
class KuduTable {
 public:
  const std::string& name() const { return name_; }
  bool is_open() const { return proxy_; }

 private:
  friend class KuduClient;
  friend class KuduScanner;

  KuduTable(const std::tr1::shared_ptr<KuduClient>& client,
            const std::string& name);
  Status Open();

  std::tr1::shared_ptr<KuduClient> client_;
  std::tr1::shared_ptr<tserver::TabletServerServiceProxy> proxy_;
  std::string name_;

  DISALLOW_COPY_AND_ASSIGN(KuduTable);
};

// A single scanner. This class is not thread-safe, though different
// scanners on different threads may share a single KuduTable object.
class KuduScanner {
 public:
  // Initialize the scanner. The given 'table' object must remain valid
  // for the lifetime of this scanner object.
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

 private:
  Status CheckForErrors();

  bool open_;

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
