// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include "kudu/tablet/transactions/transaction_tracker.h"

#include <algorithm>
#include <limits>
#include <vector>

#include <boost/foreach.hpp>
#include <boost/thread/locks.hpp>
#include <boost/thread/mutex.hpp>

#include "kudu/gutil/strings/substitute.h"
#include "kudu/tablet/transactions/transaction_driver.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"

namespace kudu {
namespace tablet {

METRIC_DEFINE_gauge_uint64(tablet, all_transactions_inflight,
                           "Transactions In Flight",
                           kudu::MetricUnit::kTransactions,
                           "Number of transactions currently in-flight, including any type.");
METRIC_DEFINE_gauge_uint64(tablet, write_transactions_inflight,
                           "Write Transctions In Flight",
                           kudu::MetricUnit::kTransactions,
                           "Number of write transactions currently in-flight");
METRIC_DEFINE_gauge_uint64(tablet, alter_schema_transactions_inflight,
                           "Alter Schema Transactions In Flight",
                           kudu::MetricUnit::kTransactions,
                           "Number of alter schema transactions currently in-flight");
using boost::bind;
using std::vector;
using strings::Substitute;

TransactionsInFlight::TransactionsInFlight()
    : all_transactions_inflight(0),
      write_transactions_inflight(0),
      alter_schema_transactions_inflight(0) {
}

TransactionTracker::TransactionTracker() {
}

TransactionTracker::~TransactionTracker() {
  boost::lock_guard<simple_spinlock> l(lock_);
  CHECK_EQ(pending_txns_.size(), 0);
}

void TransactionTracker::Add(TransactionDriver *driver) {
  boost::lock_guard<simple_spinlock> l(lock_);
  IncrementCounters(driver->tx_type());
  pending_txns_.insert(driver);
}

void TransactionTracker::IncrementCounters(Transaction::TransactionType tx_type) {
  ++txns_in_flight_.all_transactions_inflight;
  switch (tx_type) {
    case Transaction::WRITE_TXN:
      ++txns_in_flight_.write_transactions_inflight;
      break;
    case Transaction::ALTER_SCHEMA_TXN:
      ++txns_in_flight_.alter_schema_transactions_inflight;
      break;
  }
}

void TransactionTracker::DecrementCounters(Transaction::TransactionType tx_type) {
  DCHECK_GT(txns_in_flight_.all_transactions_inflight, 0);
  --txns_in_flight_.all_transactions_inflight;
  switch (tx_type) {
    case Transaction::WRITE_TXN:
      DCHECK_GT(txns_in_flight_.write_transactions_inflight, 0);
      --txns_in_flight_.write_transactions_inflight;
      break;
    case Transaction::ALTER_SCHEMA_TXN:
      DCHECK_GT(txns_in_flight_.alter_schema_transactions_inflight, 0);
      --txns_in_flight_.alter_schema_transactions_inflight;
      break;
  }
}

void TransactionTracker::Release(TransactionDriver *driver) {
  boost::lock_guard<simple_spinlock> l(lock_);
  DecrementCounters(driver->tx_type());

  if (PREDICT_FALSE(pending_txns_.erase(driver) != 1)) {
    LOG(FATAL) << "Could not remove pending transaction from map: "
        << driver->ToStringUnlocked();
  }
}

void TransactionTracker::GetPendingTransactions(
    vector<scoped_refptr<TransactionDriver> >* pending_out) const {
  DCHECK(pending_out->empty());
  boost::lock_guard<simple_spinlock> l(lock_);
  BOOST_FOREACH(const scoped_refptr<TransactionDriver>& tx, pending_txns_) {
    // Increments refcount of each transaction.
    pending_out->push_back(tx);
  }
}

int TransactionTracker::GetNumPendingForTests() const {
  boost::lock_guard<simple_spinlock> l(lock_);
  return pending_txns_.size();
}

void TransactionTracker::WaitForAllToFinish() const {
  // Wait indefinitely.
  CHECK_OK(WaitForAllToFinish(MonoDelta::FromNanoseconds(std::numeric_limits<int64_t>::max())));
}

Status TransactionTracker::WaitForAllToFinish(const MonoDelta& timeout) const {
  const int complain_ms = 1000;
  int wait_time = 250;
  int num_complaints = 0;
  MonoTime start_time = MonoTime::Now(MonoTime::FINE);
  while (1) {
    vector<scoped_refptr<TransactionDriver> > txns;
    GetPendingTransactions(&txns);

    if (txns.empty()) {
      break;
    }

    MonoDelta diff = MonoTime::Now(MonoTime::FINE).GetDeltaSince(start_time);
    if (diff.MoreThan(timeout)) {
      return Status::TimedOut(Substitute("Timed out waiting for all transactions to finish. "
                                         "$0 transactions pending. Waited for $1",
                                         txns.size(), diff.ToString()));
    }
    int64_t waited_ms = diff.ToMilliseconds();
    if (waited_ms / complain_ms > num_complaints) {
      LOG(WARNING) << Substitute("TransactionTracker waiting for $0 outstanding transactions to"
                                 " complete now for $1 ms", txns.size(), waited_ms);
      num_complaints++;
    }
    wait_time = std::min(wait_time * 5 / 4, 1000000);

    LOG(INFO) << "Dumping currently running transactions: ";
    BOOST_FOREACH(scoped_refptr<TransactionDriver> driver, txns) {
      LOG(INFO) << driver->ToString();
    }
    SleepFor(MonoDelta::FromMicroseconds(wait_time));
  }
  return Status::OK();
}

void TransactionTracker::StartInstrumentation(const scoped_refptr<MetricEntity>& metric_entity) {
  METRIC_all_transactions_inflight.InstantiateFunctionGauge(
    metric_entity, Bind(&TransactionTracker::NumAllTransactionsInFlight,
                         Unretained(this)))
    ->AutoDetach(&metric_detacher_);
  METRIC_write_transactions_inflight.InstantiateFunctionGauge(
    metric_entity, Bind(&TransactionTracker::NumWriteTransactionsInFlight,
                         Unretained(this)))
    ->AutoDetach(&metric_detacher_);
  METRIC_alter_schema_transactions_inflight.InstantiateFunctionGauge(
    metric_entity, Bind(&TransactionTracker::NumAlterSchemaTransactionsInFlight,
                         Unretained(this)))
    ->AutoDetach(&metric_detacher_);
}

uint64_t TransactionTracker::NumAllTransactionsInFlight() const {
  boost::lock_guard<simple_spinlock> l(lock_);
  return txns_in_flight_.all_transactions_inflight;
}

uint64_t TransactionTracker::NumWriteTransactionsInFlight() const {
  boost::lock_guard<simple_spinlock> l(lock_);
  return txns_in_flight_.write_transactions_inflight;
}

uint64_t TransactionTracker::NumAlterSchemaTransactionsInFlight() const {
  boost::lock_guard<simple_spinlock> l(lock_);
  return txns_in_flight_.alter_schema_transactions_inflight;
}

}  // namespace tablet
}  // namespace kudu
