// Copyright (c) 2013, Cloudera, inc

#include "rpc/rpc-test-base.h"

#include <string>

#include <gtest/gtest.h>
#include <gutil/gscoped_ptr.h>
#include <sasl/sasl.h>

#include "rpc/sasl_common.h"

using std::string;

namespace kudu {
namespace rpc {

class TestSaslRpc : public RpcTestBase {
 public:
  virtual void SetUp() {
    RpcTestBase::SetUp();
    ASSERT_STATUS_OK(InitSASL("sasl_rpc-test"));
  }
};

// Basic sanity test for the Cyrus SASL library
TEST_F(TestSaslRpc, TestSaslAnonNoConn) {
  const char* kAuthMethod = "ANONYMOUS";

  const char **mechs = sasl_global_listmech();
  while (*mechs != NULL) {
    LOG(INFO) << "SASL library found mechanism: " << *mechs;
    ++mechs;
  }

  // Callbacks we support.
  // They must be defined here, but if they are NULL then the client library
  // will prompt us by returning SASL_INTERACT during start/step below.
  sasl_callback_t callbacks[] = {
    { SASL_CB_CANON_USER, NULL, NULL }, // Handle with interactions, i.e. the library will
    { SASL_CB_USER, NULL, NULL },       // return SASL_INTERACT during sasl_client_start()
    { SASL_CB_LIST_END, NULL, NULL }
  };

  LOG(INFO) << "Creating new client...";

  /* The SASL context kept for the life of the connection */
  sasl_conn_t* newconn = NULL;

  /* client new connection */
  int result = sasl_client_new(
      "kudurpc",    // The service we are using
      "localhost",  // The fully qualified domain name of the server we're connecting to
      NULL, NULL,   // Local and remote IP address strings
                    // (NULL disables mechanisms which require this info)
      callbacks,    // Connection-specific callbacks
      0,            // Security flags
      &newconn);

  gscoped_ptr<sasl_conn_t, SaslDeleter> conn(newconn);

  ASSERT_EQ(SASL_OK, result) << sasl_errstring(result, NULL, NULL) << ": " <<
    sasl_errdetail(conn.get());

  string user = "yojimbo";
  sasl_interact_t *client_interact = NULL;
  const char *out, *mechusing;
  unsigned outlen;

  do {
    result = sasl_client_start(conn.get(),        // The same context from above
                               kAuthMethod,       // The list of mechanisms from the server
                               &client_interact,  // Filled in if an interaction is needed
                               &out,              // Filled in on success
                               &outlen,           // Filled in on success
                               &mechusing);       // Filled in on success

    if (result == SASL_INTERACT) {
      LOG(INFO) << "Interactions...";
      if (client_interact != NULL) {
        LOG(INFO) << client_interact->challenge;
        LOG(INFO) << client_interact->prompt << " (default: ) " << client_interact->defresult;
        LOG(INFO) << client_interact->id;
        switch (client_interact->id) {
          case SASL_CB_USER:
            client_interact->result = user.c_str();
            client_interact->len = user.length();
            break;
          default:
            break;
        }
      }
    }
  } while (result == SASL_INTERACT); // The mechanism may ask us to fill
                                     // in things many times. Result is
                                     // SASL_OK on success.

  ASSERT_EQ(SASL_OK, result) << sasl_errstring(result, NULL, NULL) << ": " <<
    sasl_errdetail(conn.get());
}

} // namespace rpc
} // namespace kudu

