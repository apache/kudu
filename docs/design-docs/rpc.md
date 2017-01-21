<!---
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# RPC

## Intro

The RPC layer makes communication with remote processes look like local
function calls. You can make either asynchronous calls, in which you provide a
callback which is invoked later, or synchronous calls, where your thread blocks
until the remote system responds.

The wire format of Kudu RPC is very close to the wire format of Hadoop IPC in
hadoop-3 and beyond. It is not identical since there are still some java-isms
left in Hadoop IPC which we did not want to inherit. In addition, Kudu RPC has
developed some extra features such as deadline propagation which are not
available in Hadoop. However, the overall structure of the wire protocol is
very similar.

We use protocol buffers for serialization, and libev for non-blocking I/O.

For some code examples, look in rpc-test.cc and rpc_stub-test.

## Overview

```
                                        +------------------------------------+
                                        | AcceptorPool                       |
                                        |   a pool of threads which          |
  +-------------------------+           |   call accept()                    |
  | Proxy                   |           +------------------------------------+
  |                         |                          | new socket
  | The proxy is the object |                          V
  | which has the remote    |           +------------------------------------+
  | method definitions.     | --------> | Messenger                          |
  |                         |           |                                    |
  +-------------------------+           | +-----------+ +-----------+        |
                                        | | reactor 1 | | reactor 2 | ...    |
  +-------------------------+           | +-----------+ +-----------+        |
  | ResponseCallback        | <-------- |                                    |<-.
  |                         |           +------------------------------------+  |
  | The callback which gets |                          |                        |
  | invoked when the remote |                          V                        |
  | end replies or the call |           +------------------------------------+  |
  | otherwise terminates.   |           | ServicePool                        |  |
  +-------------------------+           |   a pool of threads which          |  | Call responses
                                        |   pull new inbound calls from a    |  | sent back via
                                        |   work queue.                      |  | messenger.
                                        +------------------------------------+  |
                                                       |                        |
                                                       v                        |
                                        +------------------------------------+  |
                                        | ServiceIf                          |  |
                                        |   user-implemented class which     | /
                                        |   handles new inbound RPCs         |
                                        +------------------------------------+
```

Each reactor has a thread which uses epoll to handle many sockets using
non-blocking I/O. Blocking calls are implemented by the Proxy using
non-blocking calls-- from the point of view of the Messenger, all calls are
non-blocking.

The acceptor pool and the service pool are optional components. If you don't
expect anyone to be connecting to you, you do not have to start them. If a server
expects to listen on multiple ports (eg for different protocols), multiple
AcceptorPools may be attached.

## Proxy classes

Proxy classes are used by the client to send calls to a remote service.
Calls may be made synchronously or asynchronously -- the synchronous calls are simply
a wrapper around the asynchronous version, which makes the call and then waits
on the callback to be triggered.

In order to make a call, the user must provide a method name, a request protobuf,
a response protobuf, an RpcController, and a callback.

Each RpcController object corresponds to exactly one in-flight call on the client.
This class is where per-call settings may be adjusted before making an RPC --
currently this is just timeout functionality, but in the future may include
other call properties such as tracing information, priority classes, deadline
propagation, etc.

Upon issuing the asynchronous request, the RPC layer enqueues the call to be sent
to the server and immediately returns. During this period, the caller thread
may continue to send other RPCs or perform other processing while waiting for
the callback to be triggered. In the future, we will provide an RPC cancellation
function on the RpcController object in case the user determines that the call
is no longer required.

When the call completes, the RPC layer will invoke the provided ResponseCallback
function from within the context of the reactor thread. Given this,
ResponseCallbacks should be careful to never block, as it would prevent other
threads from concurrent sending or receiving RPCs.

The callback is invoked exactly one time, regardless of the call's termination state.
The user can determine the call's state by invoking methods on the RpcController object,
for example to determine whether the call succeded, timed out, or suffered a
transport error. In the case that the call succeeds, the user-provided response protobuf
will have been initialized to contain the result.

Please see the accompanying documentation in the Proxy and RpcController classes
for more information on the specific API, as well as the test cases in rpc-test.cc
for example usage.

## Generated Code

In general, clients will use auto-generated subclasses of Proxy and ServiceIf to
get additional type safety and nicer APIs.

The generated proxy object has the same API as the generic Proxy, except that
methods are generated for each RPC defined within the protobuf service. Each
RPC has a synchronous and async version, corresponding to Proxy::AsyncRequest and
Proxy::SyncRequest. These generated methods have an identical API to the generic
one except that they are type-safe and do not require the method name to be passed.

The generated ServiceIf class contains pure virtual methods for each of the RPCs
in the service. Each method to be implemented has an API like:

  void MethodName(const RequestPB *req,
     ResponsePB *resp, ::kudu::rpc::RpcContext *context);

The request PB is the user-provided request, and the response PB is a cleared
protobuf ready to store the RPC response. Once the RPC response has been filled in,
the service should call context->RespondSuccess(). This method may be called
from any thread in the application at any point either before or after the
actual handler method returns.

In the case of an unexpected error, the generated code may alternatively call
context->RespondFailure(...). However, for any error responses which should be
parseable by the client code, it is preferable to define an error response inside
the response protobuf itself -- this is a much more flexible way of returning
actionable information with an error, given that Status just holds a string
and not much else.

See rpc/rpc-test-base.h for an example service implementation, as well as the
documentation comments in rpc/service_if.h.

## `ServiceIf` classes

ServiceIf classes are abstract interfaces that the server implements to handle
incoming RPCs. In general, each generated service has several virtual methods
which you can override in order to implement the relevant function call.

There is a ServicePool which you can use to coordinate several worker threads
handling callbacks.

## Exactly once semantics

Exactly once semantics can be enabled for RPCs that require them by using
the 'track_rpc_result' option when declaring a service interface method.

Example:
```
service CalculatorService {
  rpc AddExactlyOnce(ExactlyOnceRequestPB) returns (ExactlyOnceResponsePB) {
    option (kudu.track_rpc_result) = true;
  }
}
```

This works in tandem with unique identifier for an RPC (RequestIdPB, see
rpc_header.proto) that must be specified on the RPC header.

When both the option was selected and the unique identifier is available
the server will make sure that, independently of how many retries, an RPC
with a certain id is executed exactly once and that all retries receive
the same response.

Note that enabling the option only guarantees exactly once semantics for
the lifetime of the server, making sure that responses survive crashes
and are also available in replicas (for replicated RPCs, like writes) needs
that actions be taken outside of the RPC subsystem.

## RPC Sidecars

RPC sidecars are used to avoid excess copies for large volumes of data.
Prior to RPC sidecars, the sequence of steps for creating an RPC response
on the server side would be as follows:

1. Write the prepared response to a Google protobuf message.
2. Pass the message off to the InboundCall class, which serializes the
   protobuf into a process-local buffer.
3. Copy the process-local buffer to the kernel buffer (send() to a socket).

The client follows these steps in reverse order. On top of the extra copy,
this procedure also forces us to use std::string, which is difficult for
compilers to inline code for and requires that reserved bytes are nulled out,
which is an unnecessary call to memset.

Instead, sidecars provide a mechanism to indicate the need to pass a large
store of data to the InboundCall class, which manages the response to a single
RPC on the server side. When send()-ing the rest of the message (i.e., the
protobuf), the sidecar's data is directly written to the socket.

The data is appended directly after the main message protobuf. Here's what
a typical message looks like without sidecars:

```
+------------------------------------------------+
| Total message length (4 bytes)                 |
+------------------------------------------------+
| RPC Header protobuf length (variable encoding) |
+------------------------------------------------+
| RPC Header protobuf                            |
+------------------------------------------------+
| Main message length (variable encoding)        |
+------------------------------------------------+
| Main message protobuf                          |
+------------------------------------------------+
```

In this case, the main message length is equal to the protobuf's byte size.
Since there are no sidecars, the header protobuf's sidecar_offsets list
will will be empty.

Here's what it looks like with the sidecars:

```
+------------------------------------------------+
| Total message length (4 bytes)                 |
+------------------------------------------------+
| RPC Header protobuf length (variable encoding) |
+------------------------------------------------+
| RPC Header protobuf                            |
+------------------------------------------------+
| Main message length (variable encoding)        |
+------------------------------------------------+ --- 0
| Main message protobuf                          |
+------------------------------------------------+ --- sidecar_offsets(0)
| Sidecar 0                                      |
+------------------------------------------------+ --- sidecar_offsets(1)
| Sidecar 1                                      |
+------------------------------------------------+ --- sidecar_offsets(2)
| Sidecar 2                                      |
+------------------------------------------------+ --- ...
| ...                                            |
+------------------------------------------------+
```

When there are sidecars, the sidecar_offsets member in the header will be a
nonempty list, whose values indicate the offset, measured from the beginning
of the main message protobuf, of the start of each sidecar. The number
of offsets will indicate the number of sidecars.

Then, on the client side, the sidecars locations are decoded and made available
by RpcController::GetSidecars() (which returns the pointer to the array of all
the sidecars). The caller must be sure to check that the sidecar index in the
sidecar array is correct and in-bounds.

More information is available in rpc/rpc_sidecar.h.

## Wire Protocol

### Connection establishment and connection header

After the client connects to a server, the client first sends a connection header.
The connection header consists of a magic number "hrpc" and three byte flags,
for a total of 7 bytes:

```
+----------------------------------+
|  "hrpc" 4 bytes                  |
+----------------------------------+
|  Version (1 byte)                |
+----------------------------------+
|  ServiceClass (1 byte)           |
+----------------------------------+
|  AuthProtocol (1 byte)           |
+----------------------------------+
```
Currently, the RPC version is 9. The ServiceClass and AuthProtocol fields are unused.


### Message framing and request/response headers

Aside from the initial connection header described above, all other messages are
serialized as follows:
```
  total_size: (32-bit big-endian integer)
    the size of the rest of the message, not including this 4-byte header

  header: varint-prefixed header protobuf
    - client->server messages use the RequestHeader protobuf
    - server->client messages use the ResponseHeader protobuf

  body: varint-prefixed protobuf
    - for typical RPC calls, this is the user-specified request or response protobuf
    - for RPC calls which caused an error, the response is an ErrorStatusPB
    - during negotiation, this is a NegotiatePB
```

### Example packet capture

An example call (captured with strace on rpc-test.cc) follows:

```
   "\x00\x00\x00\x17"   (total_size: 23 bytes to follow)
   "\x09"  RequestHeader varint: 9 bytes
    "\x08\x0a\x1a\x03\x41\x64\x64\x20\x01" (RequestHeader protobuf)
      Decoded with protoc --decode=RequestHeader rpc_header.proto:
      callId: 10
      methodName: "Add"
      requestParam: true

   "\x0c"  Request parameter varint: 12 bytes
    "\x08\xd4\x90\x80\x91\x01\x10\xf8\xcf\xc4\xed\x04" Request parameter
      Decoded with protoc --decode=kudu.rpc_test.AddRequestPB rpc/rtest.proto
      x: 304089172
      y: 1303455736
```

## Negotiation

After the initial connection header is sent, negotiation begins. Negotiation
consists of a sequence of request/response messages sent between the client and
server. Each message is of type `NegotiatePB` and includes a `step` field which
identifies the negotiation protocol step. Each `NegotiatePB` is framed as usual
using `RequestHeader` or `ResponseHeader` messages with `call_id` -33.

The Kudu negotiation protocol allows the client and server to communicate
supported RPC feature flags and supported SASL authentication mechanisms,
initiate an optional TLS handshake, and perform SASL negotiation.

#### Step 1: Negotiate

The client and server swap RPC feature flags and supported SASL mechanisms. This
step always takes exactly one round trip.

```
Client                                                                    Server
   |                                                                        |
   | +----NegotiatePB-----------------------------+                         |
   | | step = NEGOTIATE                           |                         |
   | | supported_features = <client RPC features> | ----------------------> |
   | | auths = <client SASL mechanisms>           |                         |
   | +--------------------------------------------+                         |
   |                                                                        |
   |                         +----NegotiatePB-----------------------------+ |
   |                         | step = NEGOTIATE                           | |
   | <---------------------- | supported_features = <server RPC features> | |
   |                         | auths = <server SASL mechanisms>           | |
   |                         +--------------------------------------------+ |
```

RPC feature flags allow the client and server to know what optional features the
other end can support, for example TLS encryption. There are several advantages
of using feature flags over version numbers for this purpose:

* since we have both a Java and C++ client, this allows us to add features in
  different orders, or decide to not support a feature at all in one client or
  the other. For example, the C++ client is likely to gain support for a
  shared-memory transport long before the Java one.
* this allows much more flexibility in backporting RPC system features across
  versions. For example, if we introduce feature 'A' in Kudu 2.0, and feature
  'B' in Kudu 2.1, we are still able to selectively backport 'B' without 'A' to
  Kudu 1.5.
* the set of supported features can be determined by code-level support as well
  as conditionally based on configuration or machine capability.

#### Step 2: TLS Handshake

If both the server and client support the `TLS` RPC feature flag, the client
initiates a TLS handshake, after which both sides wrap the socket in the TLS
protected channel. If either the client or server does not support the `TLS`
flag, then this step is skipped. This step takes as many round trips as
necessary to complete the TLS handshake.

```
Client                                                                    Server
   |                                                                        |
   | +----NegotiatePB------------------------+                              |
   | | step = TLS_HANDSHAKE                  |                              |
   | | tls_handshake = <TLS handshake token> | ---------------------------> |
   | +---------------------------------------+                              |
   |                                                                        |
   |                              +----NegotiatePB------------------------+ |
   |                              | step = TLS_HANDSHAKE                  | |
   | <--------------------------- | tls_handshake = <TLS handshake token> | |
   |                              +---------------------------------------+ |
   |                                                                        |
   |            <...repeat until TLS handshake is complete...>              |
```

The client and server repeat `TLS_HANDSHAKE` round-trips until the TLS handshake
is complete, at which point both ends wrap their respective TCP socket in the
new TLS channel. All subsequent messages will be encrypted.

#### Step 3: SASL Negotiation

The client and server now initiate a SASL handshake. The client is responsible
for choosing which SASL mechanism is used, with the restriction that it must be
in the set of mutually supported SASL mechanisms exchanged in the `NEGOTIATE`
step. Depending on the mechanism, SASL negotiation may serve to authenticate the
client to the server, and vice versa.

SASL negotiation may take one or more round trips. The first and last messages
are always a `SASL_INITIATE` from the client and a `SASL_SUCCESS` from the
server, respectively. In between `SASL_INITIATE` and `SASL_SUCCESS`, zero or
more pairs of `SASL_CHALLENGE` and `SASL_RESPONSE` messages from server and
client, respectively, may occur depending on the mechanism.

```
Client                                                                    Server
   |                                                                        |
   | +----NegotiatePB----------------+                                      |
   | | step = SASL_INITIATE          |                                      |
   | | auths[0] = <chosen mechanism> | -----------------------------------> |
   | | token = <SASL token>          |                                      |
   | +-------------------------------+                                      |
   |                                                                        |
   |  <...SASL_INITIATE is followed by 0 or more SASL_CHALLENGE +           |
   |      SASL_RESPONSE steps...>                                           |
   |                                                                        |
   |                                              +----NegotiatePB--------+ |
   |                                              | step = SASL_CHALLENGE | |
   | <------------------------------------------- | token = <SASL token>  | |
   |                                              +-----------------------+ |
   |                                                                        |
   | +----NegotiatePB-------+                                               |
   | | step = SASL_RESPONSE |                                               |
   | | token = <SASL token> | --------------------------------------------> |
   | +----------------------+                                               |
   |                                                                        |
   |                                                +----NegotiatePB------+ |
   | <--------------------------------------------- | step = SASL_SUCCESS | |
   |                                                +---------------------+ |
```

## Connection Context

Once the SASL negotiation is complete, before the first request, the client
sends the server a special call with `call_id` -3. The body of this call is a
ConnectionContextPB. The server should not respond to this call.

## Steady state

During steady state operation, the client sends call protobufs prefixed by
`RequestHeader` protobufs. The server sends responses prefixed by
`ResponseHeader` protobufs.

The client must send calls in strictly increasing `call_id` order. The server
may reject repeated calls or calls with lower IDs. The server's responses may
arrive out-of-order, and use the `call_id` in the response to associate a
response with the correct call.
