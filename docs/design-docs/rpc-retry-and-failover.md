
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

# Motivation

Kudu has many RPCs that need error handling. When an RPC fails due to a timeout
or network failure, the sender needs to retry it in the same or in a different
server, but it often doesn't know whether the original RPC succeeded. Certain
RPCs are not idempotent i.e. if handled twice they will lead to incorrect state
and thus we need a mechanism by which the sender can retry the RPC but the receiver
is guaranteed to execute it **Exactly Once**.

# Problem breakdown

[1] introduces 4 subproblems that need to be solved to obtain exactly-once semantics on
replicated RPCs, we'll use the same terminology to avoid redefining the concepts:

- Identification - Each RPC must have a unique identifier.
- Completion Record - Each RPC must have a record of its completion.
- Retry rendezvous - When an RPC is retried it must find the completion record of the
                     previous attempt.
- Garbage collection - After a while completion records must be garbage collected.

## Design options

We can address these problems in multiple ways and, in particular, at multiple abstraction
layers:

1. Completely encapsulated at the RPC layer - This is the option that [1] presents at length,
allowing to "bolt-on" these properties almost independently of what the underlying RPCs
are. This option, while very general, would require a lot of work particularly as for
replicated RPCs we'd need to come up with a generic, durable, Completion Record storage mechanism.
While "elegant" in some ways this option also seems weird in other ways: for instance
it's not totally clear what happens when an RPC mutates multiple "server objects" or
what these server objects really are.

2. Handled ad-hoc by the client and the tablet server/master, outside of the RPC layer -
With this option, we choose to handle errors at the TabletServer/TabletPeer, ad-hoc, and for
each different RPC. For specific RPCs like Write() this option seems to map to existing
components quite well. For instance the completion record can be the raft log and replica
replay would mean that it would be relatively easy to implement a retry rendezvous
mechanism. However this option lacks in generality and it would likely require duplicate
error handling for operations that are not tablet server transactions.

3. A hybrid of the above - Certain parts of the problem would be handled by the RPC layer,
such as retrying and retry rendez-vous logic, but other parts of the logic would be implemented
ad-hoc in other layers. For instance, for Write()s, the RPC layer would know how to sequence and
retry the RPCs but would delegate durability and cross-server replicated operation rendezvous
to the tablet peer/transaction manager.

# Design choices

We chose option 3, in which most of the sub-component is encapsulated in the RPC layer, allowing
to provide limited (non-persistent) bolt-on exactly once semantics for generic RPCs, but the API is
exposed so that we can perform ad-hoc handling of persistence where required (e.g. for Write RPCs). 

We addressed each of the four sub-problems mentioned in the previous section the following way:

1. Identification - Each individual RPC *attempt* must be uniquely identified. That is, not only do
we need to identify each RPC a client executes, but we need to distinguish between different attempts
(retries) of the same RPC. This diverges from [1] which identifies each RPC, but not different
attempts of the same request. We need to keep this additional information because of the hybrid
design choice we made. That is, because a part of the exactly once mechanism is bolt-on and one
part is ad-hoc there are actually two serialization points for replicated RPC rendezvous, in which
case we need to distinguish between different retries and thus need a new identifier.

Each individual RPC attempt contains the following information:
- Client ID - A UUID that is generated per client, independently.
- Sequence number- An integer ID that uniquely identifies a request, even across machines and attempts.
- Attempt number - An integer that uniquely identifies each attempt of a request.

2. CompletionRecord - A completion record of an RPC is the response that is sent back to the client
once the RPC is complete. Once a response for a request is built, it is cached in the RPC subsystem
and clients will always receive the same response for the same request. The RPC subsystem does not
handle response durability though, it is up to the ad-hoc integration to make sure that either responses
are durably stored or can be rebuilt, consistently.

3. Retry rendezvous - We make sure that multiple attempts at the same RPC in the same server meet
by making them go through a new component, the **ResultTracker**. This component is responsible for
figuring out the state of the RPC, among:
- NEW - It's the first time the server has seen the RPC and it should be executed.
- IN_PROGRESS - The RPC has been previously allowed to execute but hasn't yet completed.
- COMPLETED - The RPC has already completed and it's response has been cached.
- STALE - The RPC is old enough that the server no longer caches its response, but new enough that
the server still remembers it has deleted the corresponding response.

4. Garbage collection - We opted to implement the basic watermark-based mechanism for garbage
collection mentioned in [1]: Each time a client sends a request it sends the "Request ID" of the
first incomplete request it knows about. This lets the server know that it can delete the responses
to all the previous requests. In addition, we also implemented a time-based garbage collection
mechanism (which was needed to garbage collect whole clients anyway since we didn't implement the
client lease system of [1]). Time based garbage collection deletes responses for a client that are
older than a certain time (and not in-progress). Whole client state is deleted after
another (longer) time period has elapsed.

## Note on client leases

We opted for not implementing the distributed client lease mechanism in [1].
This does allow for cases where an RPC is executed twice: for instance if a client attempts an RPC
and then is silent for a period greater than the time-based garbage collection period before
re-attempting the same RPC. We consider that this is unlikely enough not to justify implementing
a complex lease management mechanism, but for documentation purposes **the following scenario could
cause double execution**:

- The user sets a timeout on an operation that is longer that the time-based garbage collection period
(10 minutes, by default) the client then attempts the operation, which is successful on the server.
However the client is partitioned from the server before receiving the reply. The partition lasts
more than the time-based garbage collection period but less than the user-set timeout, meaning the
client continues to retry it. In this case the operation could be executed twice.

# Lifecyle of an Exactly-Once RPC

Clients assign themselves a UUID that acts as their unique identifier for their lifetime.
Clients have a component, the **Request Tracker** that is responsible for assigning new RPC 
*Sequence number*s and tracking which ones are outstanding. Along with the outstanding requests,
the *Request Tracker* is responsible for tracking the *First incomplete sequence number*, i.e. the id
of the first outstanding RPC. This is important for garbage collection purposes, as we'll see later.
The client system is also responsible for keeping a counter of the times an RPC is attempted and
making sure that each time an attempt is performed it is assigned a new *AttemptNumber* based on this
counter. Together these four elements form a *Request Id* and are set in the RPC header that takes
the form:

    =========================================
    |              RequestIdPB              |
    ----------------------------------------
    |    - client_id : string               |
    |    - seq_no : int64                   |
    |    - first_incomplete_seq_no : int64  |
    |    - attempt_no : int64               |
    =========================================

When an RPC reaches the server, the *Request Id* is passed to a **ResultTracker** the server-side
component responsible for storing Completion Records (the response protobufs), and for doing
the Retry Rendezvous. For each of the RPCs this component will determine which of the following
states it is in:

- NEW - This is the first time the server has seen this request from the client. In this case
the request will be passed on to an RPC handler for execution.
- IN_PROGRESS - This is a retry of a previous request, but the original request is still being
executed somewhere in the server. The request is dropped and not executed. It will receive a
response at the same time as the original retry once the latter completes.
- COMPLETED - This is a retry for a request that has previously completed. The request is dropped
and not executed. The response of the original request, which must be still in memory, is sent back
to the client.
- STALE - This is either a new request or a retry for a request that is no longer being tracked,
although the client itself still is. The request is dropped and not executed. An appropriate error
is sent back to the client.

If the **ResultTracker** returns that the request is *NEW*, then execution proceeds, for instance
in the case of a Write() this means that it will be sent to the *TransactionManager* for replication
and eventually to be applied to the tablet.

Once execution of the request is completed successfully it is sent to the **ResultTracker** which
will store the response in memory (to be able to reply to future retries) and reply back to the
client.

## Important Details

- The response for errors is not stored. This is for two reasons: Errors (are not supposed) to have
side-effects; Errors might be transient, for instance a write to a non-leader replica may fail but
a retry might be successful if the replica is elected leader.

- The mechanism above, just by itself, does not handle replicated RPCs. If an RPC is
replicated, i.e. if an RPC must be executed exactly once across a set of nodes (e.g. Write()), its
idempotency is not totally covered by the mechanism above.

- Responses are not stored persistently so, for replicated RPCs, implementations must take care that
the same request always has the same response, i.e. that the exact same response can be rebuilt
from history.

## Declaring RPCs as Exactly Once

To make sure the results of an RPC are tracked **transiently** on the server side (we'll cover how
we made sure that write results were tracked **persistently** in the following section), all that
is required is that the service definition enables the appropriate option. For instance in the case
of writes this is done the following way:

```
service TabletServerService {

  ...
  rpc Write(WriteRequestPB) returns (WriteResponsePB)  {
    option (kudu.rpc.track_rpc_result) = true;
  }
  ...
}
```

# Exactly Once semantics for replicated, fault-tolerant, RPCs

RPCs that are replicated, for fault tolerance, require more than the mechanics above. In particular
they have the following additional requirements:

- Cross-replica rendezvous - The **ResultTracker** makes sure that, for a single server, all attempts
of an RPC from a client directly to that server serialized. However this does not take into account
attempts from the same client to other servers, which must also be serialized in some way.

- Response rebuilding - When a server crashes it loses some in-memory state. Requests for
completed operations are durably stored on-disk so that that volatile state can be rebuilt, but
responses are not. When a request is replayed to rebuild lost state, it's response must be stored
again and it must be the same as the original response sent to the client.

## Concrete Example: Exactly Once semantics for writes

Writes (any operation that mutates rows on a tablet server) are the primary use case for exactly
once semantics and thus it was implemented first (for reference this landed in commit ```6d2679bd```
and this is the corresponding [gerrit](https://gerrit.cloudera.org/#/c/3449/)). Examining this
change is likely to enlighten adding Exactly Once semantics to other replicated RPCs. A lot of
the changes were mechanical, the relevant ones are the following:

1. The cross-replica retry rendezvous is implemented in ```transaction_driver.{h,cc}```, its inner
workings are detailed in the header. This basically makes sure that when an RPC is received from
a client and a different attempt of the same RPC is received from another replica (a previous leader),
we execute only one of those attempts (the replica one).

2. Response rebuilding is implemented in ```tablet_bootstrap.cc```, which basically adds the ability
to rebuild the original response, when a request is replayed on tablet bootstrap.

### References

[1][Implementing Linearizability at Large Scale and Low Latency](http://web.stanford.edu/~ouster/cgi-bin/papers/rifl.pdf)
