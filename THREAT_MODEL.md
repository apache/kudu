<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to you under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Apache Kudu — Security Threat Model

## §1 Header

- **Project:** Apache Kudu
- **Modeled against:** `master` HEAD as of 2026-06-05 (latest released docs line).
- **Authors:** ASF Security team (v0 draft, generated via the
  `threat-model-producer` rubric), for the Apache Kudu PMC to review.
- **Status:** **DRAFT v0 — draft-first, not yet maintainer-ratified.** Most
  claims are *(inferred)* and must be confirmed; see §14.
- **Version binding:** versioned with the project; a report against release *N*
  is triaged against the model as it stood at *N*.
- **Reporting cross-reference:** findings that violate a §8 property should be
  reported privately per the project's disclosure channel (`security@apache.org`);
  findings under §3 or §9 are closed citing this document.
- **Provenance legend:** *(documented)* = stated in Kudu's own docs/site;
  *(maintainer)* = confirmed by a Kudu PMC member; *(inferred)* = reasoned from
  docs/code/domain knowledge, not yet confirmed (each has a §14 question).
- **Draft confidence:** ~17 documented / 0 maintainer / ~23 inferred.

**What Kudu is.** Apache Kudu is a distributed, columnar storage engine for
fast analytics on fast-changing data. A cluster is a set of **master** servers
(catalog/metadata, tablet placement) and **tablet servers** (store and serve
row data in tablets, replicated via Raft consensus). Clients (Impala, Spark, the
Kudu API) talk to masters and tablet servers over an RPC protocol; an HTTP
**Web UI** exposes status/metrics. Kudu is designed to run **inside a trusted
cluster network**, and several of its security controls are **optional and off
by default** — the threat model is largely about which controls the operator has
enabled and what the cluster assumes about its network.

## §2 Scope and intended use

- **Primary intended use** *(inferred)*: an operator-deployed distributed
  storage engine inside an organization's analytics cluster (often alongside
  Impala/Spark on a Kerberized Hadoop-style network), serving authenticated
  clients.
- **Deployment shape** *(documented)*: long-running master + tablet-server
  daemons; clients connect over RPC; a Web UI is served on a separate HTTP(S)
  port.
- **Caller roles** (a distributed service — the role splits):
  - **client** — untrusted until authenticated (Kerberos / token).
  - **operator / admin** — trusted for the cluster; owns the daemon flags,
    ACLs, Kerberos/Ranger/KMS config, the hosts.
  - **peer** — a master or tablet server participating in inter-node RPC and
    Raft replication; authenticated but a candidate adversary if compromised.
  - **unauthenticated network client** — permitted by default (auth =
    `optional`), trusted implicitly via network placement.

**Component-family table** *(inferred — confirm in §14)*:

| Family | Entry point | Touches outside process? | In model? |
| --- | --- | --- | --- |
| Master RPC (catalog, metadata, authz tokens) | master RPC port | network, disk | **yes** |
| Tablet-server RPC (read/write/scan) | tserver RPC port | network, disk | **yes** |
| Inter-node RPC + Raft consensus | master↔tserver, tserver↔tserver | network | **yes** (peer boundary) |
| Authz (coarse ACLs + optional Ranger) | ACL flags, Ranger plugin | network (Ranger) | **yes** |
| Web UI | HTTP(S) status port | network | **yes** |
| At-rest storage / encryption | local FS, KMS | disk, KMS | **yes** (when enabled) |
| Client SDKs / bundled tools / examples | C++/Java/Python clients | varies | **per-component** — confirm which are supported (§14) |

## §3 Out of scope (explicit non-goals)

- **Defending a cluster that runs with authentication `optional` on an
  untrusted network.** Kudu documents `--rpc_authentication=optional` as the
  default and tells operators to set `=required` to secure a cluster; an
  unauthenticated-accessible cluster reachable by attackers is an operator
  posture choice, not a Kudu defect (pending the §5a/§14 ruling). *(documented)*
- **The surrounding infrastructure** — the Kerberos KDC, the Ranger service,
  the KMS, HDFS, the OS, and the cluster network fabric. Kudu integrates with
  these; it does not own their security. *(inferred)*
- **Bundled client SDKs / examples / tools** — threat-modeled separately;
  integrators should not extend core daemon guarantees to them. *(inferred — §14)*

## §4 Trust boundaries and data flow

The **primary trust boundary is the authentication + authorization layer**
(Kerberos/token auth → coarse ACL → optional Ranger fine-grained), *when the
operator has enabled enforcement*. Data flow and trust transitions:

1. Client → **master/tserver RPC** → crosses the network boundary. Untrusted
   until authenticated; **but with auth=`optional` (default), unauthenticated
   connections from reachable subnets are allowed.** *(documented)*
2. Authenticated client → **operation** (DDL on master, DML/scan on tserver):
   crosses the authz boundary (User/Superuser ACL; Ranger table/column policy
   if enabled). *(documented)*
3. Master ↔ tablet server, tablet server ↔ tablet server → **inter-node RPC +
   Raft**: peers authenticate via internal X.509 certs/tokens; a compromised
   authenticated peer is the Byzantine-participant case. *(inferred)*
4. Tablet server → **local storage** (and KMS if at-rest encryption enabled):
   leaves the RPC boundary. *(inferred)*

**Reachability preconditions per family** (triager's first test):
- A master/tserver-RPC finding is in-model only if reachable by a network
  client **before** the auth gate (when auth=required), or by an authenticated
  client **beyond** its ACL/Ranger grant.
- A Web-UI finding is in-model only if it leaks row data (which is redacted by
  default) or crosses the operator boundary — **note fine-grained authz is not
  enforced on the Web UI** (§9). *(documented)*
- An inter-node finding is in-model only if a non-peer can inject into the
  consensus/replication channel, or a single compromised peer can violate a §8
  cluster property beyond its share.

## §5 Assumptions about the environment

- **Operator-controlled flags and config** *(inferred)*: the `--rpc_*`,
  ACL, Ranger, and encryption flags are trusted operator inputs.
- **Network placement** *(documented)*: Kudu assumes a trusted cluster network;
  with auth optional, "the cluster does not prevent access from unauthenticated
  users," so network-level controls are load-bearing until the operator sets
  `=required`.
- **Kerberos / Ranger / KMS** *(documented/inferred)*: provisioned and trusted
  by the operator when those features are enabled.
- **What the daemons do to the host** *(inferred — §14)*: open RPC + HTTP
  listening sockets; read/write local data dirs and WAL; talk to KDC/Ranger/KMS;
  not expected to run as root.

## §5a Build-time and configuration variants (the security-envelope knobs)

This is the heart of Kudu's model — the security-relevant **defaults are the
less-secure value**, so the model is ambiguous until the PMC rules on each
(§14 wave 1):

| Knob | Default | Effect on model | Maintainer stance |
| --- | --- | --- | --- |
| `--rpc_authentication` | **`optional`** *(documented)* | Unauthenticated connections allowed from reachable subnets; the auth boundary is not enforced | **?** supported posture vs must-set-`required` — §14.1 |
| Coarse-grained User ACL (`--user_acl`) | **`*` (all authenticated users)** *(documented)* | Any authenticated principal has user-level access absent a narrower ACL | **?** §14.2 |
| `--rpc_encryption` | **`optional`** *(documented)* | RPC may be plaintext; confidentiality/integrity on the wire not guaranteed | **?** §14.3 |
| At-rest encryption (`--encrypt_data_at_rest`) | **off** *(documented)* | Data files unencrypted on disk unless enabled (+ KMS) | **?** operator responsibility (§10) |
| Fine-grained authz (Apache Ranger) | **off (optional, 1.12+)** *(documented)* | No table/column-level policy unless Ranger is wired; and **not enforced on the Web UI** | **?** §14.4 |
| Web UI exposure | **on; row data redacted, metadata exposed** *(documented)* | Metadata/metrics visible; row data redacted by default | operator responsibility (§10) |

**Insecure-default ruling needed.** For each row whose default is the
less-secure value, the PMC must rule: is the default the *supported production
posture* (→ a report against it is `VALID`), or a *dev/cluster-internal default
operators must change* (→ `OUT-OF-MODEL: non-default-build`, requirement moves to
§10)? The docs lean toward the latter ("To secure a cluster, use
`--rpc_authentication=required`"), but an explicit PMC call is needed because it
reshapes §8/§10/§11a/§13 together.

## §6 Assumptions about inputs

Per-surface trust table (distributed service — rows are RPC/endpoints):

| Surface | Input | Attacker-controllable? | Operator must enforce |
| --- | --- | --- | --- |
| Master RPC (DDL, metadata, authz-token issue) | request + creds | **yes** (pre-auth when auth=optional) | `rpc_authentication=required`; narrow ACL *(documented)* |
| Tablet-server RPC (write/scan) | request + creds + predicates | **yes** (authenticated client) | Ranger/ACL authz; tablet-level checks *(documented)* |
| Inter-node RPC / Raft | replication + consensus messages | peer-controllable | internal cert/token auth on the channel *(inferred — §14)* |
| Web UI | HTTP request | **yes** (network) | front with auth/proxy; row-data redaction on *(documented)* |
| Daemon flags / ACL / Ranger / KMS config | config | **no — operator-trusted** | filesystem perms on config + keytabs *(inferred)* |

Size/shape/rate *(inferred — §14)*: scan predicates and write batches flow into
memory/CPU on tablet servers; whether an authenticated client can trigger
super-linear cost / OOM and whether that is in-model needs a §8 resource line.

## §7 Adversary model

**In scope:**
- **Unauthenticated network client** (when `rpc_authentication=required`):
  tries to reach master/tserver RPC or the Web UI without valid credentials.
- **Authenticated lower-privileged user**: a legitimate principal trying to
  exceed its grant — read/write a table its ACL/Ranger policy forbids, escalate
  via authz-token handling, or reach admin RPCs reserved for the Superuser ACL.
- **Authenticated-but-Byzantine peer**: a compromised tablet/master server that
  passed the internal handshake and then behaves arbitrarily in Raft/replication.

**Capabilities:** arbitrary RPC/HTTP traffic; for an authenticated principal,
any operation their credentials permit; cannot (assumed) read operator keytabs/
config or the host beyond the daemon's own identity.

**Explicitly out of scope:**
- The **operator/admin** and anyone with the daemon flags/keytabs — already won.
- Any client on a **default optional-auth** cluster reachable over the network —
  out of model pending the §5a/§14 ruling.
- **≥ honest-majority compromise** of tablet replicas (Raft tolerates a minority
  of faulty replicas; mass compromise is out of scope — §14 confirms the bound).

## §8 Security properties the project provides

Each conditional on the relevant §5a knob being set securely. *(All
*(inferred)* pending §14 — Kudu documents mechanisms, not committed "properties".)*

1. **Client authentication** via Kerberos / internal tokens *when
   `rpc_authentication=required`*. Violation symptom: an unauthenticated client
   performs an RPC that requires identity. Severity: **critical**. *(inferred)*
2. **Coarse-grained authorization** (User/Superuser ACL) *when ACLs are
   narrowed*. Violation symptom: a principal performs an action outside its ACL.
   Severity: **critical**. *(documented mechanism / inferred property)*
3. **Fine-grained authorization** via Ranger *when enabled* (table/column).
   Violation symptom: a principal reads/writes a table/column its Ranger policy
   forbids (excluding the Web UI, §9). Severity: **critical**. *(documented)*
4. **In-transit confidentiality/integrity** via TLS *when
   `rpc_encryption=required`*. Violation symptom: plaintext/forgeable RPC on a
   required-encryption cluster. Severity: **high**. *(documented mechanism)*
5. **At-rest confidentiality** *when `--encrypt_data_at_rest=true` + KMS*.
   Violation symptom: readable data files given disk access. Severity: **high**.
   *(documented mechanism)*
6. **Web-UI row-data redaction** (default on). Violation symptom: row values
   exposed in the Web UI. Severity: **high**. *(documented)*
7. **Raft replication safety / replica consistency** — a committed write is
   durable and consistent across replicas given **< majority** faulty replicas.
   Violation symptom: divergent replicas, lost commit, split-brain. Severity:
   **critical** (distributed; observable cross-node). *(inferred — §14)*
8. **Resource/availability** *(inferred — §14)*: **needs a line** — is an
   authenticated client able to OOM a tablet server via a crafted scan a bug, or
   only pre-auth exhaustion? Propose: pre-auth exhaustion in-model; expensive
   authenticated queries not. Confirm threshold in §14.

## §9 Security properties the project does *not* provide

- **No protection with `rpc_authentication=optional`.** The cluster permits
  unauthenticated access; every §8 authn/authz property is void. *(documented)*
- **No fine-grained authorization on the Web UI.** Ranger policies are not
  enforced there; the Web UI is gated only by its own (coarse) controls.
  *(documented)*
- **No at-rest encryption by default.** Data files are plaintext on disk unless
  explicitly enabled. *(documented)*
- **Loopback connections are not encrypted** (by design, for performance).
  *(documented)*

**False-friend properties (call out explicitly):**
- **`rpc_authentication=optional` is not "authentication on."** "Optional" means
  authenticate *if offered, otherwise allow* — it does **not** enforce identity.
  Operators frequently read "optional" as "enabled." *(documented)*
- **User ACL `*` is not "locked down."** It grants user-level access to **all
  authenticated principals**, not "no access." *(documented)*
- **The Web UI's metadata exposure is by design** — it redacts row data but
  surfaces schema/metrics/cluster state; treating the Web UI as a
  fine-grained-authz surface is a mistake. *(documented)*

**Well-known attack classes left to the operator:** network-level access to an
optional-auth cluster (place behind a trusted boundary); credential/keytab theft
(host security); SSRF/abuse from co-located compute (Impala/Spark) reaching the
cluster; KMS/key-management for at-rest. One line each — notice, not tutorial.

## §10 Downstream / operator responsibilities

For Kudu the "user" is the **operator** running the cluster:
- **Set `--rpc_authentication=required`** (or keep the cluster strictly on a
  trusted, isolated network). *(documented)*
- **Set `--rpc_encryption=required`** for wire confidentiality/integrity.
  *(documented)*
- **Narrow the User/Superuser ACLs** away from `*` to the intended principals.
  *(documented)*
- **Enable Apache Ranger** for table/column authorization in multi-tenant
  clusters. *(documented)*
- **Enable `--encrypt_data_at_rest` + a KMS** where disk confidentiality matters.
  *(documented)*
- **Front / restrict the Web UI** and keep row-data redaction on. *(documented)*
- **Protect keytabs, flagfiles, and data dirs** with host filesystem perms; do
  not run daemons as root. *(inferred)*

## §11 Known misuse patterns

- Running a cluster with `--rpc_authentication=optional` reachable from an
  untrusted network — unauthenticated read/write.
- Leaving the User ACL at `*` and assuming it restricts access.
- Treating the Web UI as an authorization boundary (it isn't fine-grained).
- Assuming RPC is encrypted when `--rpc_encryption=optional` left it plaintext.
- Assuming on-disk data is encrypted without `--encrypt_data_at_rest`.

## §11a Known non-findings (recurring false positives)

The highest-leverage section for keeping scan output signal-heavy:

- **"Unauthenticated client can connect / read / write"** reported against a
  cluster left at `rpc_authentication=optional`. Out of model — operator must
  set `required`. (§5a, §9) *(documented)*
- **"All authenticated users have access"** — the User ACL default is `*`;
  operator config, not a defect. (§5a, §9) *(documented)*
- **"RPC traffic is in plaintext"** against `rpc_encryption=optional`. Operator
  responsibility. (§5a, §10) *(documented)*
- **"Data files are unencrypted on disk"** without `--encrypt_data_at_rest`.
  Documented default. (§5a, §10) *(documented)*
- **"Web UI exposes cluster metadata/metrics"** — by design; row data is
  redacted. (§9) *(documented)*
- **Static-analysis "missing authz / auth bypass" hits on RPC paths** — in-model
  only if they bypass the auth gate on a `required` cluster or exceed a
  principal's ACL/Ranger grant. (§4 reachability test) *(inferred)*

## §12 Conditions that would change this model

- A change of any §5a default (e.g., shipping `rpc_authentication=required` by
  default); a new RPC surface or daemon; Web UI gaining fine-grained authz; a new
  client SDK promoted to first-class; a change to the Raft fault bound.
- **A report that cannot be routed to one §13 disposition** is evidence the
  model is incomplete — revise §8/§9 rather than make an ad-hoc call.

## §13 Triage dispositions

| Disposition | Meaning | Licensed by |
| --- | --- | --- |
| `VALID` | Bypasses the auth gate (on a `required` cluster), exceeds an ACL/Ranger grant, leaks redacted row data, or breaks a Raft safety property within the fault bound. | §8, §6, §7 |
| `VALID-HARDENING` | No §8 property broken, but the API/config makes a §11 misuse too easy; hardened at maintainer discretion. | §11 |
| `OUT-OF-MODEL: trusted-input` | Requires control of operator config (flags, keytabs, ACLs, KMS). | §6 |
| `OUT-OF-MODEL: adversary-not-in-scope` | Requires operator privilege, or ≥ majority replica compromise. | §7 |
| `OUT-OF-MODEL: non-default-build` | Only manifests under an insecure §5a default the PMC rules operator-must-change (optional auth/encryption, ACL `*`, at-rest off). | §5a |
| `BY-DESIGN: property-disclaimed` | Concerns a property §9 disclaims (Web-UI authz, loopback plaintext, etc.). | §9 |
| `KNOWN-NON-FINDING` | Matches a §11a pattern. | §11a |
| `MODEL-GAP` | Cannot be routed above → revise the model. | §12 |

## §14 Open questions for the maintainers

Grouped in waves; each states a **proposed answer** to confirm/correct/strike.
Every *(inferred)* tag above maps to one of these.

**Wave 1 — scope & the insecure defaults (these reshape everything):**
1. **`rpc_authentication=optional` default.** Proposed: the supported production
   posture is `=required` (or a strictly trusted isolated network); reports
   against an optional-auth cluster reachable by attackers are
   `OUT-OF-MODEL: non-default-build`. Correct? (→ §5a, §3, §11a)
2. **User ACL `*` default.** Proposed: the `*` default is a convenience;
   operators are expected to narrow it; "any authenticated user has access" is
   by-design, not a bug. Correct? (→ §5a, §9)
3. **`rpc_encryption=optional` default.** Proposed: plaintext RPC under the
   default is operator responsibility, not a vulnerability; confidentiality is a
   §8 property only when `=required`. Correct? (→ §5a, §9)
4. **Ranger / Web-UI authz.** Proposed: fine-grained authz is opt-in (Ranger)
   and explicitly **not** enforced on the Web UI; we state that in §9. Agree?
   (→ §5a, §9)

**Wave 2 — properties & enforcement:**
5. **Authz-token handling.** Are the internal authentication tokens / X.509
   certs a trust boundary a lower-privileged authenticated client could abuse to
   escalate, or fully operator/internal? (→ §6, §8)
6. **Raft fault bound.** Confirm the safety/consistency guarantee holds under
   `< majority` faulty replicas, and that ≥ majority compromise is out of scope.
   (→ §7, §8)
7. **Resource limits.** Can an authenticated client trigger super-linear CPU/
   memory (crafted scans/predicates) on a tablet server, and is that in-model or
   a by-design "expensive query"? Where's the line? (→ §6, §8)

**Wave 3 — surfaces & meta:**
8. **First-class components.** Which client SDKs/tools are supported for security
   purposes vs. community/unsupported (→ §2/§3 carve-out)?
9. **At-rest + KMS.** Any caveats on the at-rest encryption guarantee (key
   rotation, what's covered vs. WAL/metadata)? (→ §8, §9)
10. **Coexistence.** This is a new `THREAT_MODEL.md` + `SECURITY.md`; should they
    be canonical, with the website security page staying the operator how-to?
    (→ meta)

## §15 Machine-readable companion

Deferred for v0; a `threat-model.yaml` sidecar (surfaces → trust, §5a defaults,
§8 properties, §11a suppressions, §13 labels) can be generated once the prose is
ratified.
