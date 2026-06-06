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

# Security Policy

## Reporting a Vulnerability

Please report suspected security vulnerabilities in Apache Kudu privately to the
Apache Security Team at <security@apache.org>, following the ASF process at
<https://www.apache.org/security/>. Do not open public GitHub issues or pull
requests for security reports.

## Threat Model

Apache Kudu's security threat model — what is in and out of scope, the security
properties the project provides and disclaims, the adversary model, the
configuration flags whose defaults change the security envelope, and how findings
are triaged — is documented in [THREAT_MODEL.md](./THREAT_MODEL.md).

Note that several of Kudu's security controls are **optional and off by default**
(for example `--rpc_authentication` and `--rpc_encryption` default to
`optional`, and the coarse user ACL defaults to `*`); Kudu is designed to run
inside a trusted cluster network and expects the operator to enable enforcement.
The threat model is largely about which controls the operator has enabled. See
`THREAT_MODEL.md` §5a, §9, and §11a.

## Operational Security Setup

Operator-facing security configuration — Kerberos authentication, coarse ACLs and
Apache Ranger fine-grained authorization, TLS/RPC encryption, and encryption at
rest — is documented at <https://kudu.apache.org/docs/security.html>.
