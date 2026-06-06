# Agent guidance

This file is read by automated agents (security scanners, code analyzers,
AI assistants) operating on this repository. It points them at the
human-authored references they should consult before producing output.

## Security

Security model: [SECURITY.md](./SECURITY.md), which links to the project's
threat model at [THREAT_MODEL.md](./THREAT_MODEL.md).

Agents that scan this repository should consult `THREAT_MODEL.md` for the
project's in-scope / out-of-scope declarations, the security properties it
provides and disclaims, the configuration flags whose defaults change the
security envelope, and the known non-findings (recurring false positives)
before reporting issues. In particular, several Kudu security controls are
optional and off by default (auth/encryption default to `optional`, the user
ACL defaults to `*`) and Kudu assumes a trusted cluster network — see
`THREAT_MODEL.md` §5a, §9, and §11a.
