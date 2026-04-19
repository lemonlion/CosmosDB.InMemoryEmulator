# Security Policy

## Reporting a Vulnerability

If you discover a security vulnerability in CosmosDB.InMemoryEmulator, please report it responsibly.

**Do not open a public issue.** Instead, please email **security@lemonlion.dev** or use [GitHub's private vulnerability reporting](https://github.com/lemonlion/CosmosDB.InMemoryEmulator/security/advisories/new).

We will acknowledge receipt within 48 hours and aim to provide a fix or mitigation within 7 days for confirmed vulnerabilities.

## Scope

This library is a **testing fake** — it is not intended for production use and does not handle real data or authentication. Security concerns are primarily around:

- Dependency vulnerabilities (transitive NuGet packages)
- Code injection via untrusted inputs to the SQL parser or JavaScript trigger engine
