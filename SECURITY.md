# Security Policy

## Supported Versions

| Version | Supported          |
|---------|--------------------|
| 1.x     | :white_check_mark: |

## Reporting a Vulnerability

If you discover a security vulnerability, please report it responsibly.

**Do NOT open a public GitHub issue.**

Instead, please email the maintainers or use [GitHub's private vulnerability reporting](https://github.com/Neftedollar/Orleans.Streaming.Redis/security/advisories/new).

We will acknowledge your report within 48 hours and aim to release a fix within 7 days for critical issues.

## Scope

This package connects to Redis on behalf of your application. Security considerations:

- **Connection strings** — Never commit Redis connection strings to source control. Use environment variables or secret managers.
- **TLS** — Use `ssl=true` in your StackExchange.Redis connection string for production deployments.
- **ACLs** — Use a dedicated Redis user with minimal permissions (XADD, XREADGROUP, XACK, XLEN on the configured key prefix).
- **Network** — Ensure Redis is not exposed to the public internet. Use private networking or VPN.
