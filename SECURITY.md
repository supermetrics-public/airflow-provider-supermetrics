# Security Policy

## Pre-Release Software

**This project is in early beta.** It has not undergone a formal security audit. Use at your own risk. Do not use in environments where a security vulnerability could have significant consequences.

## Current Limitations

- No formal security audit has been performed
- No bug bounty program exists
- No SLA for security fixes
- No promise of backports — security fixes land on `main` only
- Until a stable release policy is finalized, **no version is declared "supported"** for security purposes

## Supported Versions

| Version | Supported |
|---------|-----------|
| latest `main` | best-effort |
| any pinned beta release | no backports |

## Reporting a Vulnerability

Do not open a public GitHub issue for security vulnerabilities.

Report vulnerabilities privately via GitHub's [Security Advisory](https://github.com/supermetrics-public/airflow-provider-supermetrics/security/advisories/new) feature.

Include:

- Description of the vulnerability and its potential impact
- Steps to reproduce or a proof-of-concept
- Affected versions (if known)

### What to expect

- We will acknowledge receipt when we can, but **no response time is guaranteed**
- We will investigate and, if confirmed, attempt to publish a fix on `main`
- We will not issue CVEs for pre-release versions unless required by downstream dependents
- We may close reports as `wontfix` for issues that only affect beta builds with no stable release

## Scope

This policy covers the `airflow-provider-supermetrics` package itself. It does not cover:

- The Supermetrics platform or API
- The Supermetrics Python SDK (`supermetrics` package)
- Apache Airflow itself

For Supermetrics platform security issues, contact security@supermetrics.com.
