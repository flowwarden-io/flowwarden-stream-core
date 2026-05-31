# Security Policy

## Supported Versions

The following versions of FlowWarden Stream Core receive security updates:

| Version    | Supported          |
| ---------- | ------------------ |
| 1.0.0-rc.x | :white_check_mark: |
| < 1.0.0    | :x:                |

Once a stable 1.0.0 release is published, this table will be updated to reflect the supported major/minor lines.

## Reporting a Vulnerability

We take the security of FlowWarden Stream Core seriously. If you believe you have found a security vulnerability, please report it responsibly through one of the following channels.

**Please do NOT open a public GitHub issue for security vulnerabilities.**

### Preferred: GitHub Security Advisories

Use the GitHub "Report a vulnerability" button to open a private security advisory:

[https://github.com/flowwarden-io/flowwarden-stream-core/security/advisories/new](https://github.com/flowwarden-io/flowwarden-stream-core/security/advisories/new)

This channel allows us to coordinate the fix privately before public disclosure and to request a CVE if appropriate.

### Alternative: Email

If you prefer not to use GitHub Security Advisories, send a report to:

**security@flowwarden.io**

Please encrypt sensitive details if possible.

### What to include in your report

- A clear description of the vulnerability
- Steps to reproduce (or a proof-of-concept if available)
- The affected version(s) of FlowWarden Stream Core
- The potential impact (data exposure, code execution, denial of service, etc.)
- Any suggested mitigation or fix, if known

## Response Timeline

We aim to respond to security reports according to the following timeline:

| Stage                | Target time      |
| -------------------- | ---------------- |
| Acknowledgement      | Within 72 hours  |
| Initial assessment   | Within 7 days    |
| Fix and disclosure   | Depends on severity, in coordination with the reporter |

These are best-effort targets for a project maintained as a side project. Critical vulnerabilities will be prioritized.

## Disclosure Policy

We follow a **coordinated disclosure** model:

- Vulnerabilities are kept private until a fix is available.
- We will work with the reporter to agree on a disclosure timeline (typically 30 to 90 days, shorter for actively exploited issues).
- Once a fix is released, we publish a security advisory describing the vulnerability, the fix, and credit the reporter (unless they prefer to remain anonymous).

## Out of Scope

The following are generally not considered security vulnerabilities in FlowWarden Stream Core itself:

- Issues in user code that misuses the library (e.g., handlers that do not validate input)
- Vulnerabilities in dependencies that have already been disclosed and have published fixes — please report those upstream
- Configuration issues in user deployments (insufficient MongoDB authentication, exposed credentials, etc.)

Thank you for helping keep FlowWarden Stream Core and its users safe.
