# Security Policy

## Supported Versions

This project is actively maintained on the latest `main` branch.

## Reporting a Vulnerability

If you discover a security issue:

1. Do **not** open a public issue with exploit details.
2. Contact the maintainer privately.
3. Include:
   - description of the issue
   - impact
   - reproduction steps
   - suggested fix (if available)

## Response Targets

- Initial acknowledgment: within 3 business days
- Triage/update: within 7 business days
- Fix timeline: depends on severity and complexity

## Hardening Guidance

- Move DB credentials to environment variables/secret manager
- Restrict database network access to trusted IPs
- Use least-privilege DB users
- Rotate credentials regularly
- Add monitoring for unusual notification volume
