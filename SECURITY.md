# Security Policy

## Reporting a vulnerability

If you discover a security vulnerability in this repository, **please do not open a public GitHub issue**. Instead, report it privately by emailing **<info@civicdatalab.in>** with the subject line `[SECURITY] IDS-DRR-Data-Management`.

Please include:
- A description of the vulnerability and its potential impact
- Steps to reproduce or a proof-of-concept (if available)
- Any suggested fix or mitigation

We will acknowledge your report within **5 business days** and aim to resolve confirmed vulnerabilities within **30 days**. We will credit reporters in the release notes unless you request otherwise.

---

## Scope

This repository contains:

- A Django application that exposes a GraphQL API (Strawberry) for indicator data, risk scores, and administrative geographies
- Data import management commands (`import_geojson`, `import_indicators`, `import_data`)
- PostgreSQL/PostGIS data models and Redis caching

Security concerns relevant to this scope include: dependency vulnerabilities, unsafe handling of user-supplied `config.toml` values, SQL injection (mitigated by Django's ORM), and unintended exposure of imported data via the GraphQL endpoint.

Deployments are responsible for securing their own hosting infrastructure, TLS termination, database access controls, and any error-reporting services they integrate.

---

## Privacy

The platform processes **administrative-unit aggregates only**: statistics at the level of states, districts, sub-districts, etc. It does not collect, store, or process personally identifiable information (PII) about individuals at any point in the pipeline.

Deployments are responsible for ensuring that the upstream data they import complies with applicable data-protection law in their jurisdiction.

---

## Dependencies

Python dependencies are declared in [`requirements.txt`](requirements.txt). We recommend periodic audits using tools such as `pip-audit`:

```bash
pip install pip-audit
pip-audit
```

---

## Contact

For security or privacy concerns, contact **CivicDataLab** at <info@civicdatalab.in>.
