# Contributing to IDS-DRR Data Management

Thank you for your interest in contributing. This project is maintained by [CivicDataLab](https://civicdatalab.in) and we welcome contributions from researchers, practitioners, and civic technologists.

---

## Ways to contribute

| Type | How |
|------|-----|
| Bug reports | Open a [GitHub issue](https://github.com/CivicDataLab/IDS-DRR-Data-Management/issues) |
| Feature suggestions | Open an issue first, then a PR after alignment |
| Documentation fixes | PR directly against `dev` |
| Questions and partnerships | Email <info@civicdatalab.in> |

---

## Before you open a pull request

1. **Check for an open issue.** Search [existing issues](https://github.com/CivicDataLab/IDS-DRR-Data-Management/issues) before opening a new one to avoid duplication.
1. **For non-trivial changes, open an issue first.** This lets us align on scope before you invest time writing code.
1. **Reference the issue in your PR description.** Use `Closes #<issue-number>` or `Relates to #<issue-number>`.

---

## Development setup

Using Docker (recommended):

```bash
git clone https://github.com/CivicDataLab/IDS-DRR-Data-Management.git
cd IDS-DRR-Data-Management
docker compose up -d --build
docker exec context_layer_Backend python manage.py migrate
```

The API will be available at <http://localhost:8000>. See the [Data Management API docs](https://ids-drr.readthedocs.io/en/latest/platform/data-management.html) for the full setup, including environment variables, deployment configuration, and data import.

---

## Submitting a pull request

1. Fork the repository and create a branch from `dev`.
1. Make your changes. Keep commits focused: one logical change per commit.
1. Confirm tests pass: `python manage.py test`.
1. Open a PR against `dev` with a clear title and a brief description of what changed and why.

---

## Code style

We follow the [OCP Software Development Handbook](https://ocp-software-handbook.readthedocs.io/en/latest/python/index.html). Formatting and linting are handled by [Ruff](https://docs.astral.sh/ruff/), configured in `pyproject.toml` and enforced in CI.

Install the hooks once after setting up your environment so checks run on every commit:

```bash
pre-commit install
```

Run the checks manually at any time:

```bash
ruff format . # auto-format
ruff check .  # lint
pre-commit run --all-files
```

- Python 3.12+.
- Avoid adding dependencies not declared in `requirements.txt` without discussion.
- Deployment-specific configuration (e.g. state lists, CSV paths, indicator slugs) belongs in `config.toml`, not in the codebase.

---

## License

By contributing, you agree that your contributions will be licensed under the [GNU AGPL v3.0](LICENSE).
