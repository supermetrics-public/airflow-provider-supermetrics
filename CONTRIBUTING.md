# Contributing

Contributions are welcome and appreciated. Every bit helps.

> **Note:** This project is in early beta. Interfaces and internals may change significantly between releases.

## Types of Contributions

### Report Bugs

Open an issue at https://github.com/supermetrics-public/airflow-provider-supermetrics/issues and include:

- Your OS name and version
- Python, Airflow, and provider package versions
- Steps to reproduce the bug
- Any relevant tracebacks or logs

### Fix Bugs

Browse open issues tagged `bug` + `help wanted` — these are fair game for anyone.

### Implement Features

Browse open issues tagged `enhancement` + `help wanted`. Before starting significant work, open an issue first to discuss the approach and avoid wasted effort.

### Write Documentation

Documentation improvements are always welcome — docstrings, examples, usage guides, or corrections to existing docs.

### Submit Feedback

File an issue at https://github.com/supermetrics-public/airflow-provider-supermetrics/issues. When proposing a feature:

- Explain the use case in detail
- Keep scope narrow — smaller proposals are easier to evaluate and merge
- Remember this is a small-team project; response times may vary

## Development Setup

```bash
# Clone and set up the environment
git clone https://github.com/supermetrics-public/airflow-provider-supermetrics.git
cd airflow-provider-supermetrics
uv sync --all-extras

# Run tests
.venv/bin/python -m pytest

# Lint and format
.venv/bin/ruff check src tests
.venv/bin/black --check src tests

# Type check
.venv/bin/mypy src
```

## Pull Request Guidelines

- One logical change per PR
- Include tests for new behavior
- All existing tests must pass
- Code must pass `ruff`, `black`, and `mypy` without errors
- Update `plan.md` if you complete a tracked item

## Code of Conduct

This project follows the [Contributor Covenant](https://www.contributor-covenant.org/version/2/1/code_of_conduct/). By participating you agree to abide by its terms.

## License

By contributing, you agree your contributions will be licensed under the Apache-2.0 License.
