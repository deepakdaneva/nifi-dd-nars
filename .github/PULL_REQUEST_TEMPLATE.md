<!--
Title must follow Conventional Commits, e.g. "feat: add tar.zst support" or "fix(archive): close stream on error".
Feature and fix branches target `development`. Only `development` and `hotfix/*` may target `main`.
-->

## What and why

<!-- What does this change and why is it needed? Link the issue: "Closes #123". -->

## How it was tested

<!-- Commands run, new or updated tests. -->

## Checklist

- [ ] Branch created from `development` and named `feature/*`, `fix/*`, `bugfix/*`, `chore/*`, `docs/*`, `refactor/*`, `perf/*`, `test/*`, `build/*` or `ci/*`
- [ ] `./mvnw verify` passes locally (`./mvnw spotless:apply` run)
- [ ] Tests added or updated
- [ ] Public API changes documented (Javadoc and README)
- [ ] Breaking changes called out with `!` in the title or a `BREAKING CHANGE:` footer
