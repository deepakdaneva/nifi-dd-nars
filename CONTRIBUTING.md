# Contributing to nifi-dd-nars

Thank you for helping improve nifi-dd-nars. This guide explains how the branches, versions and releases work so that
every change follows the same path from idea to Maven Central.

## Ground rules

- Be respectful. This project follows the [Code of Conduct](CODE_OF_CONDUCT.md).
- Report security problems privately, never in a public issue. See [SECURITY.md](SECURITY.md).
- Open an issue before starting large changes so the approach can be agreed first.

## Branching model

| Branch                                                                     | Purpose                                                                     | Created from  | Merges into                    |
|----------------------------------------------------------------------------|-----------------------------------------------------------------------------|---------------|--------------------------------|
| `main`                                                                     | Stable, released code only. Every release is tagged `vX.Y.Z`.               | -             | -                              |
| `development`                                                              | Integration branch and default branch. Always carries a `-SNAPSHOT` version. | -             | `main` (release promotion)     |
| `feature/*`, `fix/*`, `bugfix/*`, `chore/*`, `docs/*`, `refactor/*`, `perf/*`, `test/*`, `build/*`, `ci/*` | Day-to-day work.                                      | `development` | `development`                  |
| `hotfix/*`                                                                 | Urgent fix for the latest release.                                          | `main`        | `main`                         |
| `release-sync/*`                                                           | Created by automation after a release.                                      | `main`        | `development`                  |

Rules enforced by the repository:

- Nobody pushes directly to `main` or `development`; every change arrives through a pull request.
- Pull requests into `development` must come from a branch named as above.
- Pull requests into `main` must come from `development`, a `hotfix/*` branch or the automated release pull request.
- The `CI OK` and `PR policy` checks must pass before merging.
- Force pushes and deletion of `main`, `development` and `v*` tags are blocked.

## Making a change

```bash
git switch development
git pull
git switch -c feature/short-description
# work, commit
./mvnw spotless:apply   # format code and license headers
./mvnw verify           # build, test and check formatting
git push -u origin feature/short-description
```

Then open a pull request into `development`.

### Commit messages and pull request titles

This project uses [Conventional Commits](https://www.conventionalcommits.org/). Feature pull requests are
squash-merged, so the pull request title becomes the commit message on `development` and drives the next version:

| Title                                       | Effect on the next release |
|---------------------------------------------|----------------------------|
| `fix: handle empty archives`                | patch (`1.2.3` → `1.2.4`)  |
| `feat: support tar.zst`                     | minor (`1.2.3` → `1.3.0`)  |
| `feat!: drop Java 11` or a `BREAKING CHANGE:` footer | major (`1.2.3` → `2.0.0`) |
| `docs:`, `test:`, `refactor:`, `perf:`, `build:`, `ci:`, `chore:` | no release on their own |

## Versioning

Versions follow [Semantic Versioning](https://semver.org/). `development` always holds the next `X.Y.Z-SNAPSHOT`;
`main` holds released versions only. Do not edit versions in `pom.xml` by hand: the release automation does it.

## Snapshots

Every merge into `development` publishes `X.Y.Z-SNAPSHOT` to the Maven Central snapshot repository
(`https://central.sonatype.com/repository/maven-snapshots/`). To try a snapshot:

```xml
<repositories>
    <repository>
        <id>central-snapshots</id>
        <url>https://central.sonatype.com/repository/maven-snapshots/</url>
        <releases><enabled>false</enabled></releases>
        <snapshots><enabled>true</enabled></snapshots>
    </repository>
</repositories>
```

## Release process (maintainers)

1. **Promote.** Open a pull request from `development` into `main` and merge it with a **merge commit** (not squash),
   so every Conventional Commit reaches `main`.
2. **Approve the version.** The `Release` workflow opens or updates a release pull request on `main` that sets the
   new version in every `pom.xml` and updates `CHANGELOG.md`. Review the version and changelog, then merge it.
3. **Tag and publish.** Merging the release pull request tags `vX.Y.Z` and creates the GitHub Release. Publishing to
   Maven Central waits for a maintainer to approve the `maven-central` environment in the workflow run.
4. **Sync back.** The workflow opens a `release-sync/vX.Y.Z` pull request that brings the release into `development`
   and moves it to the next `-SNAPSHOT`. Merge it with a **merge commit**.

### One-time maintainer setup

| Where                                  | Secret                                   | Value                                                                 |
|----------------------------------------|------------------------------------------|-----------------------------------------------------------------------|
| Repository secret                      | `RELEASE_TOKEN`                          | Fine-grained personal access token for this repository with *Contents* and *Pull requests* read/write |
| Environments `maven-central` and `maven-snapshots` | `CENTRAL_USERNAME`, `CENTRAL_PASSWORD` | User token generated at [central.sonatype.com](https://central.sonatype.com/account) |
| Environments `maven-central` and `maven-snapshots` | `GPG_PRIVATE_KEY`, `GPG_PASSPHRASE`    | ASCII-armored signing key published to a public key server, and its passphrase |

### Hotfixes

Create `hotfix/short-description` from `main`, open a pull request into `main` with a `fix:` title, then follow
steps 2 to 4 above. The sync pull request carries the fix back into `development`.

## License

By contributing you agree that your contributions are licensed under the [Apache License 2.0](LICENSE).
