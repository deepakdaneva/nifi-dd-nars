# Security policy

## Supported versions

Only the latest release line receives security fixes.

| Version           | Supported |
|-------------------|-----------|
| 2.x (latest)   | Yes       |
| Older releases    | No        |

## Reporting a vulnerability

Please do **not** open a public issue for security problems.

Report it privately through
[GitHub private vulnerability reporting](https://github.com/deepakdaneva/nifi-dd-nars/security/advisories/new).
Include the affected version, a description of the issue, and steps or a sample input to reproduce it.

You can expect an acknowledgement within 7 days. Once the issue is confirmed, a fix is prepared on a private branch,
released as a patch version, and published together with a GitHub Security Advisory that credits the reporter
(unless you prefer to stay anonymous).

## Dependency updates

Dependencies are monitored by Dependabot and code is scanned with CodeQL. Pull requests that introduce dependencies with
known vulnerabilities of moderate or higher severity fail the `Dependency review` check.
