# Introduction

The team behind the [Naksha](https://github.com/heremaps/naksha) gratefully accepts contributions via
[pull requests](https://help.github.com/articles/about-pull-requests/) filed against the
[GitHub project](https://github.com/heremaps/naksha/pulls).

# Signing each Commit

As part of filing a pull request we ask you to sign off the
[Developer Certificate of Origin](https://developercertificate.org/) (DCO) in each commit.
Any Pull Request with commits that are not signed off will be reject by the
[DCO check](https://probot.github.io/apps/dco/).

A DCO is lightweight way for a contributor to confirm that you wrote or otherwise have the right
to submit code or documentation to a project. Simply add `Signed-off-by` as shown in the example below
to indicate that you agree with the DCO.

An example signed commit message:

```
    README.md: Fix minor spelling mistake

    Signed-off-by: John Doe <john.doe@example.com>
```

Git has the `-s` flag that can sign a commit for you, see example below:

`$ git commit -s -m 'README.md: Fix minor spelling mistake'`

# Branching strategy

Depending on whether your contribution is towards Naksha v1 or v2+, refer respective Branching/Versioning strategy:

* [v1.x.x Branching/Versioning strategy](docs/BRANCHING_v1.md)
* [v2+ Branching/Versioning strategy](docs/BRANCHING.md)

Contributors are expected to make changes into their own `feature_*` branches, created out of respective main branch. 

# Local execution

From Local machine:
* Validate service builds and starts - as per [Getting Started](README.md#getting-started) section.
* Validate your changes and normal functioning of service as per [Usage](README.md#usage) instructions.

# Before making a pull-request

Eventually, before making a pull request, ensure following:

1. If any **new secret** parameter is introduced, then mention in pull-request as an indication for the repo-owner to add the secret in respective cloud environments.
2. If any **new config** (not a secret) parameter is introduced in [default-config.json](here-naksha-lib-hub/src/main/resources/config/default-config.json),
ensure the same is added into **docker** [cloud-config.json](docker/cloud-config.json) as well.
