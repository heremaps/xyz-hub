# Branching Strategy & Deployment Triggers

**NOTE** - This strategy is **deprecated** and is currently only applicable for Naksha v1.x.x related fixes/enhancements.
For v2+ enhancements, refer [this](BRANCHING.md) document.

[img_strategy]: diagrams/branches_and_deployment.png

The diagram below provides view of:
* How various branches can be used for enhancements/maintenances/fixes to the repository
* How deployment to various environments gets triggered automatically from various branches

**In General**:

| Branch(es) | Description                                                                                                      |
|-----------|------------------------------------------------------------------------------------------------------------------|
| `develop` | (under develop) should represent most up-to-date (may not be 100% stable) version in **DEV** environment |
| `int`     | (intergration) should represent most stable version in **E2E** environment                                       |
| `master`  | (main) should represent most stable version in **Prod** environment                                              |
| `hotfix_xxx` | (hot fixes) are the branches, can be forked from any of the above branches, to make and merge a quick fix        |

![Branching_Deployment_strategy][img_strategy]
