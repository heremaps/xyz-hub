# Branching & Versioning Strategy

[img_strategy]: diagrams/versioning.png

As per strategy illustrated in diagram below:
* We follow [Semantic versioning 2.0.0](https://semver.org)
* There will be one main branch named after each respective Major version (i.e. `v1`, `v2`, `v3` ...) to manage all future fixes/enhancements against that version.
* **Tagging** format should follow `{Major}.{minor}.{patch}[-rc-{jira-id}.{n}]`
  * All **stable** code from main branch should follow **standard** tagging `{Major}.{minor}.{patch}` e.g. `2.1.3`, `3.0.0`
  * Individual feature branch can have an **extended** tag `{Major}.{minor}.{patch}-rc-{jira-id}.{n}` (e.g. `2.2.0-rc-1234-1`, `3.0.0-rc-5678-10`) to publish pre-release version for integration/user acceptance testing if needed. 
* Internal **file versioning** should **NEVER** use extended format (i.e. **NEVER** use `-rc-{jira-id}.{n}` internally)

![Branching_Deployment_strategy][img_strategy]

## Add more about versioning here
