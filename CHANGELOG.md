# Changelog
## 26.7.0

### New Features ✨

- Add a flag to disable consuming from a topic by @evanh in [#745](https://github.com/getsentry/taskbroker/pull/745)

### Bug Fixes 🐛

- (gocd) Pass --config to migration job for use_yaml_config pools by @enochtangg in [#753](https://github.com/getsentry/taskbroker/pull/753)
- (lint) Make clippy happy by @bmckerry in [#749](https://github.com/getsentry/taskbroker/pull/749)
- Repair migration config-repo parse and pass --config for yaml pools by @enochtangg in [#755](https://github.com/getsentry/taskbroker/pull/755)

### Internal Changes 🔧

- (client) Make BatchPushTaskWorker the default by @evanh in [#751](https://github.com/getsentry/taskbroker/pull/751)
- (integration) Add postgres-backed push mode integration tests by @evanh in [#746](https://github.com/getsentry/taskbroker/pull/746)
- (workerchild) Don't set future tracking env var by @bmckerry in [#747](https://github.com/getsentry/taskbroker/pull/747)
- Pin rust to 1.96 by @untitaker in [#750](https://github.com/getsentry/taskbroker/pull/750)

### Other

- debug(gocd): Instrument migration job to find why --config is dropped by @enochtangg in [#754](https://github.com/getsentry/taskbroker/pull/754)

## 26.6.0

### Bug Fixes 🐛

- Allow configuring the push_task_timeout by @evanh in [#698](https://github.com/getsentry/taskbroker/pull/698)

## 26.5.2

### New Features ✨

- (config) Add Validation to Several Config Fields by @george-sentry in [#664](https://github.com/getsentry/taskbroker/pull/664)
- (query retries) Enable query retry with default of 3 retries by @victoria-yining-huang in [#665](https://github.com/getsentry/taskbroker/pull/665)
- (taskbroker) Undo Claim on Send Failure by @george-sentry in [#661](https://github.com/getsentry/taskbroker/pull/661)
- Add default metrics tags to Sentry errors by @untitaker in [#659](https://github.com/getsentry/taskbroker/pull/659)

### Bug Fixes 🐛

- (config) Legacy retry topic uses deadletter cluster by @untitaker in [#666](https://github.com/getsentry/taskbroker/pull/666)

### Internal Changes 🔧

- (config) Introduce multi-topic config, deprecate old fields by @untitaker in [#663](https://github.com/getsentry/taskbroker/pull/663)

## 26.5.1

### New Features ✨

#### Taskbroker

- Add kafka_consume_retry_topic config flag by @untitaker in [#646](https://github.com/getsentry/taskbroker/pull/646)
- Implement retry support for raw topics by @untitaker in [#630](https://github.com/getsentry/taskbroker/pull/630)

#### Other

- (worker) Add taskworker.task.failed log for failed tasks by @s-starostin in [#635](https://github.com/getsentry/taskbroker/pull/635)

### Bug Fixes 🐛

- Fix pre-commit CI failures by @untitaker in [#644](https://github.com/getsentry/taskbroker/pull/644)
- Fix taskbroker CI issues by @untitaker in [#640](https://github.com/getsentry/taskbroker/pull/640)

### Other

- release: 26.5.0 by @markstory in [d054115d](https://github.com/getsentry/taskbroker/commit/d054115df0a6a61c7cb1b45160a9f5d74cecb095)

## 26.5.0

### New Features ✨

#### Taskbroker

- Batch Status Updates by @george-sentry in [#618](https://github.com/getsentry/taskbroker/pull/618)
- More Push Taskbroker Metrics by @george-sentry in [#631](https://github.com/getsentry/taskbroker/pull/631)

#### Other

- (client) Add TaskProducer by @bmckerry in [#633](https://github.com/getsentry/taskbroker/pull/633)
- (examples) Add task that produces by @bmckerry in [#638](https://github.com/getsentry/taskbroker/pull/638)

### Bug Fixes 🐛

- (client) Use correct default topic name by @bmckerry in [#636](https://github.com/getsentry/taskbroker/pull/636)
- (iswf) Reverts change to reporting logic that silences NoRetriesRemaining exceptions by @GabeVillalobos in [#627](https://github.com/getsentry/taskbroker/pull/627)

### Internal Changes 🔧

- (deps) Bump urllib3 from 2.6.3 to 2.7.0 by @dependabot in [#632](https://github.com/getsentry/taskbroker/pull/632)
- (deps-dev) Bump pytest from 8.3.3 to 9.0.3 by @dependabot in [#629](https://github.com/getsentry/taskbroker/pull/629)

## 26.4.2

### New Features ✨

- (iswf) Adds silenced_exceptions parameter to tasks, exposes this and report_timeout_errors in task registration by @GabeVillalobos in [#608](https://github.com/getsentry/taskbroker/pull/608)

### Bug Fixes 🐛

- Add a received->pushed latency metric by @evanh in [#613](https://github.com/getsentry/taskbroker/pull/613)
- Updates file style after Black version update by @GabeVillalobos in [#614](https://github.com/getsentry/taskbroker/pull/614)

### Internal Changes 🔧

- (control) Turn on deployment in sentry-control by @dmajere in [#616](https://github.com/getsentry/taskbroker/pull/616)
- (gocd) Gocd-jsonnet 3.0.1 by @dmajere in [#615](https://github.com/getsentry/taskbroker/pull/615)
- (tests) Use tempfile everywhere by @untitaker in [#620](https://github.com/getsentry/taskbroker/pull/620)

### Other

- feat(client) Improve import ergonomics for scheduler by @markstory in [#611](https://github.com/getsentry/taskbroker/pull/611)

## 26.4.1

### New Features ✨

- Separate push Taskworker from pull Taskworker by @evanh in [#593](https://github.com/getsentry/taskbroker/pull/593)

### Internal Changes 🔧

- (eco) Adds report_timeout_errors options to Task definitions, allows ProcessingDeadlineExceeded to be retried by @GabeVillalobos in [#592](https://github.com/getsentry/taskbroker/pull/592)
- (store) Reorganize Migrations by @george-sentry in [#596](https://github.com/getsentry/taskbroker/pull/596)
- (taskbroker) Flatten Imports by @george-sentry in [#600](https://github.com/getsentry/taskbroker/pull/600)

### Other

- feat(schedules) Improve schedule entry isolation by @markstory in [#599](https://github.com/getsentry/taskbroker/pull/599)

## 26.4.0

### New Features ✨

#### Taskbroker

- Add Useful Push Taskbroker Metrics by @george-sentry in [#595](https://github.com/getsentry/taskbroker/pull/595)
- Add Claimed Status to Handle Push Failures by @george-sentry in [#586](https://github.com/getsentry/taskbroker/pull/586)

#### Other

- (postgres) Change the Postgres Adapter to be Partition Aware by @george-sentry in [#591](https://github.com/getsentry/taskbroker/pull/591)

### Bug Fixes 🐛

#### Ci

- Fix image publishing on release branches by @hubertdeng123 in [#598](https://github.com/getsentry/taskbroker/pull/598)
- Fix branch prefix in assemble-taskbroker-image condition by @hubertdeng123 in [#597](https://github.com/getsentry/taskbroker/pull/597)

### Internal Changes 🔧

- (store) Restructure Store Module by @george-sentry in [#594](https://github.com/getsentry/taskbroker/pull/594)

### Other

- fix(scheduler) Make schedule changes take effect immediately by @markstory in [#590](https://github.com/getsentry/taskbroker/pull/590)
