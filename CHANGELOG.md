# Changelog
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

