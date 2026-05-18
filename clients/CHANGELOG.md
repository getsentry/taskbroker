# Changelog
## 0.1.15

### New Features ✨

- (client) Add TaskProducer by @bmckerry in [#633](https://github.com/getsentry/taskbroker/pull/633)
- (examples) Add task that produces by @bmckerry in [#638](https://github.com/getsentry/taskbroker/pull/638)

### Bug Fixes 🐛

- (client) Use correct default topic name by @bmckerry in [#636](https://github.com/getsentry/taskbroker/pull/636)
- (iswf) Reverts change to reporting logic that silences NoRetriesRemaining exceptions by @GabeVillalobos in [#627](https://github.com/getsentry/taskbroker/pull/627)

## 0.1.14

### New Features ✨

- (taskbroker-client) Add pass_headers option to task registration by @untitaker in [#623](https://github.com/getsentry/taskbroker/pull/623)

### Bug Fixes 🐛

- (taskbroker-client) Update docstrings to reflect msgpack serialization by @untitaker in [#624](https://github.com/getsentry/taskbroker/pull/624)

## 0.1.13

- No documented changes.

## 0.1.12

### Bug Fixes 🐛

- (taskworker) Improve Queue Size Metrics by @george-sentry in [#612](https://github.com/getsentry/taskbroker/pull/612)

### Internal Changes 🔧

- (deps-dev) Bump black from 24.10.0 to 26.3.1 in /clients/python by @dependabot in [#606](https://github.com/getsentry/taskbroker/pull/606)

## 0.1.11

### New Features ✨

- Add volatile schedule storage by @markstory in [#605](https://github.com/getsentry/taskbroker/pull/605)

## 0.1.9

### New Features ✨

- (taskbroker) Dual-write new parameters_bytes by @untitaker in [#602](https://github.com/getsentry/taskbroker/pull/602)

### Other

- release: 26.5.0 by @untitaker in [0206415b](https://github.com/getsentry/taskbroker/commit/0206415bffe6b91c4934fe3e3682e78f2659f13c)

## 26.5.0

### New Features ✨

- (taskbroker) Dual-write new parameters_bytes by @untitaker in [#602](https://github.com/getsentry/taskbroker/pull/602)

## 0.1.8

### New Features ✨

- (taskworker) Add Push Mode to Taskworker by @george-sentry in [#576](https://github.com/getsentry/taskbroker/pull/576)

### Internal Changes 🔧

- Bump sentry-arroyo to 2.38.7 by @bmckerry in [#585](https://github.com/getsentry/taskbroker/pull/585)
- Bump sentry-arroyo to 2.38.5 by @bmckerry in [#583](https://github.com/getsentry/taskbroker/pull/583)

### Other

- Add headers and hooks to taskbroker client by @gricha in [#587](https://github.com/getsentry/taskbroker/pull/587)

## 0.1.7

- feat(client) Make the ProducerFactory depend on a protocol by @markstory in [#578](https://github.com/getsentry/taskbroker/pull/578)

## 0.1.6

- fix(client) Align logger names with package name by @markstory in [#572](https://github.com/getsentry/taskbroker/pull/572)

## 0.1.5

### New Features ✨

- Add simpler API for creating external tasks by @markstory in [#570](https://github.com/getsentry/taskbroker/pull/570)

## 0.1.4

- No documented changes.

## 0.1.3

- chore(client) Move types-protobuf to dev deps and fix version by @markstory in [#563](https://github.com/getsentry/taskbroker/pull/563)

## 0.1.2

### Bug Fixes 🐛

- Relax grpc constraint by @markstory in [#554](https://github.com/getsentry/taskbroker/pull/554)

## 0.1.1

### New Features ✨

#### Python

- Move scheduler files and tests from the prototype by @markstory in [#540](https://github.com/getsentry/taskbroker/pull/540)
- Move worker files and tests from python-client branch by @markstory in [#538](https://github.com/getsentry/taskbroker/pull/538)
- Move client.py and tests from python-client branch by @markstory in [#537](https://github.com/getsentry/taskbroker/pull/537)

#### Other

- Add workflow and craft tooling for client libs by @markstory in [#548](https://github.com/getsentry/taskbroker/pull/548)
- Add application to worker requests by @markstory in [#545](https://github.com/getsentry/taskbroker/pull/545)
- Add docker image and readme for example app by @markstory in [#544](https://github.com/getsentry/taskbroker/pull/544)
- Add example app and CI tests by @markstory in [#541](https://github.com/getsentry/taskbroker/pull/541)

### Other

- fix(client) Fix mistakes in the client release tools by @markstory in [#551](https://github.com/getsentry/taskbroker/pull/551)
- feat(client) Add the first chunk of the taskbroker_client by @markstory in [#531](https://github.com/getsentry/taskbroker/pull/531)

