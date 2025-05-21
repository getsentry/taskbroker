local taskbroker = import './pipelines/taskbroker.libsonnet';
local pipedream = import 'github.com/getsentry/gocd-jsonnet/libs/pipedream.libsonnet';

// Pipedream can be configured using this object, you can learn more about the
// configuration options here: https://github.com/getsentry/gocd-jsonnet#readme
local pipedream_config = {
  name: 'taskbroker',
  auto_deploy: true,
  materials: {
    taskbroker_repo: {
      git: 'git@github.com:getsentry/taskbroker.git',
      shallow_clone: true,
      branch: 'main',
      destination: 'taskbroker',
    },
  },
  rollback: {
    material_name: 'taskbroker_repo',
    stage: 'deploy-primary',
    elastic_profile_id: 'taskbroker',
  },
  exclude_regions: ['de', 'us', 'customer-7'],
};

pipedream.render(pipedream_config, taskbroker)
