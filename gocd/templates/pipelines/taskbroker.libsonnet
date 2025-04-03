local gocdtasks = import 'github.com/getsentry/gocd-jsonnet/libs/gocd-tasks.libsonnet';

local checks_stage = {
  checks: {
    fetch_materials: true,
    jobs: {
      checks: {
        timeout: 20,
        elastic_profile_id: 'taskbroker',
        environment_variables: {
          GITHUB_TOKEN: '{{SECRET:[devinfra-github][token]}}',
        },
        tasks: [
          gocdtasks.script(importstr '../bash/check-github-runs.sh'),
          gocdtasks.script(importstr '../bash/check-cloudbuild.sh'),
        ],
      },
    },
  },
};

local deploy_canary_stage(region) =
  if region == 'us' then
    [
      {
        'deploy-canary': {
          fetch_materials: true,
          jobs: {
            deploy: {
              timeout: 30,
              elastic_profile_id: 'taskbroker',
              environment_variables: {
                LABEL_SELECTOR: 'service=taskbroker,env=canary',
              },
              tasks: [
                gocdtasks.script(importstr '../bash/deploy.sh'),
                gocdtasks.script(importstr '../bash/wait-canary.sh'),
              ],
            },
          },
        },
      },
    ] else [];

local deployPrimaryStage = {
  'deploy-primary': {
    fetch_materials: true,
    jobs: {
      deploy: {
        timeout: 30,
        elastic_profile_id: 'taskbroker',
        environment_variables: {
          LABEL_SELECTOR: 'service=taskbroker',
        },
        tasks: [
          gocdtasks.script(importstr '../bash/deploy.sh'),
        ],
      },
    },
  },
};

function(region) {
  environment_variables: {
    // SENTRY_REGION is used by the dev-infra scripts to connect to GKE
    SENTRY_REGION: region,
  },
  materials: {
    taskbroker_repo: {
      git: 'git@github.com:getsentry/taskbroker.git',
      shallow_clone: true,
      branch: 'main',
      destination: 'taskbroker',
    },
  },
  lock_behavior: 'unlockWhenFinished',
  stages: [checks_stage] + deploy_canary_stage(region) + [deployPrimaryStage],
}
