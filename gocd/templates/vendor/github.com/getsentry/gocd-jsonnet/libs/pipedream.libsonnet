/**

This libraries main purpose is to generate a set of pipelines that constitute
a pipedream.

"pipedream" is the overall deployment process for a service at Sentry, where
that service is deployed to multiple regions organized into groups.

Key concepts:
- Groups: Collections of regions that are deployed together
- Regions: Individual deployment targets within a group
- Regions within a group run as parallel jobs within a single pipeline
- Groups are chained sequentially (or fan out in parallel mode)

The entry point is `render(config, pipeline_fn)` where:
- config: Pipedream configuration (name, materials, rollback, etc.)
- pipeline_fn(region): Callback that returns a pipeline definition for a region

Pipedream will:
1. Generate one pipeline per group
2. Aggregate jobs from all regions in the group (running in parallel)
3. Chain pipelines together with upstream materials
4. Append a final 'pipeline-complete' stage

*/
local getsentry = import './getsentry.libsonnet';
local gocd_pipelines = import './gocd-pipelines.libsonnet';
local gocd_stages = import './gocd-stages.libsonnet';
local gocd_tasks = import './gocd-tasks.libsonnet';

local pipeline_name(name, region=null) =
  if region != null then 'deploy-' + name + '-' + region else 'deploy-' + name;

// POSIX-shell preamble injected at the top of every script task in a group
// pipeline's region-keyed jobs. The renderer sets PIPEDREAM_GROUP_REGIONS to
// the full list of regions in the group, so by default every region's job
// runs. To target a subset (e.g. one region) override PIPEDREAM_GROUP_REGIONS
// in GoCD's "Trigger with options" — jobs whose SENTRY_REGION isn't in the
// (possibly subsetted) list exit 0 cleanly.
//
// Stays POSIX (single-bracket [, no [[) so it works under dash, which is the
// /bin/sh on most Linux GoCD agents and would silently no-op `[[` expressions.
local deploy_target_gate = |||
  if [ -n "${PIPEDREAM_GROUP_REGIONS:-}" ] && [ -n "${SENTRY_REGION:-}" ]; then
    case ",${PIPEDREAM_GROUP_REGIONS}," in
      *",${SENTRY_REGION},"*) ;;
      *)
        echo "Skipping $SENTRY_REGION (not in PIPEDREAM_GROUP_REGIONS=$PIPEDREAM_GROUP_REGIONS)"
        exit 0
        ;;
    esac
  fi
|||;

local wrap_task(task) =
  if std.objectHas(task, 'script') then
    { script: deploy_target_gate + task.script }
  else
    task;

local is_autodeploy(pipedream_config) =
  !std.objectHas(pipedream_config, 'auto_deploy') || pipedream_config.auto_deploy == true;

// Regions that are excluded by default and must be explicitly included
local default_excluded_regions = ['control', 'prod-control', 'snty-tools'];

local is_excluded_region = function(region, config)
  std.objectHas(config, 'exclude_regions') && std.length(std.find(region, config.exclude_regions)) > 0;

local is_included_region = function(region, config)
  std.objectHas(config, 'include_regions') && std.length(std.find(region, config.include_regions)) > 0;

local is_default_excluded_region = function(region)
  std.length(std.find(region, default_excluded_regions)) > 0;

local should_include_region = function(region, config)
  !is_excluded_region(region, config) && (!is_default_excluded_region(region) || is_included_region(region, config));

local get_stage_name(stage) =
  std.objectFields(stage)[0];

local get_stage_jobs(stage) =
  local stage_name = get_stage_name(stage);
  if std.objectHas(stage[stage_name], 'jobs') then
    stage[stage_name].jobs
  else
    {};

local get_stage_props(stage) =
  local stage_name = get_stage_name(stage);
  local props = stage[stage_name];
  { [k]: props[k] for k in std.objectFields(props) if k != 'jobs' && k != 'environment_variables' };

local get_stage_env_vars(stage) =
  local stage_name = get_stage_name(stage);
  local props = stage[stage_name];
  if std.objectHas(props, 'environment_variables') then props.environment_variables else {};

local get_pipeline_env_vars(pipeline) =
  if std.objectHas(pipeline, 'environment_variables') then pipeline.environment_variables else {};

// Cascade down environment variables with precedence: job > stage > pipeline
local merge_env_vars(pipeline_env, stage_env, job_env) =
  pipeline_env + stage_env + job_env;

// This function returns a "trigger pipeline", if configured for manual deploys.
// This pipeline is used so users don't need to know what the first pipedream
// region is, instead they just look for the `deploy-<service name>` pipeline.
// For autodeploy pipedreams we don't need a trigger so null is returned.
local pipedream_trigger_pipeline(pipedream_config) =
  if is_autodeploy(pipedream_config) == true then
    null
  else
    local name = pipedream_config.name;
    local materials = pipedream_config.materials;

    {
      name: pipeline_name(name),
      pipeline: {
        group: name,
        display_order: 0,
        materials: materials,
        lock_behavior: 'unlockWhenFinished',
        stages: [
          gocd_stages.basic('pipeline-complete', [gocd_tasks.noop], { approval: 'manual', fetch_materials: false }),
        ],
      },
    };

// pipedream_rollback_pipeline creates a pipeline that will rollback a
// pipedream deployment to a previous deployment.
//
// pipedream_config:  The configuration passed into the render() function
// service_pipelines: The user facing deploys in pipedream
// trigger_pipeline:  The `deploy-<service name>` pipeline if the pipedream is
//                    a manual deploy.
local pipedream_rollback_pipeline(pipedream_config, service_pipelines, trigger_pipeline) =
  if std.objectHas(pipedream_config, 'rollback') then
    local name = pipedream_config.name;
    local final_pipeline = service_pipelines[std.length(service_pipelines) - 1];

    // Rollbacks work by calling two devinfra-deployment-infra scripts:
    //    gocd-pause-and-cancel-pipelines
    //    gocd-emergency-deploy
    // Both of these scripts tag `--pipeline=<pipeline name>` flags to determine
    // which pipelines to operate on.
    //
    // We want to pause and cancel the manual trigger pipeline, if it exists,
    // and all of the deployment pipelines. => all_pipeline_flags
    // Then we want to re-run the primary deploy stages on JUST the deployment
    // pipelines (i.e. not the manual trigger) => region_pipeline_flags
    local region_pipeline_flags = std.join(' ', std.map(function(p) '--pipeline=' + p.name, service_pipelines));
    local all_pipeline_flags = if trigger_pipeline == null then
      region_pipeline_flags
    else
      region_pipeline_flags + ' --pipeline=' + trigger_pipeline.name;

    // If we ever change the final stage in pipedream (i.e. add or remove a
    // final stage) we may want the material for the rollback pipeline to look
    // an existing stage, for example allow rollbacks to deploys with a
    // `deploy-primary` instead of `pipeline-complete` where no existing
    // deploys have a `pipeline-complete` stage.
    local final_stage = if std.objectHas(pipedream_config.rollback, 'final_stage') then
      pipedream_config.rollback.final_stage
    else
      gocd_pipelines.final_stage_name(final_pipeline);

    {
      // Check that the defined stage name exists on the final pipeline,
      // otherwise this won't be discovered as an issue until GoCD tries to
      // load the config repo.
      assert gocd_pipelines.check_stage_exists(final_pipeline, pipedream_config.rollback.stage),
      assert gocd_pipelines.check_stage_exists(final_pipeline, final_stage),

      name: 'rollback-' + name,
      pipeline: {
        group: name,
        display_order: 1,
        environment_variables: {
          GOCD_ACCESS_TOKEN: '{{SECRET:[devinfra][gocd_access_token]}}',
          ROLLBACK_MATERIAL_NAME: pipedream_config.rollback.material_name,
          ROLLBACK_STAGE: pipedream_config.rollback.stage,
          REGION_PIPELINE_FLAGS: region_pipeline_flags,
          ALL_PIPELINE_FLAGS: all_pipeline_flags,
          TRIGGERED_BY: '',
        },
        materials: {
          [final_pipeline.name + '-' + final_stage]: {
            pipeline: final_pipeline.name,
            stage: final_stage,
          },
        },
        lock_behavior: 'unlockWhenFinished',
        stages: [
          {
            pause_pipelines: {
              approval: {
                type: 'manual',
              },
              jobs: {
                rollback: {
                  elastic_profile_id: pipedream_config.rollback.elastic_profile_id,
                  tasks: [
                    gocd_tasks.script(importstr './bash/pause-pipelines.sh'),
                  ],
                },
              },
            },
          },
          {
            start_rollback: {
              jobs: {
                rollback: {
                  elastic_profile_id: pipedream_config.rollback.elastic_profile_id,
                  tasks: [
                    gocd_tasks.script(importstr './bash/rollback.sh'),
                  ],
                },
              },
            },
          },
          {
            incident_resolved: {
              approval: {
                type: 'manual',
              },
              jobs: {
                rollback: {
                  elastic_profile_id: pipedream_config.rollback.elastic_profile_id,
                  tasks: [
                    gocd_tasks.script(importstr './bash/unpause-and-unlock-pipelines.sh'),
                  ],
                },
              },
            },
          },
        ],
      },
    }
  else
    null;

// generate_group_pipeline creates a single pipeline for a group by:
// 1. Getting all regions in the group
// 2. Filtering regions based on exclude/include config
// 3. Aggregating jobs from all regions into parallel jobs per stage
// 4. Appending a 'pipeline-complete' stage
//
// pipedream_config: The configuration passed into render()
// pipeline_fn:      Callback that takes a region and returns a GoCD pipeline
// group:            The group name to create a pipeline for
// display_order:    The order of the pipeline in the GoCD UI
local generate_group_pipeline(pipedream_config, pipeline_fn, group, display_order) =
  local service_name = pipedream_config.name;

  local all_regions = getsentry.get_targets(group);
  local regions = std.filter(
    function(r) should_include_region(r, pipedream_config),
    all_regions
  );

  // Cache pipeline_fn results to avoid redundant calls per region
  local region_pipelines = { [r]: pipeline_fn(r) for r in regions };

  // Validate that each stage object has exactly one key. In Jsonnet, a missing
  // comma between stage definitions silently merges them into a single object,
  // causing stages to be lost. Catch this at build time.
  assert std.foldl(
    function(acc, region)
      local p = region_pipelines[region];
      local stages = if std.objectHas(p, 'stages') then p.stages else [];
      assert std.foldl(
        function(acc2, stage)
          local keys = std.objectFields(stage);
          assert std.length(keys) == 1 :
                 'Stage object has %d keys (%s) — each stage must have exactly one key. '
                 % [std.length(keys), std.join(', ', keys)]
                 + 'This usually means a missing comma between stage definitions.';
          true,
        stages,
        true
      );
      true,
    regions,
    true
  );

  local template_pipeline = region_pipelines[regions[0]];

  // Validate pipeline-level attributes (materials, lock_behavior, etc.)
  // are consistent across all regions in the group.
  local pipeline_props(p) =
    { [k]: p[k] for k in std.objectFields(p) if k != 'stages' && k != 'environment_variables' };
  local template_props = pipeline_props(template_pipeline);
  assert std.foldl(
    function(acc, r)
      local props = pipeline_props(region_pipelines[r]);
      assert props == template_props :
             'Pipeline-level attributes differ across regions in group. '
             + "Region '%s' differs." % [r];
      true,
    regions[1:],
    true
  );

  // Collect all unique stages across all regions in the group.
  // If a region doesn't define a stage that another region has,
  // it simply contributes no jobs to that stage.
  local all_stages = std.foldl(
    function(acc, region)
      local p = region_pipelines[region];
      local region_stages = if std.objectHas(p, 'stages') then p.stages else [];
      acc + [
        stage
        for stage in region_stages
        if !std.member([get_stage_name(s) for s in acc], get_stage_name(stage))
      ],
    regions,
    []
  );

  local get_matching_stage(p, stage_name) =
    local matching = std.filter(
      function(s) get_stage_name(s) == stage_name,
      if std.objectHas(p, 'stages') then p.stages else []
    );
    if std.length(matching) > 0 then matching[0] else null;

  // Transforms a stage by aggregating jobs from all regions.
  // Env vars identical across all regions are kept at stage level;
  // region-specific env vars are cascaded down to the job level.
  local transform_stage(stage) =
    local stage_name = get_stage_name(stage);
    local stage_props = get_stage_props(stage);

    // Validate that all regions agree on stage properties. GoCD only supports
    // stage-level attributes (approval, fetch_materials, etc.) — there is no
    // per-job override — so conflicting values across regions must be caught
    // at build time rather than silently using the first region's values.
    assert std.foldl(
      function(acc, r)
        local p = region_pipelines[r];
        local rs = get_matching_stage(p, stage_name);
        local props = if rs != null then get_stage_props(rs) else stage_props;
        assert props == stage_props :
               "Stage '%s': conflicting stage properties across regions in group. "
               % [stage_name]
               + "Region '%s' differs." % [r];
        true,
      regions[1:],
      true
    );

    // Collect merged pipeline+stage env vars for each region
    local per_region_parent_envs = {
      [region]: (
        local p = region_pipelines[region];
        local pipeline_env = get_pipeline_env_vars(p);
        local region_stage = get_matching_stage(p, stage_name);
        local stage_env = if region_stage != null then get_stage_env_vars(region_stage) else {};
        merge_env_vars(pipeline_env, stage_env, {})
      )
      for region in regions
    };

    // Env vars identical across ALL regions stay at stage level
    local first_env = per_region_parent_envs[regions[0]];
    local common_env = {
      [k]: first_env[k]
      for k in std.objectFields(first_env)
      if std.length(std.filter(
        function(r) std.objectHas(per_region_parent_envs[r], k) && per_region_parent_envs[r][k] == first_env[k],
        regions
      )) == std.length(regions)
    };

    local all_jobs = std.foldl(
      function(acc, region)
        local parent_env = per_region_parent_envs[region];
        local region_specific_env = {
          [k]: parent_env[k]
          for k in std.objectFields(parent_env)
          if !std.objectHas(common_env, k) || common_env[k] != parent_env[k]
        };
        local p = region_pipelines[region];
        local region_stage = get_matching_stage(p, stage_name);
        local stage_jobs = if region_stage != null then get_stage_jobs(region_stage) else {};

        acc + {
          // Skip the suffix when there's nothing to disambiguate: a
          // single-region group (one pipeline_fn call, names already unique)
          // or a job already keyed by region (would yield 'diff-region-region').
          [
          local skip_suffix = std.length(regions) == 1 || std.endsWith(job_name, '-' + region);
          if skip_suffix then job_name else job_name + '-' + region
          ]: (
            local job = stage_jobs[job_name];
            local job_env = if std.objectHas(job, 'environment_variables') then job.environment_variables else {};
            local merged_env = region_specific_env + job_env;
            local job_tasks = if std.objectHas(job, 'tasks') then [wrap_task(t) for t in job.tasks] else null;
            local gated_job = if job_tasks != null then job { tasks: job_tasks } else job;
            if std.length(std.objectFields(merged_env)) > 0 then
              gated_job { environment_variables: merged_env }
            else
              gated_job
          )
          for job_name in std.objectFields(stage_jobs)
        },
      regions,
      {}
    );

    {
      [stage_name]: stage_props {
        jobs: all_jobs,
      } + (
        if std.length(std.objectFields(common_env)) > 0 then
          { environment_variables: common_env }
        else
          {}
      ),
    };

  // `auto_pipeline_progression` was added as a utility for folks new to
  // pipedream. When this is false, each region will need manual approval
  // before doing any of the deployment stages.
  // We add ready + wait stages to improve the GoCD UI since it will start
  // the regions pipeline, show a green check and then a manual approval arrow.
  // If the first stage had manual approval, GoCD assumes the pipeline itself
  // needs manual approval and expects the user to manually trigger the
  // pipeline through the play+ icon, which isn't clear where in a pipedream
  // deployment we are waiting for a manual approval.
  local prepend_stages = if std.objectHas(pipedream_config, 'auto_pipeline_progression') && pipedream_config.auto_pipeline_progression == false then
    [
      // Ready runs when the upstream pipeline is complete, indicating that
      // the pipedream has progressed to the next stage.
      gocd_stages.basic('ready', [gocd_tasks.noop]),

      // The wait stage is used to wait for manual approval before continuing/
      // running the actual pipeline.
      gocd_stages.basic('wait', [gocd_tasks.noop], { approval: 'manual' }),
    ]
  else
    [];

  // Apply transform to all stages
  local transformed_stages = [
    transform_stage(stage)
    for stage in all_stages
  ];

  // Strip pipeline and stage level environment variables
  local filtered_template = {
    [k]: template_pipeline[k]
    for k in std.objectFields(template_pipeline)
    if k != 'environment_variables'
  };

  // Assemble final pipeline from template. PIPEDREAM_GROUP_REGIONS is set so
  // the deploy_target_gate preamble (injected into each region-keyed script
  // task) can validate a user-supplied DEPLOY_TARGET_REGION at runtime.
  filtered_template {
    group: service_name,
    display_order: display_order,
    environment_variables: {
      PIPEDREAM_GROUP_REGIONS: std.join(',', regions),
    },
    stages: prepend_stages + transformed_stages + [
      // This stage is added to ensure a rollback doesn't cause
      // a deployment train.
      //
      // i.e. During a rollback, de and US re-runs the final stage
      // The de final stage completes and causes the US pipeline to
      // re-run. With `pipeline-complete` as the final stage, it isn't
      // re-run by a rollback, preventing this domino effect.
      gocd_stages.basic('pipeline-complete', [gocd_tasks.noop], { fetch_materials: false }),
    ],
  };

// get_service_pipelines generates a pipeline for each group.
//
// pipedream_config: The configuration passed into render()
// pipeline_fn:      Callback that takes a region and returns a GoCD pipeline
// groups:           The group names to create pipelines for
// display_offset:   Offset for display_order (accounts for trigger/rollback)
local get_service_pipelines(pipedream_config, pipeline_fn, groups, display_offset) =
  [
    {
      name: pipeline_name(pipedream_config.name, groups[i]),
      pipeline: generate_group_pipeline(pipedream_config, pipeline_fn, groups[i], display_offset + i),
    }
    for i in std.range(0, std.length(groups) - 1)
  ];

// This is a helper function that handles pipelines that may be null
// (i.e. pipedreams that do not have a rollback pipeline configured)
local pipeline_to_array(pipeline) =
  if pipeline == null then [] else [pipeline];

{
  // render generates the trigger pipeline (if manual), group pipelines, and rollback pipeline.
  render(pipedream_config, pipeline_fn, parallel=false)::
    local groups_to_render = std.filter(
      function(group)
        local regions = getsentry.get_targets(group);
        std.length(std.filter(
          function(r) should_include_region(r, pipedream_config),
          regions
        )) > 0,
      getsentry.group_names
    );

    local test_groups_to_render = std.filter(
      function(group)
        local regions = getsentry.get_targets(group);
        std.length(std.filter(
          function(r) should_include_region(r, pipedream_config),
          regions
        )) > 0,
      getsentry.test_group_names
    );

    local trigger_pipeline = pipedream_trigger_pipeline(pipedream_config);
    local service_pipelines = get_service_pipelines(pipedream_config, pipeline_fn, groups_to_render, 2);
    local test_pipelines = get_service_pipelines(pipedream_config, pipeline_fn, test_groups_to_render, std.length(groups_to_render) + 2);
    local rollback_pipeline = pipedream_rollback_pipeline(pipedream_config, service_pipelines, trigger_pipeline);

    local all_pipelines = if parallel then pipeline_to_array(rollback_pipeline) +
                                           pipeline_to_array(trigger_pipeline) +
                                           // Chain the service pipelines together with
                                           // the trigger pipeline
                                           std.map(function(p) gocd_pipelines.chain_materials(p, trigger_pipeline), service_pipelines)
                                           +
                                           // Chain each test group to the trigger pipeline
                                           std.map(function(p) gocd_pipelines.chain_materials(p, trigger_pipeline), test_pipelines)
    else pipeline_to_array(rollback_pipeline) +
         // Chain the service pipelines together with
         // the trigger pipeline
         gocd_pipelines.chain_pipelines(
           pipeline_to_array(trigger_pipeline) + service_pipelines,
         ) +
         // Chain each test group to the trigger pipeline
         std.map(function(p) gocd_pipelines.chain_materials(p, trigger_pipeline), test_pipelines);


    // If --ext-var=output-files=true we want to return:
    //     {<file name>: <pipeline>, <file name>: <pipeline>},
    // Otherwise we want to return:
    //     { pipelines: [ <pipeline>, <pipeline> ] },
    // This toggle is useful for a few reasons:
    //     1. Multiple files (output-files=true) is helpful when reviewing
    //        the pipelines locally (i.e. you can quickly look up the customer-1
    //        pipeline file).
    //     2. The jsonnet plugin for GoCD and the GitHub validation action work
    //        best with a single file containing all pipelines.
    if std.extVar('output-files') then
      gocd_pipelines.pipelines_to_files_object(all_pipelines)
    else
      gocd_pipelines.pipelines_to_object(all_pipelines),
}
