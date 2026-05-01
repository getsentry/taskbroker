/**

This library is a set of helpers for working with GoCD pipelines.

*/

// Helper function to get the final stage of a pipeline
local final_stage_name(pipeline) =
  local final_stage = pipeline.pipeline.stages[std.length(pipeline.pipeline.stages) - 1];
  std.objectFields(final_stage)[0];

// Use this with `assert` to ensure a stage name exists
local check_stage_exists(pipeline, stage_name) =
  local stages = std.filter(function(s) std.objectFields(s)[0] == stage_name, pipeline.pipeline.stages);
  if std.length(stages) == 0 then
    error "Stage '" + stage_name + "' does not exist"
  else
    true;

// Add material to the current pipeline such that it'll depend on
// the previous pipeline.
local chain_materials(current_pipeline, upstream_pipeline) =
  if upstream_pipeline != null then
    current_pipeline {
      pipeline+: {
        materials+: {
          [upstream_pipeline.name + '-' + final_stage_name(upstream_pipeline)]: {
            pipeline: upstream_pipeline.name,
            stage: final_stage_name(upstream_pipeline),
          },
        },
      },
    }
  else current_pipeline;

// This function will add materials to pipelines such that they run
// one after the other
local chain_pipelines(pipelines) =
  [pipelines[0]] + [
    chain_materials(pipelines[i], pipelines[i - 1])
    for i in std.range(1, std.length(pipelines) - 1)
  ];

// This method takes an array of pipelines and produces an object that contains
// the pipelines in format:
// { <pipeline name>.yaml: { ...GoCD metadata + <pipeline name>: <pipeline definition> }.
// This is used to output each pipeline to a seperate file.
local pipelines_to_files_object(pipelines) =
  {
    [pipelines[i].name + '.yaml']: {
      format_version: 10,
      pipelines: {
        [pipelines[i].name]: pipelines[i].pipeline,
      },
    }
    for i in std.range(0, std.length(pipelines) - 1)
  };

// This method takes an array of pipelines and produces an object that contains
// the pipelines in format { <pipeline name>: <pipeline definition> }.
// This is used to output all pipelines in a single file.
local pipelines_to_object(pipelines) =
  {
    format_version: 10,
    pipelines: {
      [pipelines[i].name]: pipelines[i].pipeline
      for i in std.range(0, std.length(pipelines) - 1)
    },
  };

{
  chain_materials(current_pipeline, upstream_pipeline):: chain_materials(current_pipeline, upstream_pipeline),
  chain_pipelines(pipelines):: chain_pipelines(pipelines),
  pipelines_to_files_object(pipelines):: pipelines_to_files_object(pipelines),
  pipelines_to_object(pipelines):: pipelines_to_object(pipelines),
  final_stage_name(pipeline):: final_stage_name(pipeline),
  check_stage_exists(pipeline, stage_name):: check_stage_exists(pipeline, stage_name),
}
