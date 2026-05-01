/**

This library is a set of helpers for building GoCD pipelines.

*/
{
  basic(name, tasks, opts={})::
    local approval = if std.objectHas(opts, 'approval') then
      if opts.approval == 'manual' then
        {
          type: 'manual',
        }
      else if opts.approval == 'success' then
        {
          type: 'success',
          allow_only_on_success: true,
        }
      else
        null;

    local fetch_materials = if std.objectHas(opts, 'fetch_materials') then opts.fetch_materials else null;

    {
      [name]: {
        [if approval != null then 'approval' else null]: approval,
        [if fetch_materials != null then 'fetch_materials' else null]: fetch_materials,
        jobs: {
          [name]: {
            tasks: tasks,
          },
        },
      },
    },
}
