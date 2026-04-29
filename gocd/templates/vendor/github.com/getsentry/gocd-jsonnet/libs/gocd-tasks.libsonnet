/**

This library provides a set of helper functions for creating GoCD tasks

*/

// Escape comments iterates over each line of the input string and prepends a
// hash if the first character of a line is a hash.
// This is only useful for scripts with comments that are inlined since
// GoCD required '#' characters to be either a variable or be prefixed with a
// second '#' character.
local escape_comments(input) = if !std.isEmpty(input) && input[0] == '#' then
  '#' + input
else
  input;

{
  // GoCD requires at least one task for a stage to run, but in some cases
  // we don't want to run any tasks, so we can use this noop task to satisfy.
  noop:: {
    exec: {
      command: true,
    },
  },

  // GoCD scripts cannot contain comments with a single '#' character, they
  // new to be "escaped" by adding a second '#' character.
  script(input):: {
    script: std.join('\n', std.map(escape_comments, std.split(input, '\n'))),
  },
}
