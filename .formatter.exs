# Used by "mix format"
[
  inputs: ["{mix,.formatter}.exs", "{config,lib,test}/**/*.{ex,exs}"],
  line_length: 98,
  import_deps: [],
  plugins: [Styler],
  export: [
    line_length: 98,
    locals_without_parens: []
  ]
]
