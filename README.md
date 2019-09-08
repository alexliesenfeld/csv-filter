# csv-filter

This is a command line utility and a library written in Rust that applies filtering operations on CSV files. 
It is able to reduce the contents of an input CSV file based on one or many filtering criteria. You can use this tool to split data from one large CSV file into multiple smaller ones which 
* contain only a subset of the columns from the original file, and/or
* contain only a subset of values that may appear in a column, and/or
* are sorted according to one or many columns.
     

## Configuration

This tool expects you to supply a JSON configuration file containing the filtering criteria to apply to  input CSV files. 
The configuration follows the form:

```
[
  {
    "filters": [
      {
        "column": "my-column-1", // Name of the column from the input file
        "include": true,         // Whether to include the column in the output file
        "values": [              // List all values that are allowed to appear in the output file.
          "value1",
          "value2",
          "value3"
        ]
      },
      {
        "column": "my-column-2", // Name of the column from the input file   
        "include": true,         // Whether to include the column in the output file
        "max": "2010-01-01",     // Minimum value to appear in the output file (alphanumeric sorting)      
        "min": "2015-01-01"      // Maximum value to appear in the output file (alphanumeric sorting)      
      }
    ],
    "output": "output_file_1.csv", // Name of the outout file where filtering results are being written to
    "sort_columns": [              // Columns to sort the whole output file by  
      "my-column-1",
      "my-column-2"
    ]
  },
  ...
]
```
## How to use
Just clone this repo and build it using cargo (`cargo build --release`). Please find the executable `csv-filter` in the `target/release` directory. You will need Rust and Cargo installed on your machine to build this tool.

## CLI Parameters

* `configuration`: Path to the configuration file (mandatory)
* `input`: Path to the input CSV file that will be filtered (mandatory)
* `no-sort`: disables sorting functionality (see `sort_columns` in the configuration above)
* `filter-parallelism`: The number of threads to use for filtering data
* `sort-parallelism`: The number of threads to use for sorting output files. This ultimately sets how many files are being sorted at once (memory consumption my be high if output files are large)

Parameters:
`csv-filter --configuration <config-file> --input <input file> [--no-sort] [--filter-parallelism <number of threads>] [--sort-parallelism <number of threads>]`

Example (executable):
`csv-filter --configuration my_filter_configuration.json --input my_input_file.csv --no-sort --filter-parallelism 8 --sort-parallelism 4`

Example (from cargo):
`cargo run --release -- --configuration my_filter_configuration.json --input my_input_file.csv --no-sort --filter-parallelism 8 --sort-parallelism 4`

## Use as a library
You can use `csv-filter` as a library by adding the following dependency to your `Cargo.toml`: 

```toml
[dependencies]
csv-filter = "0.1"
```

## Disclaimer
This tool is a WIP and hence may not perform optimally in certain cases. At the moment the progress is as follows:
 
- [x] Implement efficient filtering using multi-threading
- [x] Implement Basic sorting of output files
- [x] Employ multi-threading for sorting
- [ ] Implement ASC/DESC for sort columns (currently implicitly ASC)
- [ ] Implement out of memory sorting of large output files

## License
`csv-filter` is free software: you can redistribute it and/or modify it under the terms of the MIT Public License.

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the MIT Public License for more details.