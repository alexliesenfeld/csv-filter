use std::time::Instant;
extern crate csv_filter;

use structopt::StructOpt;

/// Holds command line parameters provided by the user.
#[derive(StructOpt, Debug)]
struct CommandLineParameters {
    #[structopt(short, long)]
    input: String,
    #[structopt(short, long)]
    configuration: String,
    #[structopt(short, long, default_value = "output")]
    output: String,
    #[structopt(short = "ns", long = "no-sort")]
    no_sort: bool,
    #[structopt(short = "fp", long = "filter-parallelism", default_value = "1")]
    filter_parallelism: usize,
    #[structopt(short = "sp", long = "sort-parallelism", default_value = "1")]
    sort_parallelism: usize,
}

fn main() {
    let program_start = Instant::now();

    let params: CommandLineParameters = CommandLineParameters::from_args();

    csv_filter::process(
        &params.input,
        &params.configuration,
        &params.output,
        params.no_sort,
        params.filter_parallelism,
        params.sort_parallelism,
    );

    println!(
        "Finished in {} milliseconds",
        program_start.elapsed().as_millis()
    )
}
