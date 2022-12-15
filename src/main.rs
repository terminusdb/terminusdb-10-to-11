mod convert_dict;

use clap::*;
use convert_dict::*;
use std::io;

use tokio;

#[derive(Parser)]
#[command(author, version, about)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// convert an untyped dictionary to a string dictionary
    ConvertUntypedDictionary {
        /// The pfc file to convert from
        from: String,
        /// The offsets file to convert to
        to_offsets: String,
        /// The tfc file to convert to
        to_data: String,
    },
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> io::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::ConvertUntypedDictionary {
            from,
            to_offsets,
            to_data,
        } => convert_untyped_dictionary_to_files(&from, &to_offsets, &to_data).await,
    }
}
