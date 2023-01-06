mod consts;
mod convert_dict;
mod convert_layer;
mod convert_store;
mod convert_triples;
mod dataconversion;
mod reachable;

use convert_dict::*;
use convert_layer::*;
use convert_store::*;

use clap::*;
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
    /// convert a layer between a 10 store and an 11 store
    ConvertLayer {
        /// The storage dir from v10
        from: String,
        /// The storage dir for v11
        to: String,
        /// The workdir to store mappings in
        #[arg(short = 'w', long = "workdir")]
        workdir: Option<String>,
        /// Convert the layer assuming all values are strings
        #[arg(long = "naive")]
        naive: bool,
        /// The layer id to convert
        id: String,
    },
    /// convert a store from a 10 store and an 11 store
    ConvertStore {
        /// The storage dir from v10
        from: String,
        /// The storage dir for v11
        to: String,
        /// The workdir to store mappings in
        #[arg(short = 'w', long = "workdir")]
        workdir: Option<String>,
        /// Convert the store assuming all values are strings
        #[arg(long = "naive")]
        naive: bool,
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
        Commands::ConvertLayer {
            from,
            to,
            workdir,
            naive,
            id,
        } => {
            convert_layer(
                &from,
                &to,
                workdir
                    .as_ref()
                    .map(|w| w.as_str())
                    .unwrap_or("/tmp/terminusdb_10_to_11_workdir/"),
                naive,
                &id,
            )
            .await
        }
        Commands::ConvertStore {
            from,
            to,
            workdir,
            naive,
        } => {
            convert_store(
                &from,
                &to,
                workdir
                    .as_ref()
                    .map(|w| w.as_str())
                    .unwrap_or("/tmp/terminusdb_10_to_11_workdir/"),
                naive,
            )
            .await
        }
    }
}
