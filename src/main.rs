mod consts;
mod convert_dict;
mod convert_layer;
mod convert_triples;
mod dataconversion;
mod reachable;

use convert_dict::*;
use convert_layer::*;
use reachable::*;

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
        /// The layer id to convert
        id: String,
    },
    /// find reachable layers
    Reachable { store: String },
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
            id,
        } => {
            convert_layer(
                &from,
                &to,
                workdir
                    .as_ref()
                    .map(|w| w.as_str())
                    .unwrap_or("/tmp/terminusdb_10_to_11_workdir/"),
                &id,
            )
            .await
        }
        Commands::Reachable { store } => find_reachable_layers(&store).await,
    }
}
