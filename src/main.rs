mod consts;
mod convert_dict;
mod convert_layer;
mod convert_store;
mod convert_triples;
mod dataconversion;
mod reachable;

use convert_layer::*;
use convert_store::*;

use clap::*;
use std::io;

use thiserror::*;

#[derive(Parser)]
#[command(author, version, about)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
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
        /// Verbose reporting
        #[arg(short = 'v', long = "verbose")]
        verbose: bool,
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
        /// Keep going with other layers if a layer does not convert
        #[arg(short = 'c', long = "continue")]
        keep_going: bool,
        /// Verbose reporting
        #[arg(short = 'v', long = "verbose")]
        verbose: bool,
        /// Replace original directory with converted directory
        #[arg(short = 'r', long = "replace")]
        replace: bool,
        /// Cleanup work directory after successful run
        #[arg(short = 'k', long = "clean")]
        clean: bool,
    },
}

#[derive(Error, Debug)]
#[error(transparent)]
pub enum CliError {
    StoreConversion(#[from] StoreConversionError),
    LayerConversion(#[from] LayerConversionError),
    Io(#[from] io::Error),
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let result = inner_main().await;

    if let Err(e) = result {
        eprintln!("{}", e);
        std::process::exit(1);
    }
}

async fn inner_main() -> Result<(), CliError> {
    let cli = Cli::parse();

    match cli.command {
        Commands::ConvertLayer {
            from,
            to,
            workdir,
            naive,
            id,
            verbose,
        } => {
            convert_layer(
                &from,
                &to,
                workdir
                    .as_deref()
                    .unwrap_or("/tmp/terminusdb_10_to_11_workdir/"),
                naive,
                verbose,
                &id,
            )
            .await?;
        }
        Commands::ConvertStore {
            from,
            to,
            workdir,
            naive,
            keep_going,
            verbose,
            replace,
            mut clean,
        } => {
            if workdir.is_some() && clean {
                println!("Clean flag was specified, but ignored as we will not remove manually specified work directories");
                clean = false;
            };
            let default_workdir = format!("{to}/.workdir");
            convert_store(
                &from,
                &to,
                workdir.as_deref().unwrap_or(&default_workdir),
                naive,
                keep_going,
                verbose,
                replace,
                clean,
            )
            .await?;
        }
    }

    Ok(())
}
