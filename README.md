# TerminusDB 10 to 11 conversion tool

This tool allows you to convert a storage directory from TerminusDB 10
to TerminusDB 11. TerminusDB 11 has an improved storage format which
will use less storage space and increase the speed of retrieval by
around 25%.

## Installation

To install this conversion tool, download the binary from the [release page](https://github.com/terminusdb/terminusdb-10-to-11/releases/tag/v1.0.0). Alternatively, if you have cargo install, you can use `cargo install terminusdb-10-to-11` to build the binary locally and install it into your cargo bin folder.
at ...

## Invocation

```
Usage: terminusdb-10-to-11 convert-store [OPTIONS] <FROM> <TO>

Arguments:
  <FROM>  The storage dir from v10
  <TO>    The storage dir for v11

Options:
  -w, --workdir <WORKDIR>  The workdir to store mappings in
      --naive              Convert the store assuming all values are strings
  -c, --continue           Keep going with other layers if a layer does not convert
  -v, --verbose            Verbose reporting
  -r, --replace            Replace original directory with converted directory
  -k, --clean              Cleanup work directory after successful run
  -h, --help               Print help information
```

## Basic use
### For TerminusDB users
TerminusDB by default stores the database under `storage/db` in your terminusdb install path. For example, if your terminusdb is installed under `/home/joan/terminusdb`, the database directory will be at `/home/joan/terminusdb/storage/db`.

A simple invocation that should allow an in place conversion with a
backup is as follows:

```
$ terminusdb10-to-11 convert-store -krv <path to old store> <path to temporary new store dir>
```

For the example where your terminusdb is installed under `/home/joan/terminusdb` (and therefore your db is at `/home/joan/terminusdb/storage/db`), this command would be as follows:
```
$ terminusdb10-to-11 convert-store -krv /home/joan/terminusdb/storage/db /home/joan/terminusdb/storage/converted_db
```


After performing this conversion, you can no longer use the store with the old TerminusDB 10. Your old store will however have been copied to a backup folder, which the command output tells you the location of. Should you need to downgrade, you can simply move this backup folder back to the original location.

### For other store users
By default, this conversion tool assumes it is running against a TerminusDB store. This means that it expects all values in the various layers to be annotated with TerminusDB types. If you are using terminus-store in another project, this will probably not be the case for you, causing the conversion to fail.

If this is the case, you should use the `--naive` flag. This will convert the store assuming that all values are strings. Do NOT use this flag on stores used by TerminusDB though, as this will render the destination store unusable.

## What the tool does
This tool performs the following steps.

1. Find out which layers are reachable by opening the label files, and gathering any referred layers as well as their parents.
2. Convert each layer, storing the converted layer in the destination directory. If the layer was already converted (for example in a previous aborted run), the layer is skipped.
3. Copy over all labels to the destination directory.

Various flags modify this basic behavior.

### Replacing the original store after a successful run
By default, the tool will not modify the original store directory, but only build a new store in the destination directory. If you wish to automatically replace the store after a successful run, you can use `-r` or `--replace`. This will move the original store to a backup location, and then move the destination directory to the original location.

### Cleanup temporary files after a successful run
By default, the tool will not clean up temporary files used during conversion. Using `-k` or `--clean`, the workdir will be automatically removed after a successful run.

Note that this flag will be ignored if an explicit workdir was specified with `-w` or `--workdir`. This is done to prevent accidental removal of files that have nothing to do with the conversion, such as when an already existing directory was specified as a workdir.

### Verbose output
Using `-v` or `--verbose`, the tool can be made to print out the steps it goes through while converting a layer. This can be useful to get more accurate status information.

### Using a non-default workdir
By default, the conversion tool will use a subdirectory of the destination store directory to store temporary work files. using `-w` or `--workdir`, you can set up a different directory for this.

### Continue on failure
The tool will exit as soon as it encounters an error, such as a value that cannot be converted, or a layer that misses some files. Using `-c` or `--continue`, the tool can be forced to continue converting other layers. Failures will still be reported and logged, and the final exit code of the tool will indicate failure, but every reachable layer that is convertible will be converted.

Note that failing to convert a layer will also automatically fail to convert any of its child layers.

### Naive conversion
using `--naive`, the tool can be forced to ignore any type annotations in the layers, instead converting everything as strings. This is useful when converting stores that weren't created by TerminusDB.
