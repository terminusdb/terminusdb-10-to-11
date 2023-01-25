# TerminusDB 10 to 11 conversion tool

This tool allows you to convert a storage directory from TerminusDB 10
to TerminusDB 11. TerminusDB 11 has an improved storage format which
will use less storage space and increase the speed of retrieval by
around 25%.

## Installation

To install this conversion tool, download the binary from the releases
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

A simple invocation that should allow an in place conversion with a
backup is as follows:

```
$ terminusdb10-to-11 convert-store path_to_my_terminusdb -krv
```

After performing this conversion you will need to invoke TerminusDB11
in order to start as the new storage format is incompatible with
TerminusDB10.

