# memorylanedb

## Apologies. Too lazy for a proper readme

It works though.

## Setup
Build the cmdline binary by running the command below. This puts the binary in a `bin` folder in the package root
```
make build
```

Usage info is gotten by running
```
./bin/mlctl help
```

```
mlctl is a tool for inspecting memorylane databases.

Usage:

        mlctl command [arguments]

The commands are:

        put         insert a key-value pair in the db
        get         retrieve value of a key
        list        list all keys in the db
        info        print basic info
        help        print this screen
        stats       generate usage stats

Use "mlctl [command] -h" for more information about a command.
```



