# iRODS CLI
Provides streaming to/from stdin/stdout.

## How to stream data into iRODS from stdin
```
$ echo 'Hello, iRODS!' | irods put - <logical_path>
```

## How to stream data from iRODS to stdout
```
$ irods get <logical_path> -
```
