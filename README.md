# Overview

`pgchangestream` is a library used to stream changes off of a postgres database using [logical decoding](https://www.postgresql.org/docs/9.4/logicaldecoding.html). The basic API is that you provide a connection string and replication slot name, and you get change messages sent along a channel. This can be useful for any sort of data streaming -- we use it to respond to changes, load data into a data warehouse, and more.

We highly reccomend you familiarize yourself with the ideas around logical decoding before using this library to avoid sadness. For example, if you create a replicaiton slot and don't consume from it, your database will slowly use more and more disk space.

In order to use this library, you first need to ensure that an appropriate logical decoding plugin is installed on your postgres instance. Postgres comes with `test_decoding`, although [`wal2json`](https://github.com/eulerto/wal2json) is easier to use.

Once that is done, you can create a replication slot using
```
select pg_create_logical_replication_slot('my_slot', 'wal2json');
```
`my_slot` is the name you will pass in as `slotName` when creating a reader.

# Testing
The test cases require postgres to be installed and on your `PATH`.  They can be run using `go test`.
