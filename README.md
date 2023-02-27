# Colink Database Dev

A distributed database system on multiple sources built with CoLink. 

Examples include a demo database on multiple data providers and a JPMC mock database for transaction record retrieval and aggregation. See `examples/demo/` and `examples/jpmc_case/`.

## Requirements

+ Python 3.9
+ CoLink 0.2.4

## Usage

Default query examples are prepared your under `examples/.../client/query.sql`. If needed, directly run `user_setup.py` to gernerate user JWTs for new users. 

```shell
python user_setup.py <addr> <jwt>
```

### Run with Instant Server
```shell
python run_with_instant_server.py <example dir>
```

### Run on Real Server

+ Create new terminals and start protocol operator for the client and the data providers separately.

```shell
python protocol_query.py \
  --addr <addr> \
  --jwt <user jwt>
```

+ Run task.

```shell
python user_run_task.py \
  <example dir> \
  <client server addr> \
  <client user jwt> \
  <provider server addr> \
  <provider user jwt> \
  ...
```