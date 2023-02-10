# Colink Database Demo

A distributed database system demo on multiple sources built with CoLink.

## Description

+ Schema: ID | User Name | Deposit
+ Supported Query: RETRIEVE, SUM

## Requirements

+ Python 3.9
+ CoLink 0.2.3

## Usage

+ Prepare the query under `/example/client/query.sql`.

```sql
# examples
SELECT SUM(deposit) FROM t_deposit WHERE id < 18
SELECT id, deposit FROM t_deposit WHERE user_name = "Niki"
```

+ Setup the CoLink servers for the client and the data provider. See loggings for generated user JWTs.

```shell
python3 provider_setup.py <addr> <jwt> \
& python3 cilent_setup.py <addr> <jwt>
```

+ Create two new terminals and start protocol operator for the client and the data provider separately.

```shell
python3 protocol_query.py \
  --addr <addr> \
  --jwt <provider user jwt>
```

```shell
python3 protocol_query.py \
  --addr <addr> \
  --jwt <client user jwt>
```

+ Run task.

```shell
python3 user_run_task.py \
  <addr> \
  <client user jwt> \
  <provider user jwt>
```

