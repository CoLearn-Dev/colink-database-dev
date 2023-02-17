# Colink Database Demo

A distributed database system demo on multiple sources built with CoLink.

## Description

+ Schema: ID | User Name | Deposit
+ Supported Query: RETRIEVE, SUM

## Requirements

+ Python 3.9
+ CoLink 0.2.3

## Usage

+ Prepare your query under `/example/client/query.sql`.

```sql
# examples
SELECT SUM(deposit) FROM t_deposit WHERE id < 18
SELECT id, deposit FROM t_deposit WHERE user_name = "Niki"
```
For instant server simulation, directly run `protocol_query_with_instant_server.py`. Otherwise follow the steps below.

+ If needed, run `user_jwt_generator.py` to generate and import users for the client and the data providers. 

```shell
python user_jwt_generator.py <addr> <jwt>
```

+ Create two new terminals and start protocol operator for the client and the data provider separately.

```shell
python protocol_query.py \
  --addr <addr> \
  --jwt <user jwt>
```

+ Run task.

```shell
python user_run_task.py \
  <client server addr> \
  <client user jwt> \
  <provider server addr> \
  <provider user jwt>
```

