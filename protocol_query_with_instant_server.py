import logging
from typing import List
import json
from sql_processing import Query
import colink as CL
from colink import (
    CoLink,
    byte_to_str,
    ProtocolOperator,
    InstantServer,
    InstantRegistry,
)

pop = ProtocolOperator(__name__)


@pop.handle("query:client")
def run_client(cl: CoLink, param: bytes, participants: List[CL.Participant]):
    def merge_results(results, query): # Note that both the input and the output results are strings
        if query.is_aggregate():
            total = 0
            for result in results:
                total += int(result)
            return str(total)
        if query.is_retrieve():
            merged_results = []
            for result in results:
                merged_results += json.loads(result)
            return json.dumps(merged_results)

    logging.info(f"query:client protocol operator! {cl.jwt}")

    # Initiate the query
    sql = byte_to_str(cl.read_entry(byte_to_str(param)))
    query = Query(sql)
    cl.set_variable("query", bytes(query.dumps(), 'utf-8'), participants[1:])
    
    # Receive query results
    results = []
    for participant in participants[1:]:
        result = byte_to_str(cl.get_variable("result", participant))
        if result != "Table not found.":
            results.append(result)
    cl.create_entry("output", merge_results(results, query))


@pop.handle("query:provider")
def run_provider(cl: CoLink, param: bytes, participants: List[CL.Participant]):
    def run_query(table, schema, query):
        schema_name, schema_type = schema
        if query.type == "Q_RETRIEVE":
            result = []
            for row in table:
                record = {}
                for i, value in enumerate(row):
                    record[schema_name[i]] = value
                if query.pred.check(record):
                    result.append([record[col] for col in query.concerned_column])
        elif query.type == "Q_AGGREGATE_SUM":
            result = 0
            for row in table:
                record = {}
                for i, value in enumerate(row):
                    record[schema_name[i]] = value
                if query.pred.check(record):
                    result += record[query.concerned_column]
        return json.dumps(result)

    logging.info(f"query:client protocol operator! {cl.jwt}")

    query_str = byte_to_str(cl.get_variable("query", participants[0]))
    query = Query.from_json(json.loads(query_str))
    table_keys = cl.read_keys(":".join([f"{cl.get_user_id()}:", "database", query.concerned_table, "data"]), False)
    if not table_keys:
        cl.set_variable("result", bytes("Table not found.", 'utf-8'), [participants[0]])
        return
    table = []
    for key in table_keys:
        table.append(json.loads(byte_to_str(cl.read_entry(key.key_path))))
    schema_type = cl.read_entry(":".join(["database", query.concerned_table, "schema", "type"]))
    schema_name = cl.read_entry(":".join(["database", query.concerned_table, "schema", "name"]))
    schema_type = json.loads(byte_to_str(schema_type))
    schema_name = json.loads(byte_to_str(schema_name))

    result = run_query(table, (schema_name, schema_type), query)
    cl.set_variable("result", bytes(result, 'utf-8'), [participants[0]])


if __name__ == "__main__":
    logging.basicConfig(filename="protocol_query_with_instant_server.log", filemode="w", level=logging.INFO)
    ir = InstantRegistry()
    is_c = InstantServer()
    is_p = InstantServer()
    cl_c = is_c.get_colink().switch_to_generated_user()
    cl_p = is_p.get_colink().switch_to_generated_user()
    
    # Client server setup
    logging.info("Client server setup!")
    # load configuration to the server
    f = open("./example/client/config.json")
    config = json.loads(f.read())
    providers_config = config["providers"]
    f.close()
    for provider_name, provider_config in providers_config.items():
        key_name = ":".join(["config", provider_name, "type"])
        cl_c.create_entry(key_name, provider_config["type"])
        tables_name = provider_config["tables"]
        key_name = ":".join(["provider", provider_name, "tables"])
        cl_c.create_entry(key_name, json.dumps(tables_name))
    # load query to the server
    f = open("./example/client/query.sql")
    query_path = cl_c.create_entry("query", f.readline())
    f.close()

    # Provider server setup
    logging.info("Provider server setup!")
	# load the database to the server
    f = open("./example/broker_a/db.json")
    provider_config = json.loads(f.read())
    f.close()
    for table_name, table_config in provider_config.items():
        key_name = ":".join(["database", table_name, "schema"])
        fields = table_config["schema"]["field"]
        names = json.dumps([n for n, t in fields])
        types = json.dumps([t for n, t in fields])
        cl_p.create_entry(":".join([key_name, "name"]), names)
        cl_p.create_entry(":".join([key_name, "type"]), types)

        key_name = ":".join(["database", table_name, "data"])
        records = table_config["data"]
        for i, record in enumerate(records):
            cl_p.create_entry(":".join([key_name, str(i)]), json.dumps(record))

    # Start protocol operator and run task
    logging.info("Start protocol operator and run task!")
    pop.run_attach(cl_c)
    pop.run_attach(cl_p)
    participants = [
        CL.Participant(user_id=cl_c.get_user_id(), role="client"),
        CL.Participant(user_id=cl_p.get_user_id(), role="provider"),
    ]
    task_id = cl_c.run_task("query", query_path, participants, False)
    result = cl_c.read_or_wait("output")
    print(f"result: {byte_to_str(result)}")