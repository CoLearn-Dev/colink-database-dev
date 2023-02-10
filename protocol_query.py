import logging
from typing import List
import json
from sql_processing import Query
import colink as CL
from colink import (
    CoLink, 
    byte_to_str,
    ProtocolOperator
)

pop = ProtocolOperator(__name__)


@pop.handle("query:client")
def run_client(cl: CoLink, param: bytes, participants: List[CL.Participant]):
    def merge_results(results, query): # Note that both the input and the output results are strings
        if not results:
            return ""
        if query.is_aggregate() == '':
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
    sql = byte_to_str(cl.read_entry("query"))
    query = Query(sql)
    cl.set_variable("query", query.dumps(), participants[1:])
    
    # Receive query results
    results = []
    for participant in participants[1:]:
        result = byte_to_str(cl.get_variable("result", participant))
        if result != "Table not found.":
            results.append(result)
    cl.create_entry("results", merge_results(results, query))


@pop.handle("query:provider")
def run_provider(cl: CoLink, param: bytes, participants: List[CL.Participant]):
    def run_query(table, schema, query):
        schema_name, schema_type = schema
        if query.type == "Q_RETRIEVE":
            result = []
            for line in table:
                line = json.loads(byte_to_str(line))
                record = {}
                for i, value in enumerate(line):
                    record[schema_name[i]] = value
                if query.pred.check(record):
                    result.append([record[col] for col in query.concerned_column])
        elif query.type == "Q_AGGREGATE_SUM":
            result = 0
            for line in table:
                line = json.loads(byte_to_str(line))
                record = {}
                for i, value in enumerate(line):
                    record[schema_name[i]] = value
                if query.pred.check(record):
                    result += record[query.concerned_column]
        return json.dumps(result)

    logging.info(f"query:client protocol operator! {cl.jwt}")

    query_str = byte_to_str(cl.get_variable("query", participants[0]))
    query = Query().from_json(json.loads(query_str))
    table = cl.read_keys(":".join(['database', query.concerned_table, "data"]), False)
    if not table:
        cl.set_variable("result", "Table not found.", participants[0])
        return
    schema_type = cl.read_keys(":".join(['database', concerned_table, "schema", "type"]), False)
    schema_name = cl.read_keys(":".join(['database', concerned_table, "schema", "name"]), False)
    schema_type = json.loads(byte_to_str(schema_type))
    schema_name = json.loads(byte_to_str(schema_name))

    cl.set_variable("result", run_query(table, (schema_name, schema_type), query), participants[0])


if __name__ == "__main__":
    logging.basicConfig(filename="protocol_query.log", filemode="a", level=logging.INFO)
    pop.run()

