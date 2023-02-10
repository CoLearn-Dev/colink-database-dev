import logging
from typing import List
import json
import sqlparse
import colink as CL
from colink import (
    CoLink, 
    byte_to_str,
    ProtocolOperator
)

pop = ProtocolOperator(__name__)

class Predicate:
    def __init__(self, tokens_seg=None):
        if tokens_seg == None:
            return
        self.concerned_column = tokens_seg[0]
        self.value = tokens_seg.replace('\"', '')
        self.type = tokens_seg[1]

    def check(self, record):
        if self.type == '<':
            return record[self.concerned_column] < self.value
        elif: self.type == '=':
            return record[self.concerned_column] == self.value
        elif: self.type == '>':
            return record[self.concerned_column] > self.value

    def dumps(self):
        dic = {
            "concerned_column": self.concerned_column,
            "value": self.value,
            "type": self.type
        }
        return json.dumps(dic)

    def from_json(self, pred_json):
        self.concerned_column = pred_json["concerned_column"]
        self.value = pred_json["value"]
        self.type = pred_json["type"]


class Query:
    def __init__(self, sql=None):
        if sql == None:
            return
        # Parse SQL query, supported query types SUM, RETRIEVE
        tokens = sql.strip().split(' ')
        tokens = [token for token in tokens if token != '']
        for i, token in enumerate(tokens):
            if token == "SELECT":
                select_l = i+1
            if token == "FROM":
                select_r, from_l = i, i+1
            if token == "WHERE":
                from_r, where_l = i, i+1
        tokens_seg = tokens[select_l:select_r]
        if '(' in tokens_seg:
            self.type == "Q_AGGREGATE_SUM"
            self.concerned_column = tokens_seg.split('(')[1].replace(')', '')
        else:
            self.type == "Q_RETRIEVE"
            self.concerned_column = []
            for token in tokens_seg:
                self.concerned_column.append(token.replace(',', ''))
        tokens_seg = tokens[from_l:from_r]
        self.concerned_table = tokens_seg[0]
        self.pred = Predicate(tokens[where_l:])

    def is_retrieve(self):
        return self.type.find("Q_RETRIEVE") != -1

    def is_aggregate(self):
        return self.type.find("Q_AGGREGATE") != -1

    def dumps(self):
        dic = {
            "type": self.type,
            "concerned_column": self.concerned_column,
            "concerned_table": self.concerned_table,
            "predicate": self.pred.dumps()
        }
        return json.dumps(dic)

    def from_json(self, query_json):
        self.type = query_json["type"]
        self.concerned_column = query_json["concerned_column"]
        self.concerned_table = query_json["concerned_table"]
        self.pred = Predicate().from_json(query_json["predicate"])


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


@pop.handle("query:client")
def run_client(cl: CoLink, param: bytes, participants: List[CL.Participant]):
    logging.info(f"query:client protocol operator! {cl.jwt}")

    # Initiate the query
    sql = byte_to_str(cl.read_entry("query"))
    query = Query(sql)
    cl.set_variable("query", query.dumps(), participants[1:])
    
    # Receive query results
    results = []
    for participant in participants[1:]:
        result = byte_to_str(cl.get_variable("output", participant))
        if result != "Table not found.":
            results.append(result)
    cl.create_entry(f"result", merge_results(results, query))


@pop.handle("query:provider")
def run_provider(cl: CoLink, param: bytes, participants: List[CL.Participant]):
    logging.info(f"query:client protocol operator! {cl.jwt}")

    query_str = byte_to_str(cl.get_variable("query", participants[0]))
    query = Query().from_json(json.loads(query_str))
    user_id = cl.get_user_id()
    content = cl.read_keys(":".join([f'{user_id}::database', query.concerned_table, "data"]), False)

    if not content:
        cl.set_variable("output", "Table not found.", participants[0])
        return

    schema_type = cl.read_keys(":".join([f'{user_id}::database', concerned_table, "schema", "type"]), False)
    schema_name = cl.read_keys(":".join([f'{user_id}::database', concerned_table, "schema", "name"]), False)
    schema_type = json.loads(byte_to_str(schema_type))
    schema_name = json.loads(byte_to_str(schema_name))
    if query.type == "Q_RETRIEVE":
        result = []
        for line in content:
            record = json.loads(byte_to_str(line))
            if query.pred.check(record):
                result.append([record[col][1] for col in query.concerned_column])
    elif query.type == "Q_AGGREGATE_SUM":
        result = 0
        for line in content:
            record = json.loads(byte_to_str(line))
            if query.pred.check(record):
                result += record[query.concerned_column][1]
    cl.set_variable("output", json.dumps(result), participants[0])


if __name__ == "__main__":
    ir = InstantRegistry()
    is_c = InstantServer()
    is_p = InstantServer()
    cl_c = is_c.get_colink().switch_to_generated_user()
    cl_p = is_p.get_colink().switch_to_generated_user()
    pop.run_attach(cl_c)
    pop.run_attach(cl_p)

    

    participants = [
        CL.Participant(user_id=cl0.get_user_id(), role="initiator"),
        CL.Participant(user_id=cl1.get_user_id(), role="receiver"),
    ]
    task_id = cl0.run_task("greetings", "test", participants, True)
    res = cl1.read_or_wait(f"tasks:{task_id}:output")
    print(f"result: {byte_to_str(res)}")