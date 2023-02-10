import json
import sqlparse

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
        elif self.type == '=':
            return record[self.concerned_column] == self.value
        elif self.type == '>':
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


if __name__ == "__main__":
    pass
