import json
import sqlparse

class Predicate:
    def __init__(self, tokens=None):
        if tokens == None:
            return
        self.concerned_column = tokens[0]
        self.value = tokens[2]
        if self.value[0] != '\"':
            if self.value == "TRUE":
                self.value = True
            elif self.value == "FALSE":
                self.value == False
            else:
                self.value = int(self.value)
        else:
            self.value = self.value.replace('\"', '') 
        self.type = tokens[1]

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

    @staticmethod
    def from_json(pred_json):
        pred = Predicate()
        pred.concerned_column = pred_json["concerned_column"]
        pred.value = pred_json["value"]
        pred.type = pred_json["type"]
        return pred


class Query:
    def __init__(self, sql=None):
        if sql == None:
            return
        # Parse SQL query, supported query types SUM, RETRIEVE
        sql = sql.replace('\n', ' ').strip()
        tokens, i, pre = [], 0, 0
        while i < len(sql):
            if sql[i] == '\"':
                i += 1
                while sql[i] != '\"':
                    i += 1
            elif sql[i] == ' ':
                tokens.append(sql[pre:i])
                pre = i+1
            i += 1
        tokens.append(sql[pre:])
        tokens = [token for token in tokens if token != '']
        for i, token in enumerate(tokens):
            if token == "SELECT":
                select_l = i+1
            if token == "FROM":
                select_r, from_l = i, i+1
            if token == "WHERE":
                from_r, where_l = i, i+1
        tokens_select = tokens[select_l:select_r]
        if ~''.join(tokens_select).find('('):
            aggregate_type = tokens_select[0].split('(')[0]
            if aggregate_type == "EXIST":
                self.type = "Q_AGGREGATE_EXIST"
            elif aggregate_type == "COUNT":
                self.type = "Q_AGGREGATE_CNT"
            elif aggregate_type == "SUM":
                self.type = "Q_AGGREGATE_SUM"
            elif aggregate_type == "AVG":
                self.type = "Q_AGGREGATE_AVG"
            elif aggregate_type == "COUNT_UNIQUE":
                self.type = "Q_AGGREGATE_CNT_UNQ"
            self.concerned_column = tokens_select[0].split('(')[1].replace(')', '')
        else:
            self.type = "Q_RETRIEVE"
            self.concerned_column = []
            for token in tokens_select:
                self.concerned_column.append(token.replace(',', ''))
        self.concerned_table = tokens[from_l:from_r][0]
        self.pred = Predicate(tokens[where_l:])

    def is_retrieve(self):
        return self.type.find("Q_RETRIEVE") != -1

    def is_aggregate(self):
        return self.type.find("Q_AGGREGATE") != -1
    
    def is_aggregate_bool(self):
        return self.type == "Q_AGGREGATE_EXIST"

    def dumps(self):
        dic = {
            "type": self.type,
            "concerned_column": self.concerned_column,
            "concerned_table": self.concerned_table,
            "predicate": self.pred.dumps()
        }
        return json.dumps(dic)

    @staticmethod
    def from_json(query_json):
        query = Query()
        query.type = query_json["type"]
        query.concerned_column = query_json["concerned_column"]
        query.concerned_table = query_json["concerned_table"]
        query.pred = Predicate.from_json(json.loads(query_json["predicate"]))
        return query


if __name__ == "__main__":
    # test
    sql = "SELECT SUM(deposit) FROM t_deposit WHERE user_name = \"Robert\""
    query_str = Query(sql).dumps()
    print(query_str)
    query = Query.from_json(json.loads(query_str))
    print(query.dumps())
