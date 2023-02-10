//
// Created by Songwen Zhao on 11/02/22.
//

#include <cstdio>
#include <cassert>
#include <cstring>
#include <random>
#include <iostream>
#include <fstream>
#include <sstream>
#include <algorithm>
#include <cmath>
#include <queue>
#include <map>
#include <unordered_map>
#include <set>
#include <unordered_set>

using namespace std;

string outInt(int x){
	stringstream ss;
	ss << x;
	return ss.str();
}

string outFloat(double x){
	stringstream ss;
	ss << x;
	return ss.str();
}

string outString(string x){
	return "\""+x+"\"";
}

string outBool(bool x){
	return x?"true":"false";
}

string head = ""
"{\n"
"    \"t_deposit\": {\n"
"        \"schema\": {\n"
"            \"field\": [\n"
"                [\"id\", \"int\"],\n"
"                [\"user_name\", \"str\"],\n"
"                [\"deposit\", \"float\"]\n"
"            ],\n"
"            \"key\": {\n"
"                \"id\": \"primary\"\n"
"            }\n"
"        },\n"
"        \"data\": [\n";

string tail = ""
"        ]\n"
"    }\n"
"}\n";

string user_name[6] = {
	"Robert",
	"Jennifer",
	"James",
	"Michael",
	"David",
	"Linda"
};

int main(int argc, char* argv[]) {
	int n = 50; // n: the number of records
	random_device rd;
	default_random_engine r{rd()};
	uniform_int_distribution<int> deposit(10, 10000);

	string data="";
	for (int i=0; i<n; ++i){
		string record = "[" + outInt(i);
		record += ", "+outString(user_name[r()%6]);
		record += ", "+outInt(deposit(r));
		record += "]";
		if (i<n-1) data += "\t\t\t"+record+",\n";
		else data += "\t\t\t"+record+"\n";
	}
	ofstream fout("broker_a/db.json");
	fout << head << data << tail;
	fout.close();
	cout << "FINISHED: 1 table with " << n << " records created.";
	return 0;
}