import sys
import logging
import json
import colink as CL
from colink import (
    CoLink,
    byte_to_str,
    decode_jwt_without_validation
)

if __name__ == "__main__":
    logging.basicConfig(filename="user_run_task.log", filemode="a", level=logging.INFO)
    addr_c = sys.argv[1]
    jwt_c = sys.argv[2]
    addr_p = sys.argv[3]
    jwt_p = sys.argv[4]

    # Client server setup
    logging.info("Client server setup!")
    # load configuration to the server
    cl_c = CoLink(addr_c, jwt_c)
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
    cl_p = CoLink(addr_p, jwt_p)
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

    # Run task
    logging.info("Run task!")
    participants = [
        CL.Participant(
            user_id=decode_jwt_without_validation(jwt_c).user_id,
            role="client",
        ),
        CL.Participant(
            user_id=decode_jwt_without_validation(jwt_p).user_id,
            role="provider",
        ),
    ]
    task_id = cl_c.run_task("query", query_path, participants, False)
    result = cl_c.read_or_wait("output")
    print(f"result: {byte_to_str(result)}")