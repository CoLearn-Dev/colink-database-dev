import sys
import os
import logging
import json
from colink import (
    CoLink,
    generate_user,
    get_time_stamp,
    prepare_import_user_signature,
)


def user_generator(addr, jwt):
	# Import a new user to the colink server
	cl = CoLink(addr, jwt)
	pk, sk = generate_user()
	core_pub_key = cl.request_info().core_public_key
	expiration_timestamp = get_time_stamp() + 86400 * 31
	signature_timestamp, sig = prepare_import_user_signature(
	    pk, sk, core_pub_key, expiration_timestamp
	)
	user_jwt = cl.import_user(pk, signature_timestamp, expiration_timestamp, sig)
	return user_jwt
	

def initialize_client(cl, dir):
    logging.info("Client server setup!")

    # load configuration to the client server
    with open(os.path.join(dir, "config.json")) as f:
        config = json.load(f)
    providers = config["providers"]
    for provider_name, provider_info in providers.items():
        key_name = ":".join(["config", provider_name, "type"])
        cl.create_entry(key_name, provider_info["type"])
        tables_name = provider_info["tables"]
        key_name = ":".join(["provider", provider_name, "tables"])
        cl.create_entry(key_name, json.dumps(tables_name))
    # load the query to the client server
    with open(os.path.join(dir, "query.sql")) as f:
        query_path = cl.create_entry("query", f.readline())
    return query_path


def initialize_provider(cl, dir):
    logging.info("Provider server setup!")

	# load the database to the provider server
    with open(os.path.join(dir, "db.json")) as f:
        db = json.load(f)
    for table_name, table in db.items():
        key_name = ":".join(["database", table_name, "schema"])
        fields = table["schema"]["field"]
        names = json.dumps([n for n, t in fields])
        types = json.dumps([t for n, t in fields])
        cl.create_entry(":".join([key_name, "name"]), names)
        cl.create_entry(":".join([key_name, "type"]), types)

        key_name = ":".join(["database", table_name, "data"])
        records = table["data"]
        for i, record in enumerate(records):
            cl.create_entry(":".join([key_name, str(i)]), json.dumps(record))


if __name__ == "__main__":
	addr = sys.argv[1]
	jwt = sys.argv[2]
	user_jwt = user_generator(addr, jwt)
	print(f"# User generated \n\taddr: {addr}\n\tjwt: {user_jwt}\n\t")