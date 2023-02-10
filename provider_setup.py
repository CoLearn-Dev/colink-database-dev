import logging
import sys
import json
import colink as CL
from colink import (
    CoLink,
    generate_user,
    get_time_stamp,
    prepare_import_user_signature,
)

if __name__ == "__main__":
    logging.basicConfig(filename="log/provider_setup.log", filemode="a", level=logging.INFO)

	# Generate a user jwt for the provider
	addr = sys.argv[1]
	_jwt = sys.argv[2]
	cl = CoLink(addr, _jwt)
	pk, sk = generate_user()
	core_pub_key = cl.request_info().core_public_key
	expiration_timestamp = get_time_stamp() + 86400 * 31
	signature_timestamp, sig = prepare_import_user_signature(
	    pk, sk, core_pub_key, expiration_timestamp
	)
	jwt = cl.import_user(pk, signature_timestamp, expiration_timestamp, sig)
	user_id = cl.get_user_id()
	logging.info(f"Provider user generated. \n\tjwt: {jwt}\n\t user id: {user_id}")

	# Load the database to the server
	f = open("../example/broker_a/db.json")
	provider_config = json.loads(f.read())
	f.close()
	for table_name, table_config in provider_config.items():
		key_name = ":".join(["database", table_name, "schema"])
		fields = table_config["schema"]
		names = json.dumps([n for n, t in fields])
		types = json.dumps([t for n, t in fields])
		cl.create_entry(":".join([key_name, "name"]), names)
		cl.create_entry(":".join([key_name, "type"]), types)

		key_name = ":".join(["table", table_name, "data"])
		records = table_config["data"]
		for i, record in enumerate(records):
			cl.create_entry(":".join([key_name, str(i)]), json.dumps(record))
	logging.info(f"Data loaded to server.")
