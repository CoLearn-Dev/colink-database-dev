import logging
import sys
import json
import colink as CL
from colink import (
    CoLink,
    generate_user,
    get_time_stamp,
    prepare_import_user_signature
)

if __name__ == "__main__":
    logging.basicConfig(filename="log/client_setup.log", filemode="a", level=logging.INFO)

    # Generate a user jwt for the client
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
    logging.info(f"Client user generated. \n\tjwt: {jwt}\n\t user id: {user_id}")

    # Load the configuration to the server
    f = open("../example/client/config.json")
    config = json.loads(f.read())
    providers_config = config["providers"]
    f.close()
    for provider_name, provider_config in providers_config.items():
        key_name = ":".join(["config", provider_name, "type"])
        cl.create_entry(key_name, provider_config["type"])
        tables_name = provider_config["tables"]
        key_name = ":".join(["provider", provider_name, "tables"])
        cl.create_entry(key_name, json.dumps(tables_name))
    logging.info(f"Configuration loaded to server.")

    # Load the query to the server
    f = open("../example/client/query.sql")
    cl.create_entry("query", f.readline())
    f.close()
    logging.info(f"Query loaded to server.")
