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
    logging.basicConfig(filename="user_info.log", filemode="a", level=logging.INFO)
    addr = sys.argv[1]
	jwt = sys.argv[2]

	# Generate a user jwt for the provider
	cl = CoLink(addr, jwt)
	pk, sk = generate_user()
	core_pub_key = cl.request_info().core_public_key
	expiration_timestamp = get_time_stamp() + 86400 * 31
	signature_timestamp, sig = prepare_import_user_signature(
	    pk, sk, core_pub_key, expiration_timestamp
	)
	user_jwt = cl.import_user(pk, signature_timestamp, expiration_timestamp, sig)
	user_id = cl.get_user_id()
	logging.info(f"# Provider user generated \n\tjwt: {user_jwt}\n\t id: {user_id}")

    # Generate a user jwt for the client
	cl = CoLink(addr, jwt)
	pk, sk = generate_user()
	core_pub_key = cl.request_info().core_public_key
	expiration_timestamp = get_time_stamp() + 86400 * 31
	signature_timestamp, sig = prepare_import_user_signature(
	    pk, sk, core_pub_key, expiration_timestamp
	)
	user_jwt = cl.import_user(pk, signature_timestamp, expiration_timestamp, sig)
	user_id = cl.get_user_id()
	logging.info(f"# Client user generated \n\tjwt: {user_jwt}\n\t id: {user_id}")