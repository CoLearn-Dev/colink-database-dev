import sys
import colink as CL
from colink import (
    CoLink,
    generate_user,
    get_time_stamp,
    prepare_import_user_signature,
)

if __name__ == "__main__":
	addr = sys.argv[1]
	jwt = sys.argv[2]

	# Import a new user to the colink server
	cl = CoLink(addr, jwt)
	pk, sk = generate_user()
	core_pub_key = cl.request_info().core_public_key
	expiration_timestamp = get_time_stamp() + 86400 * 31
	signature_timestamp, sig = prepare_import_user_signature(
	    pk, sk, core_pub_key, expiration_timestamp
	)
	user_jwt = cl.import_user(pk, signature_timestamp, expiration_timestamp, sig)
	print(f"# User imported \n\taddr: {addr}\n\tjwt: {user_jwt}\n\t id: {cl.get_user_id()}")