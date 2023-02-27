import sys
import logging
import colink as CL
from colink import (
    CoLink,
    byte_to_str,
    decode_jwt_without_validation
)
from user_setup import initialize_provider, initialize_client

if __name__ == "__main__":
    logging.basicConfig(filename="user_run_task.log", filemode="w", level=logging.INFO)
    addr_c = sys.argv[1]
    jwt_c = sys.argv[2]
    addr_p = sys.argv[3]
    jwt_p = sys.argv[4]
    cl_c = CoLink(addr_c, jwt_c)
    cl_p = CoLink(addr_p, jwt_p)

    initialize_provider(cl_p)
    query_path = initialize_client(cl_c)

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
    task_id = cl_c.run_task("query", query_path, participants, True)
    result = cl_c.read_or_wait("output")
    print(f"result: {byte_to_str(result)}")