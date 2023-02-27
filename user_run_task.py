import sys
import os
import logging
import json
import colink as CL
from colink import (
    CoLink,
    byte_to_str,
    decode_jwt_without_validation
)
from user_setup import initialize_provider, initialize_client

if __name__ == "__main__":
    logging.basicConfig(filename="user_run_task.log", filemode="w", level=logging.INFO)
    dir = sys.argv[1]
    with open(os.path.join(dir, "config.json")) as f:
        config = json.load(f)
    
    cl_c = CoLink(sys.argv[2], sys.argv[3])
    cl_ps = []
    for i in range(config["n_providers"]):
        cl_ps.append(CoLink(sys.argv[i*2+4], sys.argv[i*2+5]))
    
    for i in range(config["n_providers"]):
        initialize_provider(cl_ps[i], os.path.join(dir, config["dirctories"]["providers"][i]))
    query_path = initialize_client(cl_c, os.path.join(dir, config["dirctories"]["client"]))

    # Run task
    logging.info("Run task!")
    participants = [
        CL.Participant(user_id=cl_c.get_user_id(), role="client")
    ]
    for cl_p in cl_ps:
        participants.append(
            CL.Participant(user_id=cl_p.get_user_id(), role="provider")
        )
    task_id = cl_c.run_task("query", query_path, participants, True)
    result = cl_c.read_or_wait("output")
    print(f"result: {byte_to_str(result)}")