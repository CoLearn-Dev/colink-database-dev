import sys
import os
import logging
import json
import colink as CL
from colink import (
    InstantServer,
    InstantRegistry,
    byte_to_str,
)
from protocol_query import pop
from user_setup import initialize_provider, initialize_client


if __name__ == "__main__":
    logging.basicConfig(filename="run_with_instant_server.log", filemode="w", level=logging.INFO)
    dir = sys.argv[1]
    with open(os.path.join(dir, "config.json")) as f:
        config = json.load(f)

    ir = InstantRegistry()
    is_c = InstantServer()
    cl_c = is_c.get_colink().switch_to_generated_user()
    pop.run_attach(cl_c)
    is_ps, cl_ps = [], []
    for i in range(config["n_providers"]):
        is_ps.append(InstantServer())
        cl_ps.append(is_ps[i].get_colink().switch_to_generated_user())
        pop.run_attach(cl_ps[i])
    
    for i in range(config["n_providers"]):
        user_dir = os.path.join(dir, config["dirctories"]["providers"][i])
        initialize_provider(cl_ps[i], user_dir)
    user_dir = os.path.join(dir, config["dirctories"]["client"])
    query_path = initialize_client(cl_c, user_dir)

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