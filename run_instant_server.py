import logging
import colink as CL
from colink import (
    InstantServer,
    InstantRegistry,
    byte_to_str,
)
from protocol_query import pop
from user_setup import initialize_provider, initialize_client


if __name__ == "__main__":
    logging.basicConfig(filename="user_run_task_with_instant_server.log", filemode="w", level=logging.INFO)
    ir = InstantRegistry()
    is_c = InstantServer()
    is_p = InstantServer()
    cl_c = is_c.get_colink().switch_to_generated_user()
    cl_p = is_p.get_colink().switch_to_generated_user()
    pop.run_attach(cl_c)
    pop.run_attach(cl_p)

    initialize_provider(cl_p)
    query_path = initialize_client(cl_c)

    # Run task
    logging.info("Run task!")
    participants = [
        CL.Participant(user_id=cl_c.get_user_id(), role="client"),
        CL.Participant(user_id=cl_p.get_user_id(), role="provider"),
    ]
    task_id = cl_c.run_task("query", query_path, participants, True)
    result = cl_c.read_or_wait("output")
    print(f"result: {byte_to_str(result)}")