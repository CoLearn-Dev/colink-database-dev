import sys
import logging
import colink as CL
from colink import CoLink, decode_jwt_without_validation

if __name__ == "__main__":
    logging.basicConfig(filename="user_run_task.log", filemode="a", level=logging.INFO)
    addr = sys.argv[1]
    jwt_c = sys.argv[2]
    jwt_p = sys.argv[3]
    user_id_c = decode_jwt_without_validation(jwt_c).user_id
    user_id_p = decode_jwt_without_validation(jwt_p).user_id
    participants = [
        CL.Participant(
            user_id=user_id_c,
            role="client",
        ),
        CL.Participant(
            user_id=user_id_p,
            role="provider",
        ),
    ]
    cl_c = CoLink(addr, jwt_c)
    cl_p = CoLink(addr, jwt_p)
    task_id_c = cl_c.run_task("query", b"", participants, True)
    logging.info(f"Task {task_id,} has been created for client.")
    task_id_p = cl_p.run_task("query", b"", participants, True)
    logging.info(f"Task {task_id,} has been created for provider.")

    print(cl_c.read_or_wait("results"))