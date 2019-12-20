import os
import pickle
import sys
import time


def watch(server_no):
    with open(f'server-{server_no}.state', 'rb') as file:
        state = pickle.load(file)

    os.system('clear')
    print(f"---- Server: {server_no} ----")
    print(f"state: {state['state']}")
    print(f"term: {state['_term']}")
    print(f"voted for: {state['voted_for']}")

    print(f"---- Log ----")
    for entry in state['log']._log[-10:]:
        print(entry)


if __name__ == "__main__":
    while True:
        watch(int(sys.argv[1]))
        time.sleep(0.5)
