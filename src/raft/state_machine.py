from enum import Enum


class State(Enum):
    LEADER = 1
    CANDIDATE = 2
    FOLLOWER = 3
