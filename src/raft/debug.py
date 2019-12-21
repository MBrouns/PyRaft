r0 = RaftController(
        0,
        RaftServer(
            server_no=0, num_servers=5, persist=True, state_machine=KVStateMachine(0)
        ),
        SockBackend(0, config.SERVERS),
    )
    r1 = RaftController(
        1,
        RaftServer(
            server_no=1, num_servers=5, persist=True, state_machine=KVStateMachine(1)
        ),
        SockBackend(1, config.SERVERS),
    )
    r2 = RaftController(
        2,
        RaftServer(
            server_no=2, num_servers=5, persist=True, state_machine=KVStateMachine(2)
        ),
        SockBackend(2, config.SERVERS),
    )
    r3 = RaftController(
        3,
        RaftServer(
            server_no=3, num_servers=5, persist=True, state_machine=KVStateMachine(3)
        ),
        SockBackend(3, config.SERVERS),
    )
    r4 = RaftController(
        4,
        RaftServer(
            server_no=4, num_servers=5, persist=True, state_machine=KVStateMachine(4)
        ),
        SockBackend(4, config.SERVERS),
    )

    def start():
        r0.start()
        r1.start()
        r2.start()
        r3.start()
        r4.start()

    def add_entry(controller, op):
        controller._machine.log.append(
            len(controller._machine.log),
            controller._machine.log.last_term,
            LogEntry(controller._machine.term, op),
        )

    def logs_same(log1, log2):
        if len(log1) != len(log2):
            raise ValueError(f"logs different length: {len(log1)} - {len(log2)}")
        for i in range(len(log2)):
            if log1[i] != log2[i]:
                raise ValueError(f"logs different in ind ex {controller.hb_since_election_timeout}/ ")
        return True

    def all_logs_same():
        assert logs_same(r0._machine.log, r1._machine.log)
        assert logs_same(r0._machine.log, r2._machine.log)
        assert logs_same(r0._machine.log, r3._machine.log)
        assert logs_same(r0._machine.log, r4._machine.log)

    start()
    time.sleep(1)
    r0._machine.become_candidate()

    client = DistDict(config.SERVERS)
