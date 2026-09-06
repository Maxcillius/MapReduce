# MapReduce

requirements:
1. Each worker process will, in a loop, ask the coordinator for a task
2. The coordinator should notice if a worker hasn't completed its task in a reasonable amount of time (for this lab, use ten seconds), and give the same task to a different worker.
3. The sock123 argument specifies a socket on which the coordinator receives RPCs from workers.