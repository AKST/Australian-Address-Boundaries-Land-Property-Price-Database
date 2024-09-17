import time
import datetime

class NotebookTimer:
    def __init__(self, message, state):
        self._message = message
        self._start = time.time()
        self._state = state

    def add(self, state):
        for k, v in state.items():
            self._state[k] += v

    def log(self):
        time_of_day = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        t = int(time.time() - self._start)
        th, tm, ts = t // 3600, t // 60 % 60, t % 60
        msg = self._message % self._state
        print(f'{time_of_day} {msg} @ {th}h {tm}m {ts}s')