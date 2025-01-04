from typing import Literal

def time_elapsed(s_time: float | int,
                e_time: float | int,
                format: Literal['s', 'ms', 'hms']) -> str:
    t = int(e_time - s_time)
    match format:
        case 's':
            return f'{t}s'
        case 'ms':
            tm, ts = t // 60, t % 60
            return f'{tm}m {ts}s'
        case 'hms':
            th, tm, ts = t // 3600, t // 60 % 60, t % 60
            return f'{th}hr {tm}m {ts}s'

