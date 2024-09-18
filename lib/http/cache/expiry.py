import calendar
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, Optional

@dataclass
class Never:
    def __str__(self):
        return 'never'

    def has_expired(self, saved, now):
        return False

@dataclass
class Delta:
    unit: str
    amount: int

    @classmethod
    def parse(cls, s):
        slice_idx = s.find(':')
        return cls(s[:slice_idx], int(s[slice_idx+1:]))

    def has_expired(self, saved, now):
        delta = timedelta(**{ self.unit: self.amount })
        return saved + delta < now

    def __str__(self):
        return f'delta:{self.unit}:{self.amount}'

@dataclass
class TillNextDayOfWeek:
    day_of_week: int

    @classmethod
    def parse(cls, s):
        dow = list(calendar.day_name).index(s)
        if 0 > dow < 6:
            raise ValueError(f'invalid day of the week, {s}')
        return cls(dow)

    def __str__(self):
        dow = calendar.day_name[self.day_of_week]
        return f'till_next_day_of_week:{dow}'

    def has_expired(self, saved, now):
        saved_weekday = saved.weekday()

        days_ahead = (self.day_of_week - saved_weekday) % 7
        days_ahead = 7 if days_ahead == 0 else days_ahead
        return saved + timedelta(days=days_ahead) < now

class CacheExpire:
    @classmethod
    def parse_expire(cls, expire_str: str | None):
        if not expire_str:
            return None
        if expire_str == 'never':
            return Never()

        slice_idx = expire_str.find(':')
        if slice_idx < 0:
            raise ValueError('Invalid Expire string')

        kind = expire_str[:slice_idx]
        if kind == 'delta':
            return Delta.parse(expire_str[slice_idx+1:])
        elif kind == 'till_next_day_of_week':
            return TillNextDayOfWeek.parse(expire_str[slice_idx+1:])
        else:
            raise ValueError(f'Invalid Expire string, {expire_str}')
