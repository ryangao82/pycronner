# -*- coding: utf-8 -*-
import logging
import time
import datetime
import threading
import uuid
from collections.abc import Callable
from typing import Union, Sequence, Any, Tuple, List
import types


logger = logging.getLogger('pycronner')


class JobStatus(object):
    def __init__(self):
        self._is_running = False
        self.last_run_time = None

    @property
    def is_running(self):
        return self._is_running

    def start(self):
        self._is_running = True
        self.last_run_time = datetime.datetime.now()

    def stop(self):
        self._is_running = False


class JobSchedule(object):
    def __init__(self):
        self._name = None
        self._second = set()
        self._minute = set()
        self._hour = set()
        self._weekday = set()
        self._day = set()
        self._tag: Any = None

    @property
    def name(self) -> str:
        return self._name

    @property
    def at_seconds(self):
        return self._second

    @property
    def at_minutes(self):
        return self._minute

    @property
    def at_hours(self):
        return self._hour

    @property
    def at_weekdays(self):
        return self._weekday

    @property
    def at_days(self):
        return self._day

    @property
    def tag(self) -> Any:
        return self._tag

    @tag.setter
    def tag(self, tag_value: Any):
        self._tag = tag_value


# noinspection PyUnresolvedReferences
class SettableJobScheduler(JobSchedule):
    def __init__(self):
        JobSchedule.__init__(self)
        self.is_daemon = False
        self._is_sealed = False
        self._interval = 0
        self._interval_unit = ''
        self._is_stop = False
        self._stop_until_time: datetime.datetime = datetime.datetime.min

    def set_name(self, value: str):
        self._check_sealed()
        self._name = value

    def add_minute(self, minute):
        self._minute.add(minute)

    def add_second(self, second):
        self._second.add(second)

    def add_hour(self, hour):
        self._hour.add(hour)

    def add_weekday(self, weekday):
        self._weekday.add(weekday)

    def add_day(self, day):
        self._day.add(day)

    def stop(self):
        self._is_stop = True

    def stop_until(self, until_time: datetime.datetime):
        self._stop_until_time = until_time

    def seal(self):
        self._second = frozenset(self._second)
        self._minute = frozenset(self._minute)
        self._hour = frozenset(self._hour)
        self._weekday = frozenset(self._weekday)
        self._day = frozenset(self._day)
        self._is_sealed = True

    def is_scheduled(self):
        return self._second or self._minute or self._hour \
               or self._weekday or self._day or self._interval

    def _check_sealed(self):
        if self._is_sealed:
            raise RuntimeError('Job Schedule is sealed')

    def set_interval(self, interval: int, unit: str):
        self._check_sealed()
        if self._interval_unit != '' and self._interval_unit != unit:
            raise ValueError('interval can only be set once')

        self._interval = interval
        self._interval_unit = unit

    def should_run(self, last_run_time: datetime.datetime):
        if self._is_stop:
            return False

        time_now = datetime.datetime.now()
        if self._stop_until_time >= time_now:
            return False

        for one_check in [(self.at_seconds, time_now.second),
                          (self.at_minutes, time_now.minute),
                          (self.at_hours, time_now.hour),
                          (self.at_days, time_now.day),
                          (self.at_weekdays, time_now.weekday())]:
            if one_check[0] and one_check[1] not in one_check[0]:
                return False

        if last_run_time:
            if self._interval > 0:
                intervals = {
                    'minute': datetime.timedelta(minutes=self._interval),
                    'second': datetime.timedelta(seconds=self._interval),
                    'hour': datetime.timedelta(hours=self._interval),
                    'day': datetime.timedelta(days=self._interval),
                }
                next_time_to_run = last_run_time + intervals[self._interval_unit]
                if next_time_to_run > time_now:
                    return False

        return True


class JobEvery(object):
    def __init__(self, interval: int, job: 'Job', scheduler: SettableJobScheduler):
        self._interval = interval
        self._job = job
        self._scheduler = scheduler

    def minute(self):
        self._scheduler.set_interval(self._interval, 'minute')
        return self._job

    def second(self):
        self._scheduler.set_interval(self._interval, 'second')
        return self._job

    def hour(self):
        self._scheduler.set_interval(self._interval, 'hour')
        return self._job

    def day(self):
        self._scheduler.set_interval(self._interval, 'day')
        return self._job


class Job(object):
    def __init__(self, scheduler: SettableJobScheduler):
        self._job_schedule = scheduler
        self._interval = None

    def every(self, interval: int):
        return JobEvery(interval, self, self._job_schedule)

    def at(self,
           weekday: Union[int, Sequence[int], None] = None,
           day: Union[int, Sequence[int], None] = None,
           hour: Union[int, Sequence[int], None] = None,
           minute: Union[int, Sequence[int], None] = None,
           second: Union[int, Sequence[int], None] = None):
        if weekday is None and day is None and hour is None and minute is None and second is None:
            raise ValueError("You must specify at least one argument")

        self._add_at(weekday, self._job_schedule.add_weekday)
        self._add_at(day, self._job_schedule.add_day)
        self._add_at(hour, self._job_schedule.add_hour)
        self._add_at(minute, self._job_schedule.add_minute)
        self._add_at(second, self._job_schedule.add_second)
        return self

    def name(self, job_name: str):
        self._job_schedule.set_name(job_name)
        return self

    def tag(self, tag_value: Any):
        self._job_schedule.tag = tag_value
        return self

    # noinspection PyMethodMayBeStatic
    def _add_at(self, value: Union[int, Sequence[int], None], add_method: Callable[[int], None]):
        if value is None:
            return

        if isinstance(value, int):
            add_method(value)
        else:
            for one_value in value:
                add_method(one_value)


class RuntimeService(object):
    def __init__(self, scheduler: 'SettableJobScheduler'):
        self._scheduler = scheduler

    def stop(self):
        self._scheduler.stop()

    def stop_until(self, until_time: datetime.datetime):
        self._scheduler.stop_until(until_time)

    def stop_for(self, for_time: datetime.timedelta):
        self._scheduler.stop_until(datetime.datetime.now() + for_time)


class RunnableJob(object):
    def __init__(self, handler: types.FunctionType, scheduler: SettableJobScheduler, schedulers: List[SettableJobScheduler], custom_scheduler: Callable[[JobSchedule], bool]):
        if not scheduler.is_scheduled():
            raise ValueError(f"job '{scheduler.name}' is not scheduled")

        self._job_schedules = schedulers
        self._handler = handler
        self._has_parameter = handler.__code__.co_argcount > 0

        if not scheduler.name:
            scheduler.set_name(self._create_handle_name())

        self._job_schedule = scheduler
        self._job_schedule.seal()
        self._job_status = JobStatus()
        self._custom_scheduler = custom_scheduler

    def should_run(self):
        if self._job_status.is_running:
            return False

        if not self._job_schedule.should_run(self._job_status.last_run_time):
            return False

        if self._custom_scheduler:
            return self._custom_scheduler(self._job_schedule)

        return True

    def run(self):
        job_thread = threading.Thread(target=self._run_internal)
        job_thread.daemon = self._job_schedule.is_daemon
        job_thread.start()

    def _run_internal(self):
        logger.debug(f'Starting job {self._job_schedule.name}')
        self._job_status.start()
        try:
            if self._has_parameter:
                self._handler(RuntimeService(self._job_schedule))
            else:
                self._handler()
        except:
            logger.exception(f'Failed to run job {self._job_schedule.name}.')
        finally:
            logger.debug(f'Completed job {self._job_schedule.name}')
            self._job_status.stop()

    def _create_handle_name(self):
        name = ''
        if hasattr(self._handler, '__module__'):
            name = getattr(self._handler, '__module__')

        if hasattr(self._handler, '__name__'):
            name += f":{getattr(self._handler, '__name__')}"

        if not name:
            return f"JOB_{uuid.uuid4()}"

        index = 0
        while True:
            index += 1
            candidate_name = name if index == 1 else f'{name}:{index}'
            if any(x.name == candidate_name for x in self._job_schedules):
                continue
            else:
                return candidate_name


class CronnerEvery(object):
    def __init__(self, interval: int, get_scheduler_func: Callable[[Any], SettableJobScheduler]):
        self._interval = interval
        self._scheduler_getter = get_scheduler_func

    @property
    def minute(self):
        return self.__set('minute')

    @property
    def second(self):
        return self.__set('second')

    @property
    def hour(self):
        return self.__set('hour')

    @property
    def day(self):
        return self.__set('day')

    def __set(self, unit):
        def register_action(action):
            self._scheduler_getter(action).set_interval(self._interval, unit)
            return action

        return register_action


class Cronner(object):
    def __init__(self):
        self._job_definitions: dict[Callable[['RuntimeService', None], bool], Tuple[Job, SettableJobScheduler]] = {}
        self._jobs: List[RunnableJob] = []
        self._custom_scheduler = None

    def name(self, job_name: str):
        def wrapper(action):
            self._get_or_add_scheduler(action).set_name(job_name)
            return action

        return wrapper

    def every(self, interval: int) -> CronnerEvery:
        return CronnerEvery(interval, self._get_or_add_scheduler)

    def at(self,
           weekday: Union[int, Sequence[int], None] = None,
           day: Union[int, Sequence[int], None] = None,
           hour: Union[int, Sequence[int], None] = None,
           minute: Union[int, Sequence[int], None] = None,
           second: Union[int, Sequence[int], None] = None):
        if weekday is None and day is None and hour is None and minute is None and second is None:
            raise ValueError("You must specify at least one argument")

        def _add_at_item(items: Union[int, Sequence[int], None], add_method: Callable[[int], None]) -> None:
            if items is None:
                return

            if isinstance(items, int):
                add_method(items)
            else:
                for item in items:
                    add_method(item)

        def register_action(action):
            _, scheduler = self._get_or_add_job(action)
            _add_at_item(day, scheduler.add_day)
            _add_at_item(weekday, scheduler.add_weekday)
            _add_at_item(hour, scheduler.add_hour)
            _add_at_item(minute, scheduler.add_minute)
            _add_at_item(second, scheduler.add_second)
            return action

        return register_action

    def do(self, action) -> Job:
        job, _ = self._get_or_add_job(action)
        return job

    def start(self):
        schedulers = [v[1] for k, v in self._job_definitions.items()]
        for key, value in self._job_definitions.items():
            self._jobs.append(RunnableJob(key, value[1], schedulers, self._custom_scheduler))

        self._job_definitions.clear()
        while True:
            try:
                for item in self._jobs:
                    if item.should_run():
                        item.run()
            except:
                logger.exception('Failed to start jobs')
                raise
            finally:
                time.sleep(1)

    @property
    def register_custom_scheduler(self):
        def register(action):
            error_message = 'register_custom_scheduler must mark a method with one parameter of type JobScheduler'
            if not hasattr(action, '__code__'):
                raise ValueError(error_message)
            if action.__code__.co_argcount != 1:
                raise ValueError(error_message)
            self._custom_scheduler = action
        return register

    def _get_or_add_scheduler(self, action):
        _, scheduler = self._get_or_add_job(action)
        return scheduler

    def _get_or_add_job(self, action):
        result = self._job_definitions.get(action)
        if result is not None:
            return result

        scheduler = SettableJobScheduler()
        result = (Job(scheduler), scheduler)
        self._job_definitions[action] = result
        return result


cronner = Cronner()
