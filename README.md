# pycronner

**pycronner** is a simple job scheduling tool in Python. **pycronner** officially supports Python 3.6+.


## Installing pycronner

```console
$ python3 -m pip install pycronner
```

## Using pycronner

Run a job every 5 minutes

```python
from pycronner import cronner

@cronner.every(5).minute
def job():
    pass

cronner.start()
```

Run a job every 5 minutes on first day every month between 8:00 AM and 10:00 AM.

```python
from pycronner import cronner

@cronner.at(day=1, hour=(8, 9))
@cronner.every(5).minute
def job():
    pass

cronner.start()
```

Not use decorator but do the same thing as above.

```python
from pycronner import cronner

def job():
    pass

cronner.do(job).every(5).minute().at(day=1, hour=(8, 9))

cronner.start()
```




