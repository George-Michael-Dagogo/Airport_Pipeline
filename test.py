from prefect.schedules import Schedule
from pre import CronClock
from prefect import task, Flow

schedule = Schedule(clocks=[CronClock("0 9 * * *")])

@task
def task1():
    print("Hello, World!")

with Flow("my-flow") as flow:
    t1 = task1()

flow.schedule = schedule


