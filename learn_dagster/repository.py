from dagster import repository

from learn_dagster.jobs.say_hello import say_hello_job, hello_cereal_job, serial
from learn_dagster.schedules.my_hourly_schedule import my_hourly_schedule
from learn_dagster.sensors.my_sensor import my_sensor


@repository
def learn_dagster():
    """
    The repository definition for this learn_dagster Dagster repository.

    For hints on building your Dagster repository, see our documentation overview on Repositories:
    https://docs.dagster.io/overview/repositories-workspaces/repositories
    """
    jobs = [say_hello_job, hello_cereal_job, serial]
    schedules = [my_hourly_schedule]
    sensors = [my_sensor]

    return jobs + schedules + sensors
