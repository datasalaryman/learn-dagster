from dagster import repository

from src.jobs.say_hello import configurable_job, diamond, say_hello_job, hello_cereal_job, serial
from src.schedules.my_hourly_schedule import my_hourly_schedule
from src.sensors.my_sensor import my_sensor


@repository
def learn_dagster():
    """
    The repository definition for this learn_dagster Dagster repository.

    For hints on building your Dagster repository, see our documentation overview on Repositories:
    https://docs.dagster.io/overview/repositories-workspaces/repositories
    """
    jobs = [say_hello_job, hello_cereal_job, serial, diamond, configurable_job]
    schedules = [my_hourly_schedule]
    sensors = [my_sensor]

    return jobs + schedules + sensors
