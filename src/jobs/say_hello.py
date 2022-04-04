from dagster import job

from src.ops.hello import (
    download_csv_configurable,
    hello, 
    hello_cereal, 
    download_cereals, 
    find_sugariest, 
    find_highest_protein_cereal,
    find_highest_calorie_cereal, 
    display_results,
    sort_by_calories
)

@job
def say_hello_job():
    """
    A job definition. This example job has a single op.

    For more hints on writing Dagster jobs, see our documentation overview on Jobs:
    https://docs.dagster.io/concepts/ops-jobs-graphs/jobs-graphs
    """
    hello()

@job
def hello_cereal_job():
    hello_cereal()

@job
def serial():
    find_sugariest(download_cereals())

@job
def diamond():
    cereals = download_cereals()
    display_results(
        most_calories=find_highest_calorie_cereal(cereals),
        most_protein=find_highest_protein_cereal(cereals),
    )

@job
def configurable_job():
    sort_by_calories(download_csv_configurable())