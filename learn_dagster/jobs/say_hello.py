from dagster import job

from learn_dagster.ops.hello import hello, hello_cereal

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