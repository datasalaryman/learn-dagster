from src.jobs.say_hello import (
    say_hello_job, 
    diamond
)


def test_say_hello():
    """
    This is an example test for a Dagster job.

    For hints on how to test your Dagster graphs, see our documentation tutorial on Testing:
    https://docs.dagster.io/concepts/testing
    """
    result = say_hello_job.execute_in_process()

    assert result.success
    assert result.output_for_node("hello") == "Hello, Dagster!"

def test_diamond():
    res = diamond.execute_in_process()
    assert res.success
    assert res.output_for_node("find_highest_protein_cereal") == "Special K"