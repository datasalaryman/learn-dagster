from src.ops.hello import (
    hello, 
    find_highest_calorie_cereal
)


def test_hello():
    """
    This is an example test for a Dagster op.

    For hints on how to test your Dagster ops, see our documentation tutorial on Testing:
    https://docs.dagster.io/concepts/testing
    """

    assert hello() == "Hello, Dagster!"

def test_find_highest_calorie_cereal():
    cereals = [
        {"name": "hi-cal cereal", "calories": 400},
        {"name": "lo-cal cereal", "calories": 50},
    ]
    result = find_highest_calorie_cereal(cereals)
    assert result == "hi-cal cereal"