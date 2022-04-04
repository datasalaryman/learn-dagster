import requests
import csv
from dagster import (
    op, 
    get_dagster_logger, 
    DagsterType, 
    In, 
    Out
)

@op
def hello():
    """
    An op definition. This example op outputs a single string.

    For more hints about writing Dagster ops, see our documentation overview on Ops:
    https://docs.dagster.io/concepts/ops-jobs-graphs/ops
    """
    return "Hello, Dagster!"

@op
def hello_cereal():
    response = requests.get("https://docs.dagster.io/assets/cereal.csv")
    lines = response.text.split("\n")
    cereals = [row for row in csv.DictReader(lines)]
    get_dagster_logger().info(f"Found {len(cereals)} cereals")

@op
def download_cereals():
    response = requests.get("https://docs.dagster.io/assets/cereal.csv")
    lines = response.text.split("\n")
    return [row for row in csv.DictReader(lines)]

@op
def find_sugariest(cereals):
    sorted_by_sugar = sorted(cereals, key=lambda cereal: cereal["sugars"])
    get_dagster_logger().info(f'{sorted_by_sugar[-1]["name"]} is the sugariest cereal')

@op
def find_highest_calorie_cereal(cereals):
    sorted_cereals = list(sorted(cereals, key=lambda cereal: cereal["calories"]))
    return sorted_cereals[-1]["name"]


@op
def find_highest_protein_cereal(cereals):
    sorted_cereals = list(sorted(cereals, key=lambda cereal: cereal["protein"]))
    return sorted_cereals[-1]["name"]


@op
def display_results(most_calories, most_protein):
    logger = get_dagster_logger()
    logger.info(f"Most caloric cereal: {most_calories}")
    logger.info(f"Most protein-rich cereal: {most_protein}")


def is_list_of_dicts(_, value):
    return isinstance(value, list) and all(
        isinstance(element, dict) for element in value
    )


SimpleDataFrame = DagsterType(
    name="SimpleDataFrame",
    type_check_fn=is_list_of_dicts,
    description="A naive representation of a data frame, e.g., as returned by csv.DictReader.",
)

@op(
    config_schema={"url": str}, 
    out=Out(SimpleDataFrame)
)
def download_csv_configurable(context):
    response = requests.get(context.op_config["url"])
    lines = response.text.split("\n")
    return [row for row in csv.DictReader(lines)]


@op(ins={"cereals": In(SimpleDataFrame)})
def sort_by_calories(cereals):
    sorted_cereals = sorted(cereals, key=lambda cereal: int(cereal["calories"]))
    get_dagster_logger().info(f'Most caloric cereal: {sorted_cereals[-1]["name"]}')