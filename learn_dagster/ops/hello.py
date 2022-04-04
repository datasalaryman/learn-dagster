import requests
import csv
from dagster import (
    op, 
    get_dagster_logger, 
    DagsterType, 
    In, 
    Out, 
    TypeCheck
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

def less_simple_data_frame_type_check(_, value):
    if not isinstance(value, list):
        return TypeCheck(
            success=False,
            description=f"LessSimpleDataFrame should be a list of dicts, got {type(value)}",
        )

    fields = [field for field in value[0].keys()]

    for i in range(len(value)):
        row = value[i]
        idx = i + 1
        if not isinstance(row, dict):
            return TypeCheck(
                success=False,
                description=(
                    f"LessSimpleDataFrame should be a list of dicts, got {type(row)} for row {idx}"
                ),
            )
        row_fields = [field for field in row.keys()]
        if fields != row_fields:
            return TypeCheck(
                success=False,
                description=(
                    f"Rows in LessSimpleDataFrame should have the same fields, got {row_fields} "
                    f"for row {idx}, expected {fields}"
                ),
            )

    return TypeCheck(
        success=True,
        description="LessSimpleDataFrame summary statistics",
        metadata={
            "n_rows": len(value),
            "n_cols": len(value[0].keys()) if len(value) > 0 else 0,
            "column_names": str(list(value[0].keys()) if len(value) > 0 else []),
        },
    )


SimpleDataFrame = DagsterType(
    name="SimpleDataFrame",
    type_check_fn=less_simple_data_frame_type_check,
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