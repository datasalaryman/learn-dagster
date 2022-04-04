import setuptools

setuptools.setup(
    name="learn_dagster",
    packages=setuptools.find_packages(exclude=["learn_dagster_tests"]),
    install_requires=[
        "dagster==0.14.7",
        "dagit==0.14.7",
        "pytest",
    ],
)
