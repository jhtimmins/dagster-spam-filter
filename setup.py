from setuptools import find_packages, setup

setup(
    name="spam_filter",
    packages=find_packages(exclude=["spam_filter_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "dagster-duckdb~=0.20",
        "dagster-duckdb-pandas~=0.20",
        "Flask~=2.3",
        "pandas~=2.0",
        "numpy~=1.25",
        "scikit-learn~=1.3"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
