from setuptools import find_packages, setup

setup(
    name="rockset-sqlalchemy",
    version="0.0.0",
    package_dir={"": "src"},
    packages=find_packages("src"),
    entry_points={
        "sqlalchemy.dialects": [
            "rockset_sqlalchemy = rockset_sqlalchemy.sqlalchemy:RocksetDialect",
            "rockset = rockset_sqlalchemy.sqlalchemy:RocksetDialect"
        ]
    },
    install_requires=[
        "rockset>=1.0.0",
        "sqlalchemy>=1.4.0,<2.0.0"
    ],
)
