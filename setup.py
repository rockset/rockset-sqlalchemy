from setuptools import find_packages, setup

setup(
    name="rockset_sqlalchemy",
    version="0.0.0",
    package_dir={"": "src"},
    packages=find_packages("src"),
    entry_points={
        "sqlalchemy.dialects": [
            "rockset_sqlalchemy = rockset_sqlalchemy.client.sqlalchemy:RocksetDialect"
        ]
    },
    extras_require=dict(sqlalchemy=["sqlalchemy>=1.0,<1.4", "geojson>=2.5.0"]),
)
