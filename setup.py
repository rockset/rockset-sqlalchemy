from setuptools import find_packages, setup

setup(
    name="rockset-sqlalchemy",
    version="0.0.1",
    author="Rockset",
    author_email="support@rockset.com",
    keywords=["Rockset", "rockset-client"],
    description="Rockset's SQLAlchemy support and DB-API specification",
    long_description=open("README.md", "r").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/rockset/rockset-sqlalchemy",
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
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: OS Independent',
    ],
)
