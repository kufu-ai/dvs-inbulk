from setuptools import setup

setup(
    name="inbulk",
    version="0.1.0",
    extras_require={
        "test": ["pytest", "pytest-mock"]
    },
    install_requires=[
        "pyyaml",
        "pandas",
        "pyarrow",
        "google-cloud-bigquery",
        "google-cloud-bigquery-storage",
        "jsonschema",
    ],
    entry_points={
        "console_scripts": [
            "inbulk = cli:main"
        ]
    }
)
