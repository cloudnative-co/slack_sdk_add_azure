from setuptools import setup, find_packages

setup(
    name="slack_sdk_add_azure",
    version="0.1",
    description="slack_sdk add Azure",
    author="sebastian",
    author_email="seba@cloudnative.co.jp",
    packages=find_packages(),
    install_requires=[
        "azure-functions",
        "azure-functions-worker",
        "azure-storage-blob",
        "slack_sdk",
    ],
    entry_points={
        "console_scripts": [
        ]
    },
)
