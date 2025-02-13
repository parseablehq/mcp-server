from setuptools import setup, find_packages

setup(
    name="mcp-parseable",
    version="0.1.0",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "httpx",
        "python-dotenv",
        "mcp"
    ],
)