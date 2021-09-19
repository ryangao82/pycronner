import setuptools


with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()


setuptools.setup(
    name="pycronner",
    packages=setuptools.find_packages("src"),
    package_dir={"":"src"},
    version="0.0.1",
    description="Job scheduling package in python",
    long_description=long_description,
    long_description_content_type="text/markdown",
    license="MIT",
    author="Ryan Gao",
    author_email="ryan.gao@live.com",
    url="https://github.com/ryangao82/pycronner",
    keywords=[
        "cron",
        "crontab",
        "schedule",
        "scheduling",
        "scheduler",
        "job scheduling",
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.6",
)