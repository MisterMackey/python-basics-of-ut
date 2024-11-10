from setuptools import setup, find_packages

setup(
    name="app",
    version="1.0.0",
    packages=find_packages(),
    install_requires=["pyspark", "requests"],
    entry_points={
        "console_scripts": ["app = app:main.main"],
    },
    author="",
    author_email="",
    description="",
    url="",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.10",
)
