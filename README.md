# spark-challenge-order-aggregation

This challenge is used by Sainsbury's for evaluating candidates for data engineering positions.

## Running the application and tests

### Prerequisites
**Java JDK 8**

```
brew tap adoptopenjdk/openjdk
brew install --cask adoptopenjdk8
```

**Python 3.8.\* or later.**

Check if python3 is installed:

```
python3 --version
```

### Dependencies and data

#### Creating a virtual environment
Ensure the pip package manager is up to date:

```
pip3 install --upgrade pip
```

From the root directory of the project, create and activate the virtual environment:

```
python3 -m venv ./venv
source ./venv/bin/activate
```

#### Installing Python requirements
This will install the packages required for the application to run:

```
pip3 install -r ./requirements.txt
```

If numpy installation fails due to the Mac M1 chip, try running the following commands:
```
pip3 install Cython
pip3 install --no-binary :all: --no-use-pep517 numpy
```

#### Generate input dataset
Generating some sample datasets to be be processed by the application
```
python3 ./input_data_generator/main_data_generator.py
```


### Execution

#### Running the application
Running the application to generate the desired output
```
python3 ./src/main.py
```

#### Running the tests
Running all unit tests to validate the application functionality
```
pytest -vv ./tests
```

### Clean-up
Post-execution clean-up to remove data and virtual environment generated during the execution process
```
rm -r ./venv
rm -r ./input_data
```

### Author
[Eniko Varga](eniko.varga@live.com)