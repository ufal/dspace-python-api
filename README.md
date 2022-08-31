[![Test dspace on dev-5](https://github.com/dataquest-dev/dspace-blackbox-testing/actions/workflows/test.yml/badge.svg)](https://github.com/dataquest-dev/dspace-blackbox-testing/actions/workflows/test.yml)

# Dspace-blackbox-testing
Blackbox tests for dspace repository

## How to write new tests
Check test.example package. Everything necessary should be there.

Test data are in `test/data` folder.
If your test data contains special characters like čřšáý and so on, it is recommended
to make `.stripped` variation of the file. 
E.g. `my_format.json` and `my_format.stripped.json` for loading data
and `my_format.test.xml` and `my_format.test.stripped.xml` for testing.

If not on dev-5 (e.g. when run on localhost), `.stripped` version of files will be loaded.
The reason for this is, that when dspace runs on windows, it has trouble with special characters.


## Settings
See const.py for constants used at testing.

To set up logs, navigate to support.logs.py and modify method set_up_logging.

## Run

In order to run tests, use command
`python -m unittest`

Recommended variation is
`python -m unittest -v 2> output.txt`
which leaves result in output.txt

Before running for the first time, requirements must be installed with following command
`pip install -r requirements.txt`

It is possible to run in Pycharm with configuration like so:

![image](https://user-images.githubusercontent.com/88670521/186934112-d0f828fd-a809-4ed8-bbfd-4457b734d8fd.png)
