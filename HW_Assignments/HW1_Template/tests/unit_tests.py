# I write and test methods one at a time.
# This file contains unit tests of individual methods.

from HW_Assignments.HW1_Template.src.CSVDataTable import CSVDataTable
import logging
import os
import json

# The logging level to use should be an environment variable, not hard coded.
logging.basicConfig(level=logging.DEBUG)

# Also, the 'name' of the logger to use should be an environment variable.
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

# This should also be an environment variable.
# Also not the using '/' is OS dependent, and windows might need `\\`
data_dir = os.path.abspath("../Data/Baseball")


def t_load():

    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }

    csv_tbl = CSVDataTable("people", connect_info, None)

    print("Created table = " + str(csv_tbl))


def test_match():

    row = {"cool": "yes", "db": "no"}
    t = {"cool": "yes"}
    result = CSVDataTable.matches_template(row, t)
    print(result)


def test_match_all():

    tmp = {"nameLast": "Williams", "birthCity": "San Diego"}
    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }

    csv_tbl = CSVDataTable("people", connect_info, None)
    result = csv_tbl.find_by_template(tmp, field_list=["birthState", "birthDay"])
    print(json.dumps(result, indent=2))



# t_load()

# test_match()

test_match_all()