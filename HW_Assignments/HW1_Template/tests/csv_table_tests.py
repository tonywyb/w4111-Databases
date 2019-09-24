from HW_Assignments.HW1_Template.src.CSVDataTable import CSVDataTable
import logging
import json
import os

logf = open('CSVDataTableTest.log', 'w')
logging.basicConfig(stream=logf, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.DEBUG)
logger = logging.getLogger(__name__)
logger.setLevel(logging.getLevelName(logging.DEBUG))

data_dir = os.path.abspath("../Data/Baseball")


def test_create():

    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }

    csv_tbl = CSVDataTable("people", connect_info, key_columns=["playerID"])

    print("Created table. Sample first 10 lines below: ")
    for r in range(10):
        print(csv_tbl.get_rows()[r])


def test_find_by_template():

    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }

    csv_tbl = CSVDataTable("people", connect_info, key_columns=["playerID"])
    res = csv_tbl.find_by_template({"nameLast": "Williams", "birthCity": "San Diego"},
                                   field_list=["playerID", "nameLast", "nameFirst", "birthYear"])
    print("Res of find by template ={}".format(res))


def test_find_by_primary_key():

    connect_info = {
        "directory": data_dir,
        "file_name": "Batting.csv"
    }
    csv_tbl = CSVDataTable(table_name="batting", connect_info=connect_info,
                         key_columns=["playerID", "teamID", "yearID", "stint"])
    res = csv_tbl.find_by_primary_key(key_fields=["willite01", "BOS", "1960", "1"],
                                 field_list=["playerID", "yearID", "teamID", "stint", "H", "AB"])
    print("Res of find by primary key={}".format(res))


def final_test():
    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }
    csv_tbl = CSVDataTable(table_name="People", connect_info=connect_info, key_columns=["playerID"])

    print("Testing find by template...")
    res = csv_tbl.find_by_template({"birthYear": "1940", "birthCountry": "USA", "birthDay": "21"}, field_list=["playerID", "birthCity", "deathYear"])
    print("Result of find by template = {}. Find by template test end.".format(res))

    print("Testing delete by template...")
    res = csv_tbl.delete_by_template({"birthYear": "1940", "birthCountry": "USA", "birthDay": "21", "deathYear": "1996"})
    print("Result of delete by template = {}.".format(res))

    print("Testing find by template again...")
    res = csv_tbl.find_by_template({"birthYear": "1940", "birthCountry": "USA", "birthDay": "21"}, field_list=["playerID", "birthCity", "deathYear"])
    print("Result of find by template = {}. Delete by template test end.".format(res))

    new_values = {"deathYear": "2000", "birthCity": "Mars"}
    print("Testing update by template, new values = {}".format(new_values))
    res = csv_tbl.update_by_template({"birthYear": "1940", "birthCountry": "USA", "birthDay": "21", "deathYear": "2002"}, new_values=new_values)
    print("Result of update by template = {}.".format(res))

    print("Testing find by template again...")
    res = csv_tbl.find_by_template({"birthYear": "1940", "birthCountry": "USA", "birthDay": "21"}, field_list=["playerID", "birthCity", "deathYear"])
    print("Result of find by template = {}. Update by template test end.".format(res))

    new_row = {"playerID": "dff9", "nameLast": "Wang", "nameFirst": "Yanbin", "weight": "150" }
    print("Row to insert=", new_row)

    print("Testing find by primary key...")
    res = csv_tbl.find_by_primary_key(["dff9"])
    print("Result of find by primary key = {}. Find by primary key test end.".format(res))

    print("Doing insert ...")
    res = csv_tbl.insert(new_row)

    print("Testing find by primary key again...")
    res = csv_tbl.find_by_primary_key(["dff9"])
    print("Result of find by primary key = {}. Insert test end.".format(res))

    new_cols = {"nameLast": "Boink", "birthCountry": "Mars"}
    print("Testing update by key, new values={}".format(new_cols))
    res = csv_tbl.update_by_key(["dff9"], new_cols)

    print("Testing Find again...")
    res = csv_tbl.find_by_primary_key(["dff9"])
    print("Result after update by key = {}. Update by key test end.".format(res))

    print("Testing delete by key...")
    res = csv_tbl.delete_by_key(["dff9"])
    print("Result of delete by key = {}.".format(res))

    print("Testing Find again...")
    res = csv_tbl.find_by_primary_key(["dff9"])
    print("Result after delete = {}. Delete by key test end.".format(res))

test_create()
test_find_by_template()
test_find_by_primary_key()
final_test()