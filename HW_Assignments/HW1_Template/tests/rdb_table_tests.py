from HW_Assignments.HW1_Template.src.RDBDataTable import RDBDataTable
import logging
import json
import pymysql
import pymysql.cursors

logf = open('RDBDtaTableTest.log', 'w')
logging.basicConfig(stream=logf, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(logging.getLevelName(logging.DEBUG))


def test_create():

    c_info = {
        "host": "localhost",
        "port": 3306,
        "user": "root",
        "passwd": "wyb5030356",
        "db": "lahman2019raw",
        "cursorclass": pymysql.cursors.DictCursor
    }
    r_dbt = RDBDataTable(table_name="People", connect_info=c_info, key_columns=["playerID"])
    print("RDB table =", r_dbt)


def test_find_by_template():

    c_info = {
        "host": "localhost",
        "port": 3306,
        "user": "root",
        "passwd": "wyb5030356",
        "db": "lahman2019raw",
        "cursorclass": pymysql.cursors.DictCursor
    }
    r_dbt = RDBDataTable(table_name="People", connect_info=c_info, key_columns=["playerID"])
    res = r_dbt.find_by_template({"nameLast": "Williams", "birthCity": "San Diego"},
                                 field_list=["playerID", "nameLast", "nameFirst", "birthYear"])
    print("Res =", json.dumps(res, indent=2))


def test_find_by_primary_key():

    c_info = {
        "host": "localhost",
        "port": 3306,
        "user": "root",
        "passwd": "wyb5030356",
        "db": "lahman2019raw",
        "cursorclass": pymysql.cursors.DictCursor
    }
    r_dbt = RDBDataTable(table_name="Batting", connect_info=c_info,
                         key_columns=["playerID", "teamID", "yearID", "stint"])
    res = r_dbt.find_by_primary_key(key_fields=["willite01", "BOS", "1960", "1"],
                                 field_list=["playerID", "yearID", "teamID", "stint", "H", "AB"])
    print("Res =", json.dumps(res, indent=2))


def final_test():
    c_info = {
        "host": "localhost",
        "port": 3306,
        "user": "root",
        "passwd": "wyb5030356",
        "db": "lahman2019raw",
        "cursorclass": pymysql.cursors.DictCursor
    }
    r_dbt = RDBDataTable(table_name="People", connect_info=c_info, key_columns=["playerID"])

    print("Testing find by template...")
    res = r_dbt.find_by_template({"birthYear": "1940", "birthCountry": "USA", "birthDay": "21"}, field_list=["playerID", "birthCity", "deathYear"])
    print("Result of find by template = {}. Find by template test end.".format(res))

    new_values = {"deathYear": "2000", "birthCity": "Mars"}
    print("Testing update by template, new values are {}".format(new_values))
    res = r_dbt.update_by_template({"birthYear": "1940", "birthCountry": "USA", "birthDay": "21", "deathYear": "2002"},
                                   new_values=new_values)
    print("Result of update by template = {}.".format(res))

    print("Testing find by template again...")
    res = r_dbt.find_by_template({"birthYear": "1940", "birthCountry": "USA", "birthDay": "21"},
                                 field_list=["playerID", "birthCity", "deathYear"])
    print("Result of find by template = {}. Update by template test end.".format(res))

    print("Testing delete by template...")
    res = r_dbt.delete_by_template({"birthYear": "1940", "birthCountry": "USA", "birthDay": "21", "deathYear": "2000"})
    print("Result of delete by template = {}.".format(res))

    print("Testing find by template again...")
    res = r_dbt.find_by_template({"birthYear": "1940", "birthCountry": "USA", "birthDay": "21"}, field_list=["playerID", "birthCity", "deathYear"])
    print("Result of find by template = {}. Delete by template test end.".format(res))

    new_row = {"playerID": "dff9", "nameLast": "Wang", "nameFirst": "Yanbin", "weight": "150" }
    print("Row to insert=", new_row)

    print("Testing find by primary key...")
    res = r_dbt.find_by_primary_key(["dff9"])
    print("Result of find by primary key = {}. Find by primary key test end.".format(res))

    print("Doing insert ...")
    res = r_dbt.insert(new_row)

    print("Testing find by primary key again...")
    res = r_dbt.find_by_primary_key(["dff9"])
    print("Result of find by primary key = {}. Insert test end.".format(res))

    print("Testing delete by key...")
    res = r_dbt.delete_by_key(["dff9"])
    print("Result of delete by key = {}.".format(res))

    print("Testing Find again...")
    res = r_dbt.find_by_template({"playerID": "dff9"})
    print("Result after delete = {}. Delete by key test end.".format(res))

    new_cols = {"nameLast": "Boink", "birthCountry": "Mars"}
    print("Testing update by key, new values={}".format(new_cols))
    res = r_dbt.update_by_key({"playerID": "dff9"}, new_cols)

    print("Testing Find again...")
    res = r_dbt.find_by_primary_key({"playerID": "dff9"})
    print("Result after update by key = {}. Update by key test end.".format(res))


test_create()
test_find_by_template()
test_find_by_primary_key()
final_test()

