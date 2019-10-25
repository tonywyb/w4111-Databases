import src.data_service.data_table_adaptor as dta
import json


def t1():

    t = dta.get_rdb_table("people", "lahman2019clean")
    print("Testing get_rdb_table ...")
    print(t)
    print("Finish testing get_rdb_table ...")


def t2():

    d = dta.get_databases()
    print("Testing get_databases ...")
    print("get database result=", json.dumps(d, indent=2))
    print("Finish testing get_databases ...")


def t3(dbname):
    d = dta.get_tables(dbname)
    print("Testing get_tables from database {}...".format(dbname))
    print("get tables result=", json.dumps(d, indent=2))
    print("Finish testing get_tables ...")


def t4(dbname, table_name):

    print("Testing get_primary_key_columns from table {} in database {} ...".format(table_name, dbname))
    d = dta.get_rdb_table(table_name, dbname).get_primary_key_columns()
    print(d)
    print("Finish testing get_primary_key_columns ...")


def t5(dbname, table_name):

    print("Testing get_row_count ...")
    d = dta.get_rdb_table(table_name, dbname).get_row_count()
    print(d)
    print("Finish testing get_row_count ...")

t1()
t2()
t3("lahman2019clean")
t4("lahman2019clean", "appearances")
t5("lahman2019clean", "appearances")
