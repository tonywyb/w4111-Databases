HW1: Understanding Databases
uni: yw3372, name:Yanbin Wang

(1) Environment and Configuration
python version: 3.6
IDE: PyCharm 2019.2.2 Professional
mysql: 8.0.17
RDBDataTable.py assumes that the default connection to database is localhost:3306 using root account.
There are two csv files used in test function, that is Data/Baseball/People.csv and Data/Baseball/Batting.csv. 
Two datatable can be also created from corresponding csv files. For People.csv, since playerID Can uniquely identify each record, I choose "playerID" as its primary key(not null). For Batting.csv, since only when (playerID, teamID, yearID, stint) can the record be uniquely identified, I choose composite primary key (playerID, teamID, yearID, stint), all of them are set to not null in MySQLWorkbench.

(2) Assumption

For CSVDataTable, we assume that there are not duplicate or conflict record in the beginning. When it comes to updating multiple rows, we update the "qualified" rows. If it causes conflict in consistency of dataTable, such as producing 2 rows with same primary key, we choose to raise exception. Only request with legal template and field list can be processed smoothly, otherwise different kinds of exception will be raised.


For RDBDataTable, we assume that once an exception/error occurs when it comes to multiple-row operation(e.g. update more than one records using template), we choose to rollback. We choose to always commit at the end of each operation.
The constructor will create conenection to certain database. When it has been tested, test program should conduct closing connection manually in the end.

(3) Error Checking

For RDBDataTable, since MYSQL can perform check for almost all situation, we only check 2 cases: 
(1) "find_by_primary_key": If the user has set primary key for the datatable before he calls this function? If not, raise a ValueError with hint message. 
(2) "find_by_template": If the template is illegal(e.g. None).

For CSVDataTable, a lot more check can be performed:
(1) template and field_list checking: "_validate_template_and_fields" checks whether the template and fields are subset of all columns the datatable have. (Need to remember the schema when creating the table). This function perform in find/delete/update_by_template functions.

(2) duplicate primary key checking: performed in function "find_by_primary_key"(return more than one record) and "update_by_template"(produce conflict rows after multiple update).

(3) Inserted record checking. it can be divided by following minor checking: 
(a) insert "None" type record into datatable
(b) new record has columns that the datatable haven't(conflict with the schema)
(c) new record includes no or incomplete primary key.
(d) new record has "None" value for primary key(s).
(e) there is already a record with the same primary key as new record existing in the datatable.

Some of the error checking will also log out message in warning level.

(4) Run
directly call "python csv_table_tests.py/rdb_table_tests.py" to run test program, "unit_tests.py" is no longer used. It will produce "CSVDataTableTest.log/RDBDataTableTest.log" to log in different level you set.

(5) Note
Since the design of test programs involves deleting records with certain kind of template. It will produces different results from the result run in first turn. You can also customize test cases using test function listed in two test files to perform  further checking.
