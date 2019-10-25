Homework 2: Database Applications
uni: yw3372, name:Yanbin Wang

(1) Environment and Configuration
python version: 3.6
IDE: PyCharm 2019.2.2 Professional
mysql: 8.0.17
data_table_adaptor.py assumes that the default connection to database is localhost:3306 using root account and pymysql.cursors.DictCursor as cursorclass.

(2) Assumption
Since all the complex functions are implemented basically by the help of dbutils, which use pymysql to execute the requests. The error checking and correctness can be proved and guaranteed. This time, we don't have to test edges cases because it will be l;eft for mysql work.

(3) Run
To run the RESTful API using flask, directly call "python api.py" to to surface data from
the web.

To test individual/fundamental functions implemented in data_table_adaptor.py and RDBDataTable.py, type in "python test/t1.py > test_output.txt" and check the output log file for any further details.

(4) Notes
Since the test for code in app.py and other files are quiet different.
I separately test them. test/test_output.txt record the test result for data_table_adaptor.py and RDBDataTable.py.
test/test_output.pdf are screen shot for testing api.py.
