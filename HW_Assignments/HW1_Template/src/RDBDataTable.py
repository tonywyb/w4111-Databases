from HW_Assignments.HW1_Template.src.BaseDataTable import BaseDataTable
import pymysql
import pymysql.cursors
import logging
import json
from Examples.SQLHelper import create_insert, create_select, create_update, create_delete, run_q


class RDBDataTable(BaseDataTable):

    """
    The implementation classes (XXXDataTable) for CSV database, relational, etc. with extend the
    base class and implement the abstract methods.
    """

    def __init__(self, table_name, connect_info, key_columns):
        """

        :param table_name: Logical name of the table.
        :param connect_info: Dictionary of parameters necessary to connect to the data.
        :param key_columns: List, in order, of the columns (fields) that comprise the primary key.
        """
        self._data = {
            "table_name": table_name,
            "connect_info": connect_info,
            "key_columns": key_columns
        }
        if table_name is None or connect_info is None:
            raise ValueError("Invalid input")

        # self._logger = logging.getLogger()
        # Set up logger
        logf = open('RDBDtaTable.log', 'w')
        logging.basicConfig(stream=logf, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO)
        self._logger = logging.getLogger(__name__)
        self._logger.setLevel(logging.getLevelName(logging.INFO))
        # self._logger.debug("RDBDataTable.__init__: data = " + json.dumps(self._data, indent=2))

        host = connect_info.get("host", "localhost")
        port = connect_info.get("port", 3306)
        user = connect_info.get("user", "root")
        password = connect_info.get("password", "wyb5030356")
        db = connect_info.get("db", None)
        cursorclass = connect_info.get("cursorclass", pymysql.cursors.DictCursor)
        charset = connect_info.get("charset", "utf8mb4")
        if not host or not port or not user or not password or not db:
            raise ValueError("Insufficient params given for connection")

        self.conn = pymysql.connect(host=host, port=port, user=user, password=password, db=db,
                               charset=charset, cursorclass=cursorclass)

    def find_by_primary_key(self, key_fields, field_list=None):
        """

        :param key_fields: The list with the values for the key_columns, in order, to use to find a record.
        :param field_list: A subset of the fields of the record to return.
        :return: None, or a dictionary containing the requested fields for the record identified
            by the key.
        """
        key_cols = self._data.get("key_columns", None)
        if key_cols is None:
            raise ValueError("Need primary key before finding anything by primary key")
        tmp = dict(zip(self._data["key_columns"], key_fields))
        return self.find_by_template(template=tmp, field_list=field_list)

    def find_by_template(self, template, field_list=None, limit=None, offset=None, order_by=None):
        """

        :param template: A dictionary of the form { "field1" : value1, "field2": value2, ...}
        :param field_list: A list of request fields of the form, ['fielda', 'fieldb', ...]
        :param limit: Do not worry about this for now.
        :param offset: Do not worry about this for now.
        :param order_by: Do not worry about this for now.
        :return: A list containing dictionaries. A dictionary is in the list representing each record
            that matches the template. The dictionary only contains the requested fields.
        """
        if template is None:
            raise ValueError("Empty template given")
        (sql, args) = create_select(self._data["table_name"], template, field_list, order_by=None, limit=None, offset=None)
        (res, data) = run_q(sql, args, conn=self.conn)

        return data

    def delete_by_key(self, key_fields):
        """

        Deletes the record that matches the key.

        :param template: A template.
        :return: A count of the rows deleted.
        """
        tmp = dict(zip(self._data["key_columns"], key_fields))
        return self.delete_by_template(tmp)

    def delete_by_template(self, template):
        """

        :param template: Template to determine rows to delete.
        :return: Number of rows deleted.
        """
        (sql, args) = create_delete(self._data["table_name"], template)
        (res, data) = run_q(sql, args, fetch=False, conn=self.conn)
        return res

    def update_by_key(self, key_fields, new_values):
        """

        :param key_fields: List of value for the key fields.
        :param new_values: A dict of field:value to set for updated row.
        :return: Number of rows updated.
        """
        tmp = dict(zip(self._data["key_columns"], key_fields))
        return self.update_by_template(tmp, new_values)

    def update_by_template(self, template, new_values):
        """

        :param template: Template for rows to match.
        :param new_values: New values to set for matching fields.
        :return: Number of rows updated.
        """
        (sql, args) = create_update(self._data["table_name"], new_values, template)
        (res, data) = run_q(sql, args, fetch=False, conn=self.conn)
        return res

    def insert(self, new_record):
        """

        :param new_record: A dictionary representing a row to add to the set of records.
        :return: None
        """
        (sql, args) = create_insert(self._data["table_name"], new_record)
        (res, data) = run_q(sql, args, fetch=False, conn=self.conn)
        return None

    def close_connection(self):
        self.conn.close()

    # def get_rows(self):
    #     return self._rows




