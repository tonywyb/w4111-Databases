# from W4111_F19_HW1.src.BaseDataTable import BaseDataTable
from HW_Assignments.HW1_Template.src.BaseDataTable import BaseDataTable
import pymysql
import pymysql.cursors
import logging
import json


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
        self._logger = logging.getLogger()

        # Set up logger
        logf = open('RDBDtaTable.log', 'w')
        logging.basicConfig(stream=logf, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO)
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.getLevelName(logging.INFO))

        self._logger.debug("RDBDataTable.__init__: data = " + json.dumps(self._data, indent=2))

    def find_by_primary_key(self, key_fields, field_list=None):
        """

        :param key_fields: The list with the values for the key_columns, in order, to use to find a record.
        :param field_list: A subset of the fields of the record to return.
        :return: None, or a dictionary containing the requested fields for the record identified
            by the key.
        """
        pass

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
        host = self._data.get("host", "localhost")
        port = self._data.get("port", 3306)
        user = self._data.get("user", "root")
        password = self._data.get("password", "wyb5030356")
        db = self._data.get("db", None)
        cursorclass = self._data.get("cursorclass", pymysql.cursors.DictCursor)
        charset = self._data.get("charset", "utf8mb4")
        if not host or not port or not user or not password or not db:
            raise ValueError("Insufficient params given for connection")
        if not template:
            raise ValueError("Empty template given")
        search_params = [key + "=%s " for key in template.keys()]
        search_params_str = "and".join(search_params)
        sql = "select " + ",".join(list(template.keys())) + " from " + self._data["table_name"] +\
              " where " + search_params_str
        search_values = tuple(template.values())
        conn = pymysql.connect(host=host, port=port, user=user, password=password, db=db,
                                      charset=charset, cursorclass=cursorclass)
        cursor = conn.cursor()
        result = []
        try:
            row_cnt = cursor.execute(sql % search_values)
            if row_cnt == 0:
                self._logger.debug("No row matched during select operation")
            else:
                row_dict = cursor.fetchall()
                for r in row_dict:
                    result.append({key: value for (key, value) in r.items() if key in field_list})
            conn.commit()
        except Exception as e:
            self._logger.warnning("Error occurs during select operation, rollback")
            conn.rollback()
        finally:
            cursor.close()
            conn.close()
        return result

    def delete_by_key(self, key_fields):
        """

        Deletes the record that matches the key.

        :param template: A template.
        :return: A count of the rows deleted.
        """
        pass

    def delete_by_template(self, template):
        """

        :param template: Template to determine rows to delete.
        :return: Number of rows deleted.
        """
        pass

    def update_by_key(self, key_fields, new_values):
        """

        :param key_fields: List of value for the key fields.
        :param new_values: A dict of field:value to set for updated row.
        :return: Number of rows updated.
        """

    def update_by_template(self, template, new_values):
        """

        :param template: Template for rows to match.
        :param new_values: New values to set for matching fields.
        :return: Number of rows updated.
        """
        pass

    def insert(self, new_record):
        """

        :param new_record: A dictionary representing a row to add to the set of records.
        :return: None
        """
        pass

    # def get_rows(self):
    #     return self._rows




