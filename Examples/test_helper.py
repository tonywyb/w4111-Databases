import Examples.SQLHelper as SQLHelper
import json

import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

def t1():

    sql = "select * from lahman2019raw.people where nameLast=%s and birthCity=%s"
    args = ('Williams', 'San Diego')

    result = SQLHelper.run_q(sql, args, fetch=True)

    print("Return code = ", result[0])
    print("Data = ")
    if result[1] is not None:
        print(json.dumps(result[1], indent=2))
    else:
        print("None.")

def t2():

    table_name = "lahman2019raw.people"
    fields = ['nameLast', 'nameFirst', 'birthYear', 'birthState', 'birthMonth']
    template = { "nameLast": "Williams", "birthCity": "San Diego"}
    sql, args = SQLHelper.create_select(table_name, template, fields)
    print("SQL = ", sql, ", args = ", args)

    result = SQLHelper.run_q(sql, args)
    if result[1] is not None:
        print(json.dumps(result[1], indent=2))
    else:
        print("None.")

def t3():

    table_name = "classicmodels.offices"
    row = {
        "officeCode": "13",
        "city": "Minas Tirith",
        "state": "Minas Tirith",
        "addressLine1": "23 Level 3",
        "phone": "Palatir",
        "country": "Gondor",
        "postalCode": "12345",
        "territory": "ME"
    }

    sql, args = SQLHelper.create_insert(table_name, row)
    print("SQL = ", sql, ", args = ", args)

    result = SQLHelper.run_q(sql, args, fetch=False, commit=True)
    if result[1] is not None:
        print(json.dumps(result[1], indent=2))
    else:
        print("None.")

def t4():

    table_name = "classicmodels.offices"
    new_cols = {
        "city": "Minas Morgul",
        "state": "Morgul Vale",
        "addressLine1": "1 Shelob Cave",
        "phone": "Ick",
        "country": "Mordor",
        "postalCode": "66666"
    }

    template = { "city": "Minas Tirith", "state": "Minas Tirith"}

    sql, args = SQLHelper.create_update(table_name, new_cols, template)
    print("SQL = ", sql, ", args = ", args)

    result = SQLHelper.run_q(sql, args, fetch=False, commit=True)
    if result[1] is not None:
        print(json.dumps(result[1], indent=2))
    else:
        print("None.")


def revenue_report(customer_number=None, order_number=None, order_status=None, country=None):
    """
    Returns the orders and total value (sum of value of order items).

    :param customer_number: Customer to get orders for. None gets all.
    :param order_number: Same for order number.
    :param order_status: Same for order status.
    :param country: Same for customer country
    :return: Matching information.
    """

    sql = """
        select
	        customers.customerNumber, customerName, customers.country,
            orders.orderNumber, orders.status, 
            (select sum(orderdetails.quantityOrdered*orderdetails.priceEach)
		        from classicmodels.orderdetails where orders.orderNumber = orderDetails.orderNumber) as order_value
        from classicmodels.customers join classicmodels.orders
        where 
	    customers.customerNumber = orders.customerNumber
    """

    args = []
    if customer_number is not None:
        sql += " and customers.customerNumber = %s"
        args.append(customer_number)
    if order_number is not None:
        sql += " and orderNumber = %s"
        args.append(order_number)
    if order_status is not None:
        sql += " and status = %s"
        args.append(order_status)
    if country is not None:
        sql += " and country = %s"
        args.append(country)

    result = SQLHelper.run_q(sql, args, fetch=True, commit=True)
    return result

def t5():

    result = revenue_report(order_status="In Process", country="Belgium")
    print("Result: number of rows = ", result[0])
    print("Data = \n", json.dumps(result[1], indent=2, default=str))

#t1()
#t2()
#t3()
#t4()

t5()