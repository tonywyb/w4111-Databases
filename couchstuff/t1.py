import couchdb

svr = couchdb.Server("http://dbuser:dbuserdbuser@localhost:5984/")
print("Server = ", svr)
db = svr["w4111f19"]
doc_id, doc_rev = db.save({'type': 'Person', 'name': 'John Doe'})