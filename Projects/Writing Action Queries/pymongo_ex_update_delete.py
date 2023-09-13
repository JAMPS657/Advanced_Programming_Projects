import pymongo
import pprint  # print JSON objects in nice format
from pymongo import MongoClient

client = MongoClient() # connects default host
db = client.testDB  # uses database testDB
for cursor in client.list_databases():
    print(cursor)
#%%    
aPerson1 = {"name": "Sojourner Truth", "birth": "1797","death": "1883-11-26", "knownFor": ["abolitionist", "public speaking", "writing", "activism"]}
aPerson2 = {"name": "Susan B. Anthony", "birth": "1820-02-15", "death": "1906-03-13", "knownFor": ["women's suffrage", "public speaking", "writing", "activism"]}
# think of this as "create table", but no need to specify fields!
peeps = db.somePeople  # new table is called somePeople

# the collection (table) is not created until you insert the first document
# “insert_one( … )” in PyMongo, insertOne( … ) in regular Mongo
peep_id = peeps.insert_one(aPerson1).inserted_id
print("\nid returned from insert: " + str(peep_id) )
peep_id = peeps.insert_one(aPerson2).inserted_id
print("\nid returned from insert: " + str(peep_id) )
print("\nOutput from peeps.find(): ")
for objs in peeps.find():
    pprint.pprint(objs)
#%%    
#    Instead of inserting one at a time, can create an array of objects and insert at once:
print("\nMultiple inserts\n")
aPerson1 = {"name": "Sojourner Truth", "birth": "1797","death": "1883-11-26", "knownFor": ["abolitionist", "public speaking", "writing", "activism"]}
aPerson2 = {"name": "Susan B. Anthony", "birth": "1820-02-15", "death": "1906-03-13", "knownFor": ["women's suffrage", "public speaking", "writing", "activism"]}
aPerson3 = {"name": "Sally Ride", "birth": "10-10-1965", "death": "1990", "knownFor": ["space flight", "public speaking"]}
aPerson4 = {"name": "Sally Ride", "birth": "10-10-1965", "death": "1990", "knownFor": ["space flight", "public speaking"]}
twoPeeps = [aPerson1, aPerson2, aPerson3, aPerson4] # or more
peeps = db.somePeople
peeps.insert_many(twoPeeps)
for objs in peeps.find():
    pprint.pprint(objs)
    
#%%    
print("\nDelete Sally Ride\n")    
peeps.delete_one({'name': 'Sally Ride'})  # deletes only the first Sally Ride
for objs in peeps.find():
    pprint.pprint(objs)
#%%    
print("\nDelete S names\n") 
peeps.delete_many({'name': {'$regex': 'S'}})  # deletes all documents with capital S in the name
for objs in peeps.find():
    pprint.pprint(objs)
#%%    
aPerson1 = {"name": "Sojourner Truth", "gender": "f"}
aPerson2 = {"name": "Grace Hopper", "gender": "f"}
peeps = db.somePeople
peeps.insert_many([aPerson1, aPerson2] )
print("\nnew insert\n")
for objs in peeps.find():
    pprint.pprint(objs)
#%%    
aQuery = {'name': {'$regex': 'Hopp'} }
aChange = {'$set': {'name': 'Admiral Grace Hopper'} }  # $set sets dictionary for change
peeps.update_one(aQuery, aChange)  # changes document that matches the query.  Only updates one
print("\nAfter Admiral\n")
for objs in peeps.find():
    pprint.pprint(objs)
#%%    
print("\nchange f to female\n")
aQuery = {'gender': 'f'}
aChange = {'$set': {'gender': 'female'} }  # $set sets the dictionary and all matches will change
peeps.update_many(aQuery,aChange)  # updates all matching documents
for objs in peeps.find():
    pprint.pprint(objs)
#%%
db.somePeople.drop()  # drop all documents
aPerson1 = {"name": "Sojourner Truth", "birth": "1797", "death": "1883-11-26", "gender": "f"}
aPerson2 = {"name": "Susan B. Anthony", "birth": "1820-02-15","death": "1906-03-13", "gender": "f"}
aPerson3 = {"name": "Grace Hopper", "birth": "1906-12-09", "death":"1992-01-01", "gender": "f"}
aPerson4 = {"name": "Sally Ride", "birth": "1951-05-26", "death": "2012-07-23", "gender": "f"}
peeps = db.somePeople
peeps.insert_many([aPerson1, aPerson2, aPerson3, aPerson4])
peeps.delete_one( {'gender': 'f'} )
print("\ndelete one")
for objs in peeps.find():
    pprint.pprint(objs)
#%%    
peeps.delete_many({'birth': {'$gt': '1900'}})  # drop everyone born after 1900
print("\ndelete many\n")
for objs in peeps.find():
    pprint.pprint(objs)
#%%    
# limit results for find
db.somePeople.drop()  # drop all documents
aPerson1 = {"name": "Sojourner Truth", "birth": "1797","death": "1883-11-26", "knownFor": ["abolitionist", "public speaking", "writing", "activism"]}
aPerson2 = {"name": "Susan B. Anthony", "birth": "1820-02-15", "death": "1906-03-13", "knownFor": ["women's suffrage", "public speaking", "writing", "activism"]}
aPerson3 = {"name": "Sally Ride", "birth": "10-10-1965", "death": "1990", "knownFor": ["space flight", "public speaking"]}
aPerson4 = {"name": "Sally Ride", "birth": "10-10-1965", "death": "1990", "knownFor": ["space flight", "public speaking"]}
peeps = db.somePeople
peeps.insert_many([aPerson1, aPerson2, aPerson3, aPerson4])
print("\nlimit output\n")
# first part {} is the find and {} means find all, the second part is the projection.  Can use 0,1 or False, True for fields you want
pprint.pprint(peeps.find_one({},{'_id': False, 'name': True, 'birth': True}))  # returns only name and birth date, first row
# find only record matching that birth date, then only print name and birth
pprint.pprint(peeps.find_one({"birth":"1820-02-15"},{'_id': False, 'name': True, 'birth': True}))  # returns only name and birth date, first row
for objs in peeps.find({}, {'_id': 0, 'birth':0, 'death': 0, 'knownFor': 0 }):  # returns only name, supresses the rest
    pprint.pprint(objs)

print("\nknown for only\n")
for objs in peeps.find({}, {'_id': 0, 'knownFor': 1 }):  # returns only knownfor.  First set of braces is filter, second is output
    pprint.pprint(objs)

# or clause in find
print("\nor clause\n")
for objs in peeps.find({"$or":[{"name":"Sojourner Truth"}, {"name":"Susan B. Anthony"}]}):  # returns only two names
    pprint.pprint(objs)