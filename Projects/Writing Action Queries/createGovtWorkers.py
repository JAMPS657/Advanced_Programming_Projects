import pymongo
import pprint
import numpy as np
import datetime

from pymongo import MongoClient

#%% Generate the relevant data
np.random.seed(3)  # set seed so everybody running it gets the same data

client = MongoClient()  # connects on default host
#client = MongoClient('localhost',27017)  # explicit connect command

db = client.db_people    

# remove entire collection, i.e. all docs in peopleDB.thePeople 
#db.thePeople.remove()
db.thePeople.drop()

# create UNIQUE INDEX
# db.thePeople.create_index( [('pid', pymongo.ASCENDING)], unique=True )

# the collection we will create
peeps = db.thePeople  


states = ["AL","AK","AZ","AZ","CA","CO","CT","DE","FL","GA", "HI","ID","IL","IN","IA","KS","KY","LA","ME","MD", "MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ", "NM","NY","NC","ND","OH","OK","OR","PA","RI","SC", "SD","TN","TX","UT","VT","VA","WA","WV","WI","WY"]

fNames = ["Bob","Mary","Isabella","Santiago","Valentina","Daniella","Alejandro","Diego","Victoria","Sofia","John","Paul","Peter","Joseph","Vicky","David","Jeffrey","William","Jennifer","Linda","Sarah","Ashley","Michelle","Amy","Julie","Julia","Hannah","Jayden","Noah","Demarco","Madison","Ava","Kayla","Jayla","Priya","Tanya","Neha","Rahul","Raj","Amit","Mohammed","Mohammad","Vivek","Fatimah","Hasan"]

mNames = ["A","B","C","D","E","F","G","H","I","J","K","L","M","N","O","P","Q","R","S","T","U","V","W","X","Y","Z"]

lNames = ["Garcia","Martinez","Gonzalez","Lopez","Torres","Ramirez","Hernandez","Baker","Jackson","Brown","Smith","Jones","Miller","White","Johnson","Wilson","Williams","Anderson","Das","Mukherjee","Simha","Liu","Li","Zhao","Zhang","Wu","Chen","Chan","Lee","Wong","Park","Kim","Ngyuen","Le","Tran","Dang","Sato","Tanaka","Takahashi"]

timeStartInsert = datetime.datetime.now()
numDocs = 2000
print("\nStart inserting " + str(numDocs) + " documents at: " + str(timeStartInsert) )
for i in range(0,numDocs):
	aPid = i
	aFName = fNames[ np.random.randint(len(fNames)) ]
	aMName = mNames[ np.random.randint(len(mNames)) ]
	aLName = lNames[ np.random.randint(len(lNames)) ]
	aName = aFName + " " + aMName + " " + aLName
	print(aName)
	aAge = np.random.randint(100) + 18
	aWeight = np.random.randint(100) + 40 # in Kilos
	aHeight = np.random.randint(150,200)  # in centimeters
	aBirth = 2019 - aAge
	aSalary = np.random.randint(100000) + 30000  # lowests paid is 30K
	aState = states[ np.random.randint( len(states) ) ]
	aChildren = []
	if (aAge > 20):
		aNumChildren = np.random.binomial(8,0.40)  # 0..8 children, binomially distributed with probability p = 0.40
		for j in range (0,aNumChildren):
			aChildren.append( fNames[ np.random.randint(len(fNames)) ] + " " + mNames[ np.random.randint(len(mNames)) ] + " " + aLName)
	else:
		aNumChildren = 0
	newPerson = {"pid":aPid,"firstName":aFName, "MI":aMName, "lastName":aLName, "state":aState, "age":aAge,"birth":aBirth, "salary":aSalary, "numChildren":aNumChildren,"children":aChildren, "weight":aWeight, "height":aHeight}
	print(newPerson)
	peeps.insert_one(newPerson)

timeEndInsert = datetime.datetime.now()
timeElapsedInsert = timeEndInsert - timeStartInsert
timeStartQueries = datetime.datetime.now()

print("\nNumber of docs in db.thePeople = " + str(db.thePeople.count()))
# print("\nAt start, output from peeps.find():")
# for objs in peeps.find():
# 	print(objs)

numQueries = 4
print("\nStart " + str(numQueries) + " random queries at: ")
print(datetime.datetime.now())
for i in range(1,numQueries):
	randPID = np.random.randint(numDocs)
	anObject = db.thePeople.find_one( {"pid":randPID} )
	print(anObject)

timeEndQueries = datetime.datetime.now()
timeElapsedQueries = timeEndQueries - timeStartQueries
	
'''
print("\nFinished random queries at: ")
print(datetime.datetime.now())


print("\nElapsed time for inserts = " + str(timeElapsedInsert) ) ;
print("\nElapsed time for queries = " + str(timeElapsedQueries) ) ;

'''

#%% Part1,Q1) All info about people who have 7 children

Q1 = peeps.find({"numChildren": 7})

for has_svn_children in Q1:
    pprint.pprint(has_svn_children)
    

#%% Part1,Q2) pid, state, and name of the children for people who have 7 children

Q1 = peeps.find({"numChildren": 7})
for r in Q1:
    if len(r["children"]) == 7:
        children_names = []
        for child in r["children"]:
            if isinstance(child, dict):
                children_names.append(child["name"])
            else:
                children_names.append(child)
        Q2 = {'pid': r["pid"], 'state': r["state"], 'children': children_names}
        pprint.pprint(Q2)

#%% Part1,Q3) All info of people who live in CA and have 6 children

Q3 = peeps.find({"$and": [{"numChildren": 6}, {"state": "CA"}]})
for kids6_CA in Q3:
    if len(kids6_CA["children"]) == 6:
        children_names = []
        for child in kids6_CA["children"]:
            children_names.append(child)
            kids6_CA["children"] = children_names
    pprint.pprint(kids6_CA)
    

#%% Part1,Q4-graded) All info of people who live in CA and have 6 or 7 children

Q4 = peeps.find({"$and": [{"numChildren": {"$in": [6, 7]}}, {"state": "CA"}]})
for kids6or7_CA in Q4:
    if len(kids6or7_CA["children"]) == 6 or len(kids6or7_CA["children"]) == 7:
        children_names = []
        for child in kids6or7_CA["children"]:
            children_names.append(child)
            kids6or7_CA["children"] = children_names
        pprint.pprint(kids6or7_CA)


#%% Part1,Q5-graded) Using $regex. List the pid and children names for all people 
#              who have a child whose name contains 'Bob A'

# perform the query using $regex
query = {"children": {"$regex": ".*Bob A.*"}}
projection = {"pid": 1, "children": 1, "_id": 0}
results = peeps.find(query, projection)

# print the results
for result in results:
    print(result)



#%% Part1,Q6) Aggregation: number of people who have 0, 1, ... 8 children

pipeline = [
            {"$group": {
                "_id": "$numChildren",
                "numInGroup": {"$sum": 1}
                        } 
            },
            {"$sort": {"_id": 1} }
]

Q6 = peeps.aggregate(pipeline)
for doc in Q6:
    print(doc)



#%% Part1,Q7) Aggregation: avgerage salary for each state

pipeline = [
    {
        "$group": {
            "_id": "$state",
            "avgSalary": {"$avg": "$salary"},
            "numInGroup": {"$sum": 1}
        }
    },
    {
        "$sort": {"_id": 1}
    }
]

Q7 = peeps.aggregate(pipeline)

for doc in Q7:
    pprint.pprint(doc)



#%% Part1,Q8) Aggregation: avgerage salary and how many people in the grouping for 
#       those living in the state WI. 

pipeline = [
    {
        "$match": {"state": "WI"}
    },
    {
        "$group": {
            "_id": "$state",
            "avgSalary": {"$avg": "$salary"},
            "numInGroup": {"$sum": 1}
        }
    }
]

Q8 = peeps.aggregate(pipeline)
for doc in Q8:
    pprint.pprint(doc)

#%% Part1,Q9-graded) Aggregation: average/min/max salary for midwest states
# Midwest states are as follows
midwest = {"state":"ND"},{"state":"SD"},{"state":"NE"},{"state":"KS"},{"state":"MN"},{"state":"IA"},
{"state":"MS"},{"state":"WI"},{"state":"IL"},{"state":"IN"},{"state":"MI"},{"state":"OH"}


pipeline = [
    {
        "$match": {"state": {"$in": ["ND", "SD", "NE", "KS", "MN", "IA", "MS", 
                                     "IL", "IN", "MI", "OH"]}}
    },
    {
        "$group": {
            "_id": "$state",
            "avgSalary": {"$avg": "$salary"},
            "minSalary": {"$min": "$salary"},
            "maxSalary": {"$max": "$salary"},
            "numInGroup": {"$sum": 1}
        }
    }
]

Q9 = peeps.aggregate(pipeline)

for doc in Q9:
    pprint.pprint(doc)


#%% Part1,Q10-graded) Aggregation: average salary in states where the average 
#                            salary within that state is >= 82,000 and 
#                            how many people in the grouping for each state

pipeline = [
    {"$group": {"_id": "$state", "avgSalary": {"$avg": "$salary"}, "numInGroup": {"$sum": 1}}},
    {"$match": {"avgSalary": {"$gte": 82000}}},
    {"$project": {"_id": 1, "avgSalary": 1, "numInGroup": 1}}
]

Q10 = peeps.aggregate(pipeline)
for doc in Q10:
    pprint.pprint(doc)



#%% Part1,Q11-graded) Aggregation: average/min/max salary for midwest states

pipeline = [
    {"$match": {"state": {"$in": ["ND", "SD", "NE", "KS", "MN", "IA", "MS", 
                                 "IL", "IN", "MI", "OH"]}}},
    {"$group": {"_id": "$state", 
                "avgSalary": {"$avg": "$salary"}, 
                "minSalary": {"$min": "$salary"},
                "maxSalary": {"$max": "$salary"},
                "numInGroup": {"$sum": 1}
               }},
    {"$match": {"avgSalary": {"$gte": 82000}}},
    {"$project": {"_id": 1, "avgSalary": 1, "numInGroup": 1, "minSalary": 1, "maxSalary": 1}}
]

Q11 = peeps.aggregate(pipeline)
for doc in Q11:
    pprint.pprint(doc)


#%% Part2,Q1 Write an example of an update for the collection you used in 
#            Part 1 that changes ONE document. Print the document before and 
#            after.

pipeline = [
    {
        "$match": {"state": "WI"}
    },
    {
        "$group": {
            "_id": "$state",
            "avgSalary": {"$avg": "$salary"},
            "numInGroup": {"$sum": 1}
        }
    }
]


# Before update
print("Before update:")
before_update = peeps.aggregate(pipeline)
for doc in before_update:
    pprint.pprint(doc)

# Update document, changing state from "WI" to "CO"
peeps.update_one({"state": "WI"}, {"$set": {"state": "CO"}})

# After update
print("After update:")
after_update = peeps.aggregate(pipeline)
for doc in after_update:
    pprint.pprint(doc) # Notice how there are 22 instead of 23 people in 
                       # the doc.




#%% Part2,Q1 Write an example of an update for the collection you used in 
#            Part 1 that changes MULTIPLE documents. Print the documents 
#            before and after.


pipeline = [
    {
        "$match": {"state": "WI"}
    },
    {
        "$group": {
            "_id": "$state",
            "avgSalary": {"$avg": "$salary"},
            "numInGroup": {"$sum": 1}
        }
    }
]

# Before updates
print("Before update:")
before_update = peeps.aggregate(pipeline)
for doc in before_update:
    pprint.pprint(doc)

# Update multiple documents, changing state from "WI" to "CO"
result = peeps.update_many({"state": "WI"}, {"$set": {"state": "CO"}})
print("Number of documents modified:", result.modified_count)

# After updates
print("After update:")
after_update = peeps.aggregate(pipeline)
for doc in after_update:
    pprint.pprint(doc) # Notice how there are 0 instead of 23 people in 
                       # the doc. The series of documents only included
                       # people from the state of WI. If the aggregation
                       # is looking for people in the state WI, but post
                       # update, there isn't anyone in the group since there
                       # are no longer documents of people with the '_id': WI






