# Databricks notebook source
# Author: Andrew J. Otis
sc = spark.sparkContext


# COMMAND ----------

# Checking current files in main DataBricks directory
dbutils.fs.ls ("/FileStore/tables/")


# COMMAND ----------

# Make a directory for relevant assignment1 files <---- only needs to be ran once
#dbutils.fs.mkdirs("FileStore/tables/assignment1")


# COMMAND ----------

# Copy files from one directory to another <---- only needs to be ran once
#dbutils.fs.cp("dbfs:/FileStore/tables/geoPoints0.csv", "dbfs:/FileStore/tables/assignment1/geoPoints0.csv")

#dbutils.fs.cp("dbfs:/FileStore/tables/geoPoints1.csv", "dbfs:/FileStore/tables/assignment1/geoPoints1.csv")


# COMMAND ----------

# Verify that relevant data is in the desired directory
dbutils.fs.ls ("/FileStore/tables/assignment1")


# COMMAND ----------

# ------------Brute Force Method (i.e. w/o using Grid Cells)------------
import math
import itertools

# Threshold distance as described in assignment description
threshold_distance = 0.75

# Read data from CSV files from the DataBricks directory
files = sc.textFile("dbfs:/FileStore/tables/assignment1")

# A function that takes a row(i.e. line) of a csv file and returns each element as a tuple with the appropriate data type
def parse_csv_line(files_line):
    id, x, y = files_line.split(",")
    return (id, float(x), float(y))

# Turn the tuples created from the "parse_csv_line" function into point representation using ".map" and assigned to a variable so that ".collect" can be used 
points = files.map(parse_csv_line)

# Using ".collect" retrieve the "points" variable as a list and labeling it "pts"
pts = points.collect()

# A function to calculate the eucledean distance between two points
def calculate_distance(point1, point2):
    return math.sqrt((point1[1] - point2[1]) ** 2 + (point1[2] - point2[2]) ** 2)

# Using list comprehensions and itertools find point pairs (i.e. points within the threshold distance of one another). 
# "point_pair" is a variable representative of all possible combinations of points from the list "pts", where those combinations are then filtered down to points that are less than or equal to the "threshold_distance" (i.e. within proximity based on set threshold_distance)
point_pair = [(p1[0], p2[0]) for p1, p2 in itertools.combinations(pts, 2) if calculate_distance(p1, p2) <= threshold_distance]

# Printing set threshold_distance and the number of point pairs which are next to the actual values
print("Distance =", threshold_distance)
print()
print(len(point_pair), point_pair )


# COMMAND ----------

# ------------Grid Cell Processing Method------------
import math

# Threshold distance and grid cell size as described in the assignment description
threshold_distance = 0.75
cell_size = 0.75

# Read data from CSV files from the DataBricks directory
files = sc.textFile("dbfs:/FileStore/tables/assignment1")

# Same function from the brute force method
def parse_csv_line(line):
    id, x, y = line.split(",")
    return (id, float(x), float(y))

# Persist the points RDD to avoid recomputation
points = files.map(parse_csv_line).persist()  

# A function to calculate the grid cell coordinates for a given point
def calculate_cell_coordinates(point, cell_size):
    x_cell = math.floor(point[1] / cell_size)
    y_cell = math.floor(point[2] / cell_size)
    return (x_cell, y_cell)

# A function to generate grid cell keys for a point's home cell and adjacent cells
def generate_cell_keys(point, cell_size):
    x_cell, y_cell = calculate_cell_coordinates(point, cell_size)
    cell_keys = []
    for i in range(-1, 2):
        for j in range(-1, 2):
            cell_keys.append((x_cell + i, y_cell + j))
    return cell_keys

# Map each point to its home cell and adjacent cells
point_cells = points.flatMap(lambda point: [(cell_key, point) for cell_key in generate_cell_keys(point, cell_size)])

# Group points by cell key
cell_points = point_cells.groupByKey()

# A function to check if two points are within the threshold distance
def check_distance(point1, point2, threshold_distance):
    distance = math.sqrt((point1[1] - point2[1]) ** 2 + (point1[2] - point2[2]) ** 2)
    return distance <= threshold_distance

# A function to generate all unique pairs of points within each cell and filter based on distance
def find_close_pairs(cell):
    points = list(cell[1])  # Convert point iterator to a list
    close_pairs = []
    for i in range(len(points)):
        for j in range(i + 1, len(points)):
            if check_distance(points[i], points[j], threshold_distance):
                close_pairs.append((points[i][0], points[j][0]))
    return close_pairs

# Find close pairs within each cell and collect the results
close_pairs = cell_points.flatMap(find_close_pairs).distinct()

# Printing set threshold_distance, cell size, and the number of point pairs
print("Distance =", threshold_distance)
print("Cell Size =", cell_size)
print()
print(close_pairs.count(), close_pairs.collect())

