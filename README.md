# CS3223 Project

## Features Implemented

### External Sort

N = number of buffer pages
 
External Sort retrieves as many tuples from the base operator as the N number of buffer pages can contain, then all the tuples inside the current buffer pages will be sorted. After the in-memory sorting, these tuples will be moved to a separate file. This process will be repeated until all tuples from the table are retrieved. Hence this process will generate several files with sorted tuples.
 
The algorithm will take 1 page from each of the N-1 files to be merged, hence generating 1 larger file/run with the sorted tuples inside. This process will be repeated until there is only one file generated after the merging process. This indicates the end of the merging phase.
 
The tuples are then retrieved one by one every next() until the whole file is read.

### Block Nested Loops Join

Block Nested Loop Join uses N-2 buffer pages to read the left table, 1 buffer to read the right table, and 1 more buffer to output the join result. The right table will be written to a separated file to be read several times. This implementation uses 4 additional variables to keep track of the position of the tuple in the left table and the right table; These variables are lbcurs (left block cursor), lcurs (left batch cursor) and rcurs (right batch cursor). After filling up the left block with pages, the right batch will be filled and the joining process begins. During the joining process, whenever outbatch is full it is returned to the caller and reinitialized, and the cursors are updated accordingly. This process repeats until the left table is exhausted. 

### Sort Merge Join

Sort Merge utilizes the external sort algorithm in Sort.java to sort the two tables.
Methods getLeftTuple() and getRightTuple() abstract the logic of retrieving the next tuple from the left and right tables, respectively.
These methods retrieve a new batch whenever the previous batch has been exhausted, which is indicated by the value of lcurs (left batch cursor) and rcurs (right batch cursor).

Partitions of the left and right tuples (leftpart and rightpart) are made every time the current left tuple (lefttuple) can be joined with the current right tuple (righttuple) (i.e. match on join conditions).
A partition is created by taking tuples from a table until the join conditions no longer match.
A cross product is then performed between the two partitions to create the result tuples, which are stored in outbatch.
If outbatch is full, it is returned, and lpcurs (left partition cursor) as well as rpcurs (right partition cursor) is modified.

If the current left tuple is less than the current right tuple, the next left tuple is fetched. If the current left tuple is greater than the current right tuple,
the next right tuple is fetched. The algorithm runs until either the left table or the right table has been exhausted, indicated by eosl and eosr, in which case outbatch is returned.

### DISTINCT

The table is first sorted using the external sort algorithm. The algorithm then tracks the latest distinct tuple that is inserted into outbatch. If the next tuple is the same as the tracked tuple, then it is not be inserted into outbatch. If it is different, it is inserted into outbatch and assigned as the tracked tuple. The process continues until the table has been exhausted.

### ORDERBY

ORDERBY uses the external sorting algorithm to sort the specified attribute according to the orderbylist generated from the user input. If DESC is added to the end of the ORDERBY query, it will change the desc static variable in Sort.java into -1, hence reversing the sorting order.

### Aggregate Functions (MIN, MAX, COUNT)

Aggregate functions are implemented on the Project.java file as they are to be done “on-the-fly”, and hence they will not incur additional I/O cost. In the open() method, two more arrays, aggIndex[] and aggData[] are initialised to hold the aggregate type of an attribute and the aggregate data of an attribute respectively. For every attribute that is to be projected, we check the aggregate type of the attribute using getAggType(), and assign a corresponding value to the aggIndex[] array, with the index being the index of the attribute in attrIndex[]. The values in the aggData[] array will then be set to the starting value depending on the aggregate function. 
 
In the next() method, if the attributes to be projected are being assigned an aggregate type in aggIndex[], then we calculate the corresponding data for the aggregate function and store the data in aggData[]. For MAX and MIN, the functions will only work on Integer data.
 
A boolean flag aggPresent is used to indicate that the projection contains aggregate data, so attribute values will not be projected. If there are no aggregate functions present, attributes will be projected normally.
 
At the end of reading the tuples, when there are no more tuples to be read, the aggregate data in the aggData[] array will then be written to outbatch.

## Bugs Found

### RandomDB is unable to generate data when the attributes are of type “REAL”

When we want to create a table with the “REAL” attribute type, the .det file must contain the “REAL” keyword for that attribute.
However, in the RandomDB.java file, to generate attributes of the “REAL” data type, it will check the .det file and see if the data type specified is equal to “FLOAT” instead of “REAL”.
This will prevent “REAL” attributes from being generated. A simple bug fix is to change the “FLOAT” to “REAL”, and attributes of type “REAL” will then be able to be generated properly. 

