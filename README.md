# 5300-Cheetah
Cheetah's DB Relation Manager project for CPSC5300/4300 at Seattle U, Spring 2021
<br>
Sprint 1: Ryan Bush and Nick Nguyen 

Usage (argument is database directory):
<pre>
$ ./sql5300 ~/cpsc5300/data
</pre>

## Tags
- <code>Milestone1</code> is playing around with the AST returned by the HyLine parser and general setup of the command loop.
- <code>Milestone2h</code> has the intructor-provided files for Milestone2. (Note that heap_storage.cpp is just a stub.)

### Sprint 1: Milestone 1 Skeleton
No problems with this code in this section. It works how it is intended. It parses mySQL statements passed in by the user. The file name is "sql5300.cpp" for the parser in the repository.

### Sprint 1: Milestone 2 Rudimentary Storage Engine
This milestone actually stores the data for our database in this class using a heap store engine.

We use the abstract class provided by the instructor to implment the database. 
1. DbBlock: Dblock is how records are stored and modified within the blocks that it creates.
2. DbFile: DbFile handles the collection of blocks making up the relation as well as file creation, deletion, and access. It uses a DbBlock class for individual blocks and a block manager for managing moving blocks to/from disk.
3. DbRelation: DbRelation represents a logical view of the tables we have created.

These abstract classes relate to the ones we wrote for milestone 2 respectively(number correlated)
1. SlottedPage
2. HeapFile
3. HeapTable

## Pass off to next group
- There is something wrong when we insert, the flags in the function db.open() in the HeapFile class. It creates the error message of "terminate called after throwing an instance of 'DbException'
  what():  Db::get: Invalid argument". We think it fails in our HeapFile get function. 
  
 - Did not implement the following functions in HeapTable:
 - virtual void del(const Handle handle);
 - virtual Handles *select();
 - virtual ValueDict *project(Handle handle);
 - virtual ValueDict *project(Handle handle, const ColumnNames *column_names);
 - virtual ValueDict *unmarshal(Dbt *data);
