# 5300-Cheetah
DB Relation Manager project


Usage (argument is database directory):
<pre>
$ ./sql5300 ~/cpsc5300/data
</pre>

## New Methods Added - all function without errors
MS3 methods: CREATE TABLE, DROP TABLE, SHOW TABLES, SHOW COLUMNS 
MS4 methods: CREATE INDEX, SHOW INDEX, DROP INDEX

## Extra Credit Video
posted to repo

## Unit Tests
There are some tests for SlottedPage and HeapTable. They can be invoked from the <code>SQL</code> prompt:
```sql
SQL> test
```
Be aware that failed tests may leave garbage Berkeley DB files lingering in your data directory. If you don't care about any data in there, you are advised to just delete them all after a failed test.
```sh
$ rm -f data/*
```

## Valgrind (Linux)
To run valgrind (files must be compiled with <code>-ggdb</code>):
```sh
$ valgrind --leak-check=full --suppressions=valgrind.supp ./sql5300 data
```
Note that we've added suppression for the known issues with the Berkeley DB library <em>vis-à-vis</em> valgrind.

---

## Sprint Invierno 
Team: Priyanka Patil, Yinhui Li
### New Methods Added - all function without errors
MS5 implemented methods: SELECT FROM TABLE, INSERT INTO TABLE, DELETE FROM TBALE  
MS6 implemented methods: lookup

On cs1, 
```
$ cd cpsc5300
$ git clone https://github.com/klundeen/5300-Cheetah.git
```
Checkout the correct Milestone
```
$ git checkout tags/Milestone5
```
```
$ git checkout tags/Milestone6
```
```
$ make
$ ./sql5300 ~/cpsc5300/data
```
---

