# 5300-Cheetah
Student DB Relation Manager project for CPSC5300 at Seattle U, Winter 2024

**Installation**
1) Clone the git repository Cheetah :
   git clone https://github.com/klundeen/5300-Cheetah.git

**Build**
1) Use make command in your command prompt to build the project
   make
   
2) Type the command to run sql5300
   ./sql5300 <path to your db environment>
   
3) To clean use command make clean
   make clean

**Output**
1) Put in SQL queries
   SQL> <SQL query>
   (for example SQL> select * from foo as f left join goober on f.x = goober.x)
   
2) To exit the program
   SQL> quit
   
3) To test heap storage functionality
   SQL> test
   
4) To exit type
   SQL> quit
   


