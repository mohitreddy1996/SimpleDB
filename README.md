# SimpleDB
Basic database management system. <br />
Data managed in the formed of heap files. Each page divided into slots and each slots containing a page/record with a header.

Features implemented :<br />
1) Tuple Insertion, deletion with proper checks on tuple descriptions etc.<br />
2) Page/DbFile/Record management implemented.<br />
3) Aggregation functions on different tables (Sum, Count, Avg, Max, Min etc).<br />
4) Query Optimiser and Cost calculation implemented. <br />
5) Transactions and Locks management implemented.<br />

APIs coded over skeleton code provided by https://sites.google.com/site/cs186fall2013/homeworks/project-1.

Testing done using JUnit. Test cases provided along with the project itself. Tests divided into Unit tests and System tests.
