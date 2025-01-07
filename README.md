Swagger can be found at: localhost:8900/swagger-ui/index.html

Just type regular SQL and post it. Result will be in response. Currently permitted one sql request per one post. 
Inserts support multiply values, separated by ",".

Supports:
- create table (string, int types)
- select from single table with expressions like (>, <, >=, <=, =, like) with "and", "or"
- update single table with same expressions
- delete from table with same expressions
- select from index (use "_id")
- truncate table
- drop table

Not supports: 
- joins
- any aggregates (max, min, avg, group by)
- having
- with
- between
- dates
- custom indexes
- fk

Create and inserts example can be found in https://github.com/davkhun/dummydb/tree/main/src/main/resources 
