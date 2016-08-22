# Scope

This is a set of scripts, leveraging Spark for parallelization, to dump data from
a SQL data base that was not indexed in a way that easily facilitates queries like
```sql
SELECT column_list FROM schema.database WHERE filter_column > value;
```
Because 
