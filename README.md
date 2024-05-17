# GetDbSchema
GetDbSchema is stored procedure written in T-SQL in order to get creation scripts of tables and indexes of all user databses in the instance.
Overall, the script performs below steps:
- First, it creates temp table named #temp to store results of of the cursor
- Cursor runs inside dynamic sql for all user specified databases (you can change it according to your needs)
- Temporary tables creatied inside the dynamic sql, store scripts regarding tables` and indexes` creation scripts, as well as  respective filegroups, alter commands and creation dates
- Create scripts` format is modified using CHAR() functions in order to point spaces for new lines
- All created temporary tables are joined
- Results are inserted into #temp table are returned in select query 
