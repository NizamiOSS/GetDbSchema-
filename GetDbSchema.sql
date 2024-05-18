

CREATE OR ALTER  PROCEDURE [dbo].[GetDbSchemaV] @Database varchar(50) = NULL

AS

BEGIN


/* create temp table to store final results */

drop table if exists #tmp


create table #tmp
(
db_name varchar(50),
table_name nvarchar(100),
create_table nvarchar(max),
alter_table nvarchar(max),
create_index nvarchar(max),
create_date datetime
)




/* start cursor to run the procedure on all required databases */

declare @dbname varchar(50)

declare @tsql nvarchar(max)

DECLARE db_cursor CURSOR FOR

   SELECT  name FROM sys.databases
   where database_id > 4 


OPEN db_cursor;
FETCH NEXT FROM db_cursor INTO @dbname;
WHILE @@FETCH_STATUS = 0
    BEGIN
        BEGIN TRY


         SET @tsql = 'use ['+@dbname+']

/* create temporary tables */

DROP TABLE IF EXISTS #filegroups;
DROP TABLE IF EXISTS #tables;
DROP TABLE IF EXISTS #row_store_indexes;
DROP TABLE IF EXISTS #col_store_indexes;
DROP TABLE IF EXISTS #all_indexes;
DROP TABLE IF EXISTS #create_dates;
DROP TABLE IF EXISTS #altered;
DROP TABLE IF EXISTS #result1;
DROP TABLE IF EXISTS #result2;


CREATE TABLE #tables
(table_name   NVARCHAR(100),
 create_table NVARCHAR(MAX)
);

CREATE TABLE #row_store_indexes
(table_name   NVARCHAR(100),
 index_name   NVARCHAR(100),
 create_index NVARCHAR(MAX)
);

CREATE TABLE #altered
(table_name   NVARCHAR(100),
 constraint_name   NVARCHAR(100),
 alter_table NVARCHAR(MAX)
);


CREATE TABLE #col_store_indexes
(table_name   NVARCHAR(100),
 index_name   NVARCHAR(100),
 create_index NVARCHAR(MAX)
);

CREATE TABLE #all_indexes
(table_name   NVARCHAR(100),
 index_name   NVARCHAR(100),
 create_index NVARCHAR(MAX)
);




/* #tables stores table name, schema name, table creation scripts of each table */

  INSERT into #tables
  SELECT s.name + ''.'' + so.name,
              ''CREATE TABLE ['' + ''''+s.name+''''+''].['' + so.name + '']'' + ''('' + CHAR(10) + o.list + '')''
       FROM sys.objects so
       JOIN sys.schemas s on s.schema_id = so.schema_id
            CROSS APPLY
       (
           SELECT CHAR(9) + ''['' + column_name + ''] '' + ''['' + data_type + '']'' + CASE data_type
                                                                                   WHEN ''ntext''
                                                                                   THEN ''''
                                                                                   WHEN ''sql_variant''
                                                                                   THEN ''''
                                                                                   WHEN ''text''
                                                                                   THEN ''''
                                                                                   WHEN ''decimal''
                                                                                   THEN ''('' + CAST(numeric_precision AS VARCHAR) + '', '' + CAST(numeric_scale AS VARCHAR) + '')''
                                                                                   ELSE COALESCE(''('' + CASE
                                                                                                           WHEN character_maximum_length = -1
                                                                                                           THEN ''MAX''
                                                                                                           ELSE CAST(character_maximum_length AS VARCHAR)
                                                                                                       END + '')'', '''')
                                                                               END + '' '' + CASE
                                                                                               WHEN EXISTS
           (
               SELECT id
               FROM syscolumns
               WHERE OBJECT_NAME(id) = so.name
                     AND name = column_name
                     AND COLUMNPROPERTY(id, name, ''IsIdentity'') = 1
           )
                                                                                               THEN ''IDENTITY('' + CAST(IDENT_SEED(so.name) AS VARCHAR) + '','' + CAST(IDENT_INCR(so.name) AS VARCHAR) + '')''
                                                                                               ELSE ''''
                                                                                           END + '''' + (CASE
                                                                                                           WHEN IS_NULLABLE = ''No''
                                                                                                           THEN '' NOT ''
                                                                                                           ELSE ''''-- --
                                                                                                       END) + ''NULL'' + CASE
                                                                                                                           WHEN information_schema.columns.COLUMN_DEFAULT IS NOT NULL
                                                                                                                           THEN '' DEFAULT '' + information_schema.columns.COLUMN_DEFAULT
                                                                                                                           ELSE ''''
                                                                                                                       END + '','' + CHAR(10)
           FROM information_schema.columns
           WHERE table_name = so.name
           ORDER BY ordinal_position FOR XML PATH('''')
       ) o(list)
            LEFT JOIN information_schema.table_constraints tc ON tc.Table_name = so.Name
                                                                 AND tc.Constraint_Type IN(''PRIMARY KEY'', ''UNIQUE'')


            CROSS APPLY
       (
           SELECT+CHAR(10) + CHAR(9) + ''['' + Column_Name + ''], ''
           FROM information_schema.key_column_usage kcu
           WHERE kcu.Constraint_Name = tc.Constraint_Name
           ORDER BY ORDINAL_POSITION FOR XML PATH('''')
       ) j(list) where so.type_desc = ''USER_TABLE''







/* #altered stores scripts with ALTER command for each table */
INSERT into #altered
        SELECT distinct s.name + ''.'' + so.name, tc.Constraint_Name, CASE
                                                                                                WHEN tc.Constraint_Name IS NULL
                                                                                                THEN ''''
                                                                                                WHEN tc.Constraint_Type = ''PRIMARY KEY''
                                                                                                THEN+CHAR(10) + ''ALTER TABLE '' + ''['' +   ''''+s.name+''''+''].['' + so.name + '']'' + '' ADD CONSTRAINT '' + ''['' + tc.Constraint_Name + '']'' + '' PRIMARY KEY '' + CHAR(10) + ''('' + CHAR(9) + LEFT(j.List, LEN(j.List) - 1) + CHAR(10) + '')'' + CHAR(10) + c.list
                                                                                                WHEN tc.Constraint_Type = ''UNIQUE''
                                                                                                THEN+CHAR(10) + ''ALTER TABLE '' + ''['' +   ''''+s.name+''''+''].['' + so.name + '']'' +  '' ADD CONSTRAINT '' + ''['' + tc.Constraint_Name + '']'' + '' UNIQUE NONCLUSTERED '' + CHAR(10) + ''('' + CHAR(9) + LEFT(j.List, LEN(j.List) - 1) + CHAR(10) + '')'' + CHAR(10) + c.list
                                                                                            END

FROM sys.objects so
       JOIN sys.schemas s on s.schema_id = so.schema_id

       LEFT JOIN information_schema.table_constraints tc ON tc.Table_name = so.Name
                                                                 AND tc.Constraint_Type IN(''PRIMARY KEY'', ''UNIQUE'')

       CROSS APPLY
       (
           SELECT+CHAR(10) + CHAR(9) + ''['' + Column_Name + ''], ''
           FROM information_schema.key_column_usage kcu
           WHERE kcu.Constraint_Name = tc.Constraint_Name
           ORDER BY ORDINAL_POSITION FOR XML PATH('''')
       ) j(list)
            CROSS APPLY
       (
           SELECT+''WITH ('' + CASE
                                 WHEN I.is_padded = 1
                                 THEN '' PAD_INDEX = ON''
                                 ELSE ''PAD_INDEX = OFF''
                             END + '','' + -- default value
                 CASE
                     WHEN ST.no_recompute = 0
                     THEN '' STATISTICS_NORECOMPUTE = OFF''
                     ELSE '' STATISTICS_NORECOMPUTE = ON''
                 END + '','' + CASE
                                 WHEN I.ignore_dup_key = 1
                                 THEN '' IGNORE_DUP_KEY = ON''
                                 ELSE '' IGNORE_DUP_KEY = OFF''
                             END + '','' + '' ONLINE = OFF'' + '','' + CASE
                                                                     WHEN I.allow_row_locks = 1
                                                                     THEN '' ALLOW_ROW_LOCKS = ON''
                                                                     ELSE '' ALLOW_ROW_LOCKS = OFF''
                                                                 END + '','' + CASE
                                                                                 WHEN I.allow_page_locks = 1
                                                                                 THEN '' ALLOW_PAGE_LOCKS = ON''
                                                                                 ELSE '' ALLOW_PAGE_LOCKS = OFF''
                                                                             END + '','' + CASE
                                                                                             WHEN I.optimize_for_sequential_key = 1
                                                                                             THEN '' OPTIMIZE_FOR_SEQUENTIAL_KEY = ON''
                                                                                             ELSE '' OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF''
                                                                                         END + '')''
           FROM sys.indexes I
                JOIN sys.tables T ON T.object_id = I.object_id
                JOIN sys.sysindexes SI ON I.object_id = SI.id
                                          AND I.index_id = SI.indid
                JOIN
           (
               SELECT *
               FROM
               (
                   SELECT IC2.object_id,
                          IC2.index_id,
                          STUFF(
                   (
                       SELECT '','' + CHAR(9) + CHAR(10) + CHAR(9) + ''['' + C.name + CASE
                                                                                      WHEN MAX(CONVERT(INT, IC1.is_descending_key)) = 1
                                                                                      THEN '']'' + '' DESC''
                                                                                      ELSE '']'' + '' ASC''
                                                                                  END
                       FROM sys.index_columns IC1
                            JOIN sys.columns C ON C.object_id = IC1.object_id
                                                  AND C.column_id = IC1.column_id
                                                  AND IC1.is_included_column = 0
                       WHERE IC1.object_id = IC2.object_id
                             AND IC1.index_id = IC2.index_id
                       GROUP BY IC1.object_id,
                                C.name,
                                index_id
                       ORDER BY MAX(IC1.key_ordinal) FOR XML PATH('''')
                   ), 1, 2, '''') KeyColumns
                   FROM sys.index_columns IC2
                   GROUP BY IC2.object_id,
                            IC2.index_id
               ) tmp3
           ) tmp4 ON I.object_id = tmp4.object_id
                     AND I.Index_id = tmp4.index_id
                JOIN sys.stats ST ON ST.object_id = I.object_id
                                     AND ST.stats_id = I.index_id
                JOIN sys.data_spaces DS ON I.data_space_id = DS.data_space_id
                JOIN sys.filegroups FG ON I.data_space_id = FG.data_space_id
                JOIN information_schema.table_constraints tc ON tc.TABLE_NAME = T.name

       ) c(list) where so.type_desc = ''USER_TABLE'';





/* #row_store_indexes store creation scripts of rowstore indexes for each table */

INSERT INTO #row_store_indexes
       SELECT s.name + ''.'' + T.name,
              I.name,
              ''CREATE '' + CASE
                              WHEN I.is_unique = 1
                              THEN ''UNIQUE ''
                              ELSE ''''
                          END + I.type_desc COLLATE DATABASE_DEFAULT + '' INDEX '' + ''['' + I.name + '']'' + '' ON '' + ''['' + SCHEMA_NAME(T.schema_id) + '']'' + ''.'' + ''['' + T.name + '']'' + CHAR(10) + ''( '' + CHAR(9) + KeyColumns + CHAR(10) + '')  '' + CHAR(10) + ISNULL(''INCLUDE (['' + IncludedColumns + '') '', '''') + ISNULL('' WHERE  '' + I.filter_definition, '''') + ''WITH ('' + CASE
                                                                                                                                                                                                                                                                                                                                                                            WHEN I.is_padded = 1
                                                                                                                                                                                                                                                                                                                                                                            THEN '' PAD_INDEX = ON''
                                                                                                                                                                                                                                                                                                                                                                            ELSE ''PAD_INDEX = OFF''
                                                                                                                                                                                                                                                                                                                                                                        END + '','' + -- default value
              CASE
                  WHEN ST.no_recompute = 0
                  THEN '' STATISTICS_NORECOMPUTE = OFF''
                  ELSE '' STATISTICS_NORECOMPUTE = ON''
              END + '','' + '' SORT_IN_TEMPDB = OFF'' + '','' + CASE
                                                              WHEN I.ignore_dup_key = 1
                                                              THEN '' IGNORE_DUP_KEY = ON''
                                                              ELSE '' IGNORE_DUP_KEY = OFF''
                                                          END + '','' + '' ONLINE = OFF'' + '','' + CASE
                                                                                                  WHEN I.allow_row_locks = 1
                                                                                                  THEN '' ALLOW_ROW_LOCKS = ON''
                                                                                                  ELSE '' ALLOW_ROW_LOCKS = OFF''
                                                                                              END + '','' + CASE
                                                                                                              WHEN I.allow_page_locks = 1
                                                                                                              THEN '' ALLOW_PAGE_LOCKS = ON''
                                                                                                              ELSE '' ALLOW_PAGE_LOCKS = OFF''
                                                                                                          END + '','' + CASE
                                                                                                                          WHEN I.optimize_for_sequential_key = 1
                                                                                                                          THEN '' OPTIMIZE_FOR_SEQUENTIAL_KEY = ON''
                                                                                                                          ELSE '' OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF''
                                                                                                                      END + '') ON ['' + DS.name + '']'' + '';'' [CreateIndexScript]
       FROM sys.indexes I
            JOIN sys.tables T ON T.object_id = I.object_id
            JOIN sys.sysindexes SI ON I.object_id = SI.id
                                      AND I.index_id = SI.indid
            JOIN
       (
           SELECT *
           FROM
           (
               SELECT IC2.object_id,
                      IC2.index_id,
                      STUFF(
               (
                   SELECT '','' + CHAR(9) + CHAR(10) + CHAR(9) + ''['' + C.name + CASE
                                                                                  WHEN MAX(CONVERT(INT, IC1.is_descending_key)) = 1
                                                                                  THEN '']'' + '' DESC''
                                                                                  ELSE '']'' + '' ASC''
                                                                              END
                   FROM sys.index_columns IC1
                        JOIN sys.columns C ON C.object_id = IC1.object_id
                                              AND C.column_id = IC1.column_id
                                              AND IC1.is_included_column = 0
                   WHERE IC1.object_id = IC2.object_id
                         AND IC1.index_id = IC2.index_id
                   GROUP BY IC1.object_id,
                            C.name,
                            index_id
                   ORDER BY MAX(IC1.key_ordinal) FOR XML PATH('''')
               ), 1, 2, '''') KeyColumns
               FROM sys.index_columns IC2
               GROUP BY IC2.object_id,
                        IC2.index_id
           ) tmp3
       ) tmp4 ON I.object_id = tmp4.object_id
                 AND I.Index_id = tmp4.index_id
            JOIN sys.stats ST ON ST.object_id = I.object_id
                                 AND ST.stats_id = I.index_id
            JOIN sys.data_spaces DS ON I.data_space_id = DS.data_space_id
            JOIN sys.filegroups FG ON I.data_space_id = FG.data_space_id
            LEFT JOIN
       (
           SELECT *
           FROM
           (
               SELECT IC2.object_id,
                      IC2.index_id,
                      STUFF(
               (
                   SELECT '','' + ''['' + C.name + '']''
                   FROM sys.index_columns IC1
                        JOIN sys.columns C ON C.object_id = IC1.object_id
                                              AND C.column_id = IC1.column_id
                                              AND IC1.is_included_column = 1
                   WHERE IC1.object_id = IC2.object_id
                         AND IC1.index_id = IC2.index_id
                   GROUP BY IC1.object_id,
                            C.name,
                            index_id FOR XML PATH('''')
               ), 1, 2, '''') IncludedColumns
               FROM sys.index_columns IC2
               GROUP BY IC2.object_id,
                        IC2.index_id
           ) tmp1
           WHERE IncludedColumns IS NOT NULL
       ) tmp2 ON tmp2.object_id = I.object_id
                 AND tmp2.index_id = I.index_id
        JOIN  sys.schemas s on s.schema_id = T.schema_id
       WHERE I.is_primary_key = 0
             AND I.is_unique_constraint = 0;




/* #row_store_indexes store creation scripts of columnstore indexes for each table. If you don`t have columnstore indexes, no need to change the script, this part won`t return any results */

INSERT INTO #col_store_indexes
       SELECT s.name + ''.'' + T.name,
              I.name,
              ''CREATE ''+ I.type_desc COLLATE DATABASE_DEFAULT + '' INDEX '' + ''['' + I.name + '']'' + '' ON '' + ''['' + SCHEMA_NAME(T.schema_id) + '']'' + ''.'' + ''['' + T.name + '']'' + CHAR(10)  + ''WITH ('' +



              CASE
                  WHEN I.type = 5
                  THEN ''DROP_EXISTING = OFF, COMPRESSION_DELAY = 0''

                  END + '') ON ['' + DS.name + '']'' + '';'' [CreateIndexScript]
       FROM sys.indexes I
            JOIN sys.tables T ON T.object_id = I.object_id
            JOIN sys.sysindexes SI ON I.object_id = SI.id
                                      AND I.index_id = SI.indid

            JOIN sys.data_spaces DS ON I.data_space_id = DS.data_space_id
            JOIN sys.filegroups FG ON I.data_space_id = FG.data_space_id
            LEFT JOIN
       (
           SELECT *
           FROM
           (
               SELECT IC2.object_id,
                      IC2.index_id,
                      STUFF(
               (
                   SELECT '','' + ''['' + C.name + '']''
                   FROM sys.index_columns IC1
                        JOIN sys.columns C ON C.object_id = IC1.object_id
                                              AND C.column_id = IC1.column_id
                                              AND IC1.is_included_column = 1
                   WHERE IC1.object_id = IC2.object_id
                         AND IC1.index_id = IC2.index_id
                   GROUP BY IC1.object_id,
                            C.name,
                            index_id FOR XML PATH('''')
               ), 1, 2, '''') IncludedColumns
               FROM sys.index_columns IC2
               GROUP BY IC2.object_id,
                        IC2.index_id
           ) tmp1
           WHERE IncludedColumns IS NOT NULL
       ) tmp2 ON tmp2.object_id = I.object_id
                 AND tmp2.index_id = I.index_id
        JOIN  sys.schemas s on s.schema_id = T.schema_id
       WHERE I.is_primary_key = 0
             AND I.is_unique_constraint = 0;




/* #filegroups stores filegroup name for each table and index */

SELECT s.name +''.'' + o.[name] table_name,
       o.[type] table_type,
       i.[name] index_name,
       i.[index_id],
       f.[name] file_name
INTO #filegroups
FROM sys.indexes i
     INNER JOIN sys.filegroups f ON i.data_space_id = f.data_space_id
     INNER JOIN sys.all_objects o ON i.[object_id] = o.[object_id]
     INNER JOIN sys.schemas s ON s.schema_id = o.schema_id
WHERE i.data_space_id = f.data_space_id
      AND o.type = ''U'' -- User Created Tables
ORDER BY f.name;


/* #create_dates stores creation and modification dates for each table*/

SELECT S.NAME +''.'' + T.[name] AS [table_name],
       create_date,
       modify_date
INTO #create_dates
FROM sys.tables T
INNER JOIN sys.schemas s ON s.schema_id = T.schema_id


/* creating indexes table_name columns in order to speed up the JOINs*/

CREATE NONCLUSTERED INDEX IX_table_name ON #tables(table_name);
CREATE NONCLUSTERED INDEX IX_table_name ON #row_store_indexes(table_name);
CREATE NONCLUSTERED INDEX IX_table_name ON #altered(table_name);



/* elimination noise values on alter_table field */

UPDATE #altered
  SET
      alter_table = NULL
WHERE alter_table NOT LIKE ''%ALTER%'';


/* #result1 stores JOIN result of tables with corresponding alter script and outer applying the resuls with filegroup values*/

     SELECT
       DISTINCT
       DB_NAME() as [db_name],
       (t.table_name),
       REPLACE(t.create_table, ''NULL,'' + CHAR(10) + '')'', ''NULL'' + CHAR(10) + '')'') + '' ON '' + ''['' + f.file_name + ''];'' AS create_table,
       REPLACE(a.alter_table, ''NULL,'' + CHAR(10) + '')'', ''NULL'' + CHAR(10) + '')'') + '' ON '' + ''['' + f.file_name + ''];'' AS alter_table,
       c.create_date
into #result1
FROM #tables t
     LEFT JOIN #create_dates c on  t.table_name = c.table_name
     LEFT JOIN #altered a on t.table_name = a.table_name

     outer apply
     (

      SELECT top 1 * FROM #filegroups f
      where t.table_name = f.table_name

     ) f




/* #all_indexes combines result of both rowstore and columnstore indexes */

     INSERT into #all_indexes
     SELECT * FROM #row_store_indexes where create_index is not null
     union
     SELECT * FROM #col_store_indexes where create_index is not null






/* #result2 stores JOIN result of tables and with all corresponding indexes, filegroup and tables` creation dates */


       SELECT
       DISTINCT
       DB_NAME() as [db_name],
       (t.table_name),

       REPLACE(t.create_table, ''NULL,'' + CHAR(10) + '')'', ''NULL'' + CHAR(10) + '')'') + '' ON '' + ''['' + f.file_name + ''];''  AS create_table,
       REPLACE(i.create_index, ''NULL,'' + CHAR(10) + '')'', ''NULL'' + CHAR(10) + '')'')  AS create_index,
       c.create_date
into #result2
FROM #tables t
     LEFT JOIN #create_dates c on  t.table_name = c.table_name
     LEFT JOIN #all_indexes i on t.table_name = i.table_name
     outer apply
     (

      SELECT top 1 * FROM #filegroups f
      where t.table_name = f.table_name

     ) f


/* above results are JOINed with ALTER scripts */

SELECT r2.[db_name],
       r2.[table_name],
       r2.[create_table],
       r1.[alter_table],
       r2.[create_index],
       r2.[create_date]

FROM #result2 r2
LEFT JOIN #result1 r1
   on r2.table_name = r1.table_name
order by r1.create_date
'
INSERT into #tmp
exec (@tsql);

END TRY
        BEGIN CATCH
            PRINT @tsql;
        END CATCH;
        FETCH NEXT FROM db_cursor INTO @dbname;
    END;
CLOSE db_cursor;
DEALLOCATE db_cursor;

SELECT * FROM #tmp
where  @Database IS NULL or [db_name] = @Database
order by [db_name], create_date

END
GO