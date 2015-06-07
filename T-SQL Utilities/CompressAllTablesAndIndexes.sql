DECLARE @InjectionSQL NVARCHAR(MAX)

DECLARE Execution_Cursor CURSOR FOR
SELECT DISTINCT 'ALTER TABLE [' + sys.schemas.name + '].[' + sys.tables.name+ '] REBUILD PARTITION = ALL WITH (DATA_COMPRESSION = PAGE)' AS InjectionSQL
   FROM
      sys.tables
         INNER JOIN sys.partitions ON sys.partitions.object_id = sys.tables.object_id
         INNER JOIN sys.schemas ON sys.tables.schema_id = sys.schemas.schema_id
      WHERE(sys.partitions.data_compression <> 2)
         AND type = 'U'
UNION
SELECT DISTINCT 'ALTER INDEX [' + sys.indexes.name + '] ON [' + sys.schemas.name + '].[' + sys.tables.name+ '] REBUILD PARTITION = ALL WITH ( STATISTICS_NORECOMPUTE = OFF, ONLINE = OFF, SORT_IN_TEMPDB = OFF, DATA_COMPRESSION = PAGE )'
   FROM
      sys.indexes
         INNER JOIN sys.tables ON sys.indexes.object_id = sys.tables.object_id
         INNER JOIN sys.schemas ON sys.tables.schema_id = sys.schemas.schema_id
         INNER JOIN sys.partitions ON sys.partitions.object_id = sys.indexes.object_id AND sys.partitions.index_id = sys.indexes.index_id
   WHERE(sys.partitions.data_compression <> 2)
      AND (sys.partitions.index_id <> 0)
OPEN Execution_Cursor
FETCH NEXT FROM Execution_Cursor INTO @InjectionSQL

WHILE @@FETCH_STATUS = 0
BEGIN
   EXECUTE (@InjectionSQL)
   FETCH NEXT FROM Execution_Cursor INTO @InjectionSQL
END CLOSE Execution_Cursor
DEALLOCATE Execution_Cursor

