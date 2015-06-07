-- Started with this: http://stackoverflow.com/questions/21781351/how-can-i-clean-up-the-ssisdb
USE SSISDB;
SET NOCOUNT ON;
IF object_id('tempdb..#DELETE_CANDIDATES') IS NOT NULL
BEGIN
    DROP TABLE #DELETE_CANDIDATES;
END;

CREATE TABLE #DELETE_CANDIDATES
(
    operation_id bigint NOT NULL PRIMARY KEY
);


DECLARE @delete_batch_size bigint = 10000;
DECLARE @rows_affected bigint = @delete_batch_size;
DECLARE @DaysRetention int = 10;

INSERT INTO
    #DELETE_CANDIDATES
(
    operation_id
)
SELECT
    IO.operation_id
FROM
    internal.operations AS IO
WHERE
    IO.start_time < DATEADD(day, -@DaysRetention, CURRENT_TIMESTAMP);

WHILE (@rows_affected = @delete_batch_size)
BEGIN
	BEGIN TRANSACTION;
		DELETE top(@delete_batch_size) T
		FROM
			internal.executable_statistics AS T
		WHERE start_time <= DATEADD(day, -@DaysRetention, CURRENT_TIMESTAMP)
		SET @rows_affected = @@ROWCOUNT
	COMMIT TRANSACTION;
	CHECKPOINT;
END

WHILE (@rows_affected = @delete_batch_size)
BEGIN
	BEGIN TRANSACTION;
		DELETE top(@delete_batch_size) T
		FROM
			internal.event_message_context AS T
			INNER JOIN
			#DELETE_CANDIDATES AS DC
				ON DC.operation_id = T.operation_id;

		SET @rows_affected = @@ROWCOUNT
	COMMIT TRANSACTION;
	CHECKPOINT;
END

WHILE (@rows_affected = @delete_batch_size)
BEGIN
	BEGIN TRANSACTION;
	DELETE top(@delete_batch_size) T
	FROM
		internal.event_messages AS T
		INNER JOIN
			#DELETE_CANDIDATES AS DC
			ON DC.operation_id = T.operation_id;
		SET @rows_affected = @@ROWCOUNT
	COMMIT TRANSACTION;
	CHECKPOINT;
END


WHILE (@rows_affected = @delete_batch_size)
BEGIN
	BEGIN TRANSACTION;
	DELETE top(@delete_batch_size) T
	FROM
		internal.operation_messages AS T
		INNER JOIN
			#DELETE_CANDIDATES AS DC
			ON DC.operation_id = T.operation_id;

		SET @rows_affected = @@ROWCOUNT
	COMMIT TRANSACTION;
	CHECKPOINT;
END

-- etc
-- Finally, remove the entry from operations
--WHILE (@rows_affected = @delete_batch_size)
--BEGIN
--	BEGIN TRANSACTION;
--	DELETE top(@delete_batch_size) T
--	FROM
--		internal.operations AS T
--		INNER JOIN
--			#DELETE_CANDIDATES AS DC
--			ON DC.operation_id = T.operation_id;

--		SET @rows_affected = @@ROWCOUNT
--	COMMIT TRANSACTION;
--	CHECKPOINT;
--END