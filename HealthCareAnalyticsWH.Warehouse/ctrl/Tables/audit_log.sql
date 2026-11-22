CREATE TABLE [ctrl].[audit_log] (
    [audit_id]          BIGINT        IDENTITY NOT NULL,
    [adf_run_id]        VARCHAR (100) NOT NULL,
    [table_id]          BIGINT        NOT NULL,
    [stage]             VARCHAR (20)  NOT NULL,
    [status]            VARCHAR (20)  NOT NULL,
    [records_written]   BIGINT        NULL,
    [start_time]        DATETIME2 (6) NULL,
    [end_time]          DATETIME2 (6) NULL,
    [created_timestamp] DATETIME2 (6) NULL
);


GO