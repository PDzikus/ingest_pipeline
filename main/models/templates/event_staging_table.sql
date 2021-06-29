DROP TABLE IF EXISTS {{ table_name }};
-- disabled sqlfluff parse of following section due to bug with UNLOGGED keyword
-- https://github.com/sqlfluff/sqlfluff/issues/1172
CREATE UNLOGGED TABLE {{ table_name }} (    -- noqa: PRS
    event_type INTEGER
    , event_time TIMESTAMP
    , user_email VARCHAR
    , phone_number VARCHAR
    , processing_date DATE
    , PRIMARY KEY (event_type, event_time, user_email, phone_number, processing_date)
);
