# get database objects overview
query_all_objects = """
SELECT object_type, COUNT(*) AS count_
FROM user_objects
WHERE object_type NOT IN ('LOB', 'TABLE PARTITION')
GROUP BY object_type
ORDER BY 1"""

# objects to process
query_objects = """
SELECT DISTINCT o.object_type, o.object_name
FROM user_objects o
WHERE o.object_type IN ('PACKAGE', 'PACKAGE BODY', 'PROCEDURE', 'FUNCTION', 'TRIGGER', 'TABLE', 'VIEW', 'SEQUENCE', 'INDEX', 'MATERIALIZED VIEW')
    AND o.object_type LIKE :object_type || '%'
    AND o.object_name NOT LIKE 'SYS\\_%' ESCAPE '\\'
    AND o.object_name NOT LIKE 'ISEQ$$_%'
    AND (o.last_ddl_time >= TRUNC(SYSDATE) + 1 - :recent OR :recent IS NULL)
    AND (o.object_type, o.object_name) NOT IN (
        SELECT
            'INDEX'         AS object_type,
            i.index_name    AS object_name
        FROM (
            SELECT
                i.table_name,
                i.index_name,
                LISTAGG(i.column_name, ', ') WITHIN GROUP (ORDER BY i.column_position) AS index_cols
            FROM user_ind_columns i
            GROUP BY i.table_name, i.index_name
        ) i
        JOIN (
            SELECT
                t.table_name,
                t.constraint_name,
                LISTAGG(t.column_name, ', ') WITHIN GROUP (ORDER BY t.position) AS constraint_cols
            FROM user_cons_columns t
            JOIN user_constraints n
                ON n.constraint_name    = t.constraint_name
            WHERE n.constraint_type     IN ('P', 'U')
            GROUP BY t.table_name, t.constraint_name
        ) c
            ON c.table_name         = i.table_name
            AND c.constraint_cols   = i.index_cols
    )
    AND (o.object_type, o.object_name) NOT IN (
        SELECT
            'TABLE'         AS object_type,
            m.mview_name    AS object_name
        FROM user_mviews m
    )
UNION ALL
SELECT 'JOB' AS object_type, j.job_name AS object_name
FROM user_scheduler_jobs j
WHERE :recent IS NULL
    AND (:object_type = 'JOB' OR :object_type IS NULL)
ORDER BY 1, 2
"""

# get table comments
query_table_comments = """
SELECT m.table_name, m.comments
FROM user_tab_comments m
WHERE m.table_name = :table_name
ORDER BY 1"""

# get column comments
query_column_comments = """
SELECT
    RPAD(LOWER(m.table_name || '.' || m.column_name), MAX(FLOOR(LENGTH(m.table_name || '.' || m.column_name) / 4) * 4 + 5) OVER ()) AS column_name,
    m.comments
FROM user_col_comments m
JOIN user_tab_cols c
    ON c.table_name         = m.table_name
    AND c.column_name       = m.column_name
LEFT JOIN user_views v
    ON v.view_name          = m.table_name
LEFT JOIN user_mviews z
    ON z.mview_name         = m.table_name
WHERE m.table_name          = :table_name
    AND (
        (
            v.view_name         IS NULL
            AND z.mview_name    IS NULL
        )
        OR m.comments           IS NOT NULL
    )
    AND m.table_name    NOT LIKE '%\\_E$' ESCAPE '\\'
    AND (
        m.column_name   NOT IN (
            'UPDATED_BY', 'UPDATED_AT', 'CREATED_BY', 'CREATED_AT'
        )
        OR m.comments   IS NOT NULL
    )
ORDER BY c.column_id"""

# constraints
query_constraints = """
SELECT constraint_type, COUNT(*) AS count_
FROM user_constraints
GROUP BY constraint_type
ORDER BY 1"""

#
query_describe_job = """
SELECT DBMS_METADATA.GET_DDL('PROCOBJ', job_name) AS object_desc
FROM user_scheduler_jobs
WHERE job_name NOT LIKE 'OTDB\\___\\_%' ESCAPE '\\'
    AND job_name NOT LIKE 'OTD\\_%' ESCAPE '\\'
    AND job_name = :object_name"""

query_describe_object = """
SELECT DBMS_METADATA.GET_DDL(REPLACE(o.object_type, ' ', '_'), o.object_name) AS object_desc
FROM user_objects o
WHERE o.object_type IN ('PACKAGE', 'PACKAGE BODY', 'PROCEDURE', 'FUNCTION', 'TRIGGER', 'TABLE', 'VIEW', 'MATERIALIZED VIEW', 'SEQUENCE', 'INDEX')
    AND o.object_type = :object_type
    AND o.object_name = :object_name"""

