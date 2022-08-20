# get database objects overview
query_summary = """
WITH a AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY a.object_type) AS r#,
        a.object_type,
        COUNT(*) AS object_count
    FROM user_objects a
    WHERE a.object_type NOT IN ('LOB', 'TABLE PARTITION')
    GROUP BY a.object_type
),
c AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY c.constraint_type) AS r#,
        c.constraint_type,
        COUNT(*) AS constraint_count
    FROM user_constraints c
    GROUP BY c.constraint_type
)
SELECT
    a.object_type,
    a.object_count,
    c.constraint_type,
    c.constraint_count
FROM a
FULL JOIN c ON c.r# = a.r#
ORDER BY NVL(a.r#, c.r#)"""

# objects to process
query_objects = """
SELECT
    object_type,
    object_name
FROM (
    SELECT DISTINCT o.object_type, o.object_name
    FROM user_objects o
    WHERE 1 = 1
        AND o.object_type NOT IN ('LOB', 'TABLE PARTITION')
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
        AND (o.object_type, o.object_name) NOT IN (
            SELECT
                'JOB'           AS object_type,
                j.job_name      AS object_name
            FROM user_scheduler_jobs j
            WHERE j.job_style NOT IN ('REGULAR')
        )
    UNION ALL
    SELECT 'JOB' AS object_type, j.job_name AS object_name
    FROM user_scheduler_jobs j
    WHERE :recent IS NULL
        AND (:object_type = 'JOB' OR :object_type IS NULL)
)
ORDER BY
    CASE object_type {}ELSE 999 END,
    object_name"""

# get APEX security (authorization) schemes names
query_apex_authz_schemes = """
SELECT
    a.authorization_scheme_id       AS auth_id,
    a.authorization_scheme_name     AS auth_name
FROM apex_application_authorization a
WHERE a.application_id = :app_id"""

# get authentication scheme marked as PROD default
query_apex_authn_default_scheme = """
SELECT NVL(MAX(a.authentication_scheme_id), 0) AS scheme_id
FROM apex_application_auth a
WHERE a.application_id                  = :app_id
    AND a.authentication_scheme_name    LIKE '%PROD_DEFAULT'"""

# get APEX names for List of Values
query_apex_lov_names = """
SELECT
    l.lov_id,
    l.list_of_values_name AS lov_name
FROM apex_application_lovs l
WHERE l.application_id = :app_id"""

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

query_describe_object = """
SELECT DBMS_METADATA.GET_DDL(REPLACE(o.object_type, ' ', '_'), o.object_name) AS object_desc
FROM user_objects o
WHERE o.object_type     = :object_type
    AND o.object_name   = :object_name"""

# export jobs
query_describe_job = """
SELECT DBMS_METADATA.GET_DDL('PROCOBJ', job_name) AS object_desc
FROM user_scheduler_jobs
WHERE job_name = :object_name"""

query_describe_job_details = """
SELECT job_name, enabled, job_priority
FROM user_scheduler_jobs j
WHERE j.job_name = :job_name"""

query_describe_job_args = """
SELECT
    j.argument_name,
    j.argument_position,
    j.argument_type,
    j.value
FROM user_scheduler_job_args j
WHERE j.job_name = :job_name
ORDER BY j.argument_position"""

# template used to extract jobs
job_template = """DECLARE
    in_job_name             CONSTANT VARCHAR2(30)   := '{}';
    in_run_immediatelly     CONSTANT BOOLEAN        := FALSE;
BEGIN
    BEGIN
        DBMS_SCHEDULER.DROP_JOB(in_job_name, TRUE);
    EXCEPTION
    WHEN OTHERS THEN
        NULL;
    END;
    --
    DBMS_SCHEDULER.CREATE_JOB (
{}
    );
    --{}
    DBMS_SCHEDULER.SET_ATTRIBUTE(in_job_name, 'JOB_PRIORITY', {});
    {}DBMS_SCHEDULER.ENABLE(in_job_name);
    COMMIT;
    --
    IF in_run_immediatelly THEN
        DBMS_SCHEDULER.RUN_JOB(in_job_name);
        COMMIT;
    END IF;
END;
/
"""

# get applications from the same schema
query_apex_applications = """
SELECT
    a.application_id,
    a.application_name,
    a.pages,
    TO_CHAR(a.last_updated_on, 'YYYY-MM-DD HH24:MI') AS last_updated_on,
    w.workspace,
    w.workspace_id
FROM apex_applications a
JOIN apex_workspace_schemas s
    ON s.workspace_id   = a.workspace_id
    AND s.schema        = a.owner
JOIN apex_workspaces w
    ON w.workspace_id   = a.workspace_id
WHERE a.owner           = :schema
ORDER BY 1"""

query_apex_app_detail = """
SELECT
    w.workspace,
    --w.workspace_id
    a.owner,
    a.application_group             AS app_group,
    a.application_id                AS app_id,
    a.alias                         AS app_alias,
    a.application_name              AS app_name,
    a.pages,
    NULLIF(a.application_items, 0)          AS items,
    NULLIF(a.application_processes, 0)      AS processes,
    NULLIF(a.application_computations, 0)   AS computations,
    NULLIF(a.application_settings, 0)       AS settings,
    NULLIF(a.lists, 0)                      AS lists,
    NULLIF(a.lists_of_values, 0)            AS lovs,
    NULLIF(a.web_services, 0)               AS ws,
    NULLIF(a.translation_messages, 0)       AS translations,
    NULLIF(a.build_options, 0)              AS build_options,
    NULLIF(a.authorization_schemes, 0)      AS authz_schemes,
    --
    CASE WHEN a.authentication_scheme_type != 'No Authentication' THEN a.authentication_scheme END AS authn_scheme,
    --a.availability_status,
    CASE WHEN a.db_session_init_code IS NOT NULL THEN 'Y' END       AS has_init_code,
    CASE WHEN a.db_session_cleanup_code IS NOT NULL THEN 'Y' END    AS has_cleanup,
    CASE WHEN a.friendly_url = 'Yes' THEN 'Y' END                   AS has_friendly_url,
    CASE WHEN a.debugging = 'Allowed' THEN 'Y' END                  AS has_debugging,
    CASE WHEN a.error_handling_function IS NOT NULL THEN 'Y' END    AS has_error_fn,
    --
    a.compatibility_mode,
    TO_CHAR(a.created_on, 'YYYY-MM-DD HH24:MI')         AS created_at,
    TO_CHAR(a.last_updated_on, 'YYYY-MM-DD HH24:MI')    AS changed_at
FROM apex_applications a
JOIN apex_workspace_schemas s
    ON s.workspace_id       = a.workspace_id
    AND s.schema            = a.owner
JOIN apex_workspaces w
    ON w.workspace_id       = a.workspace_id
WHERE a.application_id      = :app_id"""

# get date based on -r argument, r=1 means object changed today since midnight
query_today = """
SELECT
    TO_CHAR(TRUNC(SYSDATE) + 1 - :recent, 'YYYY-MM-DD') AS today,
    SYS_CONTEXT('USERENV', 'CURRENT_USER')              AS curr_user
FROM DUAL"""

query_version_apex = """
SELECT a.version_no AS version
FROM apex_release a"""

query_version_db = """
SELECT p.version_full AS version
FROM product_component_version p"""

# get database version for older systems
query_version_db_old = """
SELECT p.version
FROM product_component_version p
WHERE p.product LIKE 'Oracle Database%'"""

# get all compatible columns to export table to CSV
query_csv_columns = """
SELECT LISTAGG(c.column_name, ', ') WITHIN GROUP (ORDER BY c.column_id) AS cols
FROM user_tab_cols c
WHERE c.table_name  = UPPER(:table_name)
    AND c.data_type NOT IN ('BLOB', 'CLOB', 'XMLTYPE', 'JSON')"""

# get primary key columns to MERGE statement
query_csv_primary_columns = """
WITH p AS (
    SELECT c.column_name, c.position
    FROM user_cons_columns c
    JOIN user_constraints n
        ON n.table_name         = c.table_name
        AND n.constraint_name   = c.constraint_name
    WHERE n.table_name          = UPPER(:table_name)
        AND n.constraint_type   = 'P'
)
SELECT MAX(t.columns) AS columns
FROM (
    SELECT LISTAGG(p.column_name, ',') WITHIN GROUP (ORDER BY p.position) AS columns
    FROM p
    UNION ALL
    SELECT LISTAGG(c.column_name, ',') WITHIN GROUP (ORDER BY c.position) AS columns
    FROM user_cons_columns c
    JOIN user_constraints n
        ON n.table_name         = c.table_name
        AND n.constraint_name   = c.constraint_name
    WHERE n.table_name          = UPPER(:table_name)
        AND n.constraint_type   = 'U'
        AND NOT EXISTS (
            SELECT 1
            FROM p
        )
) t"""

# template for MERGE statement from CSV file
template_csv_merge = """MERGE INTO {table_name} t
USING (
    {csv_content_query}
) s
ON ({primary_cols_set})
{skip_update}WHEN MATCHED THEN
{skip_update}    UPDATE SET
{skip_update}        {non_primary_cols_set}
WHEN NOT MATCHED THEN
    INSERT (
        {all_cols}
    )
    VALUES (
        {all_values}
    );\n"""

# table dependencies
query_tables_dependencies = """
SELECT
    c.table_name,
    LISTAGG(r.table_name, ', ') WITHIN GROUP (ORDER BY r.table_name) AS references
FROM user_constraints c
JOIN user_constraints r
    ON r.constraint_name    = c.r_constraint_name
    AND r.owner             = c.owner
WHERE c.constraint_type     = 'R'
    AND c.owner             = c.r_owner
    AND c.status            = 'ENABLED'
GROUP BY c.table_name
ORDER BY 1"""

# grants made by current schema
query_grants_made = """
SELECT
    t.type,
    t.table_name,
    APEX_STRING.FORMAT (
        'GRANT %0 ON %1 TO %2%3;',
        t.privs,
        RPAD(LOWER(t.table_name), 30),
        LOWER(t.grantee),
        CASE WHEN t.grantable = 'YES' THEN ' WITH GRANT OPTION' END
    ) AS sql
FROM (
    SELECT
        t.type,
        t.table_name,
        LISTAGG(DISTINCT t.privilege, ', ') WITHIN GROUP (ORDER BY t.privilege) AS privs,
        LISTAGG(DISTINCT t.grantee, ', ')   WITHIN GROUP (ORDER BY t.grantee)   AS grantee,
        t.grantable
    FROM user_tab_privs_made t
    WHERE t.grantor     = USER
        AND t.type      NOT IN ('USER')
    GROUP BY
        t.type,
        t.table_name,
        t.grantable
) t
ORDER BY 1, 2, 3"""

# overwrite for old databases not supporting LISTAGG DISTINCT
query_grants_made = """
WITH t AS (
    SELECT
        t.type,
        t.table_name,
        t.grantable,
        t.grantee,
        t.privilege
    FROM user_tab_privs_made t
    WHERE t.grantor     = USER
        AND t.type      NOT IN ('USER')
),
g AS (
    SELECT
        t.type,
        t.table_name,
        t.grantable,
        LISTAGG(t.grantee, ', ') WITHIN GROUP (ORDER BY t.grantee) AS grantee
    FROM (
        SELECT
            t.type,
            t.table_name,
            t.grantable,
            t.grantee
        FROM t
        GROUP BY
            t.type,
            t.table_name,
            t.grantable,
            t.grantee
    ) t
    GROUP BY
        t.type,
        t.table_name,
        t.grantable
),
p AS (
    SELECT
        t.type,
        t.table_name,
        t.grantable,
        LISTAGG(t.privilege, ', ') WITHIN GROUP (ORDER BY t.privilege) AS privs
    FROM (
        SELECT
            t.type,
            t.table_name,
            t.grantable,
            t.privilege
        FROM t
        GROUP BY
            t.type,
            t.table_name,
            t.grantable,
            t.privilege
    ) t
    GROUP BY
        t.type,
        t.table_name,
        t.grantable
)
SELECT DISTINCT
    t.type,
    t.table_name,
    APEX_STRING.FORMAT (
        'GRANT %0 ON %1 TO %2%3;',
        p.privs,
        RPAD(LOWER(t.table_name), 30),
        LOWER(g.grantee),
        CASE WHEN t.grantable = 'YES' THEN ' WITH GRANT OPTION' END
    ) AS sql
FROM t
JOIN g
    ON g.type           = t.type
    AND g.table_name    = t.table_name
    AND g.grantable     = t.grantable
JOIN p
    ON p.type           = t.type
    AND p.table_name    = t.table_name
    AND p.grantable     = t.grantable
ORDER BY 1, 2, 3"""

# list objects needed before requested object
query_objects_before = """
SELECT DISTINCT
    d.referenced_type       AS type,
    d.referenced_name       AS name
FROM user_dependencies d
WHERE d.name                = :object_name
    AND d.referenced_owner  = USER
ORDER BY 1, 2"""

# list objects dependent on requested object
query_objects_after = """
SELECT DISTINCT
    d.type,
    d.name
FROM user_dependencies d
WHERE d.referenced_name     = :object_name
    AND d.name              != d.referenced_name
ORDER BY 1, 2"""

