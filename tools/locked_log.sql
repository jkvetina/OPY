--
-- query to help you build locked.log manually
--
WITH x AS (
    SELECT /*+ MATERIALIZE */
        '%' AS object_like
    FROM DUAL
)
SELECT
    'indexes/' || LOWER(i.index_name) || '.sql' AS locked_log
FROM (
    SELECT /*+ MATERIALIZE */
        i.table_name,
        i.index_name,
        LISTAGG(i.column_name, ', ') WITHIN GROUP (ORDER BY i.column_position) AS index_cols
    FROM user_ind_columns i
    CROSS JOIN x
    WHERE i.table_name          LIKE x.object_like
    GROUP BY i.table_name, i.index_name
) i
LEFT JOIN (
    SELECT /*+ MATERIALIZE */
        t.table_name,
        t.constraint_name,
        LISTAGG(t.column_name, ', ') WITHIN GROUP (ORDER BY t.position) AS constraint_cols
    FROM user_cons_columns t
    JOIN user_constraints n
        ON n.constraint_name    = t.constraint_name
    CROSS JOIN x
    WHERE n.constraint_type     IN ('P', 'U')
        AND t.table_name        LIKE x.object_like
    GROUP BY t.table_name, t.constraint_name
) c
    ON c.table_name         = i.table_name
    AND c.constraint_cols   = i.index_cols
WHERE c.constraint_name     IS NULL
--
UNION ALL
SELECT
    CASE o.object_type
        WHEN 'MATERIALIZED VIEW'    THEN 'mviews'
        WHEN 'PACKAGE BODY'         THEN 'packages'
        ELSE LOWER(o.object_type) || 's'
        END ||
    '/' ||
    LOWER(o.object_name) ||
    CASE o.object_type
        WHEN 'PACKAGE' THEN '.spec.sql'
        ELSE '.sql'
        END AS locked_log
FROM user_objects o
CROSS JOIN x
WHERE o.object_name LIKE x.object_like
    AND o.object_type NOT IN ('INDEX')
ORDER BY 1;


