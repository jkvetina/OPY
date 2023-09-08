--
-- move audit columns to the bottom, ignore mviews
--
PROMPT MOVE AUDIT COLUMNS
--
DECLARE
    in_table_name       CONSTANT VARCHAR2(30)   := 'XX\_%';
    in_columns          CONSTANT VARCHAR2(4000) := 'CREATED_BY,CREATED_AT,UPDATED_BY,UPDATED_AT,DELETED_BY,DELETED_AT';
BEGIN
    FOR t IN (
        SELECT t.table_name
        FROM user_tables t
        LEFT JOIN user_mviews m
            ON m.mview_name     = t.table_name
        WHERE t.table_name      LIKE UPPER(in_table_name) ESCAPE '\'
            AND m.mview_name    IS NULL
    ) LOOP
        FOR c IN (
            WITH x AS (
                SELECT
                    LEVEL AS r#,
                    UPPER(REGEXP_SUBSTR(in_columns, '[^,]+', 1, LEVEL)) AS column_name
                FROM DUAL
                CONNECT BY LEVEL <= REGEXP_COUNT(in_columns, ',') + 1
            )
            SELECT c.table_name, c.column_name
            FROM user_tab_cols c
            JOIN x
                ON x.column_name    = c.column_name
            WHERE c.table_name      = t.table_name
            ORDER BY x.r# ASC
        ) LOOP
            DBMS_OUTPUT.PUT_LINE('  MOVING ' || c.table_name || '.' || c.column_name);
            --
            EXECUTE IMMEDIATE
                'ALTER TABLE ' || c.table_name || ' MODIFY ' || c.column_name || ' INVISIBLE';
            EXECUTE IMMEDIATE
                'ALTER TABLE ' || c.table_name || ' MODIFY ' || c.column_name || ' VISIBLE';
        END LOOP;
    END LOOP;
END;
/

