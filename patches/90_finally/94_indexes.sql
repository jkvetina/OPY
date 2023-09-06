/*
-- move indexes to new tablespace
DECLARE
    in_target_tablespace CONSTANT VARCHAR2(30) := USER || '_IDX';
BEGIN
    FOR c IN (
        SELECT i.table_name, i.index_name
        FROM user_indexes i
        WHERE i.index_type          LIKE '%NORMAL%'
            AND i.tablespace_name   != in_target_tablespace
    ) LOOP
        BEGIN
            DBMS_OUTPUT.PUT_LINE(RPAD(c.table_name, ' ', 30) || ' -> ' || c.index_name);
            EXECUTE IMMEDIATE
                'ALTER INDEX ' || c.index_name ||
                ' REBUILD TABLESPACE ' || in_target_tablespace;
        EXCEPTION
        WHEN OTHERS THEN
            NULL;
        END;
    END LOOP;
END;
/
*/

-- create missing FK indexes
DECLARE
    in_prefix       CONSTANT VARCHAR2(30) := 'XX%';
BEGIN
    DBMS_OUTPUT.PUT_LINE('--');
    DBMS_OUTPUT.PUT_LINE('-- MISSING INDEXES:');
    DBMS_OUTPUT.PUT_LINE('--');
    --
    FOR c IN (
        WITH f AS (
            SELECT
                t.table_name,
                t.constraint_name AS index_name,
                LISTAGG(t.column_name, ', ') WITHIN GROUP (ORDER BY t.position) AS cols
            FROM user_cons_columns t
            JOIN user_constraints n
                ON n.constraint_name    = t.constraint_name
            WHERE n.constraint_type     = 'R'
                AND t.table_name        LIKE in_prefix || '%' ESCAPE '\'
            GROUP BY t.table_name, t.constraint_name
        )
        SELECT
            f.*, i.index_name AS existing_index, i.cols AS index_cols,
            --
            'CREATE INDEX ' || RPAD(f.index_name, 30) ||
                ' ON ' || RPAD(f.table_name, 30) || ' (' || f.cols || ') COMPUTE STATISTICS' AS fix
        FROM f
        LEFT JOIN (
            SELECT i.table_name, i.index_name, LISTAGG(i.column_name, ', ') WITHIN GROUP (ORDER BY i.column_position) AS cols
            FROM user_ind_columns i
            GROUP BY i.table_name, i.index_name
        ) i
            ON i.table_name     = f.table_name
            AND i.cols          LIKE f.cols || '%'
        WHERE i.index_name      IS NULL  -- show only missing indexes
        ORDER BY 1, 2
    ) LOOP
        DBMS_OUTPUT.PUT_LINE(c.fix || ';');
        --EXECUTE IMMEDIATE c.fix;
    END LOOP;
    --
    DBMS_OUTPUT.PUT_LINE('');
END;
/




