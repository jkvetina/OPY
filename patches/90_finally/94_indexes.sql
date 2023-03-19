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
        DBMS_OUTPUT.PUT_LINE(c.table_name || ' ->' || c.index_name);
        EXECUTE IMMEDIATE
            'ALTER INDEX ' || c.index_name ||
            ' REBUILD TABLESPACE ' || in_target_tablespace;
    END LOOP;
END;
/
*/

