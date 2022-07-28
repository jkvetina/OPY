PURGE RECYCLEBIN;

/*
BEGIN
    -- drop constraints
    FOR c IN (
        SELECT table_name, constraint_name
        FROM user_constraints
    ) LOOP
        DBMS_OUTPUT.PUT('.');
        BEGIN
            DBMS_UTILITY.EXEC_DDL_STATEMENT('ALTER TABLE ' || c.table_name || ' DROP CONSTRAINT "' || c.constraint_name || '"');
        EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE(c.table_name || ':' || c.constraint_name);
        END;
    END LOOP;
END;
/

BEGIN
    -- drop indexes
    FOR c IN (
        SELECT object_type, object_name
        FROM user_objects
        WHERE object_name NOT IN (
                SELECT object_name FROM RECYCLEBIN
            )
            AND object_type IN (
                'INDEX'
            )
    ) LOOP
        DBMS_OUTPUT.PUT('.');
        BEGIN
            DBMS_UTILITY.EXEC_DDL_STATEMENT('DROP ' || c.object_type || ' "' || c.object_name || '"');
        EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE(c.index_name);
        END;
    END LOOP;
END;
/

BEGIN
    -- remove all jobs
    FOR c IN (
        SELECT j.job_name
        FROM user_scheduler_running_jobs j
    ) LOOP
        BEGIN
            DBMS_SCHEDULER.STOP_JOB(c.job_name);
        EXCEPTION
        WHEN OTHERS THEN
            NULL;
        END;
    END LOOP;
    --
    FOR c IN (
        SELECT j.job_name
        FROM user_scheduler_jobs j
    ) LOOP
        BEGIN
            DBMS_OUTPUT.PUT('.');
            DBMS_SCHEDULER.DROP_JOB(c.job_name, TRUE);
        EXCEPTION
        WHEN OTHERS THEN
            NULL;
        END;
    END LOOP;
END;
/

BEGIN
    -- drop materialized views
    FOR c IN (
        SELECT object_name
        FROM user_objects
        WHERE object_type = 'MATERIALIZED VIEW'
    ) LOOP
        DBMS_OUTPUT.PUT('.');
        DBMS_UTILITY.EXEC_DDL_STATEMENT('DROP MATERIALIZED VIEW "' || c.object_name || '"');
    END LOOP;
END;
/

BEGIN
    -- drop objects
    FOR c IN (
        SELECT object_type, object_name
        FROM user_objects
        WHERE object_name NOT IN (
                SELECT object_name FROM RECYCLEBIN
            )
            AND object_type IN (
                'VIEW',
                'TRIGGER',
                'PACKAGE',
                'PACKAGE BODY',
                'FUNCTION',
                'PROCEDURE'
            )
        ORDER BY
            DECODE(object_type,
                'TRIGGER',      1,
                'PACKAGE BODY', 2,
                'PACKAGE',      3,
                'VIEW',         4,
                'FUNCTION',     5,
                'PROCEDURE',    6,
                9
            ), object_name DESC
    ) LOOP
        DBMS_OUTPUT.PUT('.');
        DBMS_UTILITY.EXEC_DDL_STATEMENT('DROP ' || c.object_type || ' "' || c.object_name || '"');
    END LOOP;
    DBMS_OUTPUT.PUT_LINE('');
END;
/
*/
