PURGE RECYCLEBIN;
--
DECLARE
    in_owner                CONSTANT VARCHAR2(30)   := USER;
    in_prefix               CONSTANT VARCHAR2(30)   := UPPER('XX\_%');
    --
    in_drop_all             CONSTANT NUMBER(1)      := 0;
    in_drop_tables          CONSTANT NUMBER(1)      := 0;
    in_drop_constraints     CONSTANT NUMBER(1)      := 0;
    in_drop_indexes         CONSTANT NUMBER(1)      := 0;
    in_drop_triggers        CONSTANT NUMBER(1)      := 0;
    in_drop_sequences       CONSTANT NUMBER(1)      := 0;
    in_drop_views           CONSTANT NUMBER(1)      := 0;
    in_drop_mviews          CONSTANT NUMBER(1)      := 0;
    in_drop_packages        CONSTANT NUMBER(1)      := 0;
    in_drop_functions       CONSTANT NUMBER(1)      := 0;
    in_drop_procedures      CONSTANT NUMBER(1)      := 0;
    in_drop_jobs            CONSTANT NUMBER(1)      := 0;
BEGIN
    -- drop objects
    FOR c IN (
        SELECT t.owner, t.object_type, t.object_name
        FROM all_objects t
        WHERE t.owner           = in_owner
            AND t.object_name   LIKE in_prefix ESCAPE '\'
            AND t.object_name   NOT IN (SELECT object_name FROM RECYCLEBIN)
            AND (
                (t.object_type  = 'TABLE'               AND (in_drop_tables     = 1 OR in_drop_all = 1)) OR
                (t.object_type  = 'INDEX'               AND (in_drop_indexes    = 1 OR in_drop_all = 1)) OR
                (t.object_type  = 'VIEW'                AND (in_drop_views      = 1 OR in_drop_all = 1)) OR
                (t.object_type  = 'MATERIALIZED VIEW'   AND (in_drop_mviews     = 1 OR in_drop_all = 1)) OR
                (t.object_type  = 'TRIGGER'             AND (in_drop_triggers   = 1 OR in_drop_all = 1)) OR
                (t.object_type  = 'SEQUENCE'            AND (in_drop_sequences  = 1 OR in_drop_all = 1)) OR
                (t.object_type  = 'PACKAGE'             AND (in_drop_packages   = 1 OR in_drop_all = 1)) OR
                (t.object_type  = 'PACKAGE BODY'        AND (in_drop_packages   = 1 OR in_drop_all = 1)) OR
                (t.object_type  = 'FUNCTION'            AND (in_drop_functions  = 1 OR in_drop_all = 1)) OR
                (t.object_type  = 'PROCEDURE'           AND (in_drop_procedures = 1 OR in_drop_all = 1))
            )
        ORDER BY
            DECODE(object_type,
                'PACKAGE BODY', 1,
                'PACKAGE',      2,
                'TABLE',        9,
                8
            ), object_name DESC
    ) LOOP
        DBMS_OUTPUT.PUT('.');
        DBMS_UTILITY.EXEC_DDL_STATEMENT(
            'DROP ' || c.object_type || ' ' || c.owner || '.' || '"' || c.object_name || '"' ||
            CASE WHEN c.object_type = 'TABLE' THEN ' CASCADE CONSTRAINTS' END
        );
    END LOOP;

    -- drop constraints
    IF (in_drop_constraints = 1 OR in_drop_all = 1) THEN
        FOR c IN (
            SELECT t.owner, t.table_name, t.constraint_name
            FROM all_constraints t
            WHERE t.owner           = in_owner
                AND t.table_name    LIKE in_prefix ESCAPE '\'
        ) LOOP
            DBMS_OUTPUT.PUT('.');
            BEGIN
                DBMS_UTILITY.EXEC_DDL_STATEMENT('ALTER TABLE ' || c.owner || '.' || c.table_name || ' DROP CONSTRAINT "' || c.constraint_name || '"');
            EXCEPTION
            WHEN OTHERS THEN
                DBMS_OUTPUT.PUT_LINE(c.table_name || ':' || c.constraint_name);
            END;
        END LOOP;
    END IF;

    -- remove all jobs
    IF (in_drop_jobs = 1 OR in_drop_all = 1) THEN
        FOR c IN (
            SELECT t.owner, t.job_name
            FROM all_scheduler_jobs t
            WHERE t.owner           = in_owner
                AND t.job_name      LIKE in_prefix ESCAPE '\'
        ) LOOP
            BEGIN
                DBMS_OUTPUT.PUT('.');
                DBMS_SCHEDULER.DROP_JOB(c.owner || '."' || c.job_name || '"', TRUE);
            EXCEPTION
            WHEN OTHERS THEN
                NULL;
            END;
        END LOOP;
    END IF;

    -- show summary what objects are left
    DBMS_OUTPUT.PUT_LINE('');
    DBMS_OUTPUT.PUT_LINE(in_prefix || ' OBJECTS:');
    FOR c IN (
        SELECT
            t.object_type,
            COUNT(*)            AS count_
        FROM all_objects t
        WHERE t.owner           = in_owner
            AND t.object_name   LIKE in_prefix ESCAPE '\'
        GROUP BY
            t.object_type
        UNION ALL
        SELECT
            'SCHEDULER',
            COUNT(*)            AS count_
        FROM all_scheduler_jobs t
        WHERE t.owner           = in_owner
            AND t.job_name      LIKE in_prefix ESCAPE '\'
        HAVING COUNT(*) > 0
    ) LOOP
        DBMS_OUTPUT.PUT_LINE(RPAD('  ' || c.object_type || ' ', 30, '.') || LPAD(' ' || c.count_, 8, '.'));
    END LOOP;
    DBMS_OUTPUT.PUT_LINE('');
END;
/

