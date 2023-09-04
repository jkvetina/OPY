BEGIN
    DBMS_OUTPUT.PUT_LINE('--');
    DBMS_OUTPUT.PUT_LINE('-- REFRESHING MATERIALIZED VIEWS');
    DBMS_OUTPUT.PUT_LINE('--');
    --
    FOR c IN (
        SELECT m.mview_name, SYSDATE AS start_at
        FROM all_mviews m
        WHERE m.owner           = SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA')
            AND m.mview_name    LIKE 'TRC%' ESCAPE '\'      -- adjust
        ORDER BY 1
    ) LOOP
        c.start_at := SYSDATE;
        --
        DBMS_MVIEW.REFRESH (
            list            => c.mview_name,
            method          => 'C',
            atomic_refresh  => FALSE
        );
        --
        DBMS_OUTPUT.PUT_LINE('--   ' || RPAD(c.mview_name || ' ', 40, '.') || ' ' || CEIL((SYSDATE - c.start_at) * 86400) || 's');
    END LOOP;
    --
    DBMS_OUTPUT.PUT_LINE('--');
END;
/
