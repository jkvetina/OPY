DECLARE
    v_start     DATE;
BEGIN
    FOR c IN (
        SELECT m.mview_name
        FROM all_mviews m
        WHERE m.owner           = USER                  -- adjust
            AND m.mview_name    LIKE '%' ESCAPE '\'     -- adjust
        ORDER BY 1
    ) LOOP
        v_start := SYSDATE;
        --
        DBMS_OUTPUT.PUT_LINE('--');
        DBMS_OUTPUT.PUT_LINE('-- REFRESHING ' || c.mview_name);
        --
        DBMS_MVIEW.REFRESH (
            list    => c.mview_name,
            method  => 'C'
        );
        --
        DBMS_OUTPUT.PUT_LINE('--   DONE ' || CEIL((SYSDATE - v_start) * 86400) || 's');
        DBMS_OUTPUT.PUT_LINE('--');
        DBMS_OUTPUT.PUT_LINE('');
    END LOOP;
END;
/
