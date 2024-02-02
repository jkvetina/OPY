DECLARE
    in_current_env CONSTANT VARCHAR2(30) := 'DEV';
BEGIN
    FOR c IN (
        SELECT 'DEV'    AS env_name, 16 AS env_color FROM DUAL UNION ALL    -- green
        SELECT 'UAT'    AS env_name, 13 AS env_color FROM DUAL UNION ALL    -- blue
        SELECT 'PPE'    AS env_name, 15 AS env_color FROM DUAL UNION ALL    -- orange
        SELECT 'STAGE'  AS env_name,  8 AS env_color FROM DUAL UNION ALL    -- magenta
        SELECT 'PROD'   AS env_name, 14 AS env_color FROM DUAL              -- red
    ) LOOP
        IF c.env_name LIKE in_current_env || '%' THEN
            APEX_INSTANCE_ADMIN.SET_PARAMETER('ENV_BANNER_ENABLE',  'Y');
            APEX_INSTANCE_ADMIN.SET_PARAMETER('ENV_BANNER_LABEL',   c.env_name);
            APEX_INSTANCE_ADMIN.SET_PARAMETER('ENV_BANNER_COLOR',   'accent-' || c.env_color);
            APEX_INSTANCE_ADMIN.SET_PARAMETER('ENV_BANNER_POS',     'LEFT');
        END IF;
    END LOOP;
    --
    COMMIT;
END;
/
