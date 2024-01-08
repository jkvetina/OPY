-- check recent errors
SELECT e.name, e.type, e.line, e.text
FROM user_errors e
WHERE e.text NOT LIKE 'PLW%'
ORDER BY e.name, e.sequence;

