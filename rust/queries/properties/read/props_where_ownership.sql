SELECT
    interface,
    path,
    value,
    type,
    interface_major,
    ownership
FROM propcache
WHERE
    ownership = ?
    AND value IS NOT NULL;
