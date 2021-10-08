SELECT 
    c.table_schema, 
    t.table_name, 
    c.column_name, 
    c.data_type, 
    t.table_rows, 
    t.table_rows
from information_schema.columns c
join information_schema.tables t
on c.table_name = t.table_name