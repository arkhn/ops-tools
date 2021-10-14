select t.table_schema, t.table_name, c.column_name
from information_schema."tables" t
join information_schema."columns" c
on t.table_name = c.table_name
where t.TABLE_TYPE = 'BASE TABLE'