select 
    kcu.constraint_schema, 
    kcu.table_name, 
    kcu.column_name , 
    kcu.constraint_name, 
    kcu2.table_name as referenced_table, 
    kcu2.column_name as  referenced_column
from information_schema.key_column_usage kcu 
join information_schema.referential_constraints rc using(constraint_schema, constraint_name)
join information_schema.key_column_usage kcu2 
on kcu2.constraint_schema = rc.unique_constraint_schema 
and kcu2.constraint_name  = rc.unique_constraint_name
and kcu2.ordinal_position = kcu.position_in_unique_constraint