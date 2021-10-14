select 
    t.table_schema, 
    t.table_name , 
    c.column_name, 
    c.data_type , 
    stt.n_live_tup, 
    (st.null_frac * stt.n_live_tup)/100
from information_schema."tables" t
join information_schema."columns" c
on t.table_name = c.table_name
left join pg_stat_all_tables stt
on stt.relname = t.table_name 
left join pg_catalog.pg_stats st
on st. attname  = c.column_name and st.tablename = t.table_name 
where t.TABLE_TYPE = 'BASE TABLE'