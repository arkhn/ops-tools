select 
    u1.schemaname, 
    p1.relname, 
    u1.n_live_tup, 
    p2.description 
from pg_stat_all_tables u1 
left join pg_Class p1
on u1.relname = p1.relname
left join pg_Description p2 
on p2.ObjOID = u1.relid
where p2.objsubid = 0