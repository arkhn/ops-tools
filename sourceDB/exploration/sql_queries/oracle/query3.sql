SELECT 
    uc_r.owner, 
    uc_r.table_name, 
    ucc_r.column_name, 
    uc_r.constraint_name,
    uc_p.constraint_name, 
    uc_p.table_name, 
    ucc_p.column_name
from all_constraints uc_r
join all_cons_columns ucc_r on ucc_r.constraint_name = uc_r.constraint_name
join all_constraints uc_p on uc_p.constraint_name = uc_r.r_constraint_name
join all_cons_columns ucc_p on ucc_p.constraint_name = uc_p.constraint_name
and ucc_p.position = ucc_r.position
where uc_r.constraint_type = 'R'