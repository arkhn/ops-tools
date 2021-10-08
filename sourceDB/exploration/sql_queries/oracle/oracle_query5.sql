SELECT 
    a.owner, 
    ab_.TABLE_NAME, 
    ab_.column_name as "column", 
    ac.DATA_TYPE as Dtype,
    a.num_rows as "nb_rows", 
    ab_.num_nulls as "null_count"
FROM ALL_TABLES a
JOIN ALL_TAB_COL_STATISTICS ab_
ON a.table_name = ab_.table_name
JOIN all_tab_cols ac
ON ac.table_name = ab_.table_name AND ac.COLUMN_NAME = ab_.COLUMN_NAME