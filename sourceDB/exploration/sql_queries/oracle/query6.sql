SELECT 
    a.table_name, 
    a.column_name, 
    c.comments  
from ALL_TAB_COLS a
join ALL_COL_COMMENTS c
ON a.table_name = c.table_name AND a.COLUMN_NAME = c.COLUMN_NAME 