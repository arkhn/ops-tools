SELECT 
    c.owner, 
    c.table_name, 
    abl.num_rows,
    c.comments  
from  ALL_TAB_COMMENTS c
JOIN ALL_TABLES abl
ON c.TABLE_NAME = abl.TABLE_NAME 