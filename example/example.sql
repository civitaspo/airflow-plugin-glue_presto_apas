select column_name
     , table_schema
     , table_name
  from information_schema.columns
 where table_schema = 'example_db'
   and table_name   = 'example_table'