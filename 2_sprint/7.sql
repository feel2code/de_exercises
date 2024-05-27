--pg_dump -U jovyan -h localhost -p 5432 -d de -t my_important_table -f pg_my_important_table_backup.sql

alter table public.my_important_table rename to my_important_table_old;

ALTER TABLE public.my_important_table_old
RENAME CONSTRAINT my_important_table_pkey TO my_important_table_old_pkey;

--psql -U jovyan -h localhost -p 5432 -d de -f pg_my_important_table_backup.sql