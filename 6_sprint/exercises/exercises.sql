create table VERTICA_SCHEMA.orders_v2
(
    id              integer PRIMARY KEY,
    registration_ts timestamptz,
    user_id         integer,
    is_confirmed    boolean
)
    ORDER BY id
    SEGMENTED BY HASH(id) ALL NODES
;


CREATE TABLE COPY_EX1
(
    i int primary key,
    v varchar(16)
);

/*
Вставка хотя бы одной записи обязательна для нашего примера
*/
INSERT INTO COPY_EX1
VALUES (1, 'wow');

select export_objects('', 'COPY_EX1');

select hash(10, 312823341, 22090316119000) as h1, hash(10, 193532316, 21244372081000) as h2;


CREATE TABLE MY_TABLE
(
    i  int,
    ts timestamp(6),
    v  varchar(1024),
    PRIMARY KEY (i, ts)
)
    ORDER BY i, v, ts
    SEGMENTED BY HASH(i, ts) ALL NODES;


create table VERTICA_SCHEMA.dialogs
(
    message_id   int PRIMARY KEY,
    message_ts   timestamp(6),
    message_from int REFERENCES members (id),
    message_to   int REFERENCES members (id),
    message      varchar(1000),
    message_type varchar(100)
)
    SEGMENTED BY hash(message_id) all nodes;
drop table VERTICA_SCHEMA.dialogs;

-- create and copy
drop table if exists VERTICA_SCHEMA.members cascade;

create table VERTICA_SCHEMA.members
(
    id     int PRIMARY KEY,
    age    int,
    gender varchar(8),
    email  varchar(256)
)
    order by id
    segmented by hash(id) all nodes;

copy VERTICA_SCHEMA.members (
                            id, age, gender, email)
    from
        local '/home/user/projects/s6-lessons/members.csv'
    delimiter ';';


drop table if exists VERTICA_SCHEMA.dialogs cascade;

create table VERTICA_SCHEMA.dialogs
(
    message_id    int PRIMARY KEY,
    message_ts    timestamp(6),
    message_from  int REFERENCES members (id),
    message_to    int REFERENCES members (id),
    message       varchar(1000),
    message_group int
)
    order by message_from, message_ts
    segmented by hash(message_id) all nodes;

copy VERTICA_SCHEMA.dialogs (
                            message_id, message_ts, message_from, message_to, message, message_group)
    from
        local '/home/user/projects/s6-lessons/dialogs.csv'
    delimiter ',';

-- test query
SELECT U.gender,
       U.age,
       count(M.message_id) as sent
FROM VERTICA_SCHEMA.members as U
         INNER JOIN
     VERTICA_SCHEMA.dialogs as M
     ON U.id = M.message_from
WHERE M.message_ts >= trunc(now(), 'DD') - '3 YEAR'::INTERVAL
GROUP BY 1, 2;

select *
from VERTICA_SCHEMA.dialogs;
select *
from VERTICA_SCHEMA.members;
select count(*)
from VERTICA_SCHEMA.members;
select count(*)
from VERTICA_SCHEMA.members
where age > 45
  and id < 342;
delete
from VERTICA_SCHEMA.members
where age > 45
  and id < 342;
select purge_table('members');

-- delete in Vertica
SET SESSION AUTOCOMMIT TO off;

DELETE
FROM VERTICA_SCHEMA.members
WHERE age > 45;

SELECT node_name, projection_name, deleted_row_count
FROM DELETE_VECTORS
where delete_vectors.projection_name like 'members%';

SELECT max(deleted_row_count)
FROM DELETE_VECTORS
where delete_vectors.projection_name like 'members%';

ROLLBACK;

--misc
TRUNCATE TABLE VERTICA_SCHEMA.members;

SELECT *
FROM DELETE_VECTORS
where delete_vectors.projection_name like 'members%';

ROLLBACK;

SELECT *
FROM members;


-- merge tests
copy VERTICA_SCHEMA.members (
                            id, age, gender, email)
    from
        local '/home/user/projects/s6-lessons/members.csv'
    delimiter ';';

create table members_inc
(
    id     int NOT NULL,
    age    int,
    gender char,
    email  varchar(50),
    CONSTRAINT C_PRIMARY PRIMARY KEY (id) DISABLED
)
    ORDER BY id
    SEGMENTED BY HASH(id) ALL NODES;

drop table members_inc;

CREATE TABLE members_inc LIKE members INCLUDING PROJECTIONS;
/*
INCLUDING PROJECTIONS - создаст полную копию структур для всех проекций
родительской таблицы
*/

COPY members_inc (id, age, gender, email ENFORCELENGTH)
    FROM LOCAL '/home/user/projects/s6-lessons/Тема 2. Аналитические СУБД. Vertica/11. Удаление и обновление данных в Vertica/Задание 3/members_inc.csv'
    DELIMITER ';'
    REJECTED DATA AS TABLE members_rej;

select *
from members_inc;
select *
from members;

-- merge realization
MERGE INTO VERTICA_SCHEMA.members as tgt
USING VERTICA_SCHEMA.members_inc as src
ON tgt.id = src.id
WHEN MATCHED and
            tgt.age <> src.age
        or tgt.gender <> src.gender
        or tgt.email <> src.email
    THEN
    UPDATE
    SET age=src.age,
        gender = src.gender,
        email = src.email
WHEN NOT MATCHED THEN
    INSERT (id, age, gender, email)
    VALUES (src.id, src.age, src.gender, src.email);

--partitioning
/* Прекод */
drop table if exists dialogs;

create table dialogs
(
    message_id   int PRIMARY KEY,
    message_ts   timestamp(6)                NOT NULL,
    message_from int REFERENCES members (id) NOT NULL,
    message_to   int REFERENCES members (id) NOT NULL,
    message      varchar(1000),
    message_type varchar(100)
)
    order by message_from, message_ts
    SEGMENTED BY hash(message_id) all nodes
    PARTITION BY message_ts::DATE;

--group partitioning
drop table if exists dialogs;
create table dialogs
(
    message_id   int PRIMARY KEY,
    message_ts   timestamp(6)                NOT NULL,
    message_from int REFERENCES members (id) NOT NULL,
    message_to   int REFERENCES members (id) NOT NULL,
    message      varchar(1000),
    message_type varchar(100)
)
    order by message_from, message_ts
    SEGMENTED BY hash(message_id) all nodes
    PARTITION BY message_ts::date
        GROUP BY calendar_hierarchy_day(message_ts::date, 3, 2);

--delete partitions
drop table if exists dialogs;
create table dialogs
(
    message_id   int PRIMARY KEY,
    message_ts   timestamp(6) NOT NULL,
    message_from int          NOT NULL,
    message_to   int          NOT NULL,
    message      varchar(1000),
    message_type varchar(100)
)
    order by
        message_from, message_ts
    segmented by
        hash(message_id) all nodes
    partition by
        message_ts::date
        group by
            calendar_hierarchy_day(message_ts::date, 3, 2)
;

COPY dialogs (
              message_id,
              message_ts_orig FILLER timestamp(6),
              message_ts as
                  add_months(
                          message_ts_orig,
                          case
                              when year(message_ts_orig) >= 2020
                                  then datediff('month', '2021-06-21'::timestamp, now()::timestamp) - 1
                              else 0
                              end
                  ),
              message_from,
              message_to,
              message,
              message_type
    )
    FROM LOCAL '/home/user/projects/s6-lessons/dialogs.csv'
    DELIMITER ','
    ENCLOSED BY '"';

SELECT DROP_PARTITIONS('VERTICA_SCHEMA.dialogs', '2004-01-01'::date, '2006-01-01'::date, 'true');
select count(*)
from VERTICA_SCHEMA.dialogs
where date_part('YEAR', message_ts) in ('2004', '2005');

--swap partitions
create table
    dialogs_temp like dialogs /*укажите название таблиц, новую и старую*/
    including projections;
select *
from dialogs_temp;
insert into dialogs_temp
select message_id,
       message_ts,
       message_from,
       message_to,
       message,
       coalesce(message_type, '0') as message_type /* выполнить преобразование message_type */
from dialogs /* исходная таблица */
where datediff('month', message_ts, now()) < 4 /* 4 неполных месяца от сегодня */;

/* test */
select
    /* Если запрос вернёт хотя бы одну запись,
       эта функция выдаст ошибку */
    THROW_ERROR('Остались NULL в клоне!') as test_nulls
from dialogs_temp /* клон */
where message_type is NULL;

select swap_partitions_between_tables(
               'dialogs_temp', /*укажите название таблицы 1*/
               ADD_MONTHS(current_date, -4), /*укажите начальную партицую диапазона */
               current_date, /*укажите конечнцю партицию диапаона*/
               'dialogs', /*укажите название таблицы 2*/
               'true'
       );

select
    /* Если запрос вернёт хотя бы одну запись,
       эта функция выдаст ошибку */
    THROW_ERROR('Остались NULL в основной таблице!') as test_nulls
from dialogs /* оригинал */
where message_type is NULL
  and datediff('month', message_ts, now()) < 4;

-- TODO: staging creation
--staging
drop table if exists VERTICA_SCHEMA__STAGING.dialogs;
drop table if exists VERTICA_SCHEMA__STAGING.groups;
drop table if exists VERTICA_SCHEMA__STAGING.users;
create table VERTICA_SCHEMA__STAGING.users
(
    id              int primary key,
    chat_name       varchar(200),
    registration_dt timestamp,
    country         varchar(200),
    age             int
) order by id;
create table VERTICA_SCHEMA__STAGING.groups
(
    id              int primary key,
    admin_id        int,
    group_name      varchar(100),
    registration_dt timestamp,
    is_private      boolean
) order by id, admin_id
    PARTITION BY registration_dt::date
        GROUP BY calendar_hierarchy_day(registration_dt::date, 3, 2);
create table VERTICA_SCHEMA__STAGING.dialogs
(
    message_id    int primary key,
    message_ts    timestamp NOT NULL,
    message_from  int       NOT NULL,
    message_to    int       NOT NULL,
    message       varchar(1000),
    message_group int       null
) order by message_id
    PARTITION BY message_ts::date
        GROUP BY calendar_hierarchy_day(message_ts::date, 3, 2);
alter table VERTICA_SCHEMA__STAGING.groups
    add constraint groups_admin_id_users_fk foreign key (admin_id) references VERTICA_SCHEMA__STAGING.users (id);
alter table VERTICA_SCHEMA__STAGING.dialogs
    add constraint dialogs_message_from_users_fk foreign key (message_from) references VERTICA_SCHEMA__STAGING.users (id);
alter table VERTICA_SCHEMA__STAGING.dialogs
    add constraint dialogs_message_to_users_fk foreign key (message_to) references VERTICA_SCHEMA__STAGING.users (id);

select *
from VERTICA_SCHEMA__STAGING.dialogs;


--checks
select count(*) as total, count(distinct id) as uniq, 'users' as dataset
from VERTICA_SCHEMA__STAGING.users
union all
select count(*) as total, count(distinct id) as uniq, 'groups' as dataset
from VERTICA_SCHEMA__STAGING.groups
union all
select count(*) as total, count(distinct message_id) as uniq, 'dialogs' as dataset
from VERTICA_SCHEMA__STAGING.dialogs;

SELECT count(hash(g.group_name)), count(distinct hash(g.group_name))
FROM VERTICA_SCHEMA__STAGING.groups g
LIMIT 10;

(SELECT min(u.registration_dt)       as datestamp,
        'earliest user registration' as info
 FROM VERTICA_SCHEMA__STAGING.users u)
UNION ALL
(SELECT max(u.registration_dt),
        'latest user registration'
 FROM VERTICA_SCHEMA__STAGING.users u)
UNION ALL
(SELECT min(g.registration_dt)    as datestamp,
        'earliest group creation' as info
 FROM VERTICA_SCHEMA__STAGING.groups g)
UNION ALL
(SELECT max(g.registration_dt),
        'latest group creation'
 FROM VERTICA_SCHEMA__STAGING.groups g)
UNION ALL
(SELECT min(d.message_ts)         as datestamp,
        'earliest dialog message' as info
 FROM VERTICA_SCHEMA__STAGING.dialogs d)
UNION ALL
(SELECT max(d.message_ts),
        'latest dialog message'
 FROM VERTICA_SCHEMA__STAGING.dialogs d);


(SELECT max(u.registration_dt) < now() as 'no future dates',
        true                           as 'no false-start dates',
        'users'                        as dataset
 FROM VERTICA_SCHEMA__STAGING.users u)
UNION ALL
(SELECT max(g.registration_dt) < now() as 'no future dates',
        true                           as 'no false-start dates',
        'groups'                       as dataset
 FROM VERTICA_SCHEMA__STAGING.groups g)
UNION ALL
(SELECT max(d.message_ts) < now() as 'no future dates',
        true                      as 'no false-start dates',
        'dialogs'                 as dataset
 FROM VERTICA_SCHEMA__STAGING.dialogs d);


SELECT count(*)
FROM VERTICA_SCHEMA__STAGING.groups AS g
         left join VERTICA_SCHEMA__STAGING.users AS u
                   ON u.id = g.admin_id
WHERE u.id is null;

(SELECT count(1), 'missing group admin info' as info
 FROM VERTICA_SCHEMA__STAGING.groups g
          left join VERTICA_SCHEMA__STAGING.users AS u ON u.id = g.admin_id
 WHERE u.id is null)
UNION ALL
(SELECT COUNT(1), 'missing sender info'
 FROM VERTICA_SCHEMA__STAGING.dialogs d
          left join VERTICA_SCHEMA__STAGING.users AS u ON u.id = d.message_from
 WHERE u.id is null)
UNION ALL
(SELECT COUNT(1), 'missing receiver info'
 FROM VERTICA_SCHEMA__STAGING.dialogs d
          left join VERTICA_SCHEMA__STAGING.users AS u ON u.id = d.message_to
 WHERE u.id is null)
UNION ALL
(SELECT COUNT(1), 'norm receiver info'
 FROM VERTICA_SCHEMA__STAGING.dialogs d
          right join VERTICA_SCHEMA__STAGING.users AS u ON u.id = d.message_to
 WHERE u.id is not null);


--dds
drop table if exists VERTICA_SCHEMA__DWH.h_users;
drop table if exists VERTICA_SCHEMA__DWH.h_groups;
drop table if exists VERTICA_SCHEMA__DWH.h_dialogs;
create table VERTICA_SCHEMA__DWH.h_users
(
    hk_user_id      bigint primary key,
    user_id         int,
    registration_dt datetime,
    load_dt         datetime,
    load_src        varchar(20)
)
    order by load_dt
    SEGMENTED BY hk_user_id all nodes
    PARTITION BY load_dt::date
        GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

create table VERTICA_SCHEMA__DWH.h_groups
(
    hk_group_id     int primary key,
    group_id        int,
    registration_dt timestamp,
    load_dt         datetime,
    load_src        varchar(20)
) order by load_dt
    SEGMENTED BY hk_group_id all nodes
    PARTITION BY load_dt::date
        GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

create table VERTICA_SCHEMA__DWH.h_dialogs
(
    hk_message_id int primary key,
    message_id    int,
    message_ts    timestamp NOT NULL,
    load_dt       datetime,
    load_src      varchar(20)
) order by load_dt
    SEGMENTED BY hk_message_id all nodes
    PARTITION BY load_dt::date
        GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);


--stg to dds
INSERT INTO VERTICA_SCHEMA__DWH.h_users(hk_user_id, user_id, registration_dt, load_dt, load_src)
select hash(id) as hk_user_id,
       id       as user_id,
       registration_dt,
       now()    as load_dt,
       's3'     as load_src
from VERTICA_SCHEMA__STAGING.users
where hash(id) not in (select hk_user_id from VERTICA_SCHEMA__DWH.h_users);

INSERT INTO VERTICA_SCHEMA__DWH.h_groups(hk_group_id, group_id, registration_dt, load_dt, load_src)
select hash(id) as hk_group_id,
       id       as group_id,
       registration_dt,
       now()    as load_dt,
       's3'     as load_src
from VERTICA_SCHEMA__STAGING.groups
where hash(id) not in (select hk_group_id from VERTICA_SCHEMA__DWH.h_groups);

INSERT INTO VERTICA_SCHEMA__DWH.h_dialogs(hk_message_id, message_id, message_ts, load_dt, load_src)
select hash(message_id) as hk_message_id,
       message_id       as message_id,
       message_ts,
       now()            as load_dt,
       's3'             as load_src
from VERTICA_SCHEMA__STAGING.dialogs
where hash(message_id) not in (select hk_message_id from VERTICA_SCHEMA__DWH.h_dialogs);


--dds links
drop table if exists VERTICA_SCHEMA__DWH.l_user_message;
drop table if exists VERTICA_SCHEMA__DWH.l_admins;
drop table if exists VERTICA_SCHEMA__DWH.l_groups_dialogs;
create table VERTICA_SCHEMA__DWH.l_user_message
(
    hk_l_user_message bigint primary key,
    hk_user_id        bigint not null
        CONSTRAINT fk_l_user_message_user REFERENCES VERTICA_SCHEMA__DWH.h_users (hk_user_id),
    hk_message_id     bigint not null
        CONSTRAINT fk_l_user_message_message REFERENCES VERTICA_SCHEMA__DWH.h_dialogs (hk_message_id),
    load_dt           datetime,
    load_src          varchar(20)
)
    order by load_dt
    SEGMENTED BY hk_user_id all nodes
    PARTITION BY load_dt::date
        GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

create table VERTICA_SCHEMA__DWH.l_admins
(
    hk_l_admin_id bigint primary key,
    hk_user_id    bigint not null
        CONSTRAINT fk_l_admins_user REFERENCES VERTICA_SCHEMA__DWH.h_users (hk_user_id),
    hk_group_id   bigint not null
        CONSTRAINT fk_l_admins_group REFERENCES VERTICA_SCHEMA__DWH.h_groups (hk_group_id),
    load_dt       datetime,
    load_src      varchar(20)
)
    order by load_dt
    SEGMENTED BY hk_l_admin_id all nodes
    PARTITION BY load_dt::date
        GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

create table VERTICA_SCHEMA__DWH.l_groups_dialogs
(
    hk_l_groups_dialogs bigint primary key,
    hk_message_id       bigint not null
        CONSTRAINT fk_l_groups_dialogs_message REFERENCES VERTICA_SCHEMA__DWH.h_dialogs (hk_message_id),
    hk_group_id         bigint not null
        CONSTRAINT fk_l_groups_dialogs_group REFERENCES VERTICA_SCHEMA__DWH.h_groups (hk_group_id),
    load_dt             datetime,
    load_src            varchar(20)
)
    order by load_dt
    SEGMENTED BY hk_l_groups_dialogs all nodes
    PARTITION BY load_dt::date
        GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);


--load links
INSERT INTO VERTICA_SCHEMA__DWH.l_admins(hk_l_admin_id, hk_group_id, hk_user_id, load_dt, load_src)
select hash(hg.hk_group_id, hu.hk_user_id),
       hg.hk_group_id,
       hu.hk_user_id,
       now() as load_dt,
       's3'  as load_src
from VERTICA_SCHEMA__STAGING.groups as g
         left join VERTICA_SCHEMA__DWH.h_users as hu on g.admin_id = hu.user_id
         left join VERTICA_SCHEMA__DWH.h_groups as hg on g.id = hg.group_id
where hash(hg.hk_group_id, hu.hk_user_id) not in (select hk_l_admin_id from VERTICA_SCHEMA__DWH.l_admins);

INSERT INTO VERTICA_SCHEMA__DWH.l_user_message(hk_l_user_message, hk_user_id, hk_message_id, load_dt, load_src)
select hash(hu.hk_user_id, hd.hk_message_id),
       hu.hk_user_id,
       hd.hk_message_id,
       now() as load_dt,
       's3'  as load_src
from VERTICA_SCHEMA__STAGING.dialogs as d
         join VERTICA_SCHEMA__DWH.h_users as hu on d.message_id = hu.user_id
         join VERTICA_SCHEMA__DWH.h_dialogs as hd on d.message_id = hd.message_id
where hash(hu.hk_user_id, hd.hk_message_id) not in (select hk_l_user_message from VERTICA_SCHEMA__DWH.l_user_message);

INSERT INTO VERTICA_SCHEMA__DWH.l_groups_dialogs(hk_l_groups_dialogs, hk_message_id, hk_group_id, load_dt, load_src)
select hash(hd.hk_message_id, hg.group_id),
       hd.hk_message_id,
       hg.hk_group_id,
       now() as load_dt,
       's3'  as load_src
from VERTICA_SCHEMA__STAGING.dialogs as d
         left join VERTICA_SCHEMA__DWH.h_dialogs as hd on d.message_id = hd.message_id
         right join VERTICA_SCHEMA__DWH.h_groups as hg on d.message_group = hg.group_id
where hash(hd.hk_message_id, hg.group_id) not in (select hk_l_groups_dialogs from VERTICA_SCHEMA__DWH.l_groups_dialogs);


--dds satellites and load
--drop satellites
drop table if exists VERTICA_SCHEMA__DWH.s_admins;
drop table if exists VERTICA_SCHEMA__DWH.s_user_chatinfo;
drop table if exists VERTICA_SCHEMA__DWH.s_user_socdem;
drop table if exists VERTICA_SCHEMA__DWH.s_group_private_status;
drop table if exists VERTICA_SCHEMA__DWH.s_group_name;
drop table if exists VERTICA_SCHEMA__DWH.s_dialog_info;

--s_admins
create table VERTICA_SCHEMA__DWH.s_admins
(
    hk_admin_id bigint not null
        CONSTRAINT fk_s_admins_l_admins REFERENCES VERTICA_SCHEMA__DWH.l_admins (hk_l_admin_id),
    is_admin    boolean,
    admin_from  datetime,
    load_dt     datetime,
    load_src    varchar(20)
)
    order by load_dt
    SEGMENTED BY hk_admin_id all nodes
    PARTITION BY load_dt::date
        GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

INSERT INTO VERTICA_SCHEMA__DWH.s_admins(hk_admin_id, is_admin, admin_from, load_dt, load_src)
select la.hk_l_admin_id,
       True  as is_admin,
       hg.registration_dt,
       now() as load_dt,
       's3'  as load_src
from VERTICA_SCHEMA__DWH.l_admins as la
         left join VERTICA_SCHEMA__DWH.h_groups as hg on la.hk_group_id = hg.hk_group_id;

--s_user_chatinfo;
create table VERTICA_SCHEMA__DWH.s_user_chatinfo
(
    hk_user_id bigint not null
        CONSTRAINT fk_s_user_chatinfo_h_users REFERENCES VERTICA_SCHEMA__DWH.h_users (hk_user_id),
    chat_name  varchar(200),
    load_dt    datetime,
    load_src   varchar(20)
)
    order by load_dt
    SEGMENTED BY hk_user_id all nodes
    PARTITION BY load_dt::date
        GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

INSERT INTO VERTICA_SCHEMA__DWH.s_user_chatinfo(hk_user_id, chat_name, load_dt, load_src)
select hu.hk_user_id,
       u.chat_name as chat_name,
       now()       as load_dt,
       's3'        as load_src
from VERTICA_SCHEMA__DWH.h_users as hu
         left join VERTICA_SCHEMA__STAGING.users as u on u.id = hu.user_id;

--s_user_socdem;
create table VERTICA_SCHEMA__DWH.s_user_socdem
(
    hk_user_id bigint not null
        CONSTRAINT fk_s_user_socdem_h_users REFERENCES VERTICA_SCHEMA__DWH.h_users (hk_user_id),
    country    varchar(100),
    age        int,
    load_dt    datetime,
    load_src   varchar(20)
)
    order by load_dt
    SEGMENTED BY hk_user_id all nodes
    PARTITION BY load_dt::date
        GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

INSERT INTO VERTICA_SCHEMA__DWH.s_user_socdem(hk_user_id, country, age, load_dt, load_src)
select hu.hk_user_id,
       u.country as country,
       u.age     as age,
       now()     as load_dt,
       's3'      as load_src
from VERTICA_SCHEMA__DWH.h_users as hu
         left join VERTICA_SCHEMA__STAGING.users as u on u.id = hu.user_id;

--s_group_private_status;
create table VERTICA_SCHEMA__DWH.s_group_private_status
(
    hk_group_id bigint not null
        CONSTRAINT fk_s_group_private_status_h_groups REFERENCES VERTICA_SCHEMA__DWH.h_groups (hk_group_id),
    is_private  boolean,
    load_dt     datetime,
    load_src    varchar(20)
)
    order by load_dt
    SEGMENTED BY hk_group_id all nodes
    PARTITION BY load_dt::date
        GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

INSERT INTO VERTICA_SCHEMA__DWH.s_group_private_status (hk_group_id, is_private, load_dt, load_src)
select hg.hk_group_id,
       g.is_private,
       now() as load_dt,
       's3'  as load_src
from VERTICA_SCHEMA__DWH.h_groups as hg
         left join VERTICA_SCHEMA__STAGING.groups as g on g.id = hg.group_id;

--s_group_name;
create table VERTICA_SCHEMA__DWH.s_group_name
(
    hk_group_id bigint not null
        CONSTRAINT fk_s_group_name_h_groups REFERENCES VERTICA_SCHEMA__DWH.h_groups (hk_group_id),
    group_name  varchar(200),
    load_dt     datetime,
    load_src    varchar(20)
)
    order by load_dt
    SEGMENTED BY hk_group_id all nodes
    PARTITION BY load_dt::date
        GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

INSERT INTO VERTICA_SCHEMA__DWH.s_group_name (hk_group_id, group_name, load_dt, load_src)
select hg.hk_group_id,
       g.group_name,
       now() as load_dt,
       's3'  as load_src
from VERTICA_SCHEMA__DWH.h_groups as hg
         left join VERTICA_SCHEMA__STAGING.groups as g on g.id = hg.group_id;

--s_dialog_info;
create table VERTICA_SCHEMA__DWH.s_dialog_info
(
    hk_message_id bigint not null
        CONSTRAINT fk_s_dialog_info_h_groups REFERENCES VERTICA_SCHEMA__DWH.h_dialogs (hk_message_id),
    message       varchar(1000),
    message_from  int,
    message_to    int,
    load_dt       datetime,
    load_src      varchar(20)
)
    order by load_dt
    SEGMENTED BY hk_message_id all nodes
    PARTITION BY load_dt::date
        GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

INSERT INTO VERTICA_SCHEMA__DWH.s_dialog_info (hk_message_id, message, message_from, message_to, load_dt, load_src)
select hd.hk_message_id,
       d.message,
       d.message_from,
       d.message_to,
       now() as load_dt,
       's3'  as load_src
from VERTICA_SCHEMA__DWH.h_dialogs as hd
         left join VERTICA_SCHEMA__STAGING.dialogs as d on d.message_id = hd.message_id;

--analysis
select age, count(hk_user_id)
from VERTICA_SCHEMA__DWH.s_user_socdem suc
where hk_user_id in (select hk_user_id
                     from VERTICA_SCHEMA__DWH.l_user_message
                     where hk_message_id in (select hk_message_id
                                             from VERTICA_SCHEMA__DWH.l_groups_dialogs
                                             where hk_group_id in (select hk_group_id
                                                                   from VERTICA_SCHEMA__DWH.h_groups
                                                                   order by registration_dt
                                                                   limit 10)))
group by age
order by count(hk_user_id) desc;
