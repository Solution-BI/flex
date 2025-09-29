USE SCHEMA COP_DMT_FLX{{uid}};

CREATE OR REPLACE TABLE t_flx_sec_rol
(
    rol_cod       varchar       not null primary key                comment 'Identifier of the role: uppercase, only ascii letters, numbers or underscores',
    rol_dsc       varchar       not null                            comment 'Description of the role',
    
    t_rec_dlt_flg smallint      not null default 0                  comment '[Technical] Soft-delete flag',
    t_rec_ins_tst timestamp_ntz not null default current_timestamp  comment '[Technical] Timestamp of first insertion into the table',
    t_rec_upd_tst timestamp_ntz not null default current_timestamp  comment '[Technical] Timestamp of last update into the table'

) comment='List of backend-defined roles';