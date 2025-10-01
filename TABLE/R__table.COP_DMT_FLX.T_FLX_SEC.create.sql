USE SCHEMA COP_DMT_FLX;

CREATE OR REPLACE TABLE t_flx_sec
(
    rsc_cod         varchar     not null                            comment 'Resource to which the CRUD authz applies (usually a table that can be managed through Power ON, e.g.: flex.r_flx_sce)',
    rol_cod         varchar     not null default '<DEFAULT>'        comment 'Role to which the following policy applies',
    rol_can_sel_flg smallint    not null default 0                  comment 'If 1, a user with this role can read (SELECT) a resource',
    rol_can_ins_flg smallint    not null default 0                  comment 'If 1, a user with this role can create (INSERT) a resource',
    rol_can_upd_flg smallint    not null default 0                  comment 'If 1, a user with this role can modify (UPDATE) a resource',
    rol_can_del_flg smallint    not null default 0                  comment 'If 1, a user with this role can delete a resource',
    
    t_rec_dlt_flg smallint      not null default 0                  comment '[Technical] Soft-delete flag',
    t_rec_ins_tst timestamp_ntz not null default current_timestamp  comment '[Technical] Timestamp of first insertion into the table',
    t_rec_upd_tst timestamp_ntz not null default current_timestamp  comment '[Technical] Timestamp of last update into the table'

) comment='Actual authorizations implemented for each resource/role';