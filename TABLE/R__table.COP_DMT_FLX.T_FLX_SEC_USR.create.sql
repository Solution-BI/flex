USE SCHEMA COP_DMT_FLX;

CREATE SEQUENCE IF NOT EXISTS seq__t_flx_sec_usr__id;

CREATE OR REPLACE TABLE t_flx_sec_usr
(
    id            number        default seq__t_flx_sec_usr__id.nextval  comment 'ID for writeback',
    eml_usr_cod   varchar       not null primary key                    comment 'Email of the user who needs special authorizations',
    rol_cod       varchar                                               comment 'Role for the user (if empty, `<DEFAULT>` role will be applied, as if the user did not exist)',

    cre_eml_usr_cod varchar     not null                                comment 'Email of the user who added the current user configuration',
    upd_eml_usr_cod varchar     not null                                comment 'Email of the user who last updated the current user configuration',
    
    t_rec_dlt_flg smallint      not null default 0                      comment '[Technical] Soft-delete flag',
    t_rec_ins_tst timestamp_ntz not null default current_timestamp      comment '[Technical] Timestamp of first insertion into the table',
    t_rec_upd_tst timestamp_ntz not null default current_timestamp      comment '[Technical] Timestamp of last update into the table'

) comment='List of users with a specific (non-<DEFAULT>) role (single role only)';