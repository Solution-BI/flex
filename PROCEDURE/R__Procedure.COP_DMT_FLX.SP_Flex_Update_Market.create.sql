USE SCHEMA COP_DMT_FLX{{uid}};

/*
=========================================================================
                                HISTORY
=========================================================================
Description : Create Procedure Script For Populating R_FLX_CBU Table

Author      : Noel Coquio (Solution BI France)
Created On  : 16-07-2024
=========================================================================
=========================================================================
*/

CREATE OR REPLACE PROCEDURE SP_Flex_Update_Market(JSON_INPUT VARCHAR(16777216)) 
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$

	// This method enables the standard call to a sequential array of queries
	
	function uuidHelper() {
		return ( ( ( 1+Math.random() ) * 0x10000 ) | 0 ).toString( 16 ).substring( 1 );
	}
	
	function generateuuid() {
		return (uuidHelper() + uuidHelper() + "-" + uuidHelper() + "-4" 
			+ uuidHelper().substr(0,2) + "-" + uuidHelper() + "-" + uuidHelper() + uuidHelper() + uuidHelper()
		).toLowerCase();
	}

var RUN_ID = generateuuid();

/* Logging incoming JSON data */
var user_cmd = `SELECT DISTINCT
                       REPLACE(T_REC_USER,'"')                                                 AS T_REC_EML_USR
                FROM   (SELECT T_REC_USER
                        FROM   (SELECT THIS:User AS T_REC_USER
                                FROM   LATERAL FLATTEN(parse_json('${JSON_INPUT}'))));
               `;
var user_res = snowflake.execute( {sqlText: user_cmd} );
user_res.next();
var v_eml_user = user_res.getColumnValue(1);

var log_data = snowflake.createStatement({sqlText: `CALL SP_Flex_Log_PowerOn(:1,'SP_Flex_Update_Market','${JSON_INPUT}',:2);`, binds:[RUN_ID,v_eml_user]});
log_data.execute();

/* Insert/Update/Delete Check */
var cmd_crud_check = `SELECT MAX(CASE WHEN PATH = 'InsertedRows' THEN LENGTH(VALUE)
                                      ELSE 0
                                 END) InsertedRows
                           ,MAX(CASE WHEN PATH = 'UpdatedRows' THEN LENGTH(VALUE)
                                     ELSE 0
                                END) UpdatedRows
                           ,MAX(CASE WHEN PATH = 'DeletedRows' THEN LENGTH(VALUE)
                                    ELSE 0
                                END) DeletedRows
                           ,MAX(CASE WHEN PATH = 'User' THEN VALUE
                                     ELSE ''
                                END) userName
                           ,to_varchar(CURRENT_TIMESTAMP, 'yyyy-mm-dd hh:mi:ss.ff')
                      FROM LATERAL FLATTEN(parse_json('${JSON_INPUT}'));`;

var crud_res = snowflake.execute( {sqlText: cmd_crud_check} );
crud_res.next();
var isInserted = crud_res.getColumnValue(1) > 2;
var isUpdated = crud_res.getColumnValue(2) > 2;
var isDeleted = crud_res.getColumnValue(3) > 2;

/* Insert Operation Block */
if (isInserted) {
/* Fetching data from JSON INPUT */
    var fch_data = `
SELECT DISTINCT
       REPLACE(X.ins_row:CBU_COD,'"')                                                    AS CBU_COD
      ,REPLACE(X.ins_row:CBU_DSC,'"')                                                    AS CBU_DSC
      ,REPLACE(X.ins_row:ACT_SRC_SCE_COD,'"')                                            AS ACT_SRC_SCE_COD
      ,REPLACE(X.ins_row:CMP_1ST_SRC_SCE_COD,'"')                                        AS CMP_1ST_SRC_SCE_COD
      ,REPLACE(X.ins_row:CMP_2ND_SRC_SCE_COD,'"')                                        AS CMP_2ND_SRC_SCE_COD
      ,REPLACE(X.ins_row:CMP_3RD_SRC_SCE_COD,'"')                                        AS CMP_3RD_SRC_SCE_COD
      ,COALESCE(REPLACE(X.ins_row:CUS_DIM_GRP_COD,'"'),'0')::NUMBER(10,0)                AS CUS_DIM_GRP_COD
      ,COALESCE(REPLACE(X.ins_row:EIB_USE_FLG,'"'),'0')::NUMBER(10,0)                    AS EIB_USE_FLG
      ,COALESCE(REPLACE(X.ins_row:TTY_USE_FLG,'"'),'0')::NUMBER(10,0)                    AS TTY_USE_FLG
      ,COALESCE(REPLACE(X.ins_row:VAR_NS,'"'),'1')::NUMBER(6,5)                          AS VAR_NS
      ,COALESCE(REPLACE(X.ins_row:VAR_MAT_COS,'"'),'1')::NUMBER(6,5)                     AS VAR_MAT_COS
      ,COALESCE(REPLACE(X.ins_row:VAR_MAT_OTH,'"'),'1')::NUMBER(6,5)                     AS VAR_MAT_OTH
      ,COALESCE(REPLACE(X.ins_row:VAR_MANUF_COS,'"'),'1')::NUMBER(6,5)                   AS VAR_MANUF_COS
      ,COALESCE(REPLACE(X.ins_row:VAR_MANUF_OTH,'"'),'1')::NUMBER(6,5)                   AS VAR_MANUF_OTH
      ,COALESCE(REPLACE(X.ins_row:VAR_LOG_FTC_IFO,'"'),'1')::NUMBER(6,5)                 AS VAR_LOG_FTC_IFO
      ,COALESCE(REPLACE(X.ins_row:VAR_LOG_USL,'"'),'1')::NUMBER(6,5)                     AS VAR_LOG_USL
      ,COALESCE(REPLACE(X.ins_row:VAR_LOG_OTH,'"'),'1')::NUMBER(6,5)                     AS VAR_LOG_OTH
      ,SUBSTR(REPLACE(X.USER,'"')
             ,1
             ,POSITION('@',REPLACE(X.USER,'"'),1) - 1
             )                                                             AS T_REC_INS_USR
FROM   (SELECT parse_json(b.VALUE) as ins_row
              ,a.USER
        FROM   (SELECT VALUE
                      ,THIS:User AS USER
                FROM   LATERAL FLATTEN(parse_json('${JSON_INPUT}')) f1
                WHERE  PATH LIKE '%InsertedRows%') a
               ,LATERAL FLATTEN(a.VALUE) b) X;
                    `;
/*    var log_data = snowflake.createStatement({sqlText: `CALL SP_LOG_FLX_POWERON('${fch_data}');`});*/
/*    log_data.execute();*/

    var stmt = snowflake.createStatement( {sqlText: fch_data} );
    var res = stmt.execute();
    res.next();

    /* Assigning data into local variables */
    var v_cbu_cod = res.getColumnValue(1);
    var v_cbu_dsc = res.getColumnValue(2);
    var v_act_src_sce_cod = res.getColumnValue(3);
    var v_cmp_1st_src_sce_cod = res.getColumnValue(4);
    var v_cmp_2nd_src_sce_cod = res.getColumnValue(5);
    var v_cmp_3rd_src_sce_cod = res.getColumnValue(6);

    var v_cus_dim_grp_cod = res.getColumnValue(7);
    var v_eib_use_flg = res.getColumnValue(8);
    var v_tty_use_flg = res.getColumnValue(9);

    var v_var_ns          = res.getColumnValue(10);
    var v_var_mat_cos     = res.getColumnValue(11);
    var v_var_mat_oth     = res.getColumnValue(12);
    var v_var_manuf_cos   = res.getColumnValue(13);
    var v_var_manuf_oth   = res.getColumnValue(14);
    var v_var_log_ftc_ifo = res.getColumnValue(15);
    var v_var_log_usl     = res.getColumnValue(16);
    var v_var_log_oth     = res.getColumnValue(17);

    /* Check duplicate in P_FLX_ETI_PDT_FCA */
    var dup_check_cmd = `SELECT COUNT(*) AS RowCount 
FROM   R_FLX_CBU 
WHERE  CBU_COD = '${v_cbu_cod}';`;
    var dup_check_stmt = snowflake.createStatement( {sqlText: dup_check_cmd} );
    var dup_check_res = dup_check_stmt.execute();
    dup_check_res.next();

    var v_is_dup = dup_check_res.getColumnValue(1) > 0;

    /* If not a duplicate, Insert */
    if (!v_is_dup) {

        /* Inserting Scenario to P_FLX_ETI_PDT_FCA */

        var ins_sce_cmd = `INSERT INTO COP_DMT_FLX.R_FLX_CBU
           (CBU_COD
           ,CBU_DSC
           ,ACT_SRC_SCE_COD
           ,CMP_1ST_SRC_SCE_COD
           ,CMP_2ND_SRC_SCE_COD
           ,CMP_3RD_SRC_SCE_COD
           ,CUS_DIM_GRP_COD
           ,EIB_USE_FLG
           ,TTY_USE_FLG
           ,VAR_NS
           ,VAR_MAT_COS
           ,VAR_MAT_OTH
           ,VAR_MANUF_COS
           ,VAR_MANUF_OTH
           ,VAR_LOG_FTC_IFO
           ,VAR_LOG_USL
           ,VAR_LOG_OTH
           ,T_REC_DLT_FLG
           ,T_REC_INS_TST
           ,T_REC_UPD_TST
           )
SELECT      DISTINCT
            '${v_cbu_cod}'                                 AS CBU_COD
           ,'${v_cbu_dsc}'                                 AS CBU_DSC
           ,'${v_act_src_sce_cod}'                         AS ACT_SRC_SCE_COD
           ,'${v_cmp_1st_src_sce_cod}'                     AS CMP_1ST_SRC_SCE_COD
           ,'${v_cmp_2nd_src_sce_cod}'                     AS CMP_2ND_SRC_SCE_COD
           ,'${v_cmp_3rd_src_sce_cod}'                     AS CMP_3RD_SRC_SCE_COD
           ,${v_cus_dim_grp_cod}                           AS CUS_DIM_GRP_COD
           ,${v_eib_use_flg}                               AS EIB_USE_FLG
           ,${v_tty_use_flg}                               AS TTY_USE_FLG
           ,${v_var_ns}                                    AS VAR_NS
           ,${v_var_mat_cos}                               AS VAR_MAT_COS
           ,${v_var_mat_oth}                               AS VAR_MAT_OTH
           ,${v_var_manuf_cos}                             AS VAR_MANUF_COS
           ,${v_var_manuf_oth}                             AS VAR_MANUF_OTH
           ,${v_var_log_ftc_ifo}                           AS VAR_LOG_FTC_IFO
           ,${v_var_log_usl}                               AS VAR_LOG_USL
           ,${v_var_log_oth}                               AS VAR_LOG_OTH
           ,0                                              AS T_REC_DLT_FLG
           ,TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP)            AS T_REC_SRC_TST
           ,TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP)            AS T_REC_UPD_TST`;
        snowflake.execute( {sqlText: ins_sce_cmd} );

    }
    else {
        throw "<SQLError>A row already exists for the Market.</SQLError>";
    }

}
/* End of Insert Operation Block */

/* Update Operation Block */
if (isUpdated) {

    var fch_data = `
SELECT DISTINCT
       REPLACE(X.upd_row:CBU_COD,'"')                                                     AS CBU_COD
      ,REPLACE(X.upd_row:CBU_DSC,'"')                                                     AS CBU_DSC
      ,COALESCE(REPLACE(X.upd_row:ACT_SRC_SCE_COD,'"'),'N/A')                             AS ACT_SRC_SCE_COD
      ,COALESCE(REPLACE(X.upd_row:CMP_1ST_SRC_SCE_COD,'"'),'N/A')                         AS CMP_1ST_SRC_SCE_COD
      ,COALESCE(REPLACE(X.upd_row:CMP_2ND_SRC_SCE_COD,'"'),'N/A')                         AS CMP_2ND_SRC_SCE_COD
      ,COALESCE(REPLACE(X.upd_row:CMP_3RD_SRC_SCE_COD,'"'),'N/A')                         AS CMP_3RD_SRC_SCE_COD
      ,COALESCE(REPLACE(X.upd_row:CUS_DIM_GRP_COD,'"'),'-9')::NUMBER(10,0)                AS CUS_DIM_GRP_COD
      ,COALESCE(REPLACE(X.upd_row:EIB_USE_FLG,'"'),'-9')::NUMBER(10,0)                    AS EIB_USE_FLG
      ,COALESCE(REPLACE(X.upd_row:TTY_USE_FLG,'"'),'-9')::NUMBER(10,0)                    AS TTY_USE_FLG
      ,COALESCE(REPLACE(X.upd_row:VAR_NS,'"'),'-9')::NUMBER(6,5)                          AS VAR_NS
      ,COALESCE(REPLACE(X.upd_row:VAR_MAT_COS,'"'),'-9')::NUMBER(6,5)                     AS VAR_MAT_COS
      ,COALESCE(REPLACE(X.upd_row:VAR_MAT_OTH,'"'),'-9')::NUMBER(6,5)                     AS VAR_MAT_OTH
      ,COALESCE(REPLACE(X.upd_row:VAR_MANUF_COS,'"'),'-9')::NUMBER(6,5)                   AS VAR_MANUF_COS
      ,COALESCE(REPLACE(X.upd_row:VAR_MANUF_OTH,'"'),'-9')::NUMBER(6,5)                   AS VAR_MANUF_OTH
      ,COALESCE(REPLACE(X.upd_row:VAR_LOG_FTC_IFO,'"'),'-9')::NUMBER(6,5)                 AS VAR_LOG_FTC_IFO
      ,COALESCE(REPLACE(X.upd_row:VAR_LOG_USL,'"'),'-9')::NUMBER(6,5)                     AS VAR_LOG_USL
      ,COALESCE(REPLACE(X.upd_row:VAR_LOG_OTH,'"'),'-9')::NUMBER(6,5)                     AS VAR_LOG_OTH
FROM   (SELECT parse_json(b.VALUE:Updated) as upd_row
              ,a.USER
        FROM   (SELECT VALUE
                      ,THIS:User AS USER
                FROM   LATERAL FLATTEN(parse_json('${JSON_INPUT}')) f1
                WHERE  PATH LIKE '%UpdatedRows%') a
               ,LATERAL FLATTEN(a.VALUE) b) X;
                    `;

    var stmt = snowflake.createStatement( {sqlText: fch_data} );
    var res = stmt.execute();

    var v_cbu_cod;
    var v_cbu_dsc;
    var v_act_src_sce_cod;
    var v_cmp_1st_src_sce_cod;
    var v_cmp_2nd_src_sce_cod;
    var v_cmp_3rd_src_sce_cod;
    var v_cus_dim_grp_cod;
    var v_eib_use_flg;
    var v_tty_use_flg;
    var v_var_ns;
    var v_var_mat_cos;
    var v_var_mat_oth;
    var v_var_manuf_cos;
    var v_var_manuf_oth;
    var v_var_log_ftc_ifo;
    var v_var_log_usl;
    var v_var_log_oth;
    var upd_dat_r;

    while (res.next())  {
        /* Assigning data into local variables */

        v_cbu_cod = res.getColumnValue(1);
        v_cbu_dsc = res.getColumnValue(2);
        v_act_src_sce_cod = res.getColumnValue(3);
        v_cmp_1st_src_sce_cod = res.getColumnValue(4);
        v_cmp_2nd_src_sce_cod = res.getColumnValue(5);
        v_cmp_3rd_src_sce_cod = res.getColumnValue(6);

        v_cus_dim_grp_cod = res.getColumnValue(7);
        v_eib_use_flg = res.getColumnValue(8);
        v_tty_use_flg = res.getColumnValue(9);

        v_var_ns          = res.getColumnValue(10);
        v_var_mat_cos     = res.getColumnValue(11);
        v_var_mat_oth     = res.getColumnValue(12);
        v_var_manuf_cos   = res.getColumnValue(13);
        v_var_manuf_oth   = res.getColumnValue(14);
        v_var_log_ftc_ifo = res.getColumnValue(15);
        v_var_log_usl     = res.getColumnValue(16);
        v_var_log_oth     = res.getColumnValue(17);


        upd_dat_r = `UPDATE COP_DMT_FLX.R_FLX_CBU
                     SET    ACT_SRC_SCE_COD       = COALESCE(NULLIF('${v_act_src_sce_cod}','N/A'),ACT_SRC_SCE_COD)
                           ,CMP_1ST_SRC_SCE_COD   = COALESCE(NULLIF('${v_cmp_1st_src_sce_cod}','N/A'),CMP_1ST_SRC_SCE_COD)
                           ,CMP_2ND_SRC_SCE_COD   = COALESCE(NULLIF('${v_cmp_2nd_src_sce_cod}','N/A'),CMP_2ND_SRC_SCE_COD)
                           ,CMP_3RD_SRC_SCE_COD   = COALESCE(NULLIF('${v_cmp_3rd_src_sce_cod}','N/A'),CMP_3RD_SRC_SCE_COD)
                           ,CUS_DIM_GRP_COD       = COALESCE(NULLIF(${v_cus_dim_grp_cod},-9),CUS_DIM_GRP_COD)
                           ,EIB_USE_FLG           = COALESCE(NULLIF(${v_eib_use_flg},-9),EIB_USE_FLG)
                           ,TTY_USE_FLG           = COALESCE(NULLIF(${v_tty_use_flg},-9),TTY_USE_FLG)
                           ,VAR_NS                = COALESCE(NULLIF(${v_var_ns},-9),VAR_NS)
                           ,VAR_MAT_COS           = COALESCE(NULLIF(${v_var_mat_cos},-9),VAR_MAT_COS)
                           ,VAR_MAT_OTH           = COALESCE(NULLIF(${v_var_mat_oth},-9),VAR_MAT_OTH)
                           ,VAR_MANUF_COS         = COALESCE(NULLIF(${v_var_manuf_cos},-9),VAR_MANUF_COS)
                           ,VAR_MANUF_OTH         = COALESCE(NULLIF(${v_var_manuf_oth},-9),VAR_MANUF_OTH)
                           ,VAR_LOG_FTC_IFO       = COALESCE(NULLIF(${v_var_log_ftc_ifo},-9),VAR_LOG_FTC_IFO)
                           ,VAR_LOG_USL           = COALESCE(NULLIF(${v_var_log_usl},-9),VAR_LOG_USL)
                           ,VAR_LOG_OTH           = COALESCE(NULLIF(${v_var_log_oth},-9),VAR_LOG_OTH)
                           ,T_REC_UPD_TST         = TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP)
                     WHERE  CBU_COD  = '${v_cbu_cod}';
                    `;
        stmt = snowflake.createStatement( {sqlText: upd_dat_r} );
        stmt.execute();

    }

}
/* End of Update Operation Block */

$$
;
