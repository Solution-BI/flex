USE SCHEMA COP_DMT_FLX;

/*
=========================================================================
                                HISTORY
=========================================================================
Description : Create Procedure Script For Populating P_FLX_SCE_CFG_IND Table

Author      : Noel Coquio (Solution BI France)
Created On  : 30-09-2024
=========================================================================
Modified On:    Description:                        Author:
=========================================================================
*/

CREATE OR REPLACE PROCEDURE SP_Flex_Config_Source_Scenario(JSON_INPUT VARCHAR(16777216))
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$

	// This method enables the standard call to a sequential array of queries
	
	function uuidHelper() {
		return (((1 + Math.random()) * 0x10000) | 0).toString(16).substring(1);
	}
	
	function generateuuid() {
		return (uuidHelper() + uuidHelper() + "-" + uuidHelper() + "-4" 
			+ uuidHelper().substring(0,2) + "-" + uuidHelper() + "-" + uuidHelper() + uuidHelper() + uuidHelper()
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

var log_data = snowflake.createStatement({sqlText: `CALL SP_Flex_Log_PowerOn(:1,'SP_Flex_Config_Source_Scenario','${JSON_INPUT}',:2);`, binds:[RUN_ID,v_eml_user]});
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
   var fch_data = `SELECT DISTINCT
       REPLACE(X.ins_row:SCE_ELM_KEY,'"')                             AS SCE_ELM_KEY
      ,REPLACE(X.ins_row:IND_ELM_COD,'"')                             AS IND_ELM_COD
      ,REPLACE(X.ins_row:SRC_SCE_ELM_COD,'"')                         AS SRC_SCE_ELM_COD
      ,REPLACE(X.ins_row:BGN_PER_ELM_COD,'"')                         AS BGN_PER_ELM_COD
      ,REPLACE(X.ins_row:END_PER_ELM_COD,'"')                         AS END_PER_ELM_COD
      ,REPLACE(X.ins_row:CAT_TYP_ELM_COD,'"')                         AS CAT_TYP_ELM_COD
      ,REPLACE(X.ins_row:CUS_ELM_COD,'"')                             AS CUS_ELM_COD
      ,REPLACE(X.ins_row:EIB_ELM_COD,'"')                             AS EIB_ELM_COD
      ,REPLACE(X.ins_row:ETI_ELM_COD,'"')                             AS ETI_ELM_COD
      ,REPLACE(X.ins_row:PDT_ELM_COD,'"')                             AS PDT_ELM_COD
      ,REPLACE(X.ins_row:SAL_SUP_ELM_COD,'"')                         AS SAL_SUP_ELM_COD
      ,REPLACE(X.ins_row:TTY_ELM_COD,'"')                             AS TTY_ELM_COD
      ,SUBSTR(REPLACE(X.USER,'"')
             ,1
             ,POSITION('@',REPLACE(X.USER,'"'),1) - 1
             )                                                        AS USR
      ,REPLACE(X.USER,'"')                                            AS EML_USR
FROM   (SELECT parse_json(b.VALUE) as ins_row
              ,a.USER
        FROM   (SELECT VALUE
                      ,THIS:User AS USER
                FROM   LATERAL FLATTEN(parse_json('${JSON_INPUT}')) f1
                WHERE  PATH LIKE '%InsertedRows%') a
               ,LATERAL FLATTEN(a.VALUE) b) X;
                    `;

   var stmt = snowflake.createStatement( {sqlText: fch_data} );
   var res = stmt.execute();

   /* Define local variables */
   var v_sce_elm_key;
   var v_ind_elm_cod;
   var v_src_sce_elm_cod;
   var v_bgn_per_elm_cod;
   var v_end_per_elm_cod;
   var v_cat_typ_elm_cod;
   var v_cus_elm_cod;
   var v_eib_elm_cod;
   var v_eti_elm_cod;
   var v_pdt_elm_cod;
   var v_sal_sup_elm_cod;
   var v_tty_elm_cod;
   var v_user;
   var v_eml_user;

   var dup_check_cmd;
   var dup_check_stmt;
   var dup_check_res;
   var v_is_dup;

   var sts_check_cmd;
   var sts_check_stmt;
   var sts_check_res;
   var v_is_invalid;

   var cfg_ord_num_cmd;
   var cfg_ord_num_stmt;
   var cfg_ord_num_res;
   var v_cfg_ord_num;
   var ins_cfg_ind_cmd;
   var default_dim = "$TOTAL";

   while (res.next())  {

      /* Assigning data into local variables */
      v_sce_elm_key = res.getColumnValue(1);
      v_ind_elm_cod = res.getColumnValue(2);
      v_src_sce_elm_cod = res.getColumnValue(3);
      v_bgn_per_elm_cod = res.getColumnValue(4);
      v_end_per_elm_cod = res.getColumnValue(5);
      v_cat_typ_elm_cod = res.getColumnValue(6);
      v_cus_elm_cod = res.getColumnValue(7);
      v_eib_elm_cod = res.getColumnValue(8);
      v_eti_elm_cod = res.getColumnValue(9);
      v_pdt_elm_cod = res.getColumnValue(10);
      v_sal_sup_elm_cod = res.getColumnValue(11);
      v_tty_elm_cod = res.getColumnValue(12);
      v_user = res.getColumnValue(13);
      v_eml_user = res.getColumnValue(14);

      /* Check Status in R_FLX_SCE */
      sts_check_cmd = `SELECT COUNT(*) AS RowCount 
                       FROM   COP_DMT_FLX.R_FLX_SCE 
                       WHERE  SCE_ELM_KEY           = '${v_sce_elm_key}'
                       AND    INI_STS_COD      NOT IN ('created','configuration_started');`;
      sts_check_stmt = snowflake.createStatement( {sqlText: sts_check_cmd} );
      sts_check_res = sts_check_stmt.execute();
      sts_check_res.next();

      v_is_invalid = sts_check_res.getColumnValue(1) > 0;

      if (!v_is_invalid) {

         cfg_ord_num_cmd = `SELECT TO_CHAR(TRUNC(COALESCE(MAX(CFG_ORD_NUM),0) + 1,0)) AS NEW_CFG_ORD_NUM 
                            FROM   COP_DMT_FLX.P_FLX_SCE_CFG_IND
                            WHERE  SCE_ELM_KEY = '${v_sce_elm_key}';`;
         cfg_ord_num_stmt = snowflake.createStatement( {sqlText: cfg_ord_num_cmd} );
         cfg_ord_num_res = cfg_ord_num_stmt.execute();
         cfg_ord_num_res.next();
         v_cfg_ord_num = cfg_ord_num_res.getColumnValue(1);


         /* Inserting Scenario to P_FLX_SCE_CFG_IND */
         ins_cfg_ind_cmd = `
INSERT INTO COP_DMT_FLX.P_FLX_SCE_CFG_IND
           (SCE_ELM_KEY
           ,SCE_ELM_COD
           ,CBU_COD
           ,IND_ELM_COD
           ,CFG_ORD_NUM
           ,BGN_PER_ELM_COD
           ,END_PER_ELM_COD
           ,CAT_TYP_ELM_COD
           ,CUS_ELM_COD
           ,EIB_ELM_COD
           ,ETI_ELM_COD
           ,PDT_ELM_COD
           ,SAL_SUP_ELM_COD
           ,TTY_ELM_COD
           ,SRC_SCE_ELM_COD
           ,T_REC_DLT_FLG
           ,T_REC_INS_TST
           ,T_REC_UPD_TST
           )
SELECT      SCE_ELM_KEY                                                           AS SCE_ELM_KEY
           ,SCE_ELM_COD                                                           AS SCE_ELM_COD
           ,CBU_COD                                                               AS CBU_COD
           ,'${v_ind_elm_cod}'                                                    AS IND_ELM_COD
           ,'${v_cfg_ord_num}'                                                    AS CFG_ORD_NUM
           ,COALESCE(NULLIF('${v_bgn_per_elm_cod}',''),'01')                      AS BGN_PER_ELM_COD
           ,COALESCE(NULLIF('${v_end_per_elm_cod}',''),'12')                      AS END_PER_ELM_COD
           ,COALESCE(NULLIF('${v_cat_typ_elm_cod}',''),'${default_dim}_CAT_TYP')  AS CAT_TYP_ELM_COD
           ,COALESCE(NULLIF('${v_cus_elm_cod}',''),'${default_dim}_CUS')          AS CUS_ELM_COD
           ,COALESCE(NULLIF('${v_eib_elm_cod}',''),'${default_dim}_EIB')          AS EIB_ELM_COD
           ,COALESCE(NULLIF('${v_eti_elm_cod}',''),'${default_dim}_ETI')          AS ETI_ELM_COD
           ,COALESCE(NULLIF('${v_pdt_elm_cod}',''),'${default_dim}_PDT')          AS PDT_ELM_COD
           ,COALESCE(NULLIF('${v_sal_sup_elm_cod}',''),'${default_dim}_SAL_SUP')  AS SAL_SUP_ELM_COD
           ,COALESCE(NULLIF('${v_tty_elm_cod}',''),'${default_dim}_TTY')          AS TTY_ELM_COD
           ,'${v_src_sce_elm_cod}'                                                AS SRC_SCE_ELM_COD
           ,0                                                                     AS T_REC_DLT_FLG
           ,TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP)                                   AS T_REC_INS_TST
           ,TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP)                                   AS T_REC_UPD_TST
FROM        COP_DMT_FLX.R_FLX_SCE
WHERE       SCE_ELM_KEY  = '${v_sce_elm_key}';`;
   
         snowflake.execute( {sqlText: ins_cfg_ind_cmd} );
      }
      else {
         throw "<SQLError>Scenario is completely configured, you can't change the configuration.</SQLError>";
      }
   }
}
/* End of Insert Operation Block */

/* Update Operation Block */
if (isUpdated) {

   var fch_data = `
SELECT DISTINCT
       REPLACE(X.upd_row:SCE_ELM_KEY,'"')                             AS SCE_ELM_KEY
      ,REPLACE(X.upd_row:IND_ELM_COD,'"')                             AS IND_ELM_COD
      ,REPLACE(X.upd_row:SRC_SCE_ELM_COD,'"')                         AS SRC_SCE_ELM_COD
      ,REPLACE(X.upd_row:CFG_ORD_NUM,'"')                             AS CFG_ORD_NUM
      ,REPLACE(X.upd_row:BGN_PER_ELM_COD,'"')                         AS BGN_PER_ELM_COD
      ,REPLACE(X.upd_row:END_PER_ELM_COD,'"')                         AS END_PER_ELM_COD
      ,REPLACE(X.upd_row:CAT_TYP_ELM_COD,'"')                         AS CAT_TYP_ELM_COD
      ,REPLACE(X.upd_row:CUS_ELM_COD,'"')                             AS CUS_ELM_COD
      ,REPLACE(X.upd_row:EIB_ELM_COD,'"')                             AS EIB_ELM_COD
      ,REPLACE(X.upd_row:ETI_ELM_COD,'"')                             AS ETI_ELM_COD
      ,REPLACE(X.upd_row:PDT_ELM_COD,'"')                             AS PDT_ELM_COD
      ,REPLACE(X.upd_row:SAL_SUP_ELM_COD,'"')                         AS SAL_SUP_ELM_COD
      ,REPLACE(X.upd_row:TTY_ELM_COD,'"')                             AS TTY_ELM_COD
      ,SUBSTR(REPLACE(X.USER,'"')
             ,1
             ,POSITION('@',REPLACE(X.USER,'"'),1) - 1
             )                                                        AS USR
      ,REPLACE(X.USER,'"')                                            AS EML_USR

      ,DECODE(REPLACE(X.upd_row:ID,'"')
             ,'NA',REPLACE(X.key_row:ID,'"')
             ,REPLACE(X.upd_row:ID,'"'))                              AS ID
FROM   (SELECT parse_json(b.VALUE:Updated) as upd_row
              ,parse_json(b.VALUE:Original) as key_row
              ,a.USER
        FROM   (SELECT VALUE
                      ,THIS:User AS USER
                FROM   LATERAL FLATTEN(parse_json('${JSON_INPUT}')) f1
                WHERE  PATH LIKE '%UpdatedRows%') a
               ,LATERAL FLATTEN(a.VALUE) b) X;
                  `;
   var stmt = snowflake.createStatement( {sqlText: fch_data} );
   var res = stmt.execute();

   /* Define local variables */
   var v_sce_elm_key;
   var v_ind_elm_cod;
   var v_src_sce_elm_cod;
   var v_cfg_ord_num;
   var v_bgn_per_elm_cod;
   var v_end_per_elm_cod;
   var v_cat_typ_elm_cod;
   var v_cus_elm_cod;
   var v_eib_elm_cod;
   var v_eti_elm_cod;
   var v_pdt_elm_cod;
   var v_sal_sup_elm_cod;
   var v_tty_elm_cod;
   var v_user;
   var v_eml_user;

   var dup_check_cmd;
   var dup_check_stmt;
   var dup_check_res;
   var v_is_dup;

   var sts_check_cmd;
   var sts_check_stmt;
   var sts_check_res;
   var v_is_invalid;

   var cfg_ord_num_cmd;
   var cfg_ord_num_stmt;
   var cfg_ord_num_res;
   var v_cfg_ord_num;
   var upd_cfg_ind_cmd;

   var get_cfg_ord_num;
   var get_cfg_ord_num_stmt;
   var get_cfg_ord_num_res;
   var check_cfg_ord_cmd;
   var check_cfg_ord_stmt;
   var check_cfg_ord_res;
   var cur_cfg_ord_num;
   var upd_cfg_ord_cmd;
   var upd_cfg_ord_stmt
   var upd_cfg_ord_res
   var new_cfg_ord_num = [];
   
   

   while (res.next()) {

      /* Assigning data into local variables */
      v_sce_elm_key = res.getColumnValue(1);
      v_ind_elm_cod = res.getColumnValue(2);
      v_src_sce_elm_cod = res.getColumnValue(3);
      v_cfg_ord_num = res.getColumnValue(4);
      v_bgn_per_elm_cod = res.getColumnValue(5);
      v_end_per_elm_cod = res.getColumnValue(6);
      v_cat_typ_elm_cod = res.getColumnValue(7);
      v_cus_elm_cod = res.getColumnValue(8);
      v_eib_elm_cod = res.getColumnValue(9);
      v_eti_elm_cod = res.getColumnValue(10);
      v_pdt_elm_cod = res.getColumnValue(11);
      v_sal_sup_elm_cod = res.getColumnValue(12);
      v_tty_elm_cod = res.getColumnValue(13);
      v_user = res.getColumnValue(14);
      v_eml_user = res.getColumnValue(15);
      v_id = res.getColumnValue(16);

      /* Check Status in R_FLX_SCE */
      sts_check_cmd = `SELECT COUNT(*) AS RowCount 
                       FROM   COP_DMT_FLX.R_FLX_SCE 
                       WHERE  SCE_ELM_KEY           = '${v_sce_elm_key}'
                       AND    INI_STS_COD      NOT IN ('created','configuration_started');
                      `;
      sts_check_stmt = snowflake.createStatement( {sqlText: sts_check_cmd} );
      sts_check_res = sts_check_stmt.execute();
      sts_check_res.next();

      v_is_invalid = sts_check_res.getColumnValue(1) > 0;

      if (!v_is_invalid) {
         
         /* add the new order update in the list */
         new_cfg_ord_num.push({
            id: v_id,
            cfg_ord_num: v_cfg_ord_num
         });
         
         /* Update P_FLX_SCE_CFG_IND */
         upd_cfg_ind_cmd = `UPDATE COP_DMT_FLX.P_FLX_SCE_CFG_IND
                            SET    IND_ELM_COD       = COALESCE('${v_ind_elm_cod}',IND_ELM_COD)
                                  ,SRC_SCE_ELM_COD   = COALESCE('${v_src_sce_elm_cod}',SRC_SCE_ELM_COD)
                                  ,BGN_PER_ELM_COD   = COALESCE('${v_bgn_per_elm_cod}',BGN_PER_ELM_COD)
                                  ,END_PER_ELM_COD   = COALESCE('${v_end_per_elm_cod}',END_PER_ELM_COD)
                                  ,CAT_TYP_ELM_COD   = COALESCE(NULLIF('${v_cat_typ_elm_cod}',''),CAT_TYP_ELM_COD)
                                  ,CUS_ELM_COD       = COALESCE(NULLIF('${v_cus_elm_cod}',''),CUS_ELM_COD)
                                  ,EIB_ELM_COD       = COALESCE(NULLIF('${v_eib_elm_cod}',''),EIB_ELM_COD)
                                  ,ETI_ELM_COD       = COALESCE(NULLIF('${v_eti_elm_cod}',''),ETI_ELM_COD)
                                  ,PDT_ELM_COD       = COALESCE(NULLIF('${v_pdt_elm_cod}',''),PDT_ELM_COD)
                                  ,SAL_SUP_ELM_COD   = COALESCE(NULLIF('${v_sal_sup_elm_cod}',''),SAL_SUP_ELM_COD)
                                  ,TTY_ELM_COD       = COALESCE(NULLIF('${v_tty_elm_cod}',''),TTY_ELM_COD)
                                  ,T_REC_UPD_TST     = TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP)
                            WHERE  ID  = '${v_id}';
                           `;
         stmt = snowflake.createStatement( {sqlText: upd_cfg_ind_cmd} );
         stmt.execute();

      }
      else {
         throw "<SQLError>Scenario is completely configured, you can't change the configuration.</SQLError>";
      }
   }

   if (!v_is_invalid) {

         /* Sort Order to avoid creating bad mismatches*/
         new_cfg_ord_num.sort((a, b) => a.cfg - b.cfg);

         new_cfg_ord_num.forEach(update => {
            /* Get current Order for Each ID */
            check_cfg_ord_cmd = `SELECT CFG_ORD_NUM FROM COP_DMT_FLX.P_FLX_SCE_CFG_IND WHERE id = '${v_id}'`;
            check_cfg_ord_stmt = snowflake.createStatement({ sqlText: check_cfg_ord_cmd });
            check_cfg_ord_res = check_cfg_ord_stmt.execute();

            if (!check_cfg_ord_res.next()) {
                     throw new Error("ID non trouvé dans la table existante : " + v_id);
                  }
               cur_cfg_ord_num = check_cfg_ord_res.getColumnValue(1);

               /* Update P_FLX_SCE_CFG_IND with the new order */
               upd_cfg_ord_cmd = `
                  UPDATE COP_DMT_FLX.P_FLX_SCE_CFG_IND
                     SET CFG_ORD_NUM = CASE
                                          WHEN ID = '${v_id}' THEN ${v_cfg_ord_num} -- Nouvelle position pour l'ID mis à jour
                                          WHEN CFG_ORD_NUM >= ${v_cfg_ord_num} AND CFG_ORD_NUM < ${cur_cfg_ord_num} THEN CFG_ORD_NUM + 1 -- Décaler les CFG vers le haut
                                          WHEN CFG_ORD_NUM <= ${v_cfg_ord_num} AND CFG_ORD_NUM > ${cur_cfg_ord_num} THEN CFG_ORD_NUM - 1 -- Décaler les CFG vers le bas
                                          ELSE CFG_ORD_NUM -- Les autres restent inchangés
                                       END
                                       ,T_REC_UPD_TST = TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP)
                     WHERE SCE_ELM_KEY = '${v_sce_elm_key}';
            `;
            upd_cfg_ord_stmt = snowflake.createStatement({ sqlText: upd_cfg_ord_cmd });
            upd_cfg_ord_res = upd_cfg_ord_stmt.execute();
         });
   }
   else {
         throw "<SQLError>Scenario is completely configured, you can't change the configuration.</SQLError>";
      }

}
/* End of Update Operation Block */

/* Delete Operation Block */
if (isDeleted) {
    /* Getting ID to DELETE */
    var cmd_id = `
SELECT DISTINCT
       REPLACE(X.del_row:ID,'"')                                 AS ID
FROM   (SELECT parse_json(b.VALUE) as del_row
              ,a.USER
        FROM   (SELECT VALUE
                      ,THIS:User AS USER
                FROM   LATERAL FLATTEN(parse_json('${JSON_INPUT}')) f1
                WHERE  PATH LIKE '%DeletedRows%') a
               ,LATERAL FLATTEN(a.VALUE) b) X;
                 `;

   var stmt = snowflake.createStatement( {sqlText: cmd_id} );
   var res = stmt.execute();

   var v_id;

   var sts_check_cmd;
   var sts_check_stmt;
   var sts_check_res;
   var v_is_invalid;

   var del_rec_cmd;
   var del_rec_stmt;

   var check_cfg_ord_cmd;
   var check_cfg_ord_stmt;
   var check_cfg_ord_res;
   var cur_cfg_ord_num;
   var upd_cfg_ord_cmd;

   while (res.next())  {

      /* Assigning data into local variables */
      v_id = res.getColumnValue(1);

      /* Check Status in R_FLX_SCE */
      sts_check_cmd = `SELECT COUNT(*) AS RowCount 
                       FROM   COP_DMT_FLX.R_FLX_SCE 
                              INNER JOIN COP_DMT_FLX.P_FLX_SCE_CFG_IND ON
                              (
                                 P_FLX_SCE_CFG_IND.SCE_ELM_KEY = R_FLX_SCE.SCE_ELM_KEY
                              )
                       WHERE  P_FLX_SCE_CFG_IND.ID       = '${v_id}'
                       AND    R_FLX_SCE.INI_STS_COD NOT IN ('created','configuration_started');`;
      sts_check_stmt = snowflake.createStatement( {sqlText: sts_check_cmd} );
      sts_check_res = sts_check_stmt.execute();
      sts_check_res.next();

      v_is_invalid = 0; //sts_check_res.getColumnValue(1) > 0;

      if (!v_is_invalid) {

         /* Get current Order for Each ID */
         check_cfg_ord_cmd = `SELECT SCE_ELM_KEY, CFG_ORD_NUM FROM COP_DMT_FLX.P_FLX_SCE_CFG_IND WHERE id = '${v_id}'`;
         check_cfg_ord_stmt = snowflake.createStatement({ sqlText: check_cfg_ord_cmd });
         check_cfg_ord_res = check_cfg_ord_stmt.execute();

         /* Physically DELETE data from P_FLX_SCE_CFG_IND */
         del_rec_cmd = `DELETE FROM COP_DMT_FLX.P_FLX_SCE_CFG_IND 
                        WHERE  ID = '${v_id}';
                       `;
         del_rec_stmt = snowflake.createStatement( {sqlText: del_rec_cmd} );
         del_rec_stmt.execute();

         if (!check_cfg_ord_res.next()) {
                  throw new Error("ID non trouvé dans la table existante : " + v_id);
               }
         cur_sce_elm_key = check_cfg_ord_res.getColumnValue(1);
         cur_cfg_ord_num = check_cfg_ord_res.getColumnValue(2);

         /* Update P_FLX_SCE_CFG_IND with the new order */
         upd_cfg_ord_cmd = `
            UPDATE COP_DMT_FLX.P_FLX_SCE_CFG_IND
               SET CFG_ORD_NUM = CFG_ORD_NUM - 1
                                 ,T_REC_UPD_TST = TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP)
               WHERE SCE_ELM_KEY = '${cur_sce_elm_key}'
               AND CFG_ORD_NUM > ${cur_cfg_ord_num};
            `;
         upd_cfg_ord_stmt = snowflake.createStatement({ sqlText: upd_cfg_ord_cmd });
         upd_cfg_ord_res = upd_cfg_ord_stmt.execute();

      }
      else {
         throw "<SQLError>Scenario is completely configured, you can't change the configuration.</SQLError>";
      }
   }
}
/* End of Delete Operation Block */

$$
;
