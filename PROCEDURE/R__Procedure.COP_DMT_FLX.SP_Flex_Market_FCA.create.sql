USE SCHEMA COP_DMT_FLX{{uid}};

/*
=========================================================================
                                HISTORY
=========================================================================
Description : Create Procedure Script For Populating P_FLX_ETI_PDT_FCA Table

Author      : Noel Coquio (Solution BI France)
Created On  : 04-07-2024
=========================================================================
Modified On:    Description:                        Author:
17-08-2023      CRUD Operation handling             Manan M. Shuddho
25-08-2023      Added Variability & dynamic         Manan M. Shuddho
                source tables
=========================================================================
*/

CREATE OR REPLACE PROCEDURE SP_Flex_Market_FCA(JSON_INPUT VARCHAR(16777216))
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

var log_data = snowflake.createStatement({sqlText: `CALL SP_Flex_Log_PowerOn(:1,'SP_Flex_Market_FCA','${JSON_INPUT}',:2);`, binds:[RUN_ID,v_eml_user]});
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
      ,COALESCE(REPLACE(X.ins_row:CFG_ORD_NUM,'"'),'-1')::NUMBER(2,0)                    AS CFG_ORD_NUM
      ,REPLACE(X.ins_row:ETI_ELM_COD,'"')                                                AS ETI_ELM_COD
      ,REPLACE(X.ins_row:ETI_ELM_DSC,'"')                                                AS ETI_ELM_DSC
      ,REPLACE(X.ins_row:PDT_GRP_COD,'"')                                                AS PDT_GRP_COD
      ,COALESCE(REPLACE(X.ins_row:FCA_MAT_OTH_VAL,'"'),'0')::NUMBER(32,12)               AS FCA_MAT_OTH_VAL
      ,COALESCE(REPLACE(X.ins_row:FCA_MANUF_OTH_VAL,'"'),'0')::NUMBER(32,12)             AS FCA_MANUF_OTH_VAL
      ,COALESCE(REPLACE(X.ins_row:FCA_LOG_OTH_VAL,'"'),'0')::NUMBER(32,12)               AS FCA_LOG_OTH_VAL
      ,SUBSTR(REPLACE(X.USER,'"')
             ,1
             ,POSITION('@',REPLACE(X.USER,'"'),1) - 1
             )                                                                           AS T_REC_INS_USR
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

    var v_id;
    var v_cbu_cod;
    var v_cfg_ord_num;
    var v_eti_elm_cod;
    var v_eti_elm_dsc;
    var v_pdt_grp_cod;
    var v_mat_oth_fca_val;
    var v_fca_manuf_oth_val;
    var v_fca_log_oth_val;

    var cfg_ord_num_cmd;
    var cfg_ord_num_stmt;
    var cfg_ord_num_res;

    var v_user;

    /* Retreiove CBU_COD & ETI_ELM_DSC from R_FLX_ETI */
    var eti_cmd;
    var eti_stmt;
    var eti_res;

    /* Check duplicate in P_FLX_ETI_PDT_FCA */
    var dup_check_cmd;
    var dup_check_stmt;
    var dup_check_res;
    var v_is_dup = 0;
    var ins_sce_cmd;

    while (res.next())  {

        /* Assigning data into local variables */
        v_cbu_cod = res.getColumnValue(1);
        v_cfg_ord_num = res.getColumnValue(2);
        v_eti_elm_cod = res.getColumnValue(3);
        v_eti_elm_dsc = res.getColumnValue(4);
        v_pdt_grp_cod = res.getColumnValue(5);
        v_mat_oth_fca_val = res.getColumnValue(6);
        v_fca_manuf_oth_val = res.getColumnValue(7);
        v_fca_log_oth_val = res.getColumnValue(8);
        v_user = res.getColumnValue(9);

        /* Retreive ETI data */
        eti_cmd = `SELECT ETI_ELM_DSC
                   FROM   COP_DMT_FLX.R_FLX_ETI
                   WHERE  ETI_ELM_COD = '${v_eti_elm_cod}';
                  `;
        eti_stmt = snowflake.createStatement( {sqlText: eti_cmd} );
        eti_res = eti_stmt.execute();
        eti_res.next();

        v_eti_elm_dsc = eti_res.getColumnValue(1);

        /* Check duplicate in P_FLX_ETI_PDT_FCA */
        dup_check_cmd = `SELECT COUNT(*) AS RowCount 
                             FROM   P_FLX_ETI_PDT_FCA 
                             WHERE  CBU_COD = '${v_cbu_cod}'
                             AND    ETI_ELM_COD = '${v_eti_elm_cod}'
                             AND    PDT_GRP_COD = '${v_pdt_grp_cod}';`;
        dup_check_stmt = snowflake.createStatement( {sqlText: dup_check_cmd} );
        dup_check_res = dup_check_stmt.execute();
        dup_check_res.next();

        v_is_dup = dup_check_res.getColumnValue(1) > 0;

        /* If not a duplicate, Insert */
        if (!v_is_dup) {

            if (v_cfg_ord_num === -1) {

                cfg_ord_num_cmd = `SELECT TO_CHAR(TRUNC(COALESCE(MAX(CFG_ORD_NUM),0) + 1,0)) AS NEW_CFG_ORD_NUM 
                                   FROM   COP_DMT_FLX.P_FLX_ETI_PDT_FCA
                                   WHERE  CBU_COD = '${v_cbu_cod}';`;
                cfg_ord_num_stmt = snowflake.createStatement( {sqlText: cfg_ord_num_cmd} );
                cfg_ord_num_res = cfg_ord_num_stmt.execute();
                cfg_ord_num_res.next();
                v_cfg_ord_num = cfg_ord_num_res.getColumnValue(1);

            }

            /* Inserting Scenario to P_FLX_ETI_PDT_FCA */

            ins_sce_cmd = `INSERT INTO COP_DMT_FLX.P_FLX_ETI_PDT_FCA
                                      (CBU_COD
                                      ,CFG_ORD_NUM
                                      ,ETI_ELM_COD     
                                      ,ETI_ELM_DSC     
                                      ,PDT_GRP_COD 
                                      ,FCA_MAT_OTH_VAL
                                      ,FCA_MANUF_OTH_VAL
                                      ,FCA_LOG_OTH_VAL
                                      ,T_REC_DLT_FLG   
                                      ,T_REC_INS_TST   
                                      ,T_REC_UPD_TST   
                                      )
                           SELECT      DISTINCT
                                       '${v_cbu_cod}'                      AS CBU_COD
                                      ,${v_cfg_ord_num}                    AS CFG_ORD_NUM
                                      ,'${v_eti_elm_cod}'                  AS ETI_ELM_COD     
                                      ,'${v_eti_elm_dsc}'                  AS ETI_ELM_DSC     
                                      ,'${v_pdt_grp_cod}'                  AS PDT_GRP_COD 
                                      ,${v_mat_oth_fca_val}                AS FCA_MAT_OTH_VAL
                                      ,${v_fca_manuf_oth_val}              AS FCA_MANUF_OTH_VAL
                                      ,${v_fca_log_oth_val}                AS FCA_LOG_OTH_VAL
                                      ,0                                   AS T_REC_DLT_FLG   
                                      ,TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP) AS T_REC_INS_TST   
                                      ,TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP) AS T_REC_UPD_TST   
                          `;
            snowflake.execute( {sqlText: ins_sce_cmd} );

            

        }
        else {
            throw "<SQLError>A row already exists for the Market, Entity and Product Category.</SQLError>";
        }

    }

}
/* End of Insert Operation Block */

/* Update Operation Block */
if (isUpdated) {

    var fch_data = `
SELECT DISTINCT
       REPLACE(X.upd_row:ID,'"')::NUMBER                                   AS ID
      ,REPLACE(X.upd_row:CFG_ORD_NUM,'"')                                  AS CFG_ORD_NUM
      ,REPLACE(X.upd_row:FCA_MAT_OTH_VAL,'"')::NUMBER(32,12)               AS FCA_MAT_OTH_VAL
      ,REPLACE(X.upd_row:FCA_MANUF_OTH_VAL,'"')::NUMBER(32,12)             AS FCA_MANUF_OTH_VAL
      ,REPLACE(X.upd_row:FCA_LOG_OTH_VAL,'"')::NUMBER(32,12)               AS FCA_LOG_OTH_VAL
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

    var v_id;
    var v_mat_oth_fca_val;
    var v_fca_manuf_oth_val;
    var v_fca_log_oth_val;

    var upd_dat_r;

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
    var cur_cbu_cod;
    var cur_cfg_ord_num;
    var upd_cfg_ord_cmd;
    var upd_cfg_ord_stmt
    var upd_cfg_ord_res
    var new_cfg_ord_num = [];

    while (res.next())  {

        /* Assigning data into local variables */
        v_id = res.getColumnValue(1);
        v_cfg_ord_num = res.getColumnValue(2)
        v_mat_oth_fca_val = res.getColumnValue(3);
        v_fca_manuf_oth_val = res.getColumnValue(4);
        v_fca_log_oth_val = res.getColumnValue(5);

        if(v_mat_oth_fca_val !== null) {

            upd_dat_r = `UPDATE COP_DMT_FLX.P_FLX_ETI_PDT_FCA
                        SET  FCA_MAT_OTH_VAL    = '${v_mat_oth_fca_val}'
                            ,FCA_MANUF_OTH_VAL  = '${v_fca_manuf_oth_val}'
                            ,FCA_LOG_OTH_VAL    = '${v_fca_log_oth_val}'
                            ,T_REC_UPD_TST       = TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP)
                        WHERE   ID  = '${v_id}';
                        `;
            stmt = snowflake.createStatement( {sqlText: upd_dat_r} );
            stmt.execute();

        }

        new_cfg_ord_num.push({
            id: v_id,
            cfg_ord_num: v_cfg_ord_num
        });

    }

    /* Sort Order to avoid creating bad mismatches*/
        new_cfg_ord_num.sort((a, b) => a.cfg - b.cfg);

        new_cfg_ord_num.forEach(update => {
           /* Get current Order for Each ID */
           check_cfg_ord_cmd = `SELECT CBU_COD, CFG_ORD_NUM FROM COP_DMT_FLX.P_FLX_ETI_PDT_FCA WHERE id = '${v_id}'`;
           check_cfg_ord_stmt = snowflake.createStatement({ sqlText: check_cfg_ord_cmd });
           check_cfg_ord_res = check_cfg_ord_stmt.execute();

           if (!check_cfg_ord_res.next()) {
                    throw new Error("ID non trouvé dans la table existante : " + v_id);
                }
            cur_cbu_cod     = check_cfg_ord_res.getColumnValue(1);
            cur_cfg_ord_num = check_cfg_ord_res.getColumnValue(2);

            /* Update P_FLX_ETI_PDT_FCA with the new order */
            upd_cfg_ord_cmd = `
               UPDATE COP_DMT_FLX.P_FLX_ETI_PDT_FCA
                  SET CFG_ORD_NUM = CASE
                                       WHEN ID = '${v_id}' THEN ${v_cfg_ord_num} -- Nouvelle position pour l'ID mis à jour
                                       WHEN CFG_ORD_NUM >= ${v_cfg_ord_num} AND CFG_ORD_NUM < ${cur_cfg_ord_num} THEN CFG_ORD_NUM + 1 -- Décaler les CFG vers le haut
                                       WHEN CFG_ORD_NUM <= ${v_cfg_ord_num} AND CFG_ORD_NUM > ${cur_cfg_ord_num} THEN CFG_ORD_NUM - 1 -- Décaler les CFG vers le bas
                                       ELSE CFG_ORD_NUM -- Les autres restent inchangés
                                    END
                                    ,T_REC_UPD_TST = TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP)
                  WHERE CBU_COD = '${cur_cbu_cod}';
                `;
            upd_cfg_ord_stmt = snowflake.createStatement({ sqlText: upd_cfg_ord_cmd });
            upd_cfg_ord_res = upd_cfg_ord_stmt.execute();
         });



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

   var del_rec_cmd;
   var del_rec_stmt;

   var check_cfg_ord_cmd;
   var check_cfg_ord_stmt;
   var check_cfg_ord_res;
   var cur_cfg_ord_num;
   var cur_cbu_cod;
   var upd_cfg_ord_cmd;

   while (res.next())  {

      /* Assigning data into local variables */
      v_id = res.getColumnValue(1);

      /* Get current Order for Each ID */
      check_cfg_ord_cmd = `SELECT CBU_COD, CFG_ORD_NUM FROM COP_DMT_FLX.P_FLX_ETI_PDT_FCA WHERE id = '${v_id}'`;
      check_cfg_ord_stmt = snowflake.createStatement({ sqlText: check_cfg_ord_cmd });
      check_cfg_ord_res = check_cfg_ord_stmt.execute();

      /* Physically DELETE data from P_FLX_ETI_PDT_FCA */
      del_rec_cmd = `DELETE FROM COP_DMT_FLX.P_FLX_ETI_PDT_FCA 
                     WHERE  ID = '${v_id}';
                    `;
      del_rec_stmt = snowflake.createStatement( {sqlText: del_rec_cmd} );
      del_rec_stmt.execute();

      if (!check_cfg_ord_res.next()) {
               throw new Error("ID non trouvé dans la table existante : " + v_id);
            }
      cur_cbu_cod = check_cfg_ord_res.getColumnValue(1);
      cur_cfg_ord_num = check_cfg_ord_res.getColumnValue(2);

      /* Update P_FLX_SCE_CFG_IND with the new order */
      upd_cfg_ord_cmd = `
         UPDATE COP_DMT_FLX.P_FLX_ETI_PDT_FCA
            SET CFG_ORD_NUM = CFG_ORD_NUM - 1
                              ,T_REC_UPD_TST = TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP)
            WHERE CBU_COD = '${cur_cbu_cod}'
            AND CFG_ORD_NUM > ${cur_cfg_ord_num};
         `;
      upd_cfg_ord_stmt = snowflake.createStatement({ sqlText: upd_cfg_ord_cmd });
      upd_cfg_ord_res = upd_cfg_ord_stmt.execute();

   }

}

$$
;
