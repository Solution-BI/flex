USE SCHEMA COP_DMT_FLX;

/*
=========================================================================
                                HISTORY
=========================================================================
Description : Create Procedure Script For Populating R_FLX_SCE Table

Author      : Noel Coquio (Solution BI France)
Created On  : 04-07-2024
=========================================================================
Modified On:    Description:                        Author:
06-01-2025      Add Description and comment         Noel Coquio
                special character process
=========================================================================
*/


CREATE OR REPLACE PROCEDURE SP_FLEX_CREATE_SCENARIO(JSON_INPUT VARCHAR(16777216))
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

    class Query{
        constructor(statement){
          this.statement = statement;
        }
    }


var RUN_ID = generateuuid();

/* Retreive only once the User email and user */
var v_eml_user;
var v_user;

/* need to replace special characters for the Parse_Jason function */

var v_string = JSON_INPUT;
/* var v_json_input = v_string; */
var v_json_input = v_string
                   .replaceAll("'", "QUOTE")
                   ;

/* Try to parse the JSON Message */
var check_json_cmd = `SELECT COALESCE(TRY_PARSE_JSON('${v_json_input}'),'ERROR') json_data;`;
var check_json_res = snowflake.execute( {sqlText: check_json_cmd} );
check_json_res.next();
var isJsonValid = check_json_res.getColumnValue(1);

if ( isJsonValid === "ERROR" ) {
   /* analyze the field scenario description and comment */
   var v_field = '"SCE_ELM_DSC","SCE_CMT_TXT"';
   var cmd = {sqlText: `CALL SP_FLEX_REPLACE_CHARACTER(:1, :2);`
                     ,binds:[v_json_input
                            ,v_field
                            ]
             };

   var query = new Query(snowflake.createStatement(cmd));
   query.resultSet = query.statement.execute();
   query.resultSet.next();
   v_json_input = query.resultSet.getColumnValue(1);
}

var user_cmd = `SELECT DISTINCT
                       REPLACE(T_REC_USER,'"')                                AS T_REC_EML_USR
                      ,SUBSTR(REPLACE(T_REC_USER,'"')
                             ,1
                             ,POSITION('@',REPLACE(T_REC_USER,'"'),1) - 1
                             )                                                AS T_REC_USER
                FROM   (SELECT T_REC_USER
                        FROM   (SELECT THIS:User AS T_REC_USER
                                FROM   LATERAL FLATTEN(parse_json('${v_json_input}'))));
               `;

var user_res = snowflake.execute( {sqlText: user_cmd} );
user_res.next();

v_eml_user = user_res.getColumnValue(1);
v_user = user_res.getColumnValue(2);

/* Logging incoming JSON data */
var log_data = snowflake.createStatement({sqlText: `CALL SP_Flex_Log_PowerOn(:1,'SP_Flex_Create_Scenario',:2,:3);`, binds:[RUN_ID,v_json_input,v_eml_user]});
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
                      FROM LATERAL FLATTEN(parse_json('${v_json_input}'));`;

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
       REPLACE(X.INSERTEDROWS:CBU_COD,'"')                                       AS CBU_COD
      ,REPLACE(X.INSERTEDROWS:SCE_ELM_DSC,'"')                                   AS SCE_ELM_DSC
      ,REPLACE(X.INSERTEDROWS:SCE_PRO_COD,'"')                                   AS SCE_PRO_COD
      ,REPLACE(REPLACE(X.INSERTEDROWS:DTA_YEA_COD,'"'),'null','1000')::INT       AS DTA_YEA_COD
      ,REPLACE(REPLACE(X.INSERTEDROWS:SCE_YEA_COD,'"'),'null','1000')::INT       AS SCE_YEA_COD
      ,REPLACE(X.INSERTEDROWS:LST_ACT_PER_COD,'"')                               AS LST_ACT_PER_COD
      ,REPLACE(X.INSERTEDROWS:CUR_COD,'"')                                       AS CUR_COD
      ,REPLACE(REPLACE(X.INSERTEDROWS:SCE_CMT_TXT,'null'),'"')                   AS SCE_CMT_TXT
      ,COALESCE(REPLACE(X.INSERTEDROWS:UPD_ACT_FLG,'"')::INT,0)                  AS UPD_ACT_FLG
      ,REPLACE(REPLACE(REPLACE(X.INSERTEDROWS:SCE_ELM_DSC,'"')
                      ,'DOUBLEQUOTE','"'),'QUOTE','\\\'')                        AS DUP_SCE_ELM_DSC
FROM   (SELECT parse_json(b.VALUE) as INSERTEDROWS
        FROM   (SELECT VALUE
                FROM   LATERAL FLATTEN(parse_json('${v_json_input}')) f1
                WHERE  PATH LIKE '%InsertedRows%') a
              ,LATERAL FLATTEN(a.VALUE) b) X;
                    `;

   var stmt = snowflake.createStatement( {sqlText: fch_data} );
   var res = stmt.execute();
   res.next();

   /* Assigning data into local variables */
   var v_cbu_cod = res.getColumnValue(1);
   var v_sce_elm_dsc = res.getColumnValue(2);
   var v_sce_pro_cod = res.getColumnValue(3);
   var v_dta_yea_cod = res.getColumnValue(4);
   var v_sce_yea_cod = res.getColumnValue(5);
   var v_lst_act_per_cod = res.getColumnValue(6);
   var v_cur_cod = res.getColumnValue(7);
   var v_sce_cmt_txt = res.getColumnValue(8);
   var v_upd_act_flg = res.getColumnValue(9);
   var v_dup_sce_elm_dsc = res.getColumnValue(10);

   /* Check Validate year */
   if ( v_sce_yea_cod < 2020 || v_dta_yea_cod < 2020 ) {
      throw "<SQLError>Unable to create a scenario for a scenario year or a data year lower than 2020.</SQLError>";
   }

   /* Check duplicate in R_FLX_SCE */
   var dup_check_cmd = `SELECT   COUNT(*) AS RowCount
FROM     COP_DMT_FLX.R_FLX_SCE 
WHERE    REPLACE(REPLACE(SCE_ELM_DSC,'"','DOUBLEQUOTE'),'''','QUOTE') = '${v_sce_elm_dsc}'`;
   var dup_check_stmt = snowflake.createStatement( {sqlText: dup_check_cmd} );
   var dup_check_res = dup_check_stmt.execute();
   dup_check_res.next();

   var v_is_dup = dup_check_res.getColumnValue(1) > 0;

   /* If not a duplicate, Insert */
   if (!v_is_dup) {

      /* Define the Scenario code */ 
      var sce_cod_cmd = `SELECT TO_CHAR(${v_sce_yea_cod})                                  || '_' || 
                                '${v_sce_pro_cod}'                                         || '_' || 
                                (CASE WHEN ${v_dta_yea_cod} < ${v_sce_yea_cod}  THEN 'PY'
                                      ELSE 'N' || TO_CHAR(TRUNC((${v_dta_yea_cod} - ${v_sce_yea_cod}),0))
                                 END)                                               || '_FLEX_V'  AS SCE_ELM_COD_ ;`;
      var sce_cod_stmt = snowflake.createStatement( {sqlText: sce_cod_cmd} );
      var sce_cod_res = sce_cod_stmt.execute();
      sce_cod_res.next();
      var v_sce_cod = sce_cod_res.getColumnValue(1);

/*

      var rs = snowflake.execute (
          {
              sqlText : `CALL SP_Flex_Generate_Scenario_Code(:1, :2, :3, :4);`
             ,binds:[${v_sce_yea_cod}, ${v_sce_pro_cod}, ${v_cbu_cod}, ${v_dta_yea_cod}]
          }
      );

      var v_sce_elm_cod = rs.getColumnValue(1); 
*/
      sce_cod_cmd = `SELECT '${v_sce_cod}' || TO_CHAR(TRUNC(COALESCE(MAX(TO_NUMBER(REPLACE(SCE_ELM_COD,'${v_sce_cod}'))),0) + 1,0)) AS NEW_SCE_ELM_COD 
                     FROM   COP_DMT_FLX.R_FLX_SCE
                     WHERE  SCE_ELM_COD   LIKE '${v_sce_cod}' || '%'
                     AND    CBU_COD          = '${v_cbu_cod}'
                     ;`;
      var sce_cod_stmt = snowflake.createStatement( {sqlText: sce_cod_cmd} );
      var sce_cod_res = sce_cod_stmt.execute();
      sce_cod_res.next();
      var v_sce_elm_cod = sce_cod_res.getColumnValue(1);

      var v_fca_mat_oth_flg;

      var tot_fca_flg_cmd = `SELECT COALESCE(CAST(SUM(FCA_MAT_OTH_VAL) / 
                                                  NULLIFZERO(SUM(FCA_MAT_OTH_VAL))
                                                  AS NUMBER(2,0))
                                            ,0)             FCA_MAT_OTH_FLG
                             FROM   COP_DMT_FLX.P_FLX_ETI_PDT_FCA
                             WHERE  CBU_COD = '${v_cbu_cod}';`;
      var tot_fca_flg_stmt = snowflake.createStatement( {sqlText: tot_fca_flg_cmd} );
      var tot_fca_flg_res = tot_fca_flg_stmt.execute();
      tot_fca_flg_res.next();
      v_fca_mat_oth_flg = tot_fca_flg_res.getColumnValue(1);

      /* Inserting Scenario to R_FLX_SCE */

      var ins_sce_cmd = `INSERT INTO COP_DMT_FLX.R_FLX_SCE
           (SCE_ELM_KEY
           ,SCE_ELM_COD
           ,SCE_ELM_DSC
           ,CBU_COD
           ,CUR_COD
           ,SCE_YEA_COD
           ,DTA_YEA_COD
           ,SCE_PRO_COD
           ,LST_ACT_PER_COD
           ,UPD_ACT_FLG
           ,ACT_SRC_SCE_COD
           ,CMP_1ST_SRC_SCE_COD
           ,CMP_2ND_SRC_SCE_COD
           ,CMP_3RD_SRC_SCE_COD
           ,CRE_EML_USR_COD
           ,CRE_END_TST
--           ,INI_RQT_EML_USR_COD
           ,INI_STS_COD
           ,INI_RQT_FLG
           ,CCD_STS_COD
           ,CCD_RQT_FLG
           ,DLT_STS_COD
           ,DLT_RQT_FLG
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
           ,COGS_TOT_FCA_FLG
           ,SCE_CMT_TXT
           ,SCE_CMT_TST
           ,SCE_CMT_EML_USR_COD
           ,T_REC_DLT_FLG
           ,T_REC_INS_TST
           ,T_REC_UPD_TST
           )
SELECT      DISTINCT
            '${v_cbu_cod}' || '-' || '${v_sce_elm_cod}'                               AS SCE_ELM_KEY
           ,'${v_sce_elm_cod}'                                                        AS SCE_ELM_COD
           ,REPLACE(REPLACE('${v_sce_elm_dsc}','DOUBLEQUOTE','"'),'QUOTE','''')       AS SCE_ELM_DSC
           ,'${v_cbu_cod}'                                                            AS CBU_COD
           ,'${v_cur_cod}'                                                            AS CUR_COD
           ,REPLACE('${v_sce_yea_cod}','null','1000')::INT                            AS SCE_YEA_COD
           ,REPLACE('${v_dta_yea_cod}','null','1000')::INT                            AS DTA_YEA_COD
           ,'${v_sce_pro_cod}'                                                        AS SCE_PRO_COD
           ,'${v_lst_act_per_cod}'                                                    AS LST_ACT_PER_COD
           ,'${v_upd_act_flg}'                                                        AS UPD_ACT_FLG
           ,ACT_SRC_SCE_COD                                                           AS ACT_SRC_SCE_COD
           ,CMP_1ST_SRC_SCE_COD                                                       AS CMP_1ST_SRC_SCE_COD
           ,CMP_2ND_SRC_SCE_COD                                                       AS CMP_2ND_SRC_SCE_COD
           ,CMP_3RD_SRC_SCE_COD                                                       AS CMP_3RD_SRC_SCE_COD
           ,'${v_user}'                                                               AS CRE_EML_USR_COD
           ,TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP)                                       AS CRE_END_TST
--           ,'${v_eml_user}'                                                           AS INI_RQT_EML_USR_COD
           ,'created'                                                                 AS INI_STS_COD
           ,0                                                                         AS INI_RQT_FLG
           ,'na'                                                                      AS CCD_STS_COD
           ,0                                                                         AS CCD_RQT_FLG
           ,'na'                                                                      AS DLT_STS_COD
           ,0                                                                         AS DLT_RQT_FLG
           ,CUS_DIM_GRP_COD                                                           AS CUS_DIM_GRP_COD
           ,EIB_USE_FLG                                                               AS EIB_USE_FLG
           ,TTY_USE_FLG                                                               AS TTY_USE_FLG
           ,VAR_NS                                                                    AS VAR_NS
           ,VAR_MAT_COS                                                               AS VAR_MAT_COS
           ,VAR_MAT_OTH                                                               AS VAR_MAT_OTH
           ,VAR_MANUF_COS                                                             AS VAR_MANUF_COS
           ,VAR_MANUF_OTH                                                             AS VAR_MANUF_OTH
           ,VAR_LOG_FTC_IFO                                                           AS VAR_LOG_FTC_IFO
           ,VAR_LOG_USL                                                               AS VAR_LOG_USL
           ,VAR_LOG_OTH                                                               AS VAR_LOG_OTH
           ,'${v_fca_mat_oth_flg}'                                                    AS COGS_TOT_FCA_FLG
           ,REPLACE(REPLACE('${v_sce_cmt_txt}','DOUBLEQUOTE','"'),'QUOTE','''')       AS SCE_CMT_TXT
           ,IFF('${v_sce_cmt_txt}'='null',null,TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP))   AS SCE_CMT_TST
           ,IFF('${v_sce_cmt_txt}'='null',null,'${v_eml_user}')                       AS SCE_CMT_EML_USR_COD
           ,0                                                                         AS T_REC_DLT_FLG
           ,TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP)                                       AS T_REC_SRC_TST
           ,TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP)                                       AS T_REC_UPD_TST
FROM        COP_DMT_FLX.R_FLX_CBU
WHERE       R_FLX_CBU.CBU_COD = '${v_cbu_cod}'
                            `;
      snowflake.execute( {sqlText: ins_sce_cmd} );

      var ins_clo_cmd = `INSERT INTO COP_DMT_FLX.P_FLX_GAP_CLO_CFG
            (SCE_ELM_KEY
            ,CBU_COD
            ,SCE_ELM_COD
            ,ETI_ELM_COD
            ,LV0_PDT_CAT_COD
            ,GAP_CLO_PER_COD
            ,T_REC_DLT_FLG
            ,T_REC_INS_TST
            ,T_REC_UPD_TST
            )
SELECT      DISTINCT
            '${v_cbu_cod}' || '-' || '${v_sce_elm_cod}'                               AS SCE_ELM_KEY
            ,'${v_cbu_cod}'                                                           AS CBU_COD
            ,'${v_sce_elm_cod}'                                                       AS SCE_ELM_COD
            ,ETI_ELM_COD                                                              AS ETI_ELM_COD
            ,LV0_PDT_CAT_COD                                                          AS LV0_PDT_CAT_COD
            ,12                                                                       AS GAP_CLO_PER_COD
            ,0                                                                        AS T_REC_DLT_FLG
            ,TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP)                                      AS T_REC_INS_TST
            ,TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP)                                      AS T_REC_UPD_TST
FROM        R_FLX_PDT
INNER JOIN  R_FLX_ETI ON R_FLX_PDT.CBU_COD = R_FLX_ETI.CBU_COD
INNER JOIN  R_FLX_CBU ON R_FLX_PDT.CBU_COD = R_FLX_CBU.CBU_COD
WHERE       R_FLX_PDT.CBU_COD          = '${v_cbu_cod}'
AND         R_FLX_CBU.ACT_SRC_SCE_COD IS NOT NULL
AND         R_FLX_ETI.T_REC_DLT_FLG    = 0`;

      snowflake.execute( {sqlText: ins_clo_cmd} );

   }
   else {
      var v_err_msg = "<SQLError>Scenario name \"" + v_dup_sce_elm_dsc + "\" already exists for an other scenario.</SQLError>";
      throw v_err_msg;
   }

}
/* End of Insert Operation Block */

/* Update Operation Block */
if (isUpdated) {

   /* Assigning data into local variables */
   var v_sce_elm_key;
   var v_sce_elm_dsc;
   var v_lst_act_per_cod;
   var v_cur_cod;
   var v_ini_rqt_flg;
   var v_ccd_rqt_flg;
   var v_dlt_rqt_flg;
   var v_upd_act_flg;
   var v_sce_cmt_txt;

   var v_err_msg;
   var dup_check_cmd;
   var dup_check_stmt;
   var dup_check_res;
   var v_key_dup;
   var v_is_dup;
   var v_base_sce_dsc;

   var config_check_cmd;
   var config_check_stmt;
   var config_check_res;
   var v_is_configured;

   var upd_cmd;

   var v_new_lst_act_per_cod;
   var v_new_cur_cod;
   var v_new_ini_rqt_flg;
   var v_new_ccd_rqt_flg;
   var v_new_dlt_rqt_flg;
   var v_new_sce_cmt_txt;
   var v_new_sce_cmt_tst;
   var v_new_sce_cmt_eml_usr_cod;
   var v_new_ini_sts_cod;
   var v_new_ccd_sts_cod;
   var v_new_dlt_sts_cod;
   var v_new_ini_rqt_tst;
   var v_new_ccd_rqt_tst;
   var v_new_dlt_rqt_tst;
   var v_dup_sce_elm_dsc;

   var trg_file_cmd;
   var trg_file_stmt;
   var trg_file_res;
   var v_trg_file;
   var create_file;
   var res_create_file;
   var v_res_create_file;
   
   var sce_fch_data;
   var sce_stmt;
   var sce_res;
   var upd_cmd;
   var upd_stmt;

   var sec_check_cmd;
   var sec_check_stmt;
   var sec_check_res;

   var v_can_del_flg;

   var ins_clo_cmd;
   var ins_clo_stmt;

   var fch_data = `
SELECT DISTINCT
       DECODE(REPLACE(X.upd_row:SCE_ELM_KEY,'"')
             ,'NA',REPLACE(X.key_row:SCE_ELM_KEY,'"')
             ,REPLACE(X.upd_row:SCE_ELM_KEY,'"'))                         AS SCE_ELM_KEY
      ,REPLACE(X.upd_row:SCE_ELM_DSC,'"')                                 AS SCE_ELM_DSC
      ,REPLACE(X.upd_row:LST_ACT_PER_COD,'"')                             AS LST_ACT_PER_COD
      ,REPLACE(X.upd_row:CUR_COD,'"')                                     AS CUR_COD
      ,COALESCE(REPLACE(x.upd_row:INI_RQT_FLG,'"')::INT,0)                AS INI_RQT_FLG
      ,COALESCE(REPLACE(x.upd_row:CCD_RQT_FLG,'"')::INT,0)                AS CCD_RQT_FLG
      ,COALESCE(REPLACE(x.upd_row:DLT_RQT_FLG,'"')::INT,0)                AS DLT_RQT_FLG
      ,REPLACE(REPLACE(X.upd_row:SCE_CMT_TXT,'null'),'"')                 AS SCE_CMT_TXT
      ,COALESCE(REPLACE(X.upd_row:UPD_ACT_FLG,'"')::INT,0)                AS UPD_ACT_FLG
      ,REPLACE(REPLACE(REPLACE(X.upd_row:SCE_ELM_DSC,'"')
                      ,'DOUBLEQUOTE','"'),'QUOTE','\\\'')                 AS DUP_SCE_ELM_DSC
FROM   (SELECT parse_json(b.VALUE:Updated)  as upd_row
              ,parse_json(b.VALUE:Original) as key_row
        FROM   (SELECT VALUE
                FROM   LATERAL FLATTEN(parse_json('${v_json_input}')) f1
                WHERE  PATH LIKE '%UpdatedRows%') a
              ,LATERAL FLATTEN(a.VALUE) b) X;
                    `;
   var stmt = snowflake.createStatement( {sqlText: fch_data} );
   var res = stmt.execute();

   while (res.next())  {

      /* Assigning data into local variables */
      v_sce_elm_key = res.getColumnValue(1);
      v_sce_elm_dsc = res.getColumnValue(2);
      v_lst_act_per_cod = res.getColumnValue(3);
      v_cur_cod = res.getColumnValue(4);
      v_ini_rqt_flg = res.getColumnValue(5);
      v_ccd_rqt_flg = res.getColumnValue(6);
      v_dlt_rqt_flg = res.getColumnValue(7);
      v_sce_cmt_txt = res.getColumnValue(8);
      v_upd_act_flg = res.getColumnValue(9);
      v_dup_sce_elm_dsc = res.getColumnValue(10);

      /* Check nb configured indicator in P_FLX_SCE_CFG_IND */
      config_check_cmd = `
SELECT   COUNT(*) AS RowCount
FROM     COP_DMT_FLX.P_FLX_SCE_CFG_IND 
WHERE    SCE_ELM_KEY = '${v_sce_elm_key}';
                      `;
      config_check_stmt = snowflake.createStatement( {sqlText: config_check_cmd} );
      config_check_res = config_check_stmt.execute();
      config_check_res.next();

      v_is_configured = config_check_res.getColumnValue(1) > 0;

      if ( ! v_is_configured && v_ini_rqt_flg === 1 ) {
         v_err_msg = "<SQLError>Scenario \"" + v_dup_sce_elm_dsc + "\" not configured: it can not be initialised.</SQLError>";
         throw v_err_msg;
      }

      /* Check duplicate in R_FLX_SCE */
      dup_check_cmd = `SELECT   COUNT(*) AS RowCount FROM COP_DMT_FLX.R_FLX_SCE 
      WHERE REPLACE(REPLACE(SCE_ELM_DSC,'"','DOUBLEQUOTE'),'''','QUOTE') = '${v_sce_elm_dsc}'
      AND   SCE_ELM_KEY != '${v_sce_elm_key}'`;

      dup_check_stmt = snowflake.createStatement( {sqlText: dup_check_cmd} );
      dup_check_res = dup_check_stmt.execute();
      dup_check_res.next();

      v_is_dup = dup_check_res.getColumnValue(1) > 0;

      /* If duplicate, error and stop */
      if ( v_is_dup ) {
         v_err_msg = "<SQLError>Scenario name \"" + v_dup_sce_elm_dsc + "\" already exists for an other scenario.</SQLError>";
         throw v_err_msg;
      }
      else {
      

         /* Check if user is allowed to delete scenario */
         sec_check_cmd = `
SELECT   COUNT(*) can_del_flg
FROM     COP_DMT_FLX.T_FLX_SEC_USR 
WHERE    EML_USR_COD = '${v_eml_user}'
AND      ROL_COD = 'CAN_DEL_SCENARIO';
                      `;
         sec_check_stmt = snowflake.createStatement( {sqlText: sec_check_cmd} );
         sec_check_res = sec_check_stmt.execute();
         sec_check_res.next();

         v_can_del_flg = sec_check_res.getColumnValue(1) > 0;

         if ( !v_can_del_flg && v_dlt_rqt_flg ) {
            v_dlt_rqt_flg = 0;
         }

         sce_fch_data = `
SELECT      DISTINCT
            (CASE WHEN INI_STS_COD = 'created' THEN '${v_lst_act_per_cod}'
                  ELSE LST_ACT_PER_COD
             END)                                                 AS NEW_LST_ACT_PER_COD
           ,(CASE WHEN INI_STS_COD = 'created' THEN '${v_cur_cod}'
                  ELSE CUR_COD
             END)                                                 AS NEW_CUR_COD
           ,(CASE WHEN '${v_sce_cmt_txt}'  = ''                                                           OR 
                       '${v_sce_cmt_txt}' IS NULL                                                         OR
                       '${v_sce_cmt_txt}'  = REPLACE(REPLACE(SCE_CMT_TXT,'"','DOUBLEQUOTE'),'''','QUOTE') THEN 
                       REPLACE(REPLACE(SCE_CMT_TXT,'"','DOUBLEQUOTE'),'''','QUOTE')
                  ELSE '${v_sce_cmt_txt}'
             END)                                                 AS NEW_SCE_CMT_TXT
           ,(CASE WHEN COALESCE(NEW_SCE_CMT_TXT,'NULL') = COALESCE(REPLACE(REPLACE(SCE_CMT_TXT,'"','DOUBLEQUOTE'),'''','QUOTE'),'NULL') THEN 'OLD'
                  ELSE 'NEW'
             END)                                                 AS NEW_SCE_CMT_TST
           ,(CASE WHEN COALESCE(NEW_SCE_CMT_TXT,'NULL') = COALESCE(REPLACE(REPLACE(SCE_CMT_TXT,'"','DOUBLEQUOTE'),'''','QUOTE'),'NULL') THEN SCE_CMT_EML_USR_COD
                  ELSE '${v_eml_user}'
             END)                                                 AS NEW_SCE_CMT_EML_USR_COD
           ,(CASE WHEN ${v_ini_rqt_flg} = 1 AND 
                       INI_STS_COD      = 'created' THEN 'requested'
                  ELSE INI_STS_COD
             END)                                                 AS NEW_INI_STS_COD
           ,(CASE WHEN INI_STS_COD != 'created' THEN INI_RQT_FLG
                  ELSE ${v_ini_rqt_flg}
             END)                                                 AS NEW_INI_RQT_FLG
           ,(CASE WHEN ${v_ini_rqt_flg} = 1 AND 
                       INI_STS_COD      = 'created' THEN 'NEW'
                  ELSE 'OLD'
             END)                                                 AS NEW_INI_RQT_TST
           ,(CASE WHEN ${v_ccd_rqt_flg} = 1      AND
                       INI_STS_COD      = 'done' AND
                       CCD_STS_COD      = 'na'   THEN 'requested'
                  ELSE CCD_STS_COD
             END)                                                 AS NEW_CCD_STS_COD
           ,(CASE WHEN INI_STS_COD != 'done' OR
                       CCD_STS_COD != 'na'   THEN CCD_RQT_FLG
                  ELSE ${v_ccd_rqt_flg}
             END)                                                 AS NEW_CCD_RQT_FLG
           ,(CASE WHEN ${v_ccd_rqt_flg} = 1      AND
                       INI_STS_COD      = 'done' AND
                       CCD_STS_COD      = 'na'   THEN 'NEW'
                  ELSE 'OLD'
             END)                                                 AS NEW_CCD_RQT_TST
           ,(CASE WHEN ${v_dlt_rqt_flg} = 1    AND
                       DLT_STS_COD      = 'na' THEN 'requested'
                  ELSE DLT_STS_COD
             END)                                                 AS NEW_DLT_STS_COD
           ,(CASE WHEN DLT_STS_COD != 'na' THEN DLT_RQT_FLG
                  ELSE ${v_dlt_rqt_flg}
             END)                                                 AS NEW_DLT_RQT_FLG
           ,(CASE WHEN ${v_dlt_rqt_flg} = 1 AND 
                       DLT_STS_COD      = 'na' THEN 'NEW'
                  ELSE 'OLD'
             END)                                                 AS NEW_DLT_RQT_TST
FROM        COP_DMT_FLX.R_FLX_SCE
WHERE       SCE_ELM_KEY = '${v_sce_elm_key}'
                                `;
         sce_stmt = snowflake.createStatement( {sqlText: sce_fch_data} );
         sce_res = sce_stmt.execute();
         sce_res.next();

         v_new_lst_act_per_cod     = sce_res.getColumnValue(1);
         v_new_cur_cod             = sce_res.getColumnValue(2);
         v_new_sce_cmt_txt         = sce_res.getColumnValue(3);
         v_new_sce_cmt_tst         = sce_res.getColumnValue(4);
         v_new_sce_cmt_eml_usr_cod = sce_res.getColumnValue(5);
         v_new_ini_sts_cod         = sce_res.getColumnValue(6);
         v_new_ini_rqt_flg         = sce_res.getColumnValue(7);
         v_new_ini_rqt_tst         = sce_res.getColumnValue(8);
         v_new_ccd_sts_cod         = sce_res.getColumnValue(9);
         v_new_ccd_rqt_flg         = sce_res.getColumnValue(10);
         v_new_ccd_rqt_tst         = sce_res.getColumnValue(11);
         v_new_dlt_sts_cod         = sce_res.getColumnValue(12);
         v_new_dlt_rqt_flg         = sce_res.getColumnValue(13);
         v_new_dlt_rqt_tst         = sce_res.getColumnValue(14);

         upd_cmd = `UPDATE COP_DMT_FLX.R_FLX_SCE 
                    SET    LST_ACT_PER_COD       = '${v_new_lst_act_per_cod}'
                          ,UPD_ACT_FLG           = '${v_upd_act_flg}'
                          ,SCE_ELM_DSC           = REPLACE(REPLACE('${v_sce_elm_dsc}','DOUBLEQUOTE','"'),'QUOTE','''')
                          ,CUR_COD               = '${v_new_cur_cod}'
                          ,SCE_CMT_TXT           = REPLACE(REPLACE('${v_new_sce_cmt_txt}','DOUBLEQUOTE','"'),'QUOTE','''')
                          ,SCE_CMT_TST           = (CASE WHEN '${v_new_sce_cmt_tst}' = 'OLD' THEN SCE_CMT_TST
                                                         ELSE TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP)
                                                    END)
                          ,SCE_CMT_EML_USR_COD   = '${v_new_sce_cmt_eml_usr_cod}'
                          ,INI_STS_COD           = '${v_new_ini_sts_cod}'
                          ,INI_RQT_FLG           = '${v_new_ini_rqt_flg}'
                          ,INI_RQT_TST           = (CASE WHEN '${v_new_ini_rqt_tst}' = 'OLD' THEN INI_RQT_TST
                                                         ELSE TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP)
                                                    END)
                          ,INI_RQT_EML_USR_COD   = (CASE WHEN '${v_new_ini_sts_cod}' = 'requested' THEN '${v_eml_user}'
                                                         ELSE INI_RQT_EML_USR_COD
                                                    END)
                          ,CCD_STS_COD           = '${v_new_ccd_sts_cod}'
                          ,CCD_RQT_FLG           = '${v_new_ccd_rqt_flg}'
                          ,CCD_RQT_TST           = (CASE WHEN '${v_new_ccd_rqt_tst}' = 'OLD' THEN CCD_RQT_TST
                                                         ELSE TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP)
                                                    END)
                          ,CCD_RQT_EML_USR_COD   = (CASE WHEN '${v_new_ccd_sts_cod}' = 'requested' THEN '${v_eml_user}'
                                                         ELSE CCD_RQT_EML_USR_COD
                                                    END)
                          ,DLT_STS_COD           = '${v_new_dlt_sts_cod}'
                          ,DLT_RQT_FLG           = '${v_new_dlt_rqt_flg}'
                          ,DLT_RQT_TST           = (CASE WHEN '${v_new_dlt_rqt_tst}' = 'OLD' THEN DLT_RQT_TST
                                                         ELSE TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP)
                                                    END)
                          ,DLT_RQT_EML_USR_COD   = (CASE WHEN '${v_new_dlt_sts_cod}' = 'requested' THEN '${v_eml_user}'
                                                         ELSE DLT_RQT_EML_USR_COD
                                                    END)
                          ,T_REC_UPD_TST         = TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP)
                    WHERE  SCE_ELM_KEY = '${v_sce_elm_key}'
                    AND    DLT_STS_COD = 'na';
                   `;
         upd_stmt = snowflake.createStatement( {sqlText: upd_cmd} );
         upd_stmt.execute();

         ins_clo_cmd = `INSERT INTO COP_DMT_FLX.P_FLX_GAP_CLO_CFG
                                   (SCE_ELM_KEY
                                   ,CBU_COD
                                   ,SCE_ELM_COD
                                   ,ETI_ELM_COD
                                   ,LV0_PDT_CAT_COD
                                   ,GAP_CLO_PER_COD
                                   ,T_REC_DLT_FLG
                                   ,T_REC_INS_TST
                                   ,T_REC_UPD_TST
                                   )
                        SELECT      DISTINCT
                                    R_FLX_SCE.SCE_ELM_KEY                                                    AS SCE_ELM_KEY
                                   ,R_FLX_SCE.CBU_COD                                                        AS CBU_COD
                                   ,R_FLX_SCE.SCE_ELM_COD                                                    AS SCE_ELM_COD
                                   ,R_FLX_ETI.ETI_ELM_COD                                                    AS ETI_ELM_COD
                                   ,R_FLX_PDT.LV0_PDT_CAT_COD                                                AS LV0_PDT_CAT_COD
                                   ,12                                                                       AS GAP_CLO_PER_COD
                                   ,0                                                                        AS T_REC_DLT_FLG
                                   ,TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP)                                      AS T_REC_INS_TST
                                   ,TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP)                                      AS T_REC_UPD_TST
                        FROM        COP_DMT_FLX.R_FLX_SCE
                                    INNER JOIN COP_DMT_FLX.R_FLX_ETI ON R_FLX_SCE.CBU_COD = R_FLX_ETI.CBU_COD
                                    INNER JOIN COP_DMT_FLX.R_FLX_PDT ON R_FLX_ETI.CBU_COD = R_FLX_PDT.CBU_COD
                        WHERE       R_FLX_SCE.SCE_ELM_KEY      = '${v_sce_elm_key}'
                        AND         R_FLX_SCE.ACT_SRC_SCE_COD IS NOT NULL
                        AND         R_FLX_SCE.DLT_STS_COD      = 'na'
                        AND         R_FLX_ETI.T_REC_DLT_FLG    = 0
                        AND         NOT EXISTS (SELECT NULL
                                                FROM   COP_DMT_FLX.P_FLX_GAP_CLO_CFG
                                                WHERE  P_FLX_GAP_CLO_CFG.SCE_ELM_KEY = R_FLX_SCE.SCE_ELM_KEY);
                        `;
                             
          ins_clo_stmt = snowflake.createStatement( {sqlText: ins_clo_cmd} );
          ins_clo_stmt.execute();

      }

      trg_file_cmd = `SELECT COUNT(*) AS RowCount 
                      FROM   COP_DMT_FLX.R_FLX_SCE 
                      WHERE  INI_STS_COD = 'requested';`;
      trg_file_stmt = snowflake.createStatement( {sqlText: trg_file_cmd} );
      trg_file_res = trg_file_stmt.execute();
      trg_file_res.next();
     
      v_trg_file = trg_file_res.getColumnValue(1) > 0;

      if (v_trg_file) {
         /* Create the Trigger File TRG_CC_TO_FLX.csv */
         create_file = snowflake.createStatement({sqlText: `CALL SP_Flex_Create_Trigger_File('CCD2FLX');` });
         res_create_file = create_file.execute();
         res_create_file.next();
         v_res_create_file = res_create_file.getColumnValue(1);

         if ( v_res_create_file != "Success" ) {
            v_err_msg = "<SQLError>Unable to create the trigger file: " + v_res_create_file + ".</SQLError>";
            throw v_err_msg;
         }
      }

      trg_file_cmd = `SELECT COUNT(*) AS RowCount 
                      FROM   COP_DMT_FLX.R_FLX_SCE 
                      WHERE  CCD_STS_COD = 'requested';`;
      trg_file_stmt = snowflake.createStatement( {sqlText: trg_file_cmd} );
      trg_file_res = trg_file_stmt.execute();
      trg_file_res.next();
     
      v_trg_file = trg_file_res.getColumnValue(1) > 0;
      if (v_trg_file) {
         /* Create the Trigger File TRG_FLX_TO_CC.csv */
         create_file = snowflake.createStatement({sqlText: `CALL SP_Flex_Create_Trigger_File('FLX2CCD');` });
         res_create_file = create_file.execute();
         res_create_file.next();
         v_res_create_file = res_create_file.getColumnValue(1);

         if ( v_res_create_file != "Success" ) {
            v_err_msg = "<SQLError>Unable to create the trigger file: " + v_res_create_file + ".</SQLError>";
            throw v_err_msg;
         }
      }
   /* End While */
   }

}
/* End of Update Operation Block */

/* Delete Operation Block */
if (isDeleted) {
   /* Getting SCE_ELM_COD to DELETE */
   var cmd_get_sce_elm_cod = `
SELECT DISTINCT
       REPLACE(X.delrow:SCE_ELM_KEY,'"')                                 AS SCE_ELM_KEY
      ,(CASE WHEN R_FLX_SCE.SCE_ELM_COD IS NOT NULL THEN
                  'DEL - ' || R_FLX_SCE.SCE_ELM_COD || ' - ' || 
                  TO_CHAR(CURRENT_TIMESTAMP,'YYYYMMDDHH24MISS')
             ELSE ''
        END)                                                             AS DEL_SCE_ELM_COD
      ,(CASE WHEN R_FLX_SCE.SCE_ELM_COD IS NOT NULL THEN
                  R_FLX_SCE.CBU_COD || '-DEL - ' || 
                  R_FLX_SCE.SCE_ELM_COD || ' - ' || 
                  TO_CHAR(CURRENT_TIMESTAMP,'YYYYMMDDHH24MISS')
             ELSE ''
        END)                                                             AS DEL_SCE_ELM_KEY
FROM   (SELECT parse_json(b.VALUE) as delrow
        FROM   (SELECT VALUE
                FROM   LATERAL FLATTEN(parse_json('${v_json_input}')) f1
                WHERE  PATH LIKE '%DeletedRows%') a
              ,LATERAL FLATTEN(a.VALUE) b) X
       LEFT OUTER JOIN COP_DMT_FLX.R_FLX_SCE ON 
       (
          R_FLX_SCE.SCE_ELM_KEY = REPLACE(X.delrow:SCE_ELM_KEY,'"')
       );
                                `;

   var stmt = snowflake.createStatement( {sqlText: cmd_get_sce_elm_cod} );
   var res = stmt.execute();
   res.next();

   /* Assigning data into local variables */
   var v_sce_elm_key = res.getColumnValue(1);
   var v_del_sce_elm_cod = res.getColumnValue(2);
   var v_del_sce_elm_key = res.getColumnValue(3);

   /* Logically DELETE data from R_FLX_SCE */
   var del_dat_r = `UPDATE COP_DMT_FLX.R_FLX_SCE 
                    SET    T_REC_DLT_FLG = 1 
                          ,dlt_sts_cod = 'requested'
                          ,dlt_rqt_flg = 1
                          ,dlt_rqt_tst = TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP)
                          ,dlt_rqt_eml_usr_cod = '${v_eml_user}'
                          ,T_REC_UPD_TST = TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP)
                    WHERE  SCE_ELM_KEY   = '${v_sce_elm_key}'
                    AND    T_REC_DLT_FLG = 0;
                    `;
   var stmt = snowflake.createStatement( {sqlText: del_dat_r} );
   stmt.execute();

   dlt_clo_cmd = `UPDATE COP_DMT_FLX.P_FLX_GAP_CLO_CFG
                  SET    T_REC_DLT_FLG = 1 
                         WHERE SCE_ELM_KEY = '${v_sce_elm_key}'`;
            
            dlt_clo_stmt = snowflake.createStatement( {sqlText: dlt_clo_cmd} );
            dlt_clo_stmt.execute();

}
/* End of Delete Operation Block */

$$
;
