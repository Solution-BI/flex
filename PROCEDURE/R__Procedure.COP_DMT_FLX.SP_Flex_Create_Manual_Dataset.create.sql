USE SCHEMA COP_DMT_FLX{{uid}};

/*
=========================================================================
                                HISTORY
=========================================================================
Description : Create Procedure Script For Populating R_FLX_MAN_SCE Table

Author      : Noel Coquio (Solution BI France)
Created On  : 04-07-2024
=========================================================================
Modified On:    Description:                        Author:
06-01-2025      Add Description and comment         Noel Coquio
                special character process
=========================================================================
*/


CREATE OR REPLACE PROCEDURE SP_Flex_Create_Manual_Dataset(JSON_INPUT VARCHAR(16777216))
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
   var v_field = '"MAN_SCE_ELM_DSC","MAN_SCE_CMT_TXT"';
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
var log_data = snowflake.createStatement({sqlText: `CALL SP_Flex_Log_PowerOn(:1,'SP_Flex_Create_Manual_Dataset',:2,:3);`, binds:[RUN_ID,v_json_input,v_eml_user]});
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
      ,REPLACE(X.INSERTEDROWS:MAN_SCE_ELM_DSC,'"')                               AS MAN_SCE_ELM_DSC
      ,REPLACE(X.INSERTEDROWS:MAN_SCE_CUR_COD,'"')                               AS MAN_SCE_CUR_COD
      ,REPLACE(REPLACE(X.INSERTEDROWS:MAN_SCE_CMT_TXT,'null'),'"')               AS MAN_SCE_CMT_TXT
      ,REPLACE(REPLACE(REPLACE(X.INSERTEDROWS:MAN_SCE_ELM_DSC,'"')
                      ,'DOUBLEQUOTE','"'),'QUOTE','\\\'')                        AS DUP_MAN_SCE_ELM_DSC
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
   var v_man_sce_elm_dsc = res.getColumnValue(2);
   var v_man_sce_cur_cod = res.getColumnValue(3);
   var v_man_sce_cmt_txt = res.getColumnValue(4);
   var v_dup_man_sce_elm_dsc = res.getColumnValue(5);

   /* Check duplicate in R_FLX_MAN_SCE */
   var dup_check_cmd = `SELECT   COUNT(*) AS RowCount
FROM     COP_DMT_FLX.R_FLX_MAN_SCE 
WHERE    REPLACE(REPLACE(MAN_SCE_ELM_DSC,'"','DOUBLEQUOTE'),'''','QUOTE') = '${v_man_sce_elm_dsc}'`;
   var dup_check_stmt = snowflake.createStatement( {sqlText: dup_check_cmd} );
   var dup_check_res = dup_check_stmt.execute();
   dup_check_res.next();

   var v_is_dup = dup_check_res.getColumnValue(1) > 0;

   /* If not a duplicate, Insert */
   if (!v_is_dup) {

      /* Define the Dataset code */ 
      var man_sce_cod_cmd = `SELECT 'ADD_' || TO_CHAR(CURRENT_DATE,'YYYYMMDD_')  AS MAN_SCE_ELM_COD_ ;`;
      var man_sce_cod_stmt = snowflake.createStatement( {sqlText: man_sce_cod_cmd} );
      var man_sce_cod_res = man_sce_cod_stmt.execute();
      man_sce_cod_res.next();
      var v_man_sce_cod = man_sce_cod_res.getColumnValue(1);

      man_sce_cod_cmd = `SELECT '${v_man_sce_cod}' || 
                                TRIM(TO_CHAR(TRUNC(COALESCE(MAX(TO_NUMBER(REPLACE(MAN_SCE_ELM_COD,'${v_man_sce_cod}'))),0) + 1,0),'009')) AS NEW_MAN_SCE_ELM_COD 
                         FROM   COP_DMT_FLX.R_FLX_MAN_SCE
                         WHERE  MAN_SCE_ELM_COD LIKE '${v_man_sce_cod}' || '%'
                         AND    CBU_COD            = '${v_cbu_cod}'
                     ;`;
      man_sce_cod_stmt = snowflake.createStatement( {sqlText: man_sce_cod_cmd} );
      man_sce_cod_res = man_sce_cod_stmt.execute();
      man_sce_cod_res.next();
      var v_man_sce_elm_cod = man_sce_cod_res.getColumnValue(1);

      /* Inserting Scenario to R_FLX_MAN_SCE */

      var ins_sce_cmd = `INSERT INTO COP_DMT_FLX.R_FLX_MAN_SCE
           (MAN_SCE_ELM_KEY
           ,MAN_SCE_ELM_COD
           ,MAN_SCE_ELM_DSC
           ,CBU_COD
           ,MAN_SCE_CUR_COD
           ,MAN_SCE_USE_FLG
           ,CRE_USR_COD
           ,CRE_END_TST
           ,MAN_SCE_EML_USR_COD
           ,MAN_SCE_DLT_STS_COD
           ,MAN_SCE_DLT_FLG
           ,MAN_SCE_CMT_TXT
           ,MAN_SCE_CMT_TST
           ,MAN_SCE_CMT_EML_USR_COD
           ,T_REC_DLT_FLG
           ,T_REC_INS_TST
           ,T_REC_UPD_TST
           )
SELECT      DISTINCT
            '${v_cbu_cod}' || '-' || '${v_man_sce_elm_cod}'                               AS MAN_SCE_ELM_KEY
           ,'${v_man_sce_elm_cod}'                                                        AS MAN_SCE_ELM_COD
           ,REPLACE(REPLACE('${v_man_sce_elm_dsc}','DOUBLEQUOTE','"'),'QUOTE','''')       AS MAN_SCE_ELM_DSC
           ,'${v_cbu_cod}'                                                                AS CBU_COD
           ,'${v_man_sce_cur_cod}'                                                        AS MAN_SCE_CUR_COD
           ,0                                                                             AS MAN_SCE_USE_FLG
           ,'${v_user}'                                                                   AS CRE_USR_COD
           ,TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP)                                           AS CRE_END_TST
           ,'${v_eml_user}'                                                               AS MAN_SCE_EML_USR_COD
           ,'na'                                                                          AS MAN_SCE_DLT_STS_COD
           ,0                                                                             AS MAN_SCE_DLT_FLG
           ,REPLACE(REPLACE('${v_man_sce_cmt_txt}','DOUBLEQUOTE','"'),'QUOTE','''')       AS MAN_SCE_CMT_TXT
           ,IFF('${v_man_sce_cmt_txt}'='null',null,TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP))   AS MAN_SCE_CMT_TST
           ,IFF('${v_man_sce_cmt_txt}'='null',null,'${v_eml_user}')                       AS MAN_SCE_CMT_EML_USR_COD
           ,0                                                                             AS T_REC_DLT_FLG
           ,TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP)                                           AS T_REC_SRC_TST
           ,TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP)                                           AS T_REC_UPD_TST
                            `;
      snowflake.execute( {sqlText: ins_sce_cmd} );

   }
   else {
      var v_err_msg = "<SQLError>Dataset name \"" + v_dup_man_sce_elm_dsc + "\" already exists for an other dataset.</SQLError>";
      throw v_err_msg;
   }

}
/* End of Insert Operation Block */

/* Update Operation Block */
if (isUpdated) {

   /* Assigning data into local variables */
   var v_man_sce_elm_key;
   var v_man_sce_elm_dsc;
   var v_man_sce_cur_cod;
   var v_man_sce_use_flg;
   var v_man_sce_dlt_flg;
   var v_man_sce_cmt_txt;

   var v_err_msg;
   var dup_check_cmd;
   var dup_check_stmt;
   var dup_check_res;
   var v_key_dup;
   var v_is_dup;
   var v_base_sce_dsc;

   var v_is_configured;
   var v_is_error;

   var upd_cmd;

   var v_new_cur_cod;
   var v_new_man_sce_use_flg;
   var v_new_man_sce_dlt_flg;
   var v_new_dlt_sts_cod;
   var v_new_dlt_rqt_tst;
   var v_new_man_sce_cmt_txt;
   var v_new_man_sce_cmt_tst;
   var v_new_man_sce_cmt_eml_usr_cod;
   var v_dup_man_sce_elm_dsc;
   var v_list_sce_dsc;

   var man_sce_fch_data;
   var man_sce_stmt;
   var man_sce_res;
   var upd_cmd;
   var upd_stmt;

   var sce_check_cmd;
   var sce_check_cmd;
   var sce_check_cmd;

   var v_can_del_flg;


   var fch_data = `
SELECT DISTINCT
       DECODE(REPLACE(X.upd_row:MAN_SCE_ELM_KEY,'"')
             ,'NA',REPLACE(X.key_row:MAN_SCE_ELM_KEY,'"')
             ,REPLACE(X.upd_row:MAN_SCE_ELM_KEY,'"'))                         AS MAN_SCE_ELM_KEY
      ,REPLACE(X.upd_row:MAN_SCE_ELM_DSC,'"')                                 AS MAN_SCE_ELM_DSC
      ,REPLACE(X.upd_row:MAN_SCE_CUR_COD,'"')                                 AS MAN_SCE_CUR_COD
      ,COALESCE(REPLACE(x.upd_row:MAN_SCE_USE_FLG,'"')::INT,0)                AS MAN_SCE_USE_FLG
      ,COALESCE(REPLACE(x.upd_row:MAN_SCE_DLT_FLG,'"')::INT,0)                AS MAN_SCE_DLT_FLG
      ,REPLACE(REPLACE(X.upd_row:MAN_SCE_CMT_TXT,'null'),'"')                 AS MAN_SCE_CMT_TXT
      ,REPLACE(REPLACE(REPLACE(X.upd_row:MAN_SCE_ELM_DSC,'"')
                      ,'DOUBLEQUOTE','"'),'QUOTE','\\\'')                     AS DUP_MAN_SCE_ELM_DSC
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
      v_man_sce_elm_key = res.getColumnValue(1);
      v_man_sce_elm_dsc = res.getColumnValue(2);
      v_man_sce_cur_cod = res.getColumnValue(3);
      v_man_sce_use_flg = res.getColumnValue(4);
      v_man_sce_dlt_flg = res.getColumnValue(5);
      v_man_sce_cmt_txt = res.getColumnValue(6);
      v_dup_man_sce_elm_dsc = res.getColumnValue(7);

      /* Check data in F_FLX_MAN_SCE */
      config_check_cmd = `
SELECT   COUNT(*) AS RowCount
FROM     COP_DMT_FLX.F_FLX_MAN_SCE 
WHERE    MAN_SCE_ELM_KEY = '${v_man_sce_elm_key}'
AND      T_REC_DLT_FLG   = 0;
                      `;
      config_check_stmt = snowflake.createStatement( {sqlText: config_check_cmd} );
      config_check_res = config_check_stmt.execute();
      config_check_res.next();

      v_is_configured = config_check_res.getColumnValue(1) > 0;

      if ( ! v_is_configured && v_man_sce_use_flg === 1 ) {
         v_err_msg = "<SQLError>Dataset \"" + v_dup_man_sce_elm_dsc + "\" has no data: it can not be used.</SQLError>";
         throw v_err_msg;
      }

      /* Check data in error in F_FLX_MAN_SCE */
      config_check_cmd = `
SELECT   COUNT(*) AS RowCount
FROM     COP_DMT_FLX.F_FLX_MAN_SCE 
WHERE    MAN_SCE_ELM_KEY  = '${v_man_sce_elm_key}'
AND      MAN_ITM_ERR_DTA IS NOT NULL
AND      T_REC_DLT_FLG    = 0;
                      `;
      config_check_stmt = snowflake.createStatement( {sqlText: config_check_cmd} );
      config_check_res = config_check_stmt.execute();
      config_check_res.next();

      v_is_error = config_check_res.getColumnValue(1) > 0;

      if (  v_is_error && v_man_sce_use_flg === 1 ) {
         v_err_msg = "<SQLError>Dataset \"" + v_dup_man_sce_elm_dsc + "\" has data in error: it can not be used.</SQLError>";
         throw v_err_msg;
      }

      /* Check duplicate in R_FLX_MAN_SCE */
      dup_check_cmd = `SELECT   COUNT(*) AS RowCount FROM COP_DMT_FLX.R_FLX_MAN_SCE 
      WHERE REPLACE(REPLACE(MAN_SCE_ELM_DSC,'"','DOUBLEQUOTE'),'''','QUOTE') = '${v_man_sce_elm_dsc}'
      AND   MAN_SCE_ELM_KEY != '${v_man_sce_elm_key}'`;

      dup_check_stmt = snowflake.createStatement( {sqlText: dup_check_cmd} );
      dup_check_res = dup_check_stmt.execute();
      dup_check_res.next();

      v_is_dup = dup_check_res.getColumnValue(1) > 0;

      /* If duplicate, error and stop */
      if ( v_is_dup ) {
         v_err_msg = "<SQLError>Dataset name \"" + v_dup_man_sce_elm_dsc + "\" already exists for an other dataset.</SQLError>";
         throw v_err_msg;
      }
      else {
      
         /* Check if user is allowed to delete scenario */
         sce_check_cmd = `
SELECT   COUNT(*) can_del_flg
FROM     COP_DMT_FLX.T_FLX_SEC_USR 
WHERE    EML_USR_COD = '${v_eml_user}'
AND      ROL_COD = 'CAN_DEL_SCENARIO';
                      `;
         sce_check_stmt = snowflake.createStatement( {sqlText: sce_check_cmd} );
         sce_check_res = sce_check_stmt.execute();
         sce_check_res.next();

         v_can_del_flg = sce_check_res.getColumnValue(1) > 0;

         if ( !v_can_del_flg && v_man_sce_dlt_flg ) {
            v_man_sce_dlt_flg = 0;
         }

         if ( v_man_sce_dlt_flg ) {
            /* Check if user is allowed to delete scenario */
            sce_check_cmd = `
SELECT   COUNT(DISTINCT R_FLX_SCE.SCE_ELM_KEY) nb_sce
        ,LISTAGG(DISTINCT '\\\"' || REPLACE(SCE_ELM_DSC,'''','\\\'') || '\\\"',', ') list_sce
FROM     COP_DMT_FLX.R_FLX_SCE
         INNER JOIN COP_DMT_FLX.P_FLX_SCE_CFG_IND ON 
         (
              P_FLX_SCE_CFG_IND.SCE_ELM_KEY = R_FLX_SCE.SCE_ELM_KEY 
         )
         INNER JOIN COP_DMT_FLX.R_FLX_MAN_SCE ON 
         (
              R_FLX_MAN_SCE.MAN_SCE_ELM_KEY = P_FLX_SCE_CFG_IND.CBU_COD || '-' ||
                                              P_FLX_SCE_CFG_IND.SRC_SCE_ELM_COD
         )
WHERE    R_FLX_MAN_SCE.MAN_SCE_ELM_KEY = '${v_man_sce_elm_key}'
AND      R_FLX_SCE.INI_STS_COD         IN ('created','requested');
                         `;
            sce_check_stmt = snowflake.createStatement( {sqlText: sce_check_cmd} );
            sce_check_res = sce_check_stmt.execute();
            sce_check_res.next();
   
            v_can_del_flg = sce_check_res.getColumnValue(1) < 1;
            v_list_sce_dsc = sce_check_res.getColumnValue(2);
            if ( !v_can_del_flg ) {
               v_err_msg = "<SQLError>Dataset name \"" + v_dup_man_sce_elm_dsc + "\" currently used by the unloaded scenario(s) " + v_list_sce_dsc + ". Please remove the manual dataset from the scenario sources before deleting it.</SQLError>";
               throw v_err_msg;
            }
         }

         sce_fch_data = `
SELECT      DISTINCT
            (CASE WHEN MAN_SCE_USE_FLG = 0 THEN '${v_man_sce_cur_cod}'
                  ELSE MAN_SCE_CUR_COD
             END)                                                 AS NEW_MAN_SCE_CUR_COD
           ,(CASE WHEN ${v_man_sce_dlt_flg} = 1    AND
                       MAN_SCE_DLT_STS_COD  = 'na' THEN 'requested'
                  ELSE MAN_SCE_DLT_STS_COD
             END)                                                 AS NEW_MAN_SCE_DLT_STS_COD
           ,(CASE WHEN MAN_SCE_DLT_STS_COD != 'na' THEN MAN_SCE_DLT_FLG
                  ELSE ${v_man_sce_dlt_flg}
             END)                                                 AS NEW_MAN_SCE_DLT_FLG
           ,(CASE WHEN ${v_man_sce_dlt_flg} = 1 AND 
                       MAN_SCE_DLT_STS_COD  = 'na' THEN 'NEW'
                  ELSE 'OLD'
             END)                                                 AS NEW_MAN_SCE_DLT_RQT_TST
           ,(CASE WHEN '${v_man_sce_cmt_txt}'  = ''                                                           OR 
                       '${v_man_sce_cmt_txt}' IS NULL                                                         OR
                       '${v_man_sce_cmt_txt}'  = REPLACE(REPLACE(MAN_SCE_CMT_TXT,'"','DOUBLEQUOTE'),'''','QUOTE') THEN 
                       REPLACE(REPLACE(MAN_SCE_CMT_TXT,'"','DOUBLEQUOTE'),'''','QUOTE')
                  ELSE '${v_man_sce_cmt_txt}'
             END)                                                 AS NEW_MAN_SCE_CMT_TXT
           ,(CASE WHEN COALESCE(NEW_MAN_SCE_CMT_TXT,'NULL') = COALESCE(REPLACE(REPLACE(MAN_SCE_CMT_TXT,'"','DOUBLEQUOTE'),'''','QUOTE'),'NULL') THEN 'OLD'
                  ELSE 'NEW'
             END)                                                 AS NEW_MAN_SCE_CMT_TST
           ,(CASE WHEN COALESCE(NEW_MAN_SCE_CMT_TXT,'NULL') = COALESCE(REPLACE(REPLACE(MAN_SCE_CMT_TXT,'"','DOUBLEQUOTE'),'''','QUOTE'),'NULL') THEN MAN_SCE_CMT_EML_USR_COD
                  ELSE '${v_eml_user}'
             END)                                                 AS NEW_MAN_SCE_CMT_EML_USR_COD
FROM        COP_DMT_FLX.R_FLX_MAN_SCE
WHERE       MAN_SCE_ELM_KEY = '${v_man_sce_elm_key}'
                                `;
         sce_stmt = snowflake.createStatement( {sqlText: sce_fch_data} );
         sce_res = sce_stmt.execute();
         sce_res.next();

         v_new_man_sce_cur_cod         = sce_res.getColumnValue(1);
         v_new_man_sce_dlt_sts_cod     = sce_res.getColumnValue(2);
         v_new_man_sce_dlt_flg         = sce_res.getColumnValue(3);
         v_new_man_sce_dlt_rqt_tst     = sce_res.getColumnValue(4);
         v_new_man_sce_cmt_txt         = sce_res.getColumnValue(5);
         v_new_man_sce_cmt_tst         = sce_res.getColumnValue(6);
         v_new_man_sce_cmt_eml_usr_cod = sce_res.getColumnValue(7);

         upd_cmd = `UPDATE COP_DMT_FLX.R_FLX_MAN_SCE 
                    SET    MAN_SCE_ELM_DSC             = REPLACE(REPLACE('${v_man_sce_elm_dsc}','DOUBLEQUOTE','"'),'QUOTE','''')
                          ,MAN_SCE_CUR_COD             = '${v_new_man_sce_cur_cod}'
                          ,MAN_SCE_USE_FLG             = '${v_man_sce_use_flg}'
                          ,MAN_SCE_DLT_STS_COD         = '${v_new_man_sce_dlt_sts_cod}'
                          ,MAN_SCE_DLT_FLG             = '${v_new_man_sce_dlt_flg}'
                          ,MAN_SCE_DLT_RQT_TST         = (CASE WHEN '${v_new_man_sce_dlt_rqt_tst}' = 'OLD' THEN MAN_SCE_DLT_RQT_TST
                                                               ELSE TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP)
                                                          END)
                          ,MAN_SCE_DLT_RQT_EML_USR_COD = (CASE WHEN '${v_new_man_sce_dlt_sts_cod}' = 'requested' THEN '${v_eml_user}'
                                                               ELSE MAN_SCE_DLT_RQT_EML_USR_COD
                                                          END)
                          ,MAN_SCE_CMT_TXT             = REPLACE(REPLACE('${v_new_man_sce_cmt_txt}','DOUBLEQUOTE','"'),'QUOTE','''')
                          ,MAN_SCE_CMT_TST             = (CASE WHEN '${v_new_man_sce_cmt_tst}' = 'OLD' THEN MAN_SCE_CMT_TST
                                                               ELSE TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP)
                                                          END)
                          ,MAN_SCE_CMT_EML_USR_COD     = '${v_new_man_sce_cmt_eml_usr_cod}'
                          ,T_REC_UPD_TST               = TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP)
                    WHERE  MAN_SCE_ELM_KEY     = '${v_man_sce_elm_key}'
                    AND    MAN_SCE_DLT_STS_COD = 'na';
                   `;
         upd_stmt = snowflake.createStatement( {sqlText: upd_cmd} );
         upd_stmt.execute();
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
       REPLACE(X.delrow:MAN_SCE_ELM_KEY,'"')                                 AS MAN_SCE_ELM_KEY
FROM   (SELECT parse_json(b.VALUE) as delrow
        FROM   (SELECT VALUE
                FROM   LATERAL FLATTEN(parse_json('${v_json_input}')) f1
                WHERE  PATH LIKE '%DeletedRows%') a
              ,LATERAL FLATTEN(a.VALUE) b) X
       LEFT OUTER JOIN COP_DMT_FLX.R_FLX_MAN_SCE ON 
       (
          R_FLX_MAN_SCE.MAN_SCE_ELM_KEY = REPLACE(X.delrow:MAN_SCE_ELM_KEY,'"')
       );
                                `;

   var stmt = snowflake.createStatement( {sqlText: cmd_get_sce_elm_cod} );
   var res = stmt.execute();
   res.next();

   /* Assigning data into local variables */
   var v_man_sce_elm_key = res.getColumnValue(1);

   /* Logically DELETE data from R_FLX_SCE */
   var del_dat_r = `UPDATE COP_DMT_FLX.R_FLX_MAN_SCE 
                    SET    T_REC_DLT_FLG               = 1 
                          ,MAN_SCE_DLT_STS_COD         = 'requested'
                          ,MAN_SCE_DLT_FLG             = 1
                          ,MAN_SCE_DLT_RQT_TST         = TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP)
                          ,MAN_SCE_DLT_RQT_EML_USR_COD = '${v_eml_user}'
                          ,T_REC_UPD_TST               = TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP)
                    WHERE  MAN_SCE_ELM_KEY = '${v_man_sce_elm_key}'
                    AND    T_REC_DLT_FLG   = 0;
                    `;
   var stmt = snowflake.createStatement( {sqlText: del_dat_r} );
   stmt.execute();

}
/* End of Delete Operation Block */

$$
;
