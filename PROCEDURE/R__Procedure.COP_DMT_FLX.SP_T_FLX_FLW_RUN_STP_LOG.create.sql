USE SCHEMA COP_DMT_FLX{{uid}};


CREATE OR REPLACE PROCEDURE SP_T_FLX_FLW_RUN_STP_LOG
         (DS_IS_RUN_SOFT            VARCHAR(256)
         ,ID_IS_RUN_FLOW            VARCHAR(256)
         ,DS_IS_RUN_STP_NAM_DSC     VARCHAR(256) 
         ,ID_IS_RUN_STP_SUB_ORD_VAL DOUBLE
         ,CD_IS_STEP_STATUS_VALUE   DOUBLE
         ,DS_IS_STEP_ERROR          VARCHAR(10000) 
         ,DT_IS_STEP_BEGIN          VARCHAR(30)
         ,DT_IS_STEP_END            VARCHAR(30)
         ,DS_IS_QUERY               VARCHAR(10000)
         ,MT_SRC_ROW_SUC            DOUBLE
         ,MT_SRC_ROW_ERR            DOUBLE
         ,MT_TGT_ROW_INS_SUC        DOUBLE
         ,MT_TGT_ROW_INS_ERR        DOUBLE
         ,MT_TGT_ROW_UPD_SUC        DOUBLE
         ,MT_TGT_ROW_UPD_ERR        DOUBLE
         ,MT_TGT_ROW_DEL_SUC        DOUBLE
         ,MT_TGT_ROW_DEL_ERR        DOUBLE
         )
RETURNS VARCHAR(10000)
LANGUAGE javascript
EXECUTE AS CALLER
AS
$$  
// This methods enables the insertion of the run step information

function interpretStringForSQLCall(string){
   var SINGLE_QUOTE_CHAR="'";
   var DOUBLE_QUOTE_CHAR="\"";
   var ESCAPED_SINGLE_QUOTE_CHAR="\\'";
   var ESCAPED_DOUBLE_QUOTE_CHAR="\\\"";
   var escapeStep1=string.split(SINGLE_QUOTE_CHAR).join(ESCAPED_SINGLE_QUOTE_CHAR);
   var escapeStep2=escapeStep1.split(DOUBLE_QUOTE_CHAR).join(ESCAPED_DOUBLE_QUOTE_CHAR);
   return SINGLE_QUOTE_CHAR+escapeStep2+SINGLE_QUOTE_CHAR;
}

var timestamp_default_format='YYYY-MM-DDTHH24:MI:SS.FF3Z';

// Insert log when step executed in success
  
var CD_IS_STEP_STATUS_VALUE_CONDITION = '0 AS CD_IS_STEP_STATUS_VALUE';
if (DS_IS_STEP_ERROR != null){CD_IS_STEP_STATUS_VALUE_CONDITION = `-1 AS CD_IS_STEP_STATUS_VALUE`;}
var DS_IS_STEP_ERROR_CONDITION = 'NULL AS DS_IS_STEP_ERROR';
if (DS_IS_STEP_ERROR != null){DS_IS_STEP_ERROR_CONDITION = `'${DS_IS_STEP_ERROR}' AS CD_IS_STEP_STATUS_VALUE`;}
var DS_IS_QUERY_CONDITION = 'NULL AS DS_IS_QUERY';
if (DS_IS_QUERY != null){DS_IS_QUERY_CONDITION = interpretStringForSQLCall(DS_IS_QUERY) + ' AS DS_IS_QUERY';}
  
   var step_log_query=`INSERT INTO T_FLX_FLW_RUN_STP_LOG 
           (DS_IS_RUN_SOFT
           ,ID_IS_RUN_FLOW
           ,DS_IS_RUN_STP_NAM_DSC
           ,ID_IS_RUN_STP_SUB_ORD_VAL
           ,CD_IS_STEP_STATUS_VALUE
           ,DS_IS_STEP_ERROR
           ,DT_IS_STEP_BEGIN
           ,DT_IS_STEP_END
           ,DS_IS_QUERY
           ,MT_SRC_ROW_SUC
           ,MT_SRC_ROW_ERR
           ,MT_TGT_ROW_INS_SUC
           ,MT_TGT_ROW_INS_ERR
           ,MT_TGT_ROW_UPD_SUC
           ,MT_TGT_ROW_UPD_ERR
           ,MT_TGT_ROW_DEL_SUC
           ,MT_TGT_ROW_DEL_ERR
           ,DT_IS_INSERT
           ,DS_IS_INSERT_USER
           ,DT_IS_UPDATE
           ,DS_IS_UPDATE_USER
           )
SELECT     '${DS_IS_RUN_SOFT}'                                                AS DS_IS_RUN_SOFT
          ,'${ID_IS_RUN_FLOW}'                                                AS ID_IS_RUN_FLOW
          ,'${DS_IS_RUN_STP_NAM_DSC}'                                         AS DS_IS_RUN_STP_NAM_DSC
          ,${ID_IS_RUN_STP_SUB_ORD_VAL}                                       AS ID_IS_RUN_STP_SUB_ORD_VAL
          ,${CD_IS_STEP_STATUS_VALUE_CONDITION}
          ,${DS_IS_STEP_ERROR_CONDITION}
          ,TO_TIMESTAMP('${DT_IS_STEP_BEGIN}', '${timestamp_default_format}') AS DT_IS_STEP_BEGIN
          ,TO_TIMESTAMP('${DT_IS_STEP_END}', '${timestamp_default_format}')   AS DT_IS_STEP_END
          ,${DS_IS_QUERY_CONDITION}
          ,${MT_SRC_ROW_SUC}                                                  AS MT_SRC_ROW_SUC
          ,${MT_SRC_ROW_ERR}                                                  AS MT_SRC_ROW_ERR
          ,${MT_TGT_ROW_INS_SUC}                                              AS MT_TGT_ROW_INS_SUC
          ,${MT_TGT_ROW_INS_ERR}                                              AS MT_TGT_ROW_INS_ERR
          ,${MT_TGT_ROW_UPD_SUC}                                              AS MT_TGT_ROW_UPD_SUC
          ,${MT_TGT_ROW_UPD_ERR}                                              AS MT_TGT_ROW_UPD_ERR
          ,${MT_TGT_ROW_DEL_SUC}                                              AS MT_TGT_ROW_DEL_SUC
          ,${MT_TGT_ROW_DEL_ERR}                                              AS MT_TGT_ROW_DEL_ERR
          ,CURRENT_TIMESTAMP                                                  AS DT_IS_INSERT
          ,CURRENT_USER                                                       AS DS_IS_INSERT_USER 
          ,CURRENT_TIMESTAMP                                                  AS DT_IS_UPDATE
          ,CURRENT_USER                                                       AS DS_IS_UPDATE_USER;`
   var rs = snowflake.execute( { sqlText: step_log_query} );

   return ID_IS_RUN_FLOW;
$$;

