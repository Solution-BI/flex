USE SCHEMA COP_DMT_FLX;

CREATE OR REPLACE PROCEDURE SP_T_FLX_FLW_RUN_LOG_END
         (DS_IS_RUN_SOFT VARCHAR(256)
         ,ID_IS_RUN_FLOW VARCHAR(256)
         )
RETURNS VARCHAR(256)
LANGUAGE javascript
EXECUTE AS CALLER
AS
$$
// Update of the run log table from the step results
 
   var update_log_query=`UPDATE T_FLX_FLW_RUN_LOG
SET    DT_IS_RUN_END = CURRENT_TIMESTAMP
      ,CD_IS_RUN_STATUS = (CASE WHEN CD_IS_STEP_STATUS_VALUE_MIN IS NULL                        THEN 'FAILED'
                                WHEN CD_IS_STEP_STATUS_VALUE_MIN  < 0                           THEN 'FAILED'
                                WHEN CD_IS_STEP_STATUS_VALUE_MIN  = CD_IS_STEP_STATUS_VALUE_MAX AND 
                                     CD_IS_STEP_STATUS_VALUE_MAX  = 0                           THEN 'SUCCEEDED'
                                ELSE 'SUCCEEDED WITH WARNING'
                           END) 
      ,DT_IS_UPDATE = CURRENT_TIMESTAMP
      ,DS_IS_UPDATE_USER = CURRENT_USER
FROM   (SELECT MIN(CD_IS_STEP_STATUS_VALUE) AS CD_IS_STEP_STATUS_VALUE_MIN
              ,MAX(CD_IS_STEP_STATUS_VALUE) AS CD_IS_STEP_STATUS_VALUE_MAX
        FROM   T_FLX_FLW_RUN_STP_LOG 
        WHERE  DS_IS_RUN_SOFT = '${DS_IS_RUN_SOFT}' 
        AND    ID_IS_RUN_FLOW = '${ID_IS_RUN_FLOW}') STP_LOG
WHERE  DS_IS_RUN_SOFT = '${DS_IS_RUN_SOFT}' 
AND    ID_IS_RUN_FLOW = '${ID_IS_RUN_FLOW}';`

   var rs = snowflake.execute( { sqlText: update_log_query} );
	
   return ID_IS_RUN_FLOW;
$$;
