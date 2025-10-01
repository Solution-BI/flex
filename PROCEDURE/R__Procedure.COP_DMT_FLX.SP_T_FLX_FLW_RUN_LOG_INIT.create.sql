USE SCHEMA COP_DMT_FLX;

CREATE OR REPLACE PROCEDURE SP_T_FLX_FLW_RUN_LOG_INIT   
         (ID_IS_RUN_FLOW               VARCHAR(256)
         ,DS_IS_RUN_NAMESPACE          VARCHAR(256)
         ,DS_IS_RUN_FLOW               VARCHAR(256)
         ,DS_IS_RUN_INSTANCE_NAME      VARCHAR(256)
         ,CD_IS_RUN_DATA_LOAD_STRATEGY VARCHAR(256)
         ,DS_IS_REQUESTOR_USER         VARCHAR(100)
         )
RETURNS VARCHAR(256)
LANGUAGE javascript
EXECUTE AS CALLER
AS
$$
  
// This methods enables the initialisation of the logging
// CD_IS_RUN_STATUS is set at 'ONGOING'
// DT_IS_RUN_BEGIN is set at CURRENT_TIMESTAMP
  
   var rs = snowflake.execute({sqlText:`INSERT INTO T_FLX_FLW_RUN_LOG 
             (DS_IS_RUN_SOFT
             ,ID_IS_RUN_FLOW
             ,DS_IS_RUN_NAMESPACE
             ,DS_IS_RUN_FLOW
             ,DS_IS_RUN_INSTANCE_NAME
             ,CD_IS_RUN_DATA_LOAD_STRATEGY
             ,DT_IS_RUN_BEGIN
             ,DT_IS_RUN_END
             ,CD_IS_RUN_STATUS
             ,DS_IS_REQUESTOR_USER
             ,DS_IS_LOG_FILE_PATH
             ,DS_PULL_FIELD_NAME
             ,DT_PULL_FROM
             ,DT_PULL_TO
             ,DT_PULL_SOURCE_LST
             ,DS_PULL_FILTER_DSC
             ,DT_IS_INSERT
             ,DS_IS_INSERT_USER
             ,DT_IS_UPDATE
             ,DS_IS_UPDATE_USER) 
  SELECT      'SNOW'            AS DS_IS_RUN_SOFT
             ,:1                AS ID_IS_RUN_FLOW
             ,:2                AS DS_IS_RUN_NAMESPACE
             ,:3                AS DS_IS_RUN_FLOW
             ,:4                AS DS_IS_RUN_INSTANCE_NAME
             ,:5                AS CD_IS_RUN_DATA_LOAD_STRATEGY
             ,CURRENT_TIMESTAMP AS DT_IS_RUN_BEGIN
             ,NULL              AS DT_IS_RUN_END
             ,'ONGOING'         AS CD_IS_RUN_STATUS
             ,:6                AS DS_IS_REQUESTOR_USER
             ,NULL              AS DS_IS_LOG_FILE_PATH
             ,NULL              AS DS_PULL_FIELD_NAME
             ,NULL              AS DT_PULL_FROM
             ,NULL              AS DT_PULL_TO
             ,NULL              AS DT_PULL_SOURCE_LST
             ,NULL              AS DS_PULL_FILTER_DSC
             ,CURRENT_TIMESTAMP AS DT_IS_INSERT
             ,CURRENT_USER      AS DS_IS_INSERT_USER
             ,CURRENT_TIMESTAMP AS DT_IS_UPDATE
             ,CURRENT_USER      AS DS_IS_UPDATE_USER;`
                              ,binds:[ID_IS_RUN_FLOW
                                     ,DS_IS_RUN_NAMESPACE
                                     ,DS_IS_RUN_FLOW
                                     ,DS_IS_RUN_INSTANCE_NAME
                                     ,CD_IS_RUN_DATA_LOAD_STRATEGY
                                     ,DS_IS_REQUESTOR_USER
                                     ]
                              });

   return ID_IS_RUN_FLOW;
$$;
