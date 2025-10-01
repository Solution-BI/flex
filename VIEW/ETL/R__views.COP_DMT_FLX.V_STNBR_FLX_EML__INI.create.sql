USE SCHEMA COP_DMT_FLX;

CREATE OR REPLACE VIEW V_STNBR_FLX_EML__INI
AS
WITH data_ AS (
SELECT Sce_Key,Subject,Email
FROM   (SELECT   W_FLX_SCE_EML__INI.SCE_ELM_KEY                                                               Sce_Key
                ,(CASE INI_STS_COD 
                       WHEN 'done'   THEN '[Flex] Success - ``' || W_FLX_SCE_EML__INI.SCE_ELM_DSC || '`` has been successfully loaded'
                       WHEN 'failed' THEN '[Flex] Error - ``' || W_FLX_SCE_EML__INI.SCE_ELM_DSC || '`` issue in scenario initialization'
                       ELSE '[Flex] Warning - ``' || W_FLX_SCE_EML__INI.SCE_ELM_DSC || '`` no data available in the source scenarios selected'
                  END)                                                                                        Subject
                ,W_FLX_SCE_EML__INI.EML_USR_COD                                                               Email
                ,RANK() OVER (PARTITION BY 'NULL'
                             ORDER BY W_FLX_SCE_EML__INI.SCE_ELM_KEY)                                         RNK_SCE
        FROM     COP_DMT_FLX.R_FLX_SCE
                 INNER JOIN COP_DMT_FLX.W_FLX_SCE_EML__INI ON 
                 (
                    R_FLX_SCE.SCE_ELM_KEY = W_FLX_SCE_EML__INI.SCE_ELM_KEY
                 )
        WHERE    EML_FLG = 0
       )
WHERE  RNK_SCE = 1
)
SELECT 1 order_,Sce_Key Output_
FROM   data_
UNION
SELECT 2 order_,Subject Output_
FROM   data_
UNION
SELECT 3 order_,Email Output_
FROM   data_
;

