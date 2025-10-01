USE SCHEMA COP_DMT_FLX;

CREATE OR REPLACE TABLE W_FLX_SCE_EML__INI 
         (SCE_ELM_KEY                  VARCHAR(64)                             COMMENT 'Scenario Key : concatenation of CBU and Scenario Code'
         ,SCE_ELM_DSC                  VARCHAR(60)                             COMMENT 'Scenario description'
         ,EML_USR_COD                  VARCHAR(50)                             COMMENT 'Power BI user who requested the initialization of the scenario with the domain'
         ,EML_FLG                      NUMBER(2,0)                             COMMENT 'Email sent (0/1)'
         ,T_REC_UPD_TST                TIMESTAMP_TZ                            COMMENT '[Technical] Timestamp of last update into the table'
         ) COMMENT = '[Flex] Working table to store the scenario key to send the email at the end of the initialization process';
