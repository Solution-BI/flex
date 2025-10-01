USE SCHEMA COP_DSP_FLEX;

CREATE OR REPLACE VIEW R_FLX_SCE 
         (SCE_ELM_KEY                               COMMENT 'Scenario Key : concatenation of CBU and Scenario Code'
         ,SCE_ELM_COD                               COMMENT 'Scenario code (generated automatically)'
         ,SCE_ELM_DSC                               COMMENT 'Scenario description'
         ,CBU_COD                                   COMMENT 'CBU/Market'
         ,CUR_COD                                   COMMENT 'Scenario input currency'
         ,SCE_YEA_COD                               COMMENT 'Scenario year'
         ,DTA_YEA_COD                               COMMENT 'Scenario Data year'
         ,SCE_PRO_COD                               COMMENT 'Scenario process : RF01-R12/GPS01-GPS12...'
         ,LST_ACT_PER_COD                           COMMENT 'Last closed period'
         ,UPD_ACT_FLG                               COMMENT 'Flag requesting that the Actuals are updated (using ACT_SRC_SCE_COD) and the Gap closed upon scenario initialization'
         ,ACT_SRC_SCE_COD                           COMMENT 'Actual comparable scenario'
         ,CMP_1ST_SRC_SCE_COD                       COMMENT 'First comparable scenario'
         ,CMP_2ND_SRC_SCE_COD                       COMMENT 'Second comparable scenario'
         ,CMP_3RD_SRC_SCE_COD                       COMMENT 'Third comparable scenario'
         ,CRE_EML_USR_COD                           COMMENT 'Power BI user who created the scenario'
         ,CRE_END_TST                               COMMENT 'Date and time of creation of the scenario'
         ,INI_STS_COD                               COMMENT 'Scenario initialization status: created > configuration_started > configured > init_in_progress > init_done | init_failed > '
         ,INI_RQT_FLG                               COMMENT 'Scenario configuration completion flag, it will chnage the status to configured and inittiate the process to populate scenario with Data'
         ,INI_RQT_TST                               COMMENT 'Date and time of the request of the initialization'
         ,INI_RQT_EML_USR_COD                       COMMENT 'Power BI user who requested the initialization of the scenario with the domain'
         ,INI_END_TST                               COMMENT 'Date and time the initialization process finished (changed to done or failed status)'
         ,CCD_STS_COD                               COMMENT 'Scenario copy to Controlling Cloud status: flex_only > copy_requested > copy_done | copy_failed'
         ,CCD_RQT_FLG                               COMMENT 'Scenario request to send to CCD flag, it will change the status to copy_requested and inittiate the process to send the data back to snowflake'
         ,CCD_RQT_EML_USR_COD                       COMMENT 'Power BI user who requested the copy to Controlling Cloud'
         ,CCD_RQT_TST                               COMMENT 'Date and time of the request to copy to Controlling Cloud'
         ,CCD_END_TST                               COMMENT 'Date and time the copy process finished (changed to done or failed status)'
         ,DLT_STS_COD                               COMMENT 'Deletion status: na > requested > in_progress > in_progress_step01 > in_progress_step02 > done | failed'
         ,DLT_RQT_FLG                               COMMENT 'Flag indicating that a user has requested the scenario to be deleted (Power ON writeback)'
         ,DLT_RQT_TST                               COMMENT 'Date and time of the deletion request'
         ,DLT_RQT_EML_USR_COD                       COMMENT 'Email of the user who requested the deletion'
         ,DLT_END_TST                               COMMENT 'Date and time the deletion process finished (changed to done or failed status)'
         ,CUS_DIM_GRP_COD                           COMMENT 'Customer aggregation level'
         ,EIB_USE_FLG                               COMMENT 'Business type/EIB aggregation level'
         ,TTY_USE_FLG                               COMMENT 'Territory aggregation level'
         ,VAR_NS                                    COMMENT 'Variability for the Net Sales (sensitivity to Volume sold)'
         ,VAR_MAT_COS                               COMMENT 'Variability for the Material Cost of Sales (sensitivity to Volume sold)'
         ,VAR_MAT_OTH                               COMMENT 'Variability for the Rest of Material Costs (sensitivity to Volume sold)'
         ,VAR_MANUF_COS                             COMMENT 'Variability for the Manuf. Cost of Sales (sensitivity to Volume sold)'
         ,VAR_MANUF_OTH                             COMMENT 'Variability for the Rest of Manuf. Costs (sensitivity to Volume sold)'
         ,VAR_LOG_FTC_IFO                           COMMENT 'Variability for the Log. FTC IFO (sensitivity to Volume sold)'
         ,VAR_LOG_USL                               COMMENT 'Variability for the Log. Log USL (sensitivity to Volume sold)'
         ,VAR_LOG_OTH                               COMMENT 'Variability for the Rest of Log. Costs (sensitivity to Volume sold)'
         ,COGS_TOT_FCA_FLG                          COMMENT 'FCA amount flag (amout > 0, 1 else 0)'
         ,SCE_CMT_TXT                               COMMENT 'Comment on the Scenario'
         ,SCE_CMT_TST                               COMMENT 'Date and Time of last modification of the comment'
         ,SCE_CMT_EML_USR_COD                       COMMENT 'Email of the user who modified the comment last'
         ) COMMENT = '[Flex] Flex scenario masterdata'
AS
SELECT    SCE_ELM_KEY
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
         ,INI_STS_COD
         ,INI_RQT_FLG
         ,INI_RQT_TST
         ,INI_RQT_EML_USR_COD
         ,INI_END_TST
         ,CCD_STS_COD
         ,CCD_RQT_FLG
         ,CCD_RQT_EML_USR_COD
         ,CCD_RQT_TST
         ,CCD_END_TST
         ,DLT_STS_COD
         ,DLT_RQT_FLG
         ,DLT_RQT_TST
         ,DLT_RQT_EML_USR_COD
         ,DLT_END_TST
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
FROM      COP_DMT_FLX.R_FLX_SCE
WHERE     T_REC_DLT_FLG = 0
AND       DLT_RQT_FLG = 0
;
