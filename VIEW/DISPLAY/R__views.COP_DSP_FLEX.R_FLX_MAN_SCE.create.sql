USE SCHEMA COP_DSP_FLEX;

CREATE OR REPLACE VIEW R_FLX_MAN_SCE 
         (MAN_SCE_ELM_KEY                                       COMMENT 'Dataset Key : concatenation of CBU and Dataset Code'
         ,MAN_SCE_ELM_COD                                       COMMENT 'Dataset  code (generated automatically)'
         ,MAN_SCE_ELM_DSC                                       COMMENT 'Dataset description'
         ,CBU_COD                                               COMMENT 'CBU/Market'
         ,MAN_SCE_CUR_COD                                       COMMENT 'Dataset input currency'
         ,MAN_SCE_USE_FLG                                       COMMENT 'Dataset configuration completion flag, Once selected, data cannot be modified anymore and the manual data is available to use as Source for Flex Datasets'
         ,CRE_USR_COD                                           COMMENT 'Power BI user who created the dataset - suppress the domain information ie @.....'
         ,CRE_END_TST                                           COMMENT 'Date and time of creation of the Dataset'
         ,MAN_SCE_EML_USR_COD                                   COMMENT 'Power BI user who created the dataset with the domain'
         ,MAN_SCE_DLT_STS_COD                                   COMMENT 'Deletion status: na > requested > in_progress > done | failed'
         ,MAN_SCE_DLT_FLG                                       COMMENT 'Flag indicating that a user has requested the dataset to be deleted (Power ON writeback)'
         ,MAN_SCE_DLT_RQT_TST                                   COMMENT 'Date and time of the deletion request'
         ,MAN_SCE_DLT_RQT_EML_USR_COD                           COMMENT 'Email of the user who requested the deletion'
         ,MAN_SCE_DLT_END_TST                                   COMMENT 'Date and time the deletion process finished (changed to done or failed status)'
         ,MAN_SCE_CMT_TXT                                       COMMENT 'Comment on the Dataset'
         ,MAN_SCE_CMT_TST                                       COMMENT 'Date and Time of last modification of the comment'
         ,MAN_SCE_CMT_EML_USR_COD                               COMMENT 'Email of the user who modified the comment last'
         ) COMMENT = '[Flex] Manual Dataset masterdata'
AS
SELECT    MAN_SCE_ELM_KEY
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
         ,MAN_SCE_DLT_RQT_TST
         ,MAN_SCE_DLT_RQT_EML_USR_COD
         ,MAN_SCE_DLT_END_TST
         ,MAN_SCE_CMT_TXT
         ,MAN_SCE_CMT_TST
         ,MAN_SCE_CMT_EML_USR_COD
FROM      COP_DMT_FLX.R_FLX_MAN_SCE
WHERE     T_REC_DLT_FLG   = 0
AND       MAN_SCE_DLT_FLG = 0
;
