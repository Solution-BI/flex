/****** Object:  View [flex].[VW_FLX_ADT_LOG]    Script Date: 10/2/2025 9:19:54 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO





CREATE VIEW [flex].[VW_FLX_ADT_LOG] AS
SELECT [ID]
      ,[SCE_ELM_KEY]
      ,[CBU_COD]
      ,[SCE_ELM_COD]
      ,[ETI_ELM_LIS_TXT]
      ,[CUS_ELM_LIS_TXT]
      ,[PDT_ELM_LIS_TXT]
      ,[EIB_ELM_LIS_TXT]
      ,[TTY_ELM_LIS_TXT]
      ,[IND_ELM_COD]
      ,[IND_ELM_DSC]
      ,[PER_ELM_LIS_TXT]
      ,[OLD_VAL]
      ,[NEW_VAL]
      ,[IPC_VAL]
      ,[IPC_VAL_PCT]
      ,[IPC_CMT_TXT]
      ,[T_REC_UPD_USR]
      ,[T_REC_UPD_TST]
	  ,[DIS_CHG_FLG]
      ,(CASE [IND_ELM_COD]
             WHEN 'VOL'          THEN 1
             WHEN 'NS'           THEN 2
             WHEN 'COGS_TOT'     THEN 3
             WHEN 'AP'           THEN 4
             WHEN 'COGS_TOT_FIX' THEN 5
             WHEN 'OIE'          THEN 6
             ELSE 99
        END)                             KPI_ORDER
FROM   [flex].[T_FLX_ADT_LOG]
;
GO

