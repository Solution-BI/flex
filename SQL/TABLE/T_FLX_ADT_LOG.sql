/****** Object:  Table [flex].[T_FLX_ADT_LOG]    Script Date: 10/2/2025 9:18:33 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [flex].[T_FLX_ADT_LOG](
	[ID] [bigint] NOT NULL,
	[SCE_ELM_KEY] [varchar](64) NOT NULL,
	[CBU_COD] [varchar](10) NULL,
	[SCE_ELM_COD] [varchar](50) NULL,
	[ETI_ELM_LIS_TXT] [varchar](255) NULL,
	[CUS_ELM_LIS_TXT] [varchar](max) NULL,
	[PDT_ELM_LIS_TXT] [varchar](max) NULL,
	[EIB_ELM_LIS_TXT] [varchar](max) NULL,
	[TTY_ELM_LIS_TXT] [varchar](255) NULL,
	[IND_ELM_COD] [varchar](255) NULL,
	[IND_ELM_DSC] [varchar](255) NULL,
	[PER_ELM_LIS_TXT] [varchar](255) NULL,
	[OLD_VAL] [decimal](32, 12) NULL,
	[NEW_VAL] [decimal](32, 12) NULL,
	[IPC_VAL] [decimal](32, 12) NULL,
	[IPC_VAL_PCT] [decimal](32, 12) NULL,
	[IPC_CMT_TXT] [varchar](max) NULL,
	[T_REC_UPD_USR] [varchar](255) NULL,
	[T_REC_UPD_TST] [datetime] NULL,
	[DIS_CHG_FLG] [bit] NULL,
 CONSTRAINT [PK_T_FLX_ADT_LOG] PRIMARY KEY NONCLUSTERED 
(
	[ID] ASC,
	[SCE_ELM_KEY] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [FLX_SCE_PS]([SCE_ELM_KEY])
) ON [FLX_SCE_PS]([SCE_ELM_KEY])
GO

ALTER TABLE [flex].[T_FLX_ADT_LOG] ADD  DEFAULT ((0)) FOR [DIS_CHG_FLG]
GO

