/****** Object:  Table [flex].[T_FLX_LOG_MSG_JSN]    Script Date: 10/2/2025 9:10:19 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [flex].[T_FLX_LOG_MSG_JSN](
	[SQL_TRANSACTION_ID] [varchar](256) NULL,
	[SQL_PROC_NAME] [varchar](64) NULL,
	[JSONTEXT] [nvarchar](max) NULL,
	[ROW_COUNT] [int] NULL,
	[DT_IS_LOG_JSON_END] [datetime] NULL,
	[DT_IS_PARSE_JSON_END] [datetime] NULL,
	[DT_IS_SIM_END] [datetime] NULL,
	[T_REC_INS_TST] [datetime] NULL,
	[T_REC_INS_USR] [varchar](64) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO

