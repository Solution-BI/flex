/****** Object:  Table [flex].[T_FLX_LOG]    Script Date: 10/2/2025 9:11:22 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [flex].[T_FLX_LOG](
	[EVENT_AT] [datetime] NOT NULL,
	[RUN_ID] [bigint] NOT NULL,
	[PROCESS_NAME] [varchar](128) NOT NULL,
	[EVENT_TYPE] [varchar](128) NOT NULL,
	[EVENT_PAYLOAD] [json] NULL
) ON [PRIMARY]
GO

ALTER TABLE [flex].[T_FLX_LOG] ADD  DEFAULT (getdate()) FOR [EVENT_AT]
GO

ALTER TABLE [flex].[T_FLX_LOG] ADD  DEFAULT (json_object()) FOR [EVENT_PAYLOAD]
GO

