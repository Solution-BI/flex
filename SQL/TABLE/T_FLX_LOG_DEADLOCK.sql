/****** Object:  Table [flex].[T_FLX_LOG_DEADLOCK]    Script Date: 10/2/2025 9:12:23 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [flex].[T_FLX_LOG_DEADLOCK](
	[deadlock_at] [datetime] NOT NULL,
	[object_name] [varchar](500) NOT NULL,
	[run_id] [bigint] NOT NULL,
	[session_id] [bigint] NULL
) ON [PRIMARY]
GO

ALTER TABLE [flex].[T_FLX_LOG_DEADLOCK] ADD  DEFAULT (getdate()) FOR [deadlock_at]
GO

