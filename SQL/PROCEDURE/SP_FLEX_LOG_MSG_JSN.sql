/****** Object:  StoredProcedure [flex].[SP_FLEX_LOG_MSG_JSN]    Script Date: 10/2/2025 9:08:44 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE   PROCEDURE [flex].[SP_FLEX_LOG_MSG_JSN] (@vSQL_TRANSACTION_ID VARCHAR(256)
                                                       ,@vSQL_PROC_NAME VARCHAR(64)
                                                       ,@vJSONTEXT NVARCHAR(MAX)
                                                       ,@vT_REC_INS_USR VARCHAR(64)
                                                       )
AS
BEGIN

   SET NOCOUNT ON;

   DECLARE @SQL_TRANSACTION_ID VARCHAR(256) = @vSQL_TRANSACTION_ID;
   DECLARE @SQL_PROC_NAME VARCHAR(64) = @vSQL_PROC_NAME;
   DECLARE @JSONTEXT NVARCHAR(MAX) = @vJSONTEXT;
   DECLARE @T_REC_INS_USR VARCHAR(64) = @vT_REC_INS_USR;
   
   SELECT [DEBUG_FLG]
   FROM   [flex].[T_FLX_PAR_LOG]
   WHERE  [SQL_PROC_NAME] = @SQL_PROC_NAME
   AND    [DEBUG_FLG] = 1;

   IF @@ROWCOUNT = 0 

      BEGIN
         INSERT INTO  [flex].[T_FLX_LOG_MSG_JSN] ([SQL_TRANSACTION_ID],[SQL_PROC_NAME],[T_REC_INS_TST],[T_REC_INS_USR]) 
         VALUES(@SQL_TRANSACTION_ID,@SQL_PROC_NAME, GETUTCDATE(), @T_REC_INS_USR);
      END

   ELSE
   
      BEGIN
         INSERT INTO  [flex].[T_FLX_LOG_MSG_JSN] ([SQL_TRANSACTION_ID],[SQL_PROC_NAME],[JSONTEXT],[T_REC_INS_TST],[T_REC_INS_USR]) 
         VALUES(@SQL_TRANSACTION_ID,@SQL_PROC_NAME, @JSONTEXT, GETUTCDATE(), @T_REC_INS_USR);
      END
   
END;
GO

