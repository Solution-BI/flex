USE SCHEMA COP_DMT_FLX{{uid}};

WITH create_stage AS PROCEDURE ()
RETURNS VARCHAR NOT NULL
LANGUAGE SQL
AS
$$
DECLARE
v_ENV             VARCHAR(10) := UPPER('{{env}}');
v_ADL_STORAGE     VARCHAR(256);
v_STAGE           VARCHAR(5000);

BEGIN
   IF (v_ENV = 'DEV') THEN
      v_ADL_STORAGE := 'URL = ''azure://daneutstashubjvkwicdxdzl.blob.core.windows.net/produced/ctrlcld/INI''';
   ELSEIF (v_ENV = 'QAT') THEN
      v_ADL_STORAGE := 'URL = ''azure://daneuastashubfvclsqnpwag.blob.core.windows.net/produced/ctrlcld/INI''';
   ELSEIF (v_ENV = 'PRD') THEN
      v_ADL_STORAGE := 'URL = ''azure://daneupstashubxwnxhxfaxhw.blob.core.windows.net/produced/ctrlcld/INI''';
   END IF;
      
   v_STAGE := 'CREATE OR REPLACE STAGE STG_FLX_TRF_FIL STORAGE_INTEGRATION = ' || v_ENV || 
              '_HUB_COP_UC_CTRLCLD ' || v_ADL_STORAGE || ' FILE_FORMAT = (TYPE = CSV COMPRESSION = NONE);';

   EXECUTE IMMEDIATE v_STAGE;

   RETURN v_STAGE;

END;
$$

CALL create_stage();
