USE SCHEMA COP_DMT_FLX{{uid}};

/*
=========================================================================
                                HISTORY
=========================================================================
Description : Create Procedure Script to replace character in JSON message

Author      : Noel Coquio (Solution BI France)
Created On  : 12-12-2024
=========================================================================
Modified On:    Description:                        Author:
=========================================================================
*/


CREATE OR REPLACE PROCEDURE SP_FLEX_REPLACE_CHARACTER(JSON_INPUT VARCHAR(16777216),FIELD_NAME VARCHAR(5000) DEFAULT '"SCE_ELM_DSC","SCE_CMT_TXT"')
    RETURNS VARCHAR
    LANGUAGE SQL
    EXECUTE AS CALLER
AS
$$

DECLARE


/* initialize the variable with the input message */
v_STRING         VARCHAR(16777216) := REPLACE(JSON_INPUT,'''','QUOTE');
v_FIELD_NAME     VARCHAR(100000) := FIELD_NAME;
v_LIST_COL       VARCHAR(100000);
v_LEN_FIELD      INTEGER;
V_CURR_FIELD     VARCHAR(256);
v_NEXT_FIELD     VARCHAR(256);

/* Field name for which the value must be translated */
v_FIELD          VARCHAR(256);

/* output message */
v_MESSAGE        VARCHAR(16777216) := '';

/* Technical positions */
v_POS_BEG        INTEGER;
v_POS_END        INTEGER;

field_cursor CURSOR FOR SELECT curr_.value field
      ,COALESCE(next_.value,'EOS')  next_field
FROM   TABLE(SPLIT_TO_TABLE(?,',')) list_
       LEFT OUTER JOIN TABLE(SPLIT_TO_TABLE(? ,',')) next_ ON
       (
          next_.index = list_.index + 1
       )
       INNER JOIN TABLE(SPLIT_TO_TABLE(?,',')) curr_ ON 
       (
          curr_.value = list_.value       
       );

BEGIN

    /* Retreive in the list the field to analyze and the next field needed for the analyze */
    v_POS_BEG := CHARINDEX('"Columns":[', v_STRING, 1) + 11;
    v_POS_END := CHARINDEX('],"DataColumns"', v_STRING, 1);
    v_LIST_COL := SUBSTR(v_STRING, v_POS_BEG, v_POS_END - v_POS_BEG);

    OPEN field_cursor using (v_LIST_COL,v_LIST_COL,v_FIELD_NAME);
    LOOP
        /* Retreive all the field to translate */
        FETCH field_cursor INTO V_CURR_FIELD,v_NEXT_FIELD;
        IF ( V_CURR_FIELD <> '' ) THEN
            v_MESSAGE := '';
            v_FIELD := V_CURR_FIELD || ':"';

            v_LEN_FIELD := LENGTH(v_FIELD);
            /* Retreive the first position of the field in the json message */
            v_POS_BEG := CHARINDEX(v_FIELD, v_STRING, 1);
     
            IF ( v_POS_BEG > 0 ) THEN

                LOOP
                    IF ( v_POS_BEG = 0 ) THEN
                        /* Exit of the loop when all the occurence has been reached */
                        EXIT;
                    END IF;

                    /* extract the message until the position of the field */
                    v_MESSAGE := v_MESSAGE || SUBSTR(v_STRING, 1, v_POS_BEG + v_LEN_FIELD - 1);
                    /* suppress the data previously extracted */
                    v_STRING  := SUBSTR(v_STRING, v_POS_BEG + v_LEN_FIELD);

                    /* retreive the position of the end of field */
                    IF ( v_NEXT_FIELD != 'EOS' ) THEN
                         v_POS_END := CHARINDEX('",' || v_NEXT_FIELD, v_STRING, 1);

                    ELSE
                        v_POS_END := CHARINDEX('"},', v_STRING, 1);

                        IF ( v_POS_END = 0 ) THEN
                            v_POS_END := CHARINDEX('"}]', v_STRING, 1);
                        END IF;

                    END IF;

                    /* Replace all the " by DOUBLEQUOTE in the field value and add the new value to the message */
                    v_MESSAGE := v_MESSAGE || REPLACE(SUBSTR(v_STRING, 1, v_POS_END - 1), '"', 'DOUBLEQUOTE');

                    /* suppress the field value from the json message previously extracted */
                    v_STRING  := SUBSTR(v_STRING, v_POS_END);
                    /* retreive the next occurence of the field */
                    v_POS_BEG := CHARINDEX(v_FIELD, v_STRING, 1);
                END LOOP;

                /* add the end of the json message to the new message */
                v_MESSAGE := v_MESSAGE || v_STRING;
                /* assign the new message to retreive the next new field if exists */
                v_STRING := v_MESSAGE;

            END IF;
        ELSE
            BREAK;
        END IF;
    END LOOP;
           
    IF ( v_MESSAGE = '' ) THEN
        v_MESSAGE := v_STRING;
    END IF;

    RETURN v_MESSAGE;

EXCEPTION
    WHEN OTHER THEN
         RETURN JSON_INPUT;
END;
$$
;
