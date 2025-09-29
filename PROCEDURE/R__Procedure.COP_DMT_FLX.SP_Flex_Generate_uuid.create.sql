USE SCHEMA COP_DMT_FLX{{uid}};

/*
=========================================================================
                                HISTORY
=========================================================================
Description : Create Procedure Script to return the unique identifier of a query

Author      : Noel Coquio (Solution BI France)
Created On  : 16-07-2024
=========================================================================
=========================================================================
*/

CREATE OR REPLACE PROCEDURE SP_Flex_Generate_uuid()
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$

	// This method enables the standard call to a sequential array of queries
	
	function uuidHelper() {
		return ( ( ( 1+Math.random() ) * 0x10000 ) | 0 ).toString( 16 ).substring( 1 );
	}
	
	function generateuuid() {
		return (uuidHelper() + uuidHelper() + "-" + uuidHelper() + "-4" 
			+ uuidHelper().substr(0,2) + "-" + uuidHelper() + "-" + uuidHelper() + uuidHelper() + uuidHelper()
		).toLowerCase();
	}

var RUN_ID = generateuuid();
return RUN_ID;

$$
;




