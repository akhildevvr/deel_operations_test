
import pandas as pd
from dags.common.scripts.snowflake_connect import SnowflakeDataExtract
from datetime import datetime
from common.scripts.slack_connect import *




def balance_validation(**context):
    try:


        # The following SQL query calculates the positive and negative changes wrt to the payment status. 
        # It estimates the negative and positive change , and if its more than 50% either way, it returns result.
        sql = '''WITH transaction_summary AS (
                        SELECT
                        organization_id,
                            SUM(CASE WHEN status IN ('paid', 'processing') THEN amount ELSE 0 END) AS total_negative,
                            SUM(CASE WHEN status IN ('refunded', 'failed','open','credited', 'cancelled') THEN amount ELSE 0 END) AS total_positive
                        FROM
                            DEEL_INGEST.OPERATIONS_TRANSFORM.INVOICES
                            group by
                            organization_id
                        )
                        SELECT
                        organization_id,
                        total_positive,
                        total_negative,
                        (total_positive - total_negative) AS net_change,
                        CASE 
                            WHEN DIV0(total_negative , total_positive) > 0.5 THEN 'Significant Decrease'
                            WHEN DIV0(total_positive , total_negative) > 0.5 THEN 'Significant Increase'
                        END AS change_status
                        FROM
                        transaction_summary
                       '''

        snf = SnowflakeDataExtract(conn_id='snowflake_conn', query = sql)
        
        results = snf.snowflake_extractor()
        
        if results.empty == False:
            reason = 'There are oragnizations with over 50% changes in the balance'
            
            # Calls slack notificaton fucntion.
            slack_notification_validation(context, reason)

            
            
    except ValueError as err:
        print(err.args)
        raise





def main():
    #Run some tests here
    balance_validation()

if __name__ == "__main__":
    main()      
        
    
