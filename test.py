from snowflake.sqlalchemy import URL 
from sqlalchemy import create_engine 

engine = create_engine(URL( #https://plninim-tg58176.snowflakecomputing.com
                          user= 'GEORGE9042',
                          account='plninim-tg58176',
                          #region='switzerland-north.azure',
                          password = 'George9042',
                          warehouse='COMPUTE_WH',
                          database='MY_TEST', 
                          schema='NEW',
                          role='ACCOUNTADMIN'
                          ))


connection = engine.connect()
try:
    connection.execute('select * from airports')
finally:
    connection.close()
    engine.dispose()
       
