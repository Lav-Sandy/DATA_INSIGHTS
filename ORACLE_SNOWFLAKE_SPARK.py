import sys
import findspark
findspark.init()
import numpy as np
import pandas as pd
import streamlit as st
import datacompy
import os
import io
import datetime
import cx_Oracle
import subprocess
from glob import glob
from cx_Oracle import DatabaseError
from COMPARE_SPARK import *
from Connection import *
import openpyxl
from openpyxl.styles import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
global df1,df2,Output
import time
# from pyspark import SparkConf

# .appName("app1") . master("spark://10.204.49.73:7077") \
# .appName("app1").master("local[*]") \



def ORACLE_SNOWFLAKE_PYSPARK():
    
    st.markdown("""
    <style>
           .css-k1ih3n {
                padding-top: 2rem;
                padding-bottom: 3rem;
                padding-left: 5rem;
                padding-right: 3rem;
            }

    </style>
    """, unsafe_allow_html=True)
    
    spark = SparkSession.builder \
        .appName("Oracle_to_Snow").master("local[*]") \
        .enableHiveSupport() \
        .getOrCreate()
    from pyspark import SparkConf
    MAX_MEMORY="1000000g"
    conf=SparkConf()
    conf.set("spark.executor.processTreeMetrics.enabled","true")
    conf.set("spark.shuffle.manager","SORT")
    conf.set("spark.dynamic.Allocation.enabled","true")
    conf.set("spark.dynamic.Allocation.executorIdleTimeout","2m")
    conf.set("spark.dynamic.Allocation.minexecutors",0)
    conf.set("spark.dynamic.Allocation.maxexecutors",20000)
    # conf.sets("spark.driver.memory", "40g")
    conf.set("spark.executor.memory", MAX_MEMORY)
    conf.set("spark.driver.memory", MAX_MEMORY)
    conf.set("spark.storage.safetyFraction",1)
    conf.set("spark.testing.memory", "2147480000")
    conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    conf.set("spark.sql.inMemoryColumnarStorage.compressed", "true")
    conf.set("spark.sql.inMemoryColumnarStorage.batchSize",1000000)
    conf.set("spark.sql.debug.maxToStringFields", 100000)
    conf.set("spark.hadoop.mapred.max.split.size",32000000)
    
    global df1, df2,IDPHostName,IDPPort,IDPService_Name,pwd,IDPUID,Output
    
    placeholder = st.empty()

    with placeholder.container():
        st.markdown("<h1 style = 'text-align:center; color: Black;font-size:40px;'>ORACLE TO SNOWFLAKE DATA COMPARE</h1>", unsafe_allow_html = True)
        st.markdown('<hr style = "border-top: 3px solid orange;">',unsafe_allow_html = True)
        st.markdown (""" **_The SPARK framework supports comparison of datasets from ORACLE TO SNOWFLAKE DATABASE._** <br>
                """,True)

        blink_style = """
        @keyframes blink { 
        0% {opacity:1; }
        50% {opacity:0; }
        100% {opacity:1;}
        }
        
        .blink { animation: blink 1s infinite; 
        color: blue;
        }
        
        .icon {
        font-size: 25px;
        vertical-align : middle;
        margin-right: 5px;
        
        """
        
        st.write ('<style>' + blink_style + '</style>', unsafe_allow_html = True)
        st.write ('<p class = "blink"><span class = "icon">⚠️</span> This Application is intended for NON PRODUCTION data only and developers of this application are not responsible for any violation.</p>',unsafe_allow_html = True)
        st.write ('Please follow the <a href = "https://voya.net/website/corporate.nsf/byUID/AMEP-BB4PLU"> DG policy guidelines </a>  before using any sensitive data.',unsafe_allow_html = True) 
    
    
    global df1, df2,IDPHostName,IDPPort,IDPService_Name,pwd,IDPUID,Output
    
    # placeholder = st.empty()

    


    try:

        with st.sidebar.form(key='MyForm',clear_on_submit=True):
            st.file_uploader("Upload parameter file", type=["xlsx"],key = "O2S_Spark_DB")
            pwd1=st.text_input("Please Enter Source database password", type="password",key = "O2Ssourcepassword")
            pwd2=st.text_input("Please Enter Target database password", type="password",key = "O2Stargetpassword")

            
            def DB_CONNECT():
                global orc1Host,orc1Port,orc1ServiceName,orc1UserName,Output,SFURL,SFAccount,SFUser,SFDatabase,SFSchema,SFWarehouse,SFRole
                if st.session_state.O2S_Spark_DB:
                # Parameterising the db connection details#
                    df_db = pd.read_excel(st.session_state.O2S_Spark_DB, sheet_name='Connection')
                    orc1ServiceName = str(df_db.at[0, "Value"])
                    orc1Host= str(df_db.at[1, "Value"])
                    orc1Port = str(df_db.at[2, "Value"])
                    orc1UserName = str(df_db.at[3, "Value"])
                    
                    SFAccount = str(df_db.at[16, "Value"])
                    SFUser = str(df_db.at[17, "Value"])
                    SFDatabase = str(df_db.at[18, "Value"])
                    SFSchema = str(df_db.at[19, "Value"])
                    SFWarehouse = str(df_db.at[20, "Value"])
                    SFRole = str(df_db.at[21, "Value"])
                    SFURL =  SFAccount +'.snowflakecomputing.com'
                    # Output = str(df_db.at[24, "Value"])
            
                    if 'conn1' not in st.session_state:
                        st.session_state.conn1  = connect_oracle(orc1Host,orc1Port,orc1ServiceName,orc1UserName,st.session_state.O2Ssourcepassword);
                    if 'conn2' not in st.session_state:
                        st.session_state.conn2  = connect_to_snowflake(SFAccount,SFUser,st.session_state.O2Stargetpassword,SFRole,SFWarehouse,SFDatabase);
                    if st.session_state.conn1 and st.session_state.conn2:
                        st.sidebar.success('Connection Successful', icon="✅")
                    else:
                        st.sidebar.error('Connection Failed!')
                        
                else:
                    st.info("Awaiting user inputs.")
                    #st.runtime.legacy_caching.clear_cache()
                    
                # return st.session_state.conn,st.session_state.conn1,orc1Host,orc1Port,orc1ServiceName,orc1UserName,st.session_state.pwd,Output
                
            submitted=st.form_submit_button("Connect",on_click=DB_CONNECT)


        def DB_CLOSE():
            
            if st.session_state.conn1:
                st.session_state.conn1.close()
                del st.session_state.conn1
            if st.session_state.conn2:
                st.session_state.conn2.close()
                del st.session_state.conn2
            st.info('Database Connection Closed.')

        def COMPARE():
            placeholder.empty()
            df_dbq = pd.read_excel(st.session_state.O2S_Spark_DB, sheet_name='Oracle_Snowflake')
            #st.write(pwd)
            i = 2
            tcName = df_dbq.at[i - 1, "TestName"]
            tcName1 = tcName + '.txt'
            Output = df_dbq.at[i - 1, "File_Location"]
            PrimaryKey =df_dbq.at[i - 1, "PrimaryKey"]
            # SnowflakePrimaryKey= df_dbq.at[i - 1, "PrimaryKey"]
            # FileDelimiter = df_dbq.at[0, "Delimiter"]
            SRCQuery = df_dbq.at[i - 1, "Source_Query"]
            TGTQuery = str(df_dbq.at[i - 1, "Target_Query"])
            df1 = spark.read \
            .format("jdbc") \
            .option("url",'jdbc:oracle:thin:@'+orc1Host+':'+orc1Port+'/'+orc1ServiceName) \
            .option("query", SRCQuery) \
            .option("user", orc1UserName) \
            .option("password", pwd1) \
            .option("driver", "oracle.jdbc.driver.OracleDriver") \
            .option("fetchSize", 10000).option("numPartitions", 20)\
            .load()

            
            SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
            SNOWFLAKE_OPTIONS = {
			'sfURL': os.environ.get("SNOWFLAKE_URL", SFURL),
			'sfAccount': os.environ.get("SNOWFLAKE_ACCOUNT", SFAccount),
			'sfUser': os.environ.get("SNOWFLAKE_USER", SFUser),
			'sfPassword': os.environ.get("SNOWFLAKE_PASSWORD", pwd2),
			'sfDatabase': os.environ.get("SNOWFLAKE_DATABASE", SFDatabase),
			'sfSchema': os.environ.get("SNOWFLAKE_SCHEMA", SFSchema),
			'sfWarehouse': os.environ.get("SNOWFLAKE_WAREHOUSE", SFWarehouse),
			'sfRole':  os.environ.get("SNOWFLAKE_ROLE", SFRole)
			}
            
            df2 = spark.read.format(SNOWFLAKE_SOURCE_NAME) \
            .options(**SNOWFLAKE_OPTIONS) \
            .option('query', TGTQuery) \
            .option("fetchSize", 10000).option("numPartitions", 20) \
            .load()
            
            
            start_time = datetime.datetime.now()  
            Spark_Dataframe_Summary(df1,df2)            
            
            #Data Compare Report display
            SPARK_COMP(spark,df1,df2,PrimaryKey,tcName,Output,start_time)

        
        cmp = st.sidebar.button('Compare',key="comp",on_click=COMPARE)
        
        if st.session_state.comp and st.session_state.conn1 and st.session_state.conn2:
            placeholder.empty()
            
            if st.session_state.conn1 and st.session_state.conn2:
                DB_CLOSE()
            
            else:
                pass
                
            st.runtime.legacy_caching.clear_cache()
    except (AssertionError, UnboundLocalError, NameError, RuntimeError, TypeError, ValueError) as e:
        # pass
        st.exception(e)
        if st.session_state.conn1 and st.session_state.conn2:
            DB_CLOSE()
        st.runtime.legacy_caching.clear_cache()
    finally:
        pass
        #closing database connection.
