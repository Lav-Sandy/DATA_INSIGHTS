import sys
import findspark
findspark.init()
import numpy as np
import pandas as pd
import streamlit as st
import datacompy
import os
import io
from datetime import datetime
import cx_Oracle
import subprocess
from glob import glob
from cx_Oracle import DatabaseError
from COMPARE import *
from COMPARE_SPARK import *
from Connection import *
import openpyxl
from openpyxl.styles import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
import pyspark.sql.functions as Func
global df1,df2,Output
import time
import snowflake.connector as sf
from Connection import *
from pyspark.sql.types import StringType 
from pyspark.sql.functions import col
# from pyspark import SparkConf

# .appName("app1") . master("spark://10.204.49.73:7077") \
# .appName("app1").master("local[*]") \



def FILE_SNOWFLAKE_PYSPARK():
    
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
        .appName("app1").master("local[*]") \
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
    global df1, df2, Connection1, Connection2,Output
    
    placeholder = st.empty()

    with placeholder.container():
        st.markdown("<h1 style = 'text-align:center; color: Black;font-size:40px;'>FILE TO SNOWFLAKE DATA COMPARE</h1>", unsafe_allow_html = True)
        st.markdown('<hr style = "border-top: 3px solid orange;">',unsafe_allow_html = True)
        st.markdown (""" **_The SPARK framework supports Snowflake Database comparison with multiple file types (.csv, .txt, .xls) and delimeters._** <br>
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
    

    try:
        with st.sidebar.form(key='MyForm1',clear_on_submit=True):
            st.file_uploader("Upload parameter file", type=["xlsx"],key = "F2S_Spark_File")
            pwd1=st.text_input("Please Enter Source database password", type="password",key = "F2Ssourcepassword")
        
            def DB_CONNECT():
                global SFAccount,SFUser,pwd1,SFSchema,SFRole,SFWarehouse,SFDatabase,SFurl,Output
                if st.session_state.F2S_Spark_File:
                    # Parameterising the db connection details#
                    df_db = pd.read_excel(st.session_state.F2S_Spark_File, sheet_name='Connection')
                    SFAccount = str(df_db.at[16, "Value"])
                    SFUser = str(df_db.at[17, "Value"])
                    SFDatabase = str(df_db.at[18, "Value"])
                    SFSchema = str(df_db.at[19, "Value"])
                    SFWarehouse = str(df_db.at[20, "Value"])
                    SFRole = str(df_db.at[21, "Value"])
                    SFurl =  SFAccount +'.snowflakecomputing.com'
                    
                    if 'Connection' not in st.session_state:
                        st.session_state.Connection=connect_to_snowflake(SFAccount,SFUser,st.session_state.F2Ssourcepassword,SFRole,SFWarehouse,SFDatabase);
      
                    if st.session_state.Connection:
                        st.sidebar.success('Connection Successful', icon="✅")
                    else:
                        st.sidebar.error('Connection Failed!')
                        
                else:
                    st.info("Awaiting user inputs.")
                           
                # return st.session_state.Connection,SFAccount,SFUser,SFASchema,st.session_state.pwd,SFRole,SFWarehouse,SFDatabase,SFurl,Output
            
            submitted=st.form_submit_button("Connect",on_click=DB_CONNECT)

        def DB_CLOSE():
            if st.session_state.Connection:
                st.session_state.Connection.close()
                #del st.session_state.conn
            st.info('Database Connection Closed.')

        def COMPARE():
            placeholder.empty()
            
            df_dbq = pd.read_excel(st.session_state.F2S_Spark_File, sheet_name='File_Snow_Spark')

            tcName = df_dbq.at[1, "TestName"]
            tcName1 = tcName + '.txt'
            IDPPrimaryKey =df_dbq.at[1, "Target_Primarykey"]
            FileDelimiter = df_dbq.at[1, "FileDelimiter"]
            PATH= df_dbq.at[1, "File_Location"]
            Output=PATH
            file_name = df_dbq.at[1, "File_Name"]
            
            SFQuery = df_dbq.at[1, "Target_Query"]
            SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
            SNOWFLAKE_OPTIONS = {
            'sfURL': os.environ.get("SNOWFLAKE_URL", SFurl),
            'sfAccount': os.environ.get("SNOWFLAKE_ACCOUNT", SFAccount),
            'sfUser': os.environ.get("SNOWFLAKE_USER", SFUser),
            'sfPassword': os.environ.get("SNOWFLAKE_PASSWORD", pwd1),
            'sfDatabase': os.environ.get("SNOWFLAKE_DATABASE", SFDatabase),
            'sfSchema': os.environ.get("SNOWFLAKE_SCHEMA", SFSchema),
            'sfWarehouse': os.environ.get("SNOWFLAKE_WAREHOUSE", SFWarehouse),
            'sfRole':  os.environ.get("SNOWFLAKE_ROLE", SFRole)
            }
            
            df1 = spark.read.format(SNOWFLAKE_SOURCE_NAME) \
              .options(**SNOWFLAKE_OPTIONS) \
              .option('query',SFQuery) \
              .option("fetchSize", 10000).option("numPartitions", 10) \
              .load()
            df2 = SPARK_FILE_READ  (spark , os.path.join(Output,file_name),FileDelimiter)
            
            # df2_count=df2.count()
            start_time = datetime.datetime.now() 
            Spark_Dataframe_Summary(df1,df2) 

            
            SPARK_COMP(spark,df1,df2,IDPPrimaryKey,tcName,Output,start_time)
                
                
        cmp = st.sidebar.button('Compare',on_click=COMPARE)
        
        if cmp:
            placeholder.empty()
            st.runtime.legacy_caching.clear_cache()
            if st.session_state.conn:
                DB_CLOSE()
            else:
                pass
                        
    except (AssertionError, UnboundLocalError, NameError, RuntimeError, TypeError, ValueError) as e:
        # pass
        st.exception(e)
        if st.session_state.conn:
            DB_CLOSE()
        st.runtime.legacy_caching.clear_cache()
            

    finally:
        pass
            #closing database connection.
