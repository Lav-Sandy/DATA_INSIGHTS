import sys
import findspark
findspark.init()
import numpy as np
import pandas as pd
import streamlit as st
import os
import io
from datetime import datetime
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
from pyspark.sql.types import StringType 
from pyspark.sql.functions import col
global df1,df2,Output

import re
# from pyspark import SparkConf

# .appName("app1") . master("spark://10.204.49.73:7077") \
# .appName("app1").master("local[*]") \



def FILE_ORACLE_PYSPARK():
    
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
    
    global df1, df2,IDPHostName,IDPPort,IDPService_Name,pwd,IDPUID,Output
    
    placeholder = st.empty()

    with placeholder.container():
        st.markdown("<h1 style = 'text-align:center; color: Black;font-size:40px;'>FILE TO ORACLE DATA COMPARE</h1>", unsafe_allow_html = True)
        st.markdown('<hr style = "border-top: 3px solid orange;">',unsafe_allow_html = True)
        st.markdown (""" **_The SPARK framework supports Oracle Database comparison with multiple file types (.csv, .txt, .xls) and delimiters._**  <br>

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

        with st.sidebar.form(key='MyForm',clear_on_submit=True):
            st.file_uploader("Upload parameter file", type=["xlsx"],key = "F2O_Spark_File")
            # password = st.text_input("Enter database password", type="password",key = "pwd")
            pwd1=st.text_input("Please Enter Source database password", type="password",key = "F2Osourcepassword")

            
            def DB_CONNECT():
                global orc1Host,orc1Port,orc1ServiceName,orc1UserName,Output
                if st.session_state.F2O_Spark_File:
                # Parameterising the db connection details#
                    df_db = pd.read_excel(st.session_state.F2O_Spark_File, sheet_name='Connection')
                    orc1ServiceName = str(df_db.at[0, "Value"])
                    orc1Host= str(df_db.at[1, "Value"])
                    orc1Port = str(df_db.at[2, "Value"])
                    orc1UserName = str(df_db.at[3, "Value"])
                    
            
                    if 'conn'  not in st.session_state:
                        st.session_state.conn  = connect_oracle(orc1Host,orc1Port,orc1ServiceName,orc1UserName,st.session_state.F2Osourcepassword);
                        # st.session_state.conn1 ='jdbc:oracle:thin:@'+IDPHostName+':'+IDPPort+'/'+IDPService_Name
                    if st.session_state.conn:
                        st.sidebar.success('Connection Successful', icon="✅")
                    else:
                        st.sidebar.error('Connection Failed!')
                        
                else:
                    st.info("Awaiting user inputs.")
                    #st.runtime.legacy_caching.clear_cache()
                    
                # return st.session_state.conn,IDPHostName,IDPPort,IDPService_Name,IDPUID,st.session_state.F2Osourcepassword
                
            submitted=st.form_submit_button("Connect",on_click=DB_CONNECT)


        def DB_CLOSE():
            if st.session_state.conn:
                st.session_state.conn.close()
                del st.session_state.conn
            st.info('Database Connection Closed.')

        def COMPARE():

            placeholder.empty()
            df_dbq = pd.read_excel(st.session_state.F2O_Spark_File, sheet_name='File_Oracle_Spark')
            #st.write(pwd)
            tcName = df_dbq.at[1, "TestName"]
            tcName1 = tcName + '.txt'
            IDPPrimaryKey =df_dbq.at[1, "Target_Primarykey"]
            FileDelimiter = df_dbq.at[1, "FileDelimiter"]
            PATH= df_dbq.at[1, "File_Location"]
            Output=PATH
            file_name = df_dbq.at[1, "File_Name"]
            
            orclQuery = df_dbq.at[1, "Target_Query"]
            df1 = spark.read \
            .format("jdbc") \
            .option("url",'jdbc:oracle:thin:@'+orc1Host+':'+orc1Port+'/'+orc1ServiceName) \
            .option("query", orclQuery) \
            .option("user", orc1UserName) \
            .option("password", pwd1) \
            .option("driver", "oracle.jdbc.driver.OracleDriver") \
            .option("fetchSize", 10000).option("numPartitions", 20)\
            .load()

            # st.write(spark , os.path.join(Output,file_name),FileDelimiter)
            df2 = SPARK_FILE_READ  (spark , os.path.join(Output,file_name),FileDelimiter)
            for col in df1.columns:
              df1 = df1.withColumn(col, df1[col].cast(StringType()))
            for col in df2.columns:
              df2 = df2.withColumn(col, df2[col].cast(StringType()))
            
            # df2_count=df2.count()
            start_time = datetime.datetime.now()  
            Spark_Dataframe_Summary(df1,df2)
            # with st.spinner('Wait!! Data Comparision is in progress...'):            
                
                #Data Compare Report display
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
        # st.exception(e)
        if st.session_state.conn:
            DB_CLOSE()
        st.runtime.legacy_caching.clear_cache()
            

    finally:
        pass
            #closing database connection.

