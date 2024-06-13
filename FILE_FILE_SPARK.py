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
#import cx_Oracle
import subprocess
from glob import glob
#from cx_Oracle import DatabaseError
from COMPARE_SPARK import *
#from Connection import *
import openpyxl
from openpyxl.styles import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType 
from pyspark.sql.functions import col
global df1,df2,Output
import time
import re
# import pyspark-pandas as ps
# from pyspark import SparkConf

# .appName("app1") . master("spark://10.204.49.73:7077") \
# .appName("app1").master("local[*]") \



def File_File_PYSPARK():
    
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
        .appName("Snow_to_Snow").master("local[*]") \
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
        st.markdown("<h1 style = 'text-align:center; color: Black;font-size:40px;'>SPARK FILE COMPARE</h1>", unsafe_allow_html = True)
        st.markdown('<hr style = "border-top: 3px solid orange;">',unsafe_allow_html = True)
        st.markdown (""" **_This framework supports comparison of multiple file types (.csv, .txt, .xls) and delimiters._** <br>
                    <br>
                    
                """,True)
                
        # blink_style = """
        # @keyframes blink { 
        # 0% {opacity:1; }
        # 50% {opacity:0; }
        # 100% {opacity:1;}
        # }
        
        # .blink { animation: blink 1s infinite; 
        # color: blue;
        # }
        
        # .icon {
        # font-size: 25px;
        # vertical-align : middle;
        # margin-right: 5px;
        
        # """
        
        # st.write ('<style>' + blink_style + '</style>', unsafe_allow_html = True)
        # st.write ('<p class = "blink"><span class = "icon">⚠️</span> This Application is intended for NON PRODUCTION data only and developers of this application are not responsible for any violation.</p>',unsafe_allow_html = True)
        # st.write ('Please follow the <a href = "https://voya.net/website/corporate.nsf/byUID/AMEP-BB4PLU"> DG policy guidelines </a>  before using any sensitive data.',unsafe_allow_html = True) 
    


    
    global df1, df2,IDPHostName,IDPPort,IDPService_Name,pwd,IDPUID,Output
    
    # placeholder = st.empty()

    


    try:

        
        def COMPARE():
            placeholder.empty()
            df_dbq = pd.read_excel(st.session_state.F2O_Spark_DB, sheet_name='File_File_Pyspark')
            #st.write(pwd)
            i = 2
            tcName = df_dbq.at[i - 1, "TestName"]
            tcName1 = tcName + '.txt'
            Output = df_dbq.at[i - 1, "File_Location"]
            PrimaryKey =df_dbq.at[i - 1, "PrimaryKey"]
            SRCDelimiter = df_dbq.at[0, "SRCDelimiter"]
            TGTDelimiter = df_dbq.at[0, "TGTDelimiter"]
            SRC_PATH = df_dbq.at[i - 1, "Source_File_Name"]
            TGT_PATH = str(df_dbq.at[i - 1, "Target_File_Name"])
            
            
                
            df1 = SPARK_FILE_READ (spark , os.path.join(Output,SRC_PATH),SRCDelimiter)
            print(df1.head())
            
            df2 = SPARK_FILE_READ (spark , os.path.join(Output,TGT_PATH),TGTDelimiter)
            print(df2.head())
            
            start_time = datetime.datetime.now()  
            Spark_Dataframe_Summary(df1,df2)            
            
            #Data Compare Report display
            SPARK_COMP(spark,df1,df2,PrimaryKey,tcName,Output,start_time)
            
        with st.sidebar.form(key='MyForm',clear_on_submit=True):
            st.file_uploader("Upload parameter file", type=["xlsx"],key = "F2O_Spark_DB")
            cmp=st.form_submit_button("Compare",on_click=COMPARE)
            if cmp:
                placeholder.empty()   
                st.runtime.legacy_caching.clear_cache()
        
        # cmp = st.sidebar.button('Compare',key="comp",on_click=COMPARE)
            
    except (AssertionError, UnboundLocalError, NameError, RuntimeError, TypeError, ValueError) as e:
        # pass
        st.exception(e)
        st.runtime.legacy_caching.clear_cache()
    finally:
        pass
        #closing database connection.

