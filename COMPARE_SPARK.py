### Compare 
import datacompy
import pandas as pd
import datetime
import psutil,time
import streamlit as st
import os
import io
from io import StringIO
import zipfile
from zipfile import ZipFile
#from Connection import *
global df1,df2,df3,df4
newList=[]
import re
from pyspark.sql.functions import col




def color_pass_fail(val):
    if val == 'Pass':
        return 'font-weight: bold;color:green'
    elif val == 'Fail':
        return 'font-weight:bold;color:red'
    else:
        return ''

def color_positive_negative(val):
    if val == "100.0%":
        font_color = 'green'
        font_weight = 'bold'
        return 'color: {}; font-weight: {}'.format(font_color, font_weight)
    elif (val == '0.0%'):
        font_color = 'red'
        font_weight = 'bold'
        return 'color: {}; font-weight: {}'.format(font_color, font_weight)
    else:
        font_color = 'black'
        font_weight = None
        return 'color: {}; font-weight: {}'.format(font_color, font_weight)
    # return font_color,font_weight

    
def TIME(start_time):
    end_time = datetime.datetime.now()
    Execution_time = end_time - start_time
    execution_time_seconds = Execution_time.total_seconds()
    minutes,seconds=divmod(execution_time_seconds,60)
    minutes_rounded = round(minutes,2)
    seconds_rounded = round(seconds,2)
    st.markdown('<hr style = "border-top: 3px solid orange;">',unsafe_allow_html = True)

    st.write("#### Execution Efficiency Measure ",unsafe_allow_html=True)
    st.write("⏰ Start Time : **{}**".format(start_time.strftime("%Y-%m-%d %H:%M:%S")),markdown=True)
    st.write("⏰ End Time   : **{}**".format(end_time.strftime("%Y-%m-%d %H:%M:%S")),markdown=True)
    st.write("⌛ Total Execution Time: **{} minutes, {} seconds**".format(minutes_rounded,seconds_rounded),markdown=True)


    
def Spark_Dataframe_Summary(df1,df2):
    
    st.markdown("<h1 style = 'text-align:center; color: Black;font-size:40px;'>COMPARE REPORT</h1>", unsafe_allow_html = True)
    st.markdown('<hr style = "border-top: 3px solid orange;">',unsafe_allow_html = True)
    st.header('**Data Summary**')
    st.markdown("_An Anatomy of Datasets. A Breakdown of Rows and Columns._")
    df1_count=df1.count()
    df2_count=df2.count()
    Details = {
           'Data':['Source','Target'],
           'Total Number of Columns':[len(df1.columns),len(df2.columns)],
           'Total Number of Rows':[df1_count, df2_count]
           }

    df_summary = pd.DataFrame(Details)
    st.write(df_summary.style
           .set_table_styles([
        {'selector': 'th', 'props': [('background-color', '#f7ffff'),    #00008B - Dark blue,  f7ffff - light blue
                         ('color', 'black'),
                         ('font-weight', 'bold')]},
        {'selector': 'td', 'props': [('background-color', '#E1F5FE'),('text-align','center'),
                         ('font-size', '15px')]}]),unsafe_allow_html=True)
 
    st.markdown('<hr style = "border-top: 3px solid orange;">',unsafe_allow_html = True)
    
    
    

def SPARK_COMP(spark, df1, df2, PrimaryKey,tcName,Output,start_time):
    # for col in df1.columns:
        # df1=df1.withColumnRenamed(col, col.upper())
    # # for col in df1.columns:
        # # df1=df1.withColumnRenamed(col, col.replace(" ","_"))
        
    # for col in df2.columns:
        # df2=df2.withColumnRenamed(col, col.upper())
        
    # # for col in df2.columns:
        # # df2=df2.withColumnRenamed(col, col.replace(" ","_"))

#Data Summary Report display
    with st.spinner('Please Wait!! Data Comparison is in progress...'):
        tcName1 = tcName + '.txt'
        df1_count=df1.count()
        df2_count=df2.count()
        letter_list = PrimaryKey.split(",")
        primary_list = [x for x in letter_list]
    
        comparison = datacompy.SparkCompare(spark, df1, df2, join_columns=primary_list,cache_intermediates=True,show_all_columns=True,match_rates=True)
        comparison.rows_both_mismatch.repartition(10).coalesce(10).write.format("com.databricks.spark.csv").option("header", "True").option("codec", "org.apache.hadoop.io.compress.GzipCodec").mode("append").save(os.path.join(Output,tcName+'_Missmatch_Records'))
        end_time = datetime.datetime.now()
        Executin_time = end_time - start_time
                #TIME(start_time)

        with open(os.path.join(Output,tcName1),'w') as report_file:
            report_file.write('\n''\n'+"**************************************"+'\n')
            report_file.write('SOURCE COUNT            = '+str(df1_count)+'\n')
            report_file.write('TARGET COUNT            = '+str(df2_count)+'\n')
            report_file.write("***********************************************"+'\n\n') 
            comparison.report(file=report_file)
            end_time = datetime.datetime.now()
            Execution_time = end_time - start_time
            report_file.write('\n''\n'+"********************************************************"+'\n')
            report_file.write('Start_time            = '+str(start_time)+'\n')
            report_file.write('End_time              = '+str(end_time)+'\n')
            report_file.write('Executin_time={}minutes'.format(Execution_time.total_seconds()/ 60.0)+'\n')
            report_file.write("********************************************************"+'\n\n') 
            report_file.close()
          
        st.header('**Comparison Summary**')
        st.markdown("_A detailed breakdown of data differences._")
        Text123 = '****** Columns with Equal/Unequal Values ******'
        Res = spark.read.text(Output+"\\"+tcName1)
        pandas_df = Res.toPandas()
        first_in = pandas_df.loc[pandas_df["value"] == Text123].index.values
        f_in = first_in[0]+2
        last_in = pandas_df.index[-9]
        Result123 = pandas_df.loc[f_in:last_in]
        head = ['Column Name','Source Datatype','Target Datatype','# Matches','# Mismatches','Match Rate %']
        dfRes = pd.DataFrame(Result123)
        dfRes.to_csv(Output+"\\TestTest123.txt")
        dfread = pd.read_csv(Output+"\\TestTest123.txt", header=1,usecols=[1,2,3,4,5,6],names=head,sep=r'\s{1,}')
        # dfread["Match Rate %"] = dfread['Match Rate %'].str.replace('"', '').astype(float)
        # dfread['Match Rate %'] = dfread['Match Rate %'].str.strip('"').astype(float).astype(str)+'%'
        dfread["Match Rate %"] = [float(str(i).replace('"', "")) for i in dfread["Match Rate %"]]
        dfread["Match Rate %"] = dfread['Match Rate %'].astype(str)+'%'
        st.write(dfread.style.hide_index().set_precision(2)
               .applymap(color_positive_negative, subset=['Match Rate %'])
               .set_table_styles([
            {'selector': 'th', 'props': [('background-color', '#f7ffff'),    #00008B - Dark blue,  f7ffff - light blue
                             ('color', 'black'),
                             ('font-weight', 'bold')]},
            {'selector': 'td', 'props': [('background-color', '#E1F5FE'),('text-align','center'),
                             ('font-size', '15px')]}]).render(),unsafe_allow_html=True)
        

        st.write('')
        path123 = Output+"\\"+tcName1
        st.write("Comparison Reports are saved in location: **{}**".format(path123),markdown=True)
        TIME(start_time)
        os.remove(Output+"\\TestTest123.txt")

        
def SPARK_FILE_READ(spark,file,src_delimeter):
    #READING FILE
    if file.endswith(".csv"):
        df = spark.read.option("header","true").option("delimiter", src_delimeter).csv(file)
    elif file.endswith(".txt"):
        df = spark.read.option("header","true").option("delimiter", src_delimeter).csv(file)
    elif file.endswith(".xlsx"):
        pdf1 = pd.read_excel(file, header=0, engine = 'openpyxl')
        pdf1=pdf1.astype(str)
        df = spark.createDataFrame(pdf1)
    df = df.select([col(c).cast("string") for c in df.columns])
    return df

