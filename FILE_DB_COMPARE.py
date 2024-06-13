import numpy as np
import pandas as pd
import openpyxl
import streamlit as st
import cx_Oracle
import datacompy
import os
import subprocess
import xlrd
import xlwt
from datetime import datetime
import csv
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from cx_Oracle import DatabaseError
from COMPARE import *
#from Connection import *
from Get_Credentials import * 



def FILE_DB():

    st.markdown("""
    <style>
           .css-k1ih3n {
                padding-top: 2rem;
    </style>
    """, unsafe_allow_html=True)

    global df1,df2,orc1ServiceName,orc1Host,orc1Port,orc1UserName
    
       
    def DB_CLOSE():
        
        if st.session_state.conn:
            st.session_state.conn.close()
            del st.session_state.conn
        st.info('Database Connection Closed.')
   
    
    placeholder = st.empty()

    with placeholder.container():
        st.markdown("<h1 style = 'text-align:center; color: Black;font-size:40px;'>FILE TO ORACLE DATA COMPARE</h1>", unsafe_allow_html = True)
        st.markdown('<hr style = "border-top: 3px solid orange;">',unsafe_allow_html = True)
        st.markdown (""" **_The framework supports Oracle Database comparison with multiple file types (.csv, .txt, .xls) and delimiters.<br>
        <br>
              This utility performs comparisons based on common columns, hence the header-row or column names are mandatory in source file._**
                """,True)
        st.info ("Please Connect to database first and then upload PARAM file")    
    try:
        placeholder1 = st.empty()
        with placeholder1.container():

            with st.form(key='Form1',clear_on_submit=True):              #,clear_on_submit=True
                
                st.markdown("<h1 style = 'text-align:center; color: Black;font-size:20px;'>DATABASE_DETAILS</h1>", unsafe_allow_html = True)
                env_names_to_funcs = {  "CODS_INTEG" : CODS_INTG, "CODS_ACCP" : CODS_ACCP}
                selected_db = st.sidebar.selectbox("Select ORACLE instance from below list", env_names_to_funcs.keys())
                st.sidebar.write(selected_db)
                st.text_input("Enter User Name", type="password", key = "orc1UserName")
                st.text_input("Enter Password", type="password", key = "orclpwd")
                
                
                def DB_CONNECT():
            
                    if selected_db:    
                        orc1ServiceName,orc1Host,orc1Port = env_names_to_funcs[selected_db]()
                        st.write(orc1ServiceName,orc1Host,orc1Port)
                    if 'conn' not in st.session_state:
                        st.session_state.conn  = connect_oracle(orc1Host,orc1Port,orc1ServiceName,st.session_state.orc1UserName,st.session_state.orclpwd);
                    if st.session_state.conn:
                        st.sidebar.success('Connection Successful', icon="✅")
                    else:
                        st.sidebar.error('Connection Failed!') 
                
                submitted=st.form_submit_button("Connect",on_click=DB_CONNECT)
                
             
        st.sidebar.file_uploader("Upload parameter file", type=["xlsx"],key = "F2O_File")    
        cmp = st.sidebar.button('Compare',key = "comp") 
        
        
        css = """
        <style>[data-testid="stForm"]{
        background: Lavender;
        }
        </style>
        """
        st.write(css,unsafe_allow_html=True)
            
        if st.session_state.comp and st.session_state.conn:

            df_dbq = pd.read_excel(st.session_state.F2O_File, header=None,sheet_name='File_Oracle',skiprows=2)
            df_dbq.columns = ['TestName','File_Location','File_Name','Target_Query','Target_Primarykey','Source_Count','Target_Count','Count_Check','Comparison_Result']
            placeholder.empty()
            placeholder1.empty()
            
            if len(df_dbq) == 1:
                
                global path
                path = df_dbq.at[0,"File_Location"]
                Query=df_dbq.at[0,"Target_Query"]
                PrimaryKey=df_dbq.at[0,"Target_Primarykey"]
                start_time = datetime.now()
                file_name = df_dbq.at[0,"File_Name"]
                src_file = str(os.path.join(path,file_name))
                df1= pd.read_csv(src_file,keep_default_na = False,engine='python',encoding='latin-1')  #, delimiter='[,|]'
                df2 = pd.read_sql(Query, con=st.session_state.conn)
                df2.fillna('',inplace = True)
                DataFrame_Summary(df1,df2)
                COMPARE_DB(df1,df2,PrimaryKey,start_time) 


            elif len(df_dbq) >= 1:
                start_time = datetime.now()
                results = COMPARE_MQDB(st.session_state.conn,df_dbq)
                df3 = pd.DataFrame(results)
                st.write(df3.style.hide_index()
                    .applymap(color_pass_fail, subset=["Count_Check","Comparison_Result"])
                    .set_table_styles([
                            {'selector': 'th', 'props': [('background-color', '#f7ffff'),    #00008B - Dark blue,  f7ffff - light blue
                         ('color', 'black'),
                         ('font-weight', 'bold')]},
                            {'selector': 'td', 'props': [('background-color', '#E1F5FE'),
                         ('font-size', '15px')]}
                            ]).render(), unsafe_allow_html=True)
                st.write('')
                st.download_button( label="Download Summary Report", data=df3.to_csv().encode('utf-8'), file_name='Comparison_Summary.csv',mime='text/csv',key = "F2O_download")
                st.write("Comparison Reports are saved under **Output** folder in location: **{}**".format(df_dbq.at[0,"File_Location"]),markdown=True)
                TIME(start_time)


            else:
                st.error("Database connection issues. Please try again.")
                                     
            DB_CLOSE()
            st.runtime.legacy_caching.clear_cache()
                
        elif st.session_state.comp and not st.session_state.conn:
            st.error('Database Connection Failed!')
            

    except(AssertionError, UnboundLocalError, NameError, RuntimeError, ValueError,TypeError) as e:
        st.exception(e)
        if st.session_state.conn:
            DB_CLOSE()
        st.runtime.legacy_caching.clear_cache()
        

    finally:
        #Connection.close()
        pass   


