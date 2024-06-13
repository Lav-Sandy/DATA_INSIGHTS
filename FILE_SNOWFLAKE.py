import numpy as np
import pandas as pd
import streamlit as st
import cx_Oracle
import datacompy
import os
import subprocess
from datetime import datetime
import xlrd
import xlwt
import csv
from COMPARE import *
#from Connection import *
from Get_Credentials import * 


#streamlit run C:\Users\i726386\Desktop\Streamlit\snow.py

def FILE_SNOW():

    st.markdown("""
    <style>
           .css-k1ih3n {
                padding-top: 2rem;
    </style>
    """, unsafe_allow_html=True)

    global df, df1,df2,Output,Connection
     
    placeholder = st.empty()

    with placeholder.container():
        st.markdown("<h1 style = 'text-align:center; color: Black;font-size:40px;'>FILE TO SNOWFLAKE DATA COMPARE</h1>", unsafe_allow_html = True)
        st.markdown('<hr style = "border-top: 3px solid orange;">',unsafe_allow_html = True)
        st.markdown (""" **_The framework supports comparison of Snowflake database with multiple file types (.csv, .txt, .xls) and delimiters. <br>
                    <br>
                 👉 Note: This utility performs comparisons based on common columns, hence the header-row or column names are mandatory in source file._**<br>
                 """,True)
        st.info ("Please Connect to database first and then upload PARAM file")    

    try:
       
        placeholder1 = st.empty()
        with placeholder1.container():

            with st.form(key='Form1',clear_on_submit=True):              #,clear_on_submit=True
                
                st.markdown("<h1 style = 'text-align:center; color: Black;font-size:20px;'>DATABASE_DETAILS</h1>", unsafe_allow_html = True)
                page_names_to_funcs = {
                    "IMD_INTEG": SNOWFLAKE_IMD_INTEG,
                    "IMD_ACCP": SNOWFLAKE_IMD_ACCP,
                   }   
                selected_db = st.sidebar.selectbox("Select SNOWFLAKE instance from below list", page_names_to_funcs.keys())
                st.text_input("Enter User Name", type="password", key = "SFUser")
                st.text_input("Enter Password", type="password", key = "Snflpass")
             
                def DB_CONNECT():
                    if selected_db:    
                        SFAccount,SFDatabase,SFSchema,SFWarehouse,SFRole = page_names_to_funcs[selected_db]()
                    if 'conn' not in st.session_state:
                        st.session_state.conn  = connect_to_snowflake(SFAccount,st.session_state.SFUser,st.session_state.Snflpass,SFRole,SFWarehouse,SFDatabase); 
                    if st.session_state.conn:
                        st.sidebar.success('Connection Successful', icon="✅")
                    else:
                        st.sidebar.error('Connection Failed!')  

                submitted=st.form_submit_button("Connect",on_click=DB_CONNECT)
                
        st.sidebar.file_uploader("Upload parameter file", type=["xlsx"],key = "F2S_File")
        cmp = st.sidebar.button('Compare',key = "comp") 
        
        css = """
        <style>[data-testid="stForm"]{
        background: Lavender;
        }
        </style>
        """
        st.write(css,unsafe_allow_html=True)
         
        def DB_CLOSE():
            if st.session_state.conn:
                st.session_state.conn.close()
                del st.session_state.conn
            st.info('Database Connection Closed.')
        
        if st.session_state.comp and st.session_state.conn:
            
            df_dbq = pd.read_excel(st.session_state.F2S_File,header=None, sheet_name='File_Snow',skiprows=2)     
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
                df1=df1.applymap(str)
                df2=df2.applymap(str)
                DataFrame_Summary(df1,df2)
                COMPARE_DB(df1,df2,PrimaryKey,start_time)               
                # st.session_state.conn.close()
                # st.info("Database Connection Closed.")
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
                st.download_button( label="Download Summary Report", data=df3.to_csv().encode('utf-8'), file_name='Comparison_Summary.csv',mime='text/csv',key = "F2S_download")
                st.write("Comparison Reports are saved under **Output** folder in location: **{}**".format(df_dbq.at[0,"File_Location"]),markdown=True)
                TIME(start_time)
                # st.session_state.conn.close()
                # st.info("Database Connection Closed.")
                    
            else:
                st.error("Database Connection Failed!")
                
            DB_CLOSE()
            st.runtime.legacy_caching.clear_cache()
            
    except(AssertionError, UnboundLocalError, NameError, RuntimeError, ValueError,TypeError) as e:
        st.exception(e)
        if st.session_state.conn:
            DB_CLOSE()
        st.runtime.legacy_caching.clear_cache()
        #st.info('*Awaiting User inputs*')

    finally:
        pass     

