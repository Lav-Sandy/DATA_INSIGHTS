import numpy as np
import pandas as pd
import streamlit as st
import datacompy
import os
from datetime import datetime
import cx_Oracle
import subprocess
import snowflake.connector as sf
from cx_Oracle import DatabaseError
from COMPARE import *
#from Connection import *
from Get_Credentials import *



def ORACLE_SNOWFLAKE():

    st.markdown("""
    <style>
           .css-k1ih3n {
                padding-top: 2rem;
    </style>
    """, unsafe_allow_html=True)
    
    
    global df1, df2, orc1ServiceName, orc1Host,orc1Port,orc1UserName,SFAccount,SFUser,SFDatabase,SFSchema,SFWarehouse,SFRole
    
    placeholder = st.empty()

    with placeholder.container():
        st.markdown("<h1 style = 'text-align:center; color: Black;font-size:40px;'>ORACLE TO SNOWFLAKE DATA COMPARE</h1>", unsafe_allow_html = True)
        st.markdown('<hr style = "border-top: 3px solid orange;">',unsafe_allow_html = True)
        st.markdown (""" **_This framework supports comparison of datasets from ORACLE TO SNOWFLAKE DATABASE._** <br>
                        <br>
                 """,True)
        st.info ("Please Connect to database first and then upload PARAM file")         

    try:
        
        placeholder1 = st.empty()
        page_names_to_funcs1 = {
            "CODS_INTEG": CODS_INTG,
            "CODS_ACCP": CODS_ACCP,}
        page_names_to_funcs2 = {
            "IMD_INTEG": SNOWFLAKE_IMD_INTEG,
            "IMD_ACCP": SNOWFLAKE_IMD_ACCP,
                } 
        with placeholder1.container():

            with st.form(key='Form1',clear_on_submit=True):              #,clear_on_submit=True
                st.markdown("<h1 style = 'text-align:center; color: Black;font-size:20px;'>DATABASE_DETAILS</h1>", unsafe_allow_html = True)
                                
                col1, col2 = st.columns(2)
                with col1:
                    selected_db = st.sidebar.selectbox("Select ORACLE DB instance from below list", page_names_to_funcs1.keys())
                    st.sidebar.write(selected_db)
                    st.text_input("Enter ORACLE-DB User Name", type="password", key = "orc1UserName")
                    st.text_input("Enter ORACLE-DB Password", type="password", key = "O2Ssourcepassword")
                with col2:
                    selected_db1 = st.sidebar.selectbox("Select SNOWFLAKE DB instance from below list", page_names_to_funcs2.keys())
                    st.sidebar.write(selected_db1)
                    st.text_input("Enter SNOWFLAKE-DB User Name", type="password", key = "SFUser")
                    st.text_input("Enter SNOWFLAKE-DB Password", type="password", key = "Snflpass")

                
                def DB_CONNECT():
                
                        if selected_db:    
                            orc1ServiceName,orc1Host,orc1Port = page_names_to_funcs1[selected_db]() 
                            
                        if selected_db1:    
                            SFAccount,SFDatabase,SFSchema,SFWarehouse,SFRole = page_names_to_funcs2[selected_db1]()
                                      
                        if 'conn1' not in st.session_state:
                            st.session_state.conn1  = connect_oracle(orc1Host,orc1Port,orc1ServiceName,st.session_state.orc1UserName,st.session_state.O2Ssourcepassword);
                        if 'conn2' not in st.session_state:
                            st.session_state.conn2  = connect_to_snowflake(SFAccount,st.session_state.SFUser,st.session_state.Snflpass,SFRole,SFWarehouse,SFDatabase); 
                        if st.session_state.conn1 and st.session_state.conn2:
                            st.sidebar.success('Connection Successful', icon="✅")
                        else:
                            st.sidebar.error('Connection Failed!')
                    
        
                submitted=st.form_submit_button("Connect",on_click=DB_CONNECT)
        
                
        st.sidebar.file_uploader("Upload parameter file", type=["xlsx"],key = "O2S_File")
        cmp = st.sidebar.button('Compare',key = "comp")
        
        css = """
        <style>[data-testid="stForm"]{
        background: Lavender;
        }
        </style>
        """
        st.write(css,unsafe_allow_html=True)
        
        def DB_CLOSE():
            
            if st.session_state.conn1:
                st.session_state.conn1.close()
                del st.session_state.conn1
            if st.session_state.conn2:
                st.session_state.conn2.close()
                del st.session_state.conn2
            st.info('Database Connection Closed.')


        if st.session_state.comp and st.session_state.conn1 and st.session_state.conn2:
            df_dbq = pd.read_excel(st.session_state.O2S_File,header=None, sheet_name='Oracle_Snowflake',skiprows=2)
            df_dbq.columns = ['TestName','File_Location','Table_Name','PrimaryKey','Source_Query','Target_Query','Source_Count','Target_Count','Count_Check','Comparison_Result']
            placeholder.empty()
            placeholder1.empty()
            
            if len(df_dbq) == 1:
                start_time = datetime.now()
                stageQuery1 = df_dbq.at[0, "Source_Query"]
                stageQuery2 = str(df_dbq.at[0, "Target_Query"])
                tcName = df_dbq.at[0, "TestName"]
                SQL1PK = str(df_dbq.at[0, "PrimaryKey"])
                #SQL2PK = str(df_dbq.at[0, "Target_Primarykey"])              
                df1 = pd.read_sql(stageQuery1, con=st.session_state.conn1)
                df2 = pd.read_sql(stageQuery2, con=st.session_state.conn2)
                DataFrame_Summary(df1,df2)
                COMPARE_DB(df1,df2,SQL1PK,start_time)

                
            elif len(df_dbq) >= 1:

                start_time = datetime.now()
                results = COMPARE_MQDB_DB(st.session_state.conn1,st.session_state.conn2,df_dbq)
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
                st.download_button( label="Download Summary Report", data=df3.to_csv().encode('utf-8'), file_name='Comparison_Summary.csv',mime='text/csv',key = "O2S_download")
                st.write("Comparison Reports are saved under **Output** folder in location: **{}**".format(df_dbq.at[0,"File_Location"]),markdown=True)
                TIME(start_time)
                
            else:
                st.error('Connection Failed!')

            DB_CLOSE()
            st.runtime.legacy_caching.clear_cache()
            
    except (AssertionError, UnboundLocalError, NameError, RuntimeError, TypeError, ValueError) as e:
        #pass
        st.exception(e)
        if st.session_state.conn1 and st.session_state.conn2:
            DB_CLOSE()
        st.runtime.legacy_caching.clear_cache()
      

    finally:
        pass
            #closing database connection.
        
