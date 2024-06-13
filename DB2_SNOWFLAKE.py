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
from Connection import *
from Get_Credentials import *



def DB2_SNOWFLAKE():

    st.markdown("""
    <style>
           .css-k1ih3n {
                padding-top: 2rem;
    </style>
    """, unsafe_allow_html=True)
    
    
    global df1, df2, orc1ServiceName, orc1Host,orc1Port,orc1UserName,SFAccount,SFUser,SFDatabase,SFSchema,SFWarehouse,SFRole
    
    placeholder = st.empty()

    with placeholder.container():
        st.markdown("<h1 style = 'text-align:center; color: Black;font-size:40px;'>DB2 TO SNOWFLAKE DATA COMPARE</h1>", unsafe_allow_html = True)
        st.markdown('<hr style = "border-top: 3px solid orange;">',unsafe_allow_html = True)
        st.markdown (""" **_This framework supports comparison of datasets from DB2 TO SNOWFLAKE DATABASE._** <br>
                        <br>
                 """,True)
        st.info ("Please Connect to database first and then upload PARAM file")         

    try:
        
        placeholder1 = st.empty()
        page_names_to_funcs1 = {
            "OM_DB2_INTEG": OM_DB2_INTEG,
            "OM_DB2_ACCP": OM_DB2_ACCP,}
        page_names_to_funcs2 = {
            "IMD_INTEG": SNOWFLAKE_IMD_INTEG,
            "IMD_ACCP": SNOWFLAKE_IMD_ACCP,
            "OM_ACCP": SNOWFLAKE_ORANGEMART_ACCP,
                } 
        with placeholder1.container():

            with st.form(key='Form1',clear_on_submit=True):              #,clear_on_submit=True
                st.markdown("<h1 style = 'text-align:center; color: Black;font-size:20px;'>DATABASE_DETAILS</h1>", unsafe_allow_html = True)
                                
                col1, col2 = st.columns(2)
                with col1:
                    selected_db = st.sidebar.selectbox("Select DB2 instance from below list", page_names_to_funcs1.keys())
                    #st.sidebar.write(selected_db)
                    st.text_input("Enter DB2 User Name", type="password", key = "DB2UserName")
                    st.text_input("Enter DB2 Password", type="password", key = "DB2sourcepassword")
                with col2:
                    selected_db1 = st.sidebar.selectbox("Select SNOWFLAKE DB instance from below list", page_names_to_funcs2.keys())
                    #st.sidebar.write(selected_db1)
                    st.text_input("Enter SNOWFLAKE-DB User Name", type="password", key = "SFUser")
                    st.text_input("Enter SNOWFLAKE-DB Password", type="password", key = "Snflpass")

                
                def DB_CONNECT():
                
                        if selected_db:    
                            DATABASE,HOSTNAME,PORT,PROTOCOL = page_names_to_funcs1[selected_db]() 
                            
                        if selected_db1:    
                            SFAccount,SFDatabase,SFSchema,SFWarehouse,SFRole = page_names_to_funcs2[selected_db1]()
                                      
                        if 'conn1' not in st.session_state:
                            st.session_state.conn1  = connect_db2(DATABASE,HOSTNAME,PORT,PROTOCOL,st.session_state.DB2UserName,st.session_state.DB2sourcepassword);
                        if 'conn2' not in st.session_state:
                            st.session_state.conn2  = connect_to_snowflake(SFAccount,st.session_state.SFUser,st.session_state.Snflpass,SFRole,SFWarehouse,SFDatabase); 
                        if st.session_state.conn1 and st.session_state.conn2:
                            st.sidebar.success('Connection Successful', icon="✅")
                        else:
                            st.sidebar.error('Connection Failed!')
                    
        
                submitted=st.form_submit_button("Connect",on_click=DB_CONNECT)
        
                
        st.sidebar.file_uploader("Upload parameter file", type=["xlsx"],key = "DB2_File")
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
            df_dbq = pd.read_excel(st.session_state.DB2_File,header=0, sheet_name='DB2_Snowflake')
            
            placeholder.empty()
            placeholder1.empty()
            
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
            DB_CLOSE()
            st.runtime.legacy_caching.clear_cache()
            
        else:
            pass

            
    except (AssertionError, UnboundLocalError, NameError, RuntimeError, TypeError, ValueError) as e:
        #pass
        st.exception(e)
        if st.session_state.conn1 and st.session_state.conn2:
            DB_CLOSE()
        st.runtime.legacy_caching.clear_cache()
      

    finally:
        pass
            #closing database connection.
