import streamlit as st
import numpy as np
import pandas as pd
import cx_Oracle
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from cx_Oracle import DatabaseError
from COMPARE import *
#from Connection import *
from Get_Credentials import *


def ORACLE_ORACLE():

    st.markdown("""
    <style>
           .css-k1ih3n {
                padding-top: 2rem;
    </style>
    """, unsafe_allow_html=True)
       
    global df1,df2,orc1ServiceName,orc1Host,orc1Port,orc1UserName,orc2Host,orc2Port,orc2ServiceName,orc2UserName
    
    placeholder = st.empty()

    with placeholder.container():
        st.markdown("<h1 style = 'text-align:center; color: Black;font-size:40px;'>ORACLE TO ORACLE DATABASE COMPARE</h1>", unsafe_allow_html = True)
        st.markdown('<hr style = "border-top: 3px solid orange;">',unsafe_allow_html = True)
        st.markdown (""" **_This framework supports comparison of datasets in Oracle database across various test regions (INTEG and ACCP)._** <br>
                        <br>
                             """,True)
        st.info ("Please Connect to database first and then upload PARAM file")         

    try:
        placeholder1 = st.empty()
        page_names_to_funcs = {
            "CODS_INTEG": CODS_INTG,
            "CODS_ACCP": CODS_ACCP,
            "CODS_PROD": CODS_PROD
                } 
        with placeholder1.container():

            with st.form(key='Form1',clear_on_submit=True):              #,clear_on_submit=True
                st.markdown("<h1 style = 'text-align:center; color: Black;font-size:20px;'>DATABASE_DETAILS</h1>", unsafe_allow_html = True)
                                
                col1, col2 = st.columns(2)
                with col1:
                    selected_db = st.sidebar.selectbox("Select SOURCE DB instance from below list", page_names_to_funcs.keys())
                    st.text_input("Enter Source-DB User Name", type="password", key = "orc1UserName")
                    st.text_input("Enter Source-DB Password", type="password", key = "sourcepassword")
                with col2:
                    selected_db1 = st.sidebar.selectbox("Select TARGET DB instance from below list", page_names_to_funcs.keys())
                    st.text_input("Enter Target-DB User Name", type="password", key = "orc2UserName")
                    st.text_input("Enter Target-DB Password", type="password", key = "targetpassword")
                
                def DB_CONNECT():
                
                        if selected_db:    
                            orc1ServiceName,orc1Host,orc1Port = page_names_to_funcs[selected_db]() 
                            
                        if selected_db1:    
                            orc2ServiceName,orc2Host,orc2Port = page_names_to_funcs[selected_db1]()
                            
                        if 'conn1' not in st.session_state:
                            st.session_state.conn1  = connect_oracle(orc1Host,orc1Port,orc1ServiceName,st.session_state.orc1UserName,st.session_state.sourcepassword);
                        if 'conn2' not in st.session_state:
                            st.session_state.conn2  = connect_oracle(orc2Host,orc2Port,orc2ServiceName,st.session_state.orc2UserName,st.session_state.targetpassword);
                        if st.session_state.conn1 and not st.session_state.conn2:
                            st.error("Target Connection Failed! Please provide correct username and password")
                        elif st.session_state.conn2 and not st.session_state.conn1:
                            st.error("Source Connection Failed! Please provide correct username and password")
                        elif st.session_state.conn1 and st.session_state.conn2:
                            st.sidebar.success('Connection Successful', icon="✅")
                        else:
                            st.sidebar.error('Connection Failed!')
                        
       
                submitted=st.form_submit_button("Connect",on_click=DB_CONNECT)
                
        st.sidebar.file_uploader("Upload parameter file", type=["xlsx"],key = "O2O_File")
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
            df_dbq = pd.read_excel(st.session_state.O2O_File,header=None,sheet_name='Oracle_Oracle',skiprows=2)
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
                placeholder.empty()
                DataFrame_Summary(df1,df2)
                COMPARE_DB(df1,df2,SQL1PK,start_time)
                
            elif len(df_dbq) >= 1:
                placeholder.empty()
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
                st.download_button( label="Download Summary Report", data=df3.to_csv().encode('utf-8'), file_name='Comparison_Summary.csv',mime='text/csv',key = "O2O_download")
                st.write("Comparison Reports are saved under **Output** folder in location: **{}**".format(df_dbq.at[0,"File_Location"]),markdown=True)
                TIME(start_time)

            else:
                st.error('Connection Failed!')
                
            DB_CLOSE()
            st.runtime.legacy_caching.clear_cache()

                        
    except (AssertionError, UnboundLocalError, NameError, RuntimeError, TypeError, ValueError) as e:
        st.exception(e)
        if st.session_state.conn1 and st.session_state.conn2:
            DB_CLOSE()
        st.runtime.legacy_caching.clear_cache()
            

    finally:
        pass
        
