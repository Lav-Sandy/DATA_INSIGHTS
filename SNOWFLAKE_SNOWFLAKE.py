import pandas as pd
import cx_Oracle
import datacompy
#from sqlalchemy import create_engine
#from snowflake.sqlalchemy import URL
import snowflake.connector as sf
import numpy as np
import os
import subprocess
from datetime import datetime
import xlrd
import xlwt
from COMPARE import *
#from Connection import *
from Get_Credentials import * 

def SNOWFLAKE_SNOWFLAKE():
    st.markdown("""
    <style>
           .css-k1ih3n {
                padding-top: 2rem;
    </style>
    """, unsafe_allow_html=True)
    
    global df1, df2
    placeholder = st.empty()

    with placeholder.container():
        st.markdown("<h1 style = 'text-align:center; color: Black;font-size:40px;'>SNOWFLAKE TO SNOWFLAKE DATABASE COMPARE</h1>", unsafe_allow_html = True)
        st.markdown('<hr style = "border-top: 3px solid orange;">',unsafe_allow_html = True)
        st.markdown (""" **_This framework supports comparison of datasets in SNOWFLAKE DWH across various test regions (INTEG and ACCP)._** <br>
                        <br>
 
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
                    "OM_PROD": SNOWFLAKE_OM_PROD,
                    "OM_CLOUD_PROD": SNOWFLAKE_OM_CLOUD_PROD,
                   }  

                                
                col1, col2 = st.columns(2)
                with col1:
                    selected_db = st.sidebar.selectbox("Select SOURCE DB instance from below list", page_names_to_funcs.keys())
                    st.text_input("Enter SOURCE-DB User Name", type="password", key = "SFUser")
                    st.text_input("Enter SOURCE-DB Password", type="password", key = "sourcepassword")
                with col2:
                    selected_db1 = st.sidebar.selectbox("Select TARGET DB instance from below list", page_names_to_funcs.keys())
                    st.text_input("Enter TARGET-DB User Name", type="password", key = "SFUserT")
                    st.text_input("Enter TARGET-DB Password", type="password", key = "targetpassword")
           
            
                def DB_CONNECTSF():
                    
                        if selected_db:    
                            SFAccount,SFDBSource,SFschemaSource,SFwarehouseSource,SFRoleSource = page_names_to_funcs[selected_db]()
                            
                        if selected_db1:    
                            SFAccountT,SFDBTarget,SFSchemaTarget,SFwhTarget,SFRoleTarget = page_names_to_funcs[selected_db1]()
                            
                   
                        if 'connection1' not in st.session_state:
                            st.session_state.connection1=sf.connect(account=SFAccount ,user=st.session_state.SFUser,password=st.session_state.sourcepassword,role=SFRoleSource,warehouse=SFwarehouseSource,database=SFDBSource, schema = SFschemaSource)
                        if 'connection2' not in st.session_state:
                            st.session_state.connection2=sf.connect(account=SFAccountT ,user=st.session_state.SFUserT,password=st.session_state.targetpassword,role=SFRoleTarget,warehouse=SFwhTarget,database=SFDBTarget, schema = SFSchemaTarget)
                        if st.session_state.connection1 and st.session_state.connection2:
                            st.sidebar.success('Connection Successful', icon="✅")
                        else:
                            st.sidebar.error('Connection Failed!')
                            
           
                submitted=st.form_submit_button("Connect",on_click=DB_CONNECTSF)
            
        st.sidebar.file_uploader("Upload parameter file", type=["xlsx"],key = "S2S_File")
        cmp = st.sidebar.button('Compare',key = "comp")
        
                
        css = """
        <style>[data-testid="stForm"]{
        background: Lavender;
        }
        </style>
        """
        st.write(css,unsafe_allow_html=True)
            
        def DB_CLOSE():
            
            if st.session_state.connection1:
                st.session_state.connection1.close()
                del st.session_state.connection1
            if st.session_state.connection2:
                st.session_state.connection2.close()
                del st.session_state.connection2
            st.info('Database Connection Closed.')

        if st.session_state.comp and st.session_state.connection1 and st.session_state.connection2:
            df_dbq = pd.read_excel(st.session_state.S2S_File,header=0, sheet_name='Snowflake_Snowflake')
            placeholder.empty()
            placeholder1.empty()
             
            if len(df_dbq) == 1:
                start_time = datetime.now()
                stageQuery1 = df_dbq.at[0, "Source_Query"]
                stageQuery2 = str(df_dbq.at[0, "Target_Query"])
                tcName = df_dbq.at[0, "TestName"]
                SQL1PK = str(df_dbq.at[0, "PrimaryKey"])
         
                df1 = pd.read_sql(stageQuery1, con=st.session_state.connection1)
                df2 = pd.read_sql(stageQuery2, con=st.session_state.connection2)
                placeholder.empty()
                DataFrame_Summary(df1,df2)
                COMPARE_DB(df1,df2,SQL1PK,start_time)
            
            elif len(df_dbq) >= 1:            
                placeholder.empty()
                start_time = datetime.now()
                results = COMPARE_SFDB_DB(st.session_state.connection1,st.session_state.connection2,df_dbq)
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

    finally:
        # closing database connection.
            pass
