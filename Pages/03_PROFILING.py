import pandas as pd
import streamlit as st
import numpy as np
import csv
import re
import xlrd
import xlwt
import os
import warnings
from datetime import datetime
from streamlit_extras.app_logo import add_logo
from streamlit_extras.dataframe_explorer import dataframe_explorer
from PIL import Image
from FOOTER import *
from COMPARE import *
import pandas_profiling
#from pandas_profiling import ProfileReport
from streamlit_pandas_profiling import st_profile_report
from pydantic_settings import BaseSettings



def add_logo1(logo_path, width, height):
    """Read and return a resized logo"""
    logo = Image.open(logo_path)
    modified_logo = logo.resize((width, height))
    return modified_logo
    
def headerlayout():
    st.markdown("""
    <style>
           .css-z5fcl4 {
                padding-top: 2rem;
    </style>
    """, unsafe_allow_html=True)
    
    my_logo2 = add_logo1(logo_path=r"C:\Users\91897\OneDrive\Documents\Profiling.jpg", width=150, height=150)
    col1, col2, col3 = st.columns(3)
    with col1:
        st.write(" ")
    with col2:
        st.write(" ")
    with col3:
        st.image(my_logo2)

# Web App Title
    st.markdown("<h1 style = 'text-align:center; color: Black;font-size:50px;'>DATA PROFILING UTILITY</h1>", unsafe_allow_html = True)
    st.markdown (""" **_Unveiling the Mysteries of your Data - Profiling for Deeper Insights. <br>
    This utility provides a summarized view on the overall quality of the data in a dataset._** <br>
    <br>
    """,True)
    

#remove the padding between components
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



global df,pr ,data,delimeter ,selected_columns
df = 'none'

def profile(df,selected_columns):
    summary_stats =[]
    total_records = len(df)
    for column in df.columns:
        temp_column = df[column]
        
        total_zeros = (temp_column.apply(lambda x: str(x).strip()) == '0').sum()
        percentage_zeros = (total_zeros/total_records)*100
        
        #total_nulls = df[column].isnull().sum()
        #total_nulls = ((temp_column == '') | (temp_column == 'nan')).sum()
        total_nulls = ((temp_column == '') | (temp_column == 'nan') | (temp_column.isnull())).sum()
        
        percentage_nulls = (total_nulls/total_records)*100
        
        unique_values = df[column].nunique()
        duplicates = df[column].duplicated().sum()
        
        if column in selected_columns:
            value_counts = df[column].value_counts()
            #unique_values_list = ', '.join(df[column].dropna().astype(str).unique())
            unique_values_list = ', '.join([f"{value}({count})" for value, count in value_counts.items()])
        else:
            unique_values_list = ''
            #value_counts = df
            
        
        summary_stats.append({
                'COLUMN_NAME':column,
                'RECORD_COUNT': total_records,
                'ZERO_VALUES':total_zeros,
                '% OF ZERO_VALUES':f'{percentage_zeros:.2f}%',
                'NULL_VALUES':total_nulls,
                '% OF NULL_VALUES':f'{percentage_nulls:.2f}%',
                'UNIQUE_VALUES':unique_values,
                'DUPLICATE_VALUES':duplicates,
                'VALID_VALUES(Total No of occurances)':unique_values_list
            })
                
    return pd.DataFrame(summary_stats)           
    
def color_negative(val):
    try:
        numeric_val = float(val)
        condition = numeric_val > 0
    except ValueError:
        condition = False
    font_color = 'red' if condition else 'black'
    font_weight = 'bold' if condition else None
    return 'color: {}; font-weight: {}'.format(font_color, font_weight)
    
def color_positive(val):
    condition = (val == 100.000000)
    font_color = 'green' if condition else 'black'
    font_weight = 'bold' if condition else None
    return 'color: {}; font-weight: {}'.format(font_color, font_weight)
    
def DQ_REPORT(pr,df):
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
    start_time = datetime.now()
    #pr = profile(df,st.session_state.PKey)
    
    numOfRows = df.shape[0]
    numOfCols = df.shape[1]  
    
    st.markdown("<h1 style = 'text-align:center; color: Black;font-size:40px;'>PROFILING REPORT</h1>", unsafe_allow_html = True)
    st.markdown('<hr style = "border-top: 3px solid orange;">',unsafe_allow_html = True)
    st.header('**Data Summary**')
    st.write('Total Number of **Rows** in the dataset is    : ' , numOfRows)
    st.write('Total Number of **Columns** in the dataset is : ' , numOfCols)
    st.markdown('<hr style = "border-top: 3px solid orange;">',unsafe_allow_html = True)
   
    st.header('**REPORT**')
    st.markdown("_You can expand the below table for a better view. Please click on DOWNLOAD button to save the report in csv._")
    st.dataframe(pr.style
    .applymap(color_negative, subset=["ZERO_VALUES","% OF ZERO_VALUES","NULL_VALUES", "% OF NULL_VALUES"])  #color = 'RED'
    .set_table_styles([
    {'selector': 'th', 'props': [('background-color', '#f7ffff'),    #00008B - Dark blue,  f7ffff - light blue
                                 ('color', 'black'),
                                 ('font-weight', 'bold')]},
    {'selector': 'td', 'props': [('background-color', '#E1F5FE'),
                                 ('font-size', '15px')]}
        ])
    ,use_container_width=True)
    st.write('')
    csv_data = pr.to_csv(index=False,quoting = csv.QUOTE_ALL)       #, line_terminator='\r\n'
    st.download_button( label="Download Report",
        data=io.BytesIO(csv_data.encode('utf-8')),
        file_name='Profiling_Report.csv',
        mime='text/csv',            
        key = "download"
    )
    TIME(start_time)
    st.runtime.legacy_caching.clear_cache()

def DQ_MULFILES(df_dbq):
    
    st.markdown("<h1 style = 'text-align:center; color: Black;font-size:40px;'>EXECUTION SUMMARY REPORT</h1>", unsafe_allow_html = True)
    st.markdown('<hr style = "border-top: 3px solid orange;">',unsafe_allow_html = True)
    st.write('')
            
    results = []
    for index,row in df_dbq.iterrows():
            
        placeholder1 = st.empty()
        with placeholder1.container():
            progress_bar = st.progress(0)
            time.sleep(0.1)
            progress_bar.progress((index+1)/len(df_dbq))
            st.write("Execution In-Progress! Currently running row",index+1,"out of",len(df_dbq))
        
        
        Query=row["Query"]
        PrimaryKey=row["Key_Column"]
        file_name = row["Table_Name"]
        location = str(row["File_Location"])
        PK = PrimaryKey.split(",")

        df = pd.read_sql(Query, con=st.session_state.conn)
        df_rows=df.shape[0]
        df_cols=df.shape[1]
        #placeholder.empty()
        pr = profile(df,PK)

        output_dir = os.path.join(location,'Output')
        if not os.path.exists(output_dir):
            os.mkdir(output_dir)
            
        testName = file_name + '.csv'
        file1 = os.path.join(output_dir,testName)
        
    
        csv_data = pr.to_csv(file1,index=False,quoting = csv.QUOTE_ALL, line_terminator='\r\n')
                
        results.append({'Table_Name': file_name,'Row_Count':df_rows,'Column_Count':df_cols,'Status':'Success'})
        placeholder1.empty()

    return results


def read_file2(input_file):
        
        if input_file:
                    if input_file.name[-3:] == 'csv':
                        df = pd.read_csv(input_file, delimiter=',',keep_default_na = False,engine='python')
                    elif input_file.name[-3:] == 'txt':
                        df = pd.read_csv(input_file, delimiter=delimeter,keep_default_na = False,engine='python')
                    else:
                        df = pd.read_excel(input_file)
                    return df

def PANDAS_PROFILING():

    st.markdown("""
    <style>
           .css-z5fcl4 {
                padding-top: 2rem;
    </style>
    """, unsafe_allow_html=True)
    
    global delimeter  
    file_extension = 'none'
    delimeter = 'none'
    
    placeholder = st.empty()

    with placeholder.container():
        st.markdown("<h1 style = 'text-align:center; color: Black;font-size:40px;'>PANDAS_PROFILING</h1>", unsafe_allow_html = True)
        st.markdown('<hr style = "border-top: 3px solid orange;">',unsafe_allow_html = True)
        st.markdown (""" **_Pandas Profiling” – one-stop solution for generating reports out of the pandas dataframe. <br>
                    <br>
                 👉 Note: This is an inbuilt library from Pandas and hence the not much modifications can be done with regards to output report._**<br>
                                                  <br>
                    
                """,True)

    try:
        with st.sidebar.form(key='MyForm'):
            file = st.file_uploader("Upload your input file", type=["csv"],key = "PFile")

            df = read_file2(st.session_state.PFile)
        
            submitted = st.form_submit_button("Submit")       #,on_click=DQ_REPORT
            
        if submitted:
            placeholder.empty()
            pr = df.profile_report()

            st_profile_report(pr)
            
    except (NameError,ValueError,AttributeError,RuntimeError) as e:
        #st.exception(e)
        st.info('*Awaiting user input*')
        st.runtime.legacy_caching.clear_cache()
        
    finally:
        pass   
        
    #df = pd.read_csv("https://storage.googleapis.com/tf-datasets/titanic/train.csv")

        
def DB_CLOSE():
    if st.session_state.conn:
        st.session_state.conn.close()
        del st.session_state.conn
    st.info('Database Connection Closed.')

def file_profile():
    
    st.markdown("""
    <style>
           .css-z5fcl4 {
                padding-top: 2rem;
    </style>
    """, unsafe_allow_html=True)
    
    global delimeter  
    file_extension = 'none'
    delimeter = 'none'
    
    placeholder = st.empty()

    with placeholder.container():
        st.markdown("<h1 style = 'text-align:center; color: Black;font-size:40px;'>PROFILE - FLAT FILES</h1>", unsafe_allow_html = True)
        st.markdown('<hr style = "border-top: 3px solid orange;">',unsafe_allow_html = True)
        st.markdown (""" **_This script profiles multiple file types (.csv, .txt, .xls) and delimiters. <br>
                    <br>
                 👉 Note: Header row or Column names are mandatory in flat file to derive descriptive measures._**<br>
                                                  <br>
                    
                """,True)
    
    try:
        with st.sidebar.form(key='MyForm'):
            file = st.sidebar.file_uploader("Upload your input file", type=["csv","xlsx","txt"],key = "File")

            if st.session_state.File:
                file_extension = file.name.split('.')[-1]
                if file_extension == 'txt':
                    delimeter = st.sidebar.text_input('Provide the File Delimeter and Press Enter', type="default")

            df = read_file2(st.session_state.File)
            df=df.applymap(str)
            # numOfRows = df.shape[0]
            # numOfCols = df.shape[1]  
            all_columns = df.columns.tolist()
            selected_columns = st.multiselect('Choose Column/s to fetch unique list of values',all_columns,key='PKey')
        
            submitted=st.form_submit_button("Submit")       #,on_click=DQ_REPORT
        if submitted:
            placeholder.empty()
            pr = profile(df,st.session_state.PKey)
            DQ_REPORT(pr,df)
            
    except (NameError,ValueError,AttributeError,RuntimeError) as e:
        #st.exception(e)
        st.info('*Awaiting user input*')
        st.runtime.legacy_caching.clear_cache()
        
    finally:
        pass   
        

        
def DEFAULT_VALUE():
    DEFAULT = '< PICK A VALUE >'
    return DEFAULT 
    
page_names_to_funcs = {
    "Select" : DEFAULT_VALUE,
    "BASIC_INSIGHTS": file_profile,
    "PANDAS_PROFILING": PANDAS_PROFILING,

    }         
    
selected_page = st.sidebar.selectbox("Choose one from the below list", page_names_to_funcs.keys())
if selected_page == "Select":    
    headerlayout()
else:
    page_names_to_funcs[selected_page]()  
    
footer()
