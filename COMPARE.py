### Compare 
import datacompy
import pandas as pd
from datetime import datetime
import psutil,time
import streamlit as st
import os
import io
import re
from io import StringIO
import zipfile
from zipfile import ZipFile
#from Connection import *
global df1,df2,df3,df4
newList=[]



def read_file(input_file):
    if input_file:
        if input_file.name[-3:] == 'csv':
            df = pd.read_csv(input_file, delimiter=',',keep_default_na = False,engine='python')
        elif input_file.name[-3:] == 'txt':
            text = input_file.getvalue().decode("utf-8")
            delimeter_pattern = r'[,|]'
            lines = re.split(r'\r?\n',text)
            column_names = re.split(delimeter_pattern, lines[0])
            data = [re.split(delimeter_pattern,line)[1:] for line in lines[1:]]
            df= pd.DataFrame(data,columns=column_names)
            #df = pd.read_csv(input_file, sep='[,|]',keep_default_na = False,engine='python')
        else:
            df = pd.read_excel(input_file,header=0, engine = 'openpyxl')
        return df
                    
def read_file1(input_file):
    if input_file:
        if input_file.endswith(".csv"):
            df = pd.read_csv(input_file, header=0,keep_default_na = False,engine = 'python',encoding='latin-1')     #usecols=range(0, 1000)
        elif input_file.endswith(".txt"):
            df = pd.read_csv(input_file,header=0, sep = '[:,|*]', keep_default_na = False, skipfooter=1, engine='python')
        elif input_file.endswith(".xlsx"):
            df = pd.read_excel(input_file,header=0, engine = 'openpyxl')
        return df
                    
def color_pass_fail(val):
    if val == 'Pass':
        return 'font-weight: bold;color:green'
    elif val == 'Fail':
        return 'font-weight:bold;color:red'
    else:
        return ''

def TIME(start_time):
    end_time = datetime.now()
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
    
def Key_List(df1,df2):
    keyslist1 = [] #This will be the list of headers from first file
    keyslist2 = [] #This will be the list of headers from second file
    keyslist = [] #This will be the list of headers that are the intersection between the two files
    formlists = [] #This will be the list to be displayed on the UI
    for header in df1.columns.str.lower():
        if header not in keyslist1:
            keyslist1.append(header)
    for header in df2.columns.str.lower():
        if header not in keyslist2:
            keyslist2.append(header)
    for item in keyslist1:
        if item in keyslist2:
            keyslist.append(item)
    if len(keyslist) == 0:
        st.error('Error: Files have no common headers.')
    return keyslist

def Download_Txt_File(report):
        #adding a download button to download csv file

        dwnld=st.download_button( label="Download Data Compare Report",data=report,file_name='Data_Compare_Report.txt',mime='text/csv',key='mismatch')

# @st.cache             
def DataFrame_Summary(df1,df2):
    st.write('')
    st.markdown("<h1 style = 'text-align:center; color: Black;font-size:40px;'>COMPARE REPORT</h1>", unsafe_allow_html = True)
    st.markdown('<hr style = "border-top: 3px solid orange;">',unsafe_allow_html = True)
    st.header('**Summary**')
    st.markdown("_An Anatomy of Datasets. A Breakdown of Rows and Columns._")
    data = {'Data':['Source','Target'],'Total Number of Rows':[df1.shape[0],df2.shape[0]],'Total Number of Columns':[df1.shape[1],df2.shape[1]]}
    df_summary=pd.DataFrame(data)
    row_mismatch = False
    col_mismatch = False
    for i in range(df_summary.shape[0]):
        for j in range(1,df_summary.shape[1]):
            if df_summary.iloc[i,j] != df_summary.iloc[0,j]:
                if j==1:
                    row_mismatch = True
                else:
                    col_mismatch = True
                df_summary.iloc[i,j] = '<span style = "color:red;font-weight:bold;">'+str(df_summary.iloc[i,j])+'</span>'
    st.write(df_summary.style.set_table_styles([
                    {'selector': 'th', 'props': [('background-color', '#f7ffff'),    #00008B - Dark blue,  f7ffff - light blue
                                     ('color', 'black'),
                                     ('font-weight', 'bold')]},
                    {'selector': 'td', 'props': [('background-color', '#E1F5FE'),('text-align','center'),
                                     ('font-size', '15px')]}]),unsafe_allow_html=True)      #.render()
    if row_mismatch and col_mismatch:
        st.write('')
        st.error("_❌ Number of **ROWS** and **COLUMNS** do not match._")
    elif row_mismatch:
        st.write('')
        st.error("_❌ Number of **ROWS** do not match._")
    elif col_mismatch:
        st.write('')
        st.error("_❌ Number of **COLUMNS** do not match._")
    else:
        st.write('')
        st.success("_✅ Number of **ROWS** and **COLUMNS** match._")
    st.markdown('<hr style = "border-top: 3px solid orange;">',unsafe_allow_html = True)
    
    
def COMPARE_CSV(dff1,dff2,Primary_KEY_LIST,start_time):
    
    st.header('**Data Differences**')
    compare = datacompy.Compare( dff1, dff2, join_columns=Primary_KEY_LIST, df1_name='OLD', df2_name='NEW')
    buffer2 = io.StringIO() # Create a file-like buffer to receive PDF data.
    buffer2.write(compare.report(sample_count=100))  # Write report content to the buffer
    value2 = buffer2.getvalue()  # Get the value of the buffer
    archive = io.BytesIO()
    buffer=io.BytesIO()
    is_match = compare.matches(ignore_extra_columns=True)
    if is_match:
        st.success("**✅ Exact match between source and target files.!! Please download the compare report for further reference.**")
        
        Download_Txt_File(compare.report(sample_count=100))
        # with zipfile.ZipFile(archive, "w") as zf:
            # zf.writestr("Data_Compare_Report.txt", value2)
    else:
        st.markdown("_Sample data mismatches (if any) will be shown below. This section would be rendered empty, if there are no mismatches in the datasets. Please refer to compare report for further reference._")
        #st.error("**_❌ DATA does not match._**")
        
        for x in Primary_KEY_LIST:
            newList.append(x.lower())   
        final_list = [e for e in compare.intersect_columns() if e not in newList]
        
        for col in final_list:
            df3=compare.sample_mismatch(col.lower(), sample_count=300, for_display=True)
            if df3.empty==False:
                st.write(df3.style.set_table_styles([
                            {'selector': 'th', 'props': [('background-color', '#f7ffff'),    #00008B - Dark blue,  f7ffff - light blue
                                             ('color', 'black'),
                                             ('font-weight', 'bold')]},
                            {'selector': 'td', 'props': [('background-color', '#E1F5FE'),
                                             ('font-size', '15px')]}
                        ]), unsafe_allow_html=True) #.render()
            #st.write('')
        Download_Txt_File(compare.report(sample_count=100))
        # with zipfile.ZipFile(archive, "w") as zf:
            # zf.writestr("Data_Compare_Report.txt", value2)
            # with zf.open("Mismatch.xlsx", "w") as buffer:
                # with pd.ExcelWriter(buffer) as writer:
                    # #dff1.to_excel(writer, sheet_name='SOURCE', header=True, index=False)
                    # #dff2.to_excel(writer, sheet_name='TARGET', header=True, index=False)
                    # for col in final_list:
                        # # st.write(col[20])
                        # df3=compare.sample_mismatch(col.lower(), sample_count=30, for_display=True)
                        # df4=compare.sample_mismatch(col.lower(), sample_count=100, for_display=True)

                        # if df3.empty==False and df4.empty==False:
                            # sheet_name = col[0:30].replace('/','_').replace('\\','_').replace(',','_')
                            # st.write(df3.style.set_table_styles([
                                # {'selector': 'th', 'props': [('background-color', '#f7ffff'),    #00008B - Dark blue,  f7ffff - light blue
                                                 # ('color', 'black'),
                                                 # ('font-weight', 'bold')]},
                                # {'selector': 'td', 'props': [('background-color', '#E1F5FE'),
                                                 # ('font-size', '15px')]}
                            # ]).render(), unsafe_allow_html=True)
                            # st.write('')
                            # # ##Saving to single file in buffer
                    
                            # #df4.to_excel(writer, sheet_name=f'{col[0:30]}', header=True, index=False)
                            # df4.to_excel(writer, sheet_name=sheet_name, header=True, index=False)
                            
                    
                    
        
        # Download the archive
        #archive.seek(0)
        st.markdown(""" Click on _Download Reports_ to fetch the detailed comparison report.<br>
                           """ , True)
        #st.download_button(label="Download Reports",data=df3.to_csv().encode('utf-8'), file_name='Comparison_Summary.csv',mime='text/csv',key = "mismatch")
    TIME(start_time)


  

def COMPARE_MUL_FILES(df_dbq):
    st.markdown("<h1 style = 'text-align:center; color: Black;font-size:40px;'>EXECUTION SUMMARY REPORT</h1>", unsafe_allow_html = True)
    st.markdown('<hr style = "border-top: 3px solid orange;">',unsafe_allow_html = True)
    st.write('')

    results = []
    start_time = datetime.now()
  
    for index,row in df_dbq.iterrows():
        
        placeholder = st.empty()
        with placeholder.container():
            progress_bar = st.progress(0)
            time.sleep(0.1)
            progress_bar.progress((index+1)/len(df_dbq))
            st.write("Execution In-Progress! Currently running test case",index+1,"out of",len(df_dbq))
        
        #Reading variables from Parameter file
        path = row["File_Location"]
        PrimaryKey=row["PrimaryKey"]
        s_file_name = row["Source_File_Name"]
        t_file_name = row["Target_File_Name"]
        # user_id = row["USER_ID"]
        # project = row["PROJECT_NAME"]
        # env = row["ENVIRONMENT"]
        # total_tests = row["TOTAL_NUMBER_OF_TC"]
        # src_file = str(os.path.join(path,s_file_name))
        # tgt_file = str(os.path.join(path,t_file_name))
        src_file = os.path.join(path,s_file_name)
        tgt_file = os.path.join(path,t_file_name)
        
        #Creation of dataframes
        df1=read_file1(src_file)
        df2=read_file1(tgt_file)
           

        df1_rows=df1.shape[0]
        df2_rows=df2.shape[0]
        df1=df1.applymap(str)
        df2=df2.applymap(str)
        counts_match = (df1_rows==df2_rows)
        
        location = str(row["File_Location"])
        output_dir = os.path.join(location,'Output')
        if not os.path.exists(output_dir):
            os.mkdir(output_dir)

        letter_list = PrimaryKey.split(",")
        
        newList=[]
        for x in letter_list:
            newList.append(x.lower())
        
        compare = datacompy.Compare( df1, df2, join_columns=newList, df1_name='SOURCE', df2_name='TARGET')

        final_list = [e for e in compare.intersect_columns() if e not in newList]

        is_match = compare.matches(ignore_extra_columns=True)
        
        if is_match:
            
            data1 = compare.report(sample_count=100)
            #df3['Comparison_Result'] = 'Pass'
            Comparison_Result = 'Pass'

        elif is_match == False:

            data1 = compare.report(sample_count=100)
            #df3['Comparison_Result'] = 'Fail'
            Comparison_Result = 'Fail'

        TN = row['TestName']
        FN = row['Source_File_Name']
        CN = 'Pass' if counts_match else 'Fail'
        
            
        #Compare Report naming        
        tcName1 = TN + '.txt'
        testName = TN + '.xlsx'

        file = os.path.join(output_dir,tcName1)
        file1 = os.path.join(output_dir,testName)

        with pd.ExcelWriter(file1) as writer:
            df1.to_excel(writer, sheet_name=s_file_name, header=True, index=False)
            df2.to_excel(writer, sheet_name=t_file_name, header=True, index=False)
            for col in final_list:
                df3= compare.sample_mismatch(col.lower(), sample_count=100, for_display=True) ## please increase the sample Count as requires( ex sample_count=100 if you have 100 records in source and Target)
                
                if not df3.empty:
                    sheet_name = col[0:30].replace('/','_').replace('\\','_').replace(',','_')
                    #df3.to_excel(writer, sheet_name=f'{col[0:30]}', header=True, index=False)
                    df3.to_excel(writer, sheet_name=sheet_name, header=True, index=False)
                    
            
        with open(file,'w',encoding='utf-8') as fo:

            fo.write(data1)
            fo.close() 

        results.append({'TestName': TN,'File_Name':FN,'Source_Count':df1_rows,'Target_Count':df2_rows,'Count_Check':CN,'Comparison_Result':Comparison_Result})
        placeholder.empty()
        #DB_STATS_INSERT(start_time,TN,project,total_tests,Comparison_Result,df1_rows,df2_rows,user_id,env)

    return results
 