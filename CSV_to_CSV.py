import pandas as pd
import streamlit as st
import os
from COMPARE import *
from datetime import datetime
import streamlit_toggle as tog



# def clear_cache():
    # st.legacy_caching.caching.clear_cache()
    
def CSV_CSV():

    st.markdown("""
    <style>
           .css-z5fcl4 {
                padding-top: 2rem;
    </style>
    """, unsafe_allow_html=True)
    
    
    global filenames,keyslist,data,PKey
    
    placeholder = st.empty()

    with placeholder.container():
        st.markdown("<h1 style = 'text-align:center; color: Black;font-size:40px;'>FILE COMPARE</h1>", unsafe_allow_html = True)
        st.markdown('<hr style = "border-top: 3px solid orange;">',unsafe_allow_html = True)
        st.markdown (""" **_The framework supports multiple file types (.csv, .txt, .xls) and flat files with various delimiters. <br>
                        Also multiple files can be compared at once by providing required details in a PARAMETER file.<br>
                    <br>
                 👉 Note: This utility performs comparisons based on common columns, hence the header row or column names should be same in Source and Target files._** <br>
                    <br>
                   
                """,True)
#If either of the file/s doesn't have common columns or header row, ROW WISE COMPARISONS technique is suitable. Please reach out to us for further assistance.
    
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
    
            
    try:    
        
        with st.sidebar:
            
            check = tog.st_toggle_switch(label="Enable Multiple_Files", 
                key="Key1", 
                default_value=False, 
                label_after = True, 
                inactive_color = '#f7925c', 
                active_color="#11567f", 
                track_color="#f7925c"
                )
            
        if check:
            st.sidebar.file_uploader("Upload parameter file", type=["xlsx"],key = "MF_File")
            cmp = st.sidebar.button('Submit',key = "comp") 
            df_dbq = pd.read_excel(st.session_state.MF_File, header=0,sheet_name='File_File')
            if st.session_state.MF_File and st.session_state.comp:
                placeholder.empty()
                start_time = datetime.now()
                results = COMPARE_MUL_FILES(df_dbq)
                df3 = pd.DataFrame(results)
                st.write(df3.style.hide_index()
                    .applymap(color_pass_fail, subset=["Count_Check","Comparison_Result"])
                    .set_table_styles([
                            {'selector': 'th', 'props': [('background-color', '#f7ffff'),    #00008B - Dark blue,  f7ffff - light blue
                         ('color', 'black'),
                         ('font-weight', 'bold')]},
                            {'selector': 'td', 'props': [('background-color', '#E1F5FE'),
                         ('font-size', '15px')]}
                            ]), unsafe_allow_html=True) #.render()
                st.write('')
                st.download_button( label="Download Summary Report", data=df3.to_csv().encode('utf-8'), file_name='Comparison_Summary.csv',mime='text/csv',key = "F2O_download")
                st.write("Comparison Reports are saved under **Output** folder in location: **{}**".format(df_dbq.at[0,"File_Location"]),markdown=True)
                TIME(start_time)

        else:
        # Upload source file
            with st.sidebar.form(key ='Form1'):
                st.sidebar.file_uploader("Upload Source file", type=["csv","txt","xlsx"],key='src_file')
                df1=read_file(st.session_state.src_file)
                
                # Upload target File
                st.sidebar.file_uploader("Upload Target file", type=["csv","txt","xlsx"],key='tgt_file')
                df2=read_file(st.session_state.tgt_file)
                df1=df1.applymap(str)
                df2=df2.applymap(str)
                
                df1.columns = df1.columns.astype(str).str.lower()
                df2.columns = df2.columns.astype(str).str.lower()
                
                
                df2=df2.replace('NULL','')
                
                common_columns=Key_List(df1,df2)
                st.multiselect('Select Primary Key(s)',list(common_columns),key='PKey')
            
                if not st.session_state.PKey:
                    st.info("Please Select Primary Key(s)")
                else:
                    pass

                def COMP():
                        placeholder.empty()
                        start_time = datetime.now()
                        DataFrame_Summary(df1,df2)
                        COMPARE_CSV(df1,df2,st.session_state.PKey,start_time)
                        st.runtime.legacy_caching.clear_cache()

                submitted = st.form_submit_button(label="Compare",on_click=COMP)
                if submitted:
                    placeholder.empty()
        
    except (NameError,ValueError,AttributeError,RuntimeError) as e:
        #st.exception(e)
        st.info('*Awaiting user input*')
        
    finally :
        st.write('')
    

