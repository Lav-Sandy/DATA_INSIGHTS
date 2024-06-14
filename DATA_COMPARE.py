import streamlit as st
from streamlit_extras.app_logo import add_logo
from PIL import Image
from CSV_to_CSV import *
#from FILE_DB_COMPARE import *
#from ORACLE_ORACLE import *
#from ORACLE_SNOWFLAKE import *
#from FILE_SNOWFLAKE import *
#from DB2_SNOWFLAKE import *
#from SNOWFLAKE_SNOWFLAKE import *
from FOOTER import *


st.set_page_config( page_title="Data App",layout="wide")

 
def add_logo(logo_path, width, height):
    """Read and return a resized logo"""
    logo = Image.open(logo_path)
    modified_logo = logo.resize((width, height))
    return modified_logo
        

def headerlayout():
    
    my_logo2 = add_logo(logo_path=r"C:\Users\91897\OneDrive\Documents\Compare.jpg", width=100, height=100)
    
    col1, col2, col3 = st.columns(3)
    with col1:
        st.write(" ")
    with col2:
        st.write(" ")
    with col3:
        st.image(my_logo2)


# Web App Title
    st.markdown("<h1 style = 'text-align:center; color: Black;font-size:50px;'>DATA COMPARE</h1>", unsafe_allow_html = True)
    
    st.markdown (""" **_A One Stop solution to all your data validations. <br>
                 This app supports comparison of two heterogenous data sets (aka Source and Target) and provides a comparison summary report._** <br>
                """,True)
                
                



    st.markdown(""" 📌**Note:** <br>
            The framework can be customized to suit as per any project specific needs. Please contact the below Email ID for further assistance.<br>
                           """ , True)

    
# # Hide mainmenu and footer 
    # st.markdown(""" <style>
    # #MainMenu {visibility: hidden;}
    # </style> """, unsafe_allow_html=True)
# #    footer {visibility: hidden;}
    

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

def DEFAULT_VALUE():
    DEFAULT = '< PICK A VALUE >'
    return DEFAULT

page_names_to_funcs = {
    "Select" : DEFAULT_VALUE,
    "Files Comparison": CSV_CSV,
    # "File to Oracle Compare": FILE_DB,
    # "File to Snowflake Compare": FILE_SNOW,
    # "Oracle-DB Comparison": ORACLE_ORACLE,
    # "Oracle to Snowflake Compare": ORACLE_SNOWFLAKE,
    # "Snowflake to Snowflake Compare": SNOWFLAKE_SNOWFLAKE,
    # "DB2 to Snowflake Compare": DB2_SNOWFLAKE
   }            


selected_page = st.sidebar.selectbox("Choose one from the below list", page_names_to_funcs.keys())

    
if selected_page == "Select":    
    headerlayout()
else:
    page_names_to_funcs[selected_page]()  

footer()
