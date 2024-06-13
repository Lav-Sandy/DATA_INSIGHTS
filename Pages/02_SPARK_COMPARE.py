import streamlit as st
from PIL import Image
from FOOTER import *
# from File_Oracle_Pyspark_UI import *
# from File_Snowflake_Pyspark_UI import *
# from Oracle_to_Snowflake_Pyspark import *
# from Snowflake_to_Snowflake_Pyspark import *
from FILE_FILE_SPARK import *
from streamlit_extras.app_logo import add_logo

st.set_page_config( page_title="Data Testing App",layout="wide")
#add_logo("I:\PKGDROP\Data Foundation Project\Voya.png",height=40)

def add_logo(logo_path, width, height):
    """Read and return a resized logo"""
    logo = Image.open(logo_path)
    modified_logo = logo.resize((width, height))
    return modified_logo
    

def headerlayout():

    
    my_logo2 = add_logo(logo_path=r"C:\Users\91897\OneDrive\Documents\SparkLogo.jpg", width=150, height=100)
    #st.image(my_logo2)
    
    col1, col2, col3 = st.columns(3)
    with col1:
        st.write(" ")
    with col2:
        st.write(" ")
    with col3:
        st.image(my_logo2)

# Web App Title
    st.markdown("<h1 style = 'text-align:center; color: Black;font-size:50px;'>BIG DATA-COMPARE</h1>", unsafe_allow_html = True)
    st.markdown (""" **_Spark framework supports comparison of two heterogenous data sets (aka Source and Target)._**<br>
                    **_This utility is preferable for larger data sets._**
                """,True)

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
    
    
    
    # my_logo2 =  add_logo(logo_path=r"I:\PKGDROP\Data Foundation Project\Pyspark.png", width=600, height=350)

    # col1, col2, col3 = st.columns([1,6,1])
    # with col1:
        # st.write(" ")
    # with col2:
        # st.image(my_logo2)
    # with col3:
        # st.write(" ")

    st.markdown(""" 📌**Note:** <br>
            The framework can be customized to suit as per any project specific needs. Please contact the below Email ID for further assistance.<br>
                           """ , True)
   

    

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
    "File to File  Compare": File_File_PYSPARK,
    # "File to Oracle Compare": FILE_ORACLE_PYSPARK,
    # "File to Snowflake Compare": FILE_SNOWFLAKE_PYSPARK,
    # "Oracle to Snowflake Compare": ORACLE_SNOWFLAKE_PYSPARK,
    # "Snowflake to Snowflake  Compare": SNOWFLAKE_SNOWFLAKE_PYSPARK
    
   }            


selected_page = st.sidebar.selectbox("Choose one from the below list", page_names_to_funcs.keys())

    
if selected_page == "Select":    
    headerlayout()
else:
    page_names_to_funcs[selected_page]()  
    
    

footer()
