import pandas as pd
import streamlit as st
from htbuilder import HtmlElement, div, ul, li, br, hr, a, p, img, styles, classes, fonts
from htbuilder.units import percent, px
from htbuilder.funcs import rgba, rgb

#Update the footer
@st.cache
def image(src_as_string, **style):
    return img(src=src_as_string, style=styles(**style))

def link(link, text, **style):
    return a(_href=link, _target="_blank", style=styles(**style))(text)

def layout(*args):

    style = """
    <style>
      # MainMenu {visibility: hidden;}
      footer {visibility: hidden;}
    </style>
    """

    style_div = styles(
        left=0,
        bottom=0,
        margin=px(0, 0, 0, 0),
        width=percent(100),
        text_align="center",
        height="60px",
        opacity=0.6
    )

    style_hr = styles(
    )

    body = p()
    foot = div(style=style_div)(hr(style=style_hr), body)

    st.markdown(style, unsafe_allow_html=True)

    for arg in args:
        if isinstance(arg, str):
            body(arg)
        elif isinstance(arg, HtmlElement):
            body(arg)

    st.markdown(str(foot), unsafe_allow_html=True)

# def footer():
    # myargs = [
        # # "<b> Made with</b>: Python 3.8 ",
        # # link("https://www.python.org/", image('https://i.imgur.com/ml09ccU.png',
        	# # width=px(18), height=px(18), margin= "0em")),
           # # " , Pyspark 3.3.2 ",
        # # link("https://spark.apache.org/docs/latest/api/python/", image('https://spark.apache.org/docs/2.2.0/api/python/_static/spark-logo-hd.png',
        	# # width=px(24), height=px(25), margin= "0em")),
        # # " and Streamlit 1.16 ",
        # # link("https://streamlit.io/", image('https://blog.streamlit.io/content/images/size/w1000/2021/03/logomark-color.png',
        	# # width=px(24), height=px(25), margin= "0em")), 
            # "<b> <br> Developed by </b>: Enterprise Data Management Automation Team ",
            # # "<b> <br> Used On </b>: Non Production Data ",
        # br(),
    # ]
    
    
def footer():
    myargs = [
        "<b> User Guide </b>: ",
        link("https://docs.streamlit.io/get-started", image('https://www.tuftsmedicarepreferred.org/sites/default/files/2021-05/document_free_blue_200.png',
        	width=px(25), height=px(25), margin= "0em")),
            "<b> <br> Developed by </b>: Lavanya.S ",
            "<b> <br> Contact me @ </b> <u> <i> lavanya.sandyavandanam@gmail.com  </i> </u>",

        #br(),
    ]
    layout(*myargs)
