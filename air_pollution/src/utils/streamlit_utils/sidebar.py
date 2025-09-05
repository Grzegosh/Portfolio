import sys
sys.path.append("/Users/grzegorznaporowski/Desktop/Portfolio")


import streamlit as st



class Sidebar:
    def __init__(self, msg:str, options:tuple):
        self.msg = msg
        self.options = options

        if "sidebar_visible" not in st.session_state:
            st.session_state.sidebar_visible = True

    def create_sidebar(self):
        if not isinstance(self.msg, str):
            raise TypeError("msg parameter should be a string!")

        if not isinstance(self.options, tuple):
            raise TypeError("options parameter should be a tuple!")

        add_options = st.sidebar.selectbox(
            self.msg,
            self.options
        )
        return add_options














