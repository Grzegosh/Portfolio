import streamlit as st
import os
class Background:
    def __init__(self):
        pass

    def use_css(self):
        css_path = os.path.join(os.path.dirname(__file__), "style.css")
        with open(css_path, "r") as css:
            css = f"<style>{css.read()}</style>"
        return css






