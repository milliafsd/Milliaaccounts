import streamlit as st
import pandas as pd
import sqlite3
from accounts_data import ACCOUNT_CODES

# ڈیٹا بیس کنکشن
conn = sqlite3.connect('madrasa.db', check_same_thread=False)
c = conn.cursor()

# ٹیبل بنانا
c.execute('''CREATE TABLE IF NOT EXISTS entries 
             (date TEXT, code TEXT, name TEXT, desc TEXT, income REAL, payment REAL)''')
conn.commit()

st.set_page_config(page_title="جامعہ ملیہ اسلامیہ - اکاؤنٹ سسٹم", layout="wide")

# سائیڈ بار
st.sidebar.image("https://cdn-icons-png.flaticon.com/512/2641/2641409.png", width=100)
menu = st.sidebar.radio("آپشن منتخب کریں", ["ڈیش بورڈ", "نئی انٹری (Voucher)", "رپورٹ (Ledger)"])

if menu == "ڈیش بورڈ":
    st.header("جامعہ ملیہ اسلامیہ - مالیاتی خلاصہ")
    df = pd.read_sql_query("SELECT * FROM entries", conn)
    
    col1, col2, col3 = st.columns(3)
    income_total = df['income'].sum()
    payment_total = df['payment'].sum()
    
    col1.metric("کل آمدنی", f"Rs. {income_total:,.2f}")
    col2.metric("کل اخراجات", f"Rs. {payment_total:,.2f}")
    col3.metric("موجودہ بیلنس", f"Rs. {(income_total - payment_total):,.2f}")

elif menu == "نئی انٹری (Voucher)":
    st.subheader("روزنامچہ انٹری")
    with st.form("entry_form"):
        col1, col2 = st.columns(2)
        with col1:
            date = st.date_input("تاریخ")
            # یہاں آپ کے پی ڈی ایف والے کوڈز استعمال ہو رہے ہیں
            code = st.selectbox("اکاؤنٹ کوڈ منتخب کریں", list(ACCOUNT_CODES.keys()))
        with col2:
            name = ACCOUNT_CODES[code]
            st.info(f"کھاتہ کا نام: {name}")
            amount_type = st.radio("قسم", ["آمدنی (Income)", "ادائیگی (Payment)"])
            amount = st.number_input("رقم", min_value=0.0)
        
        desc = st.text_area("تفصیل (Description)")
        submit = st.form_submit_button("محفوظ کریں")
        
        if submit:
            inc = amount if amount_type == "آمدنی (Income)" else 0
            pay = amount if amount_type == "ادائیگی (Payment)" else 0
            c.execute("INSERT INTO entries VALUES (?,?,?,?,?,?)", (date, code, name, desc, inc, pay))
            conn.commit()
            st.success("ریکارڈ محفوظ ہو گیا!")

elif menu == "رپورٹ (Ledger)":
    st.subheader("کھاتہ وار رپورٹ")
    df = pd.read_sql_query("SELECT * FROM entries", conn)
    
    selected_code = st.selectbox("فلٹر بذریعہ کوڈ", ["تمام"] + list(ACCOUNT_CODES.keys()))
    if selected_code != "تمام":
        df = df[df['code'] == selected_code]
    
    st.table(df)