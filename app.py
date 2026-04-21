import streamlit as st
import pandas as pd
import sqlite3
import os
from dbfread import DBF

# 1. تمام اکاؤنٹ کوڈز کی لسٹ (آپ کی فراہم کردہ لسٹ کے مطابق)
ACCOUNT_MASTER = {
    "001": "SADQAT", "002": "ZAKAT", "003": "GENERAL DONATION", "004": "CONSTRUCTION DONATION",
    "005": "FOOD EXPENSES", "006": "QARZ-E-HASSNA", "007": "ELECTICITY", "008": "PHONE & POSTAGE",
    "009": "SUI GAS", "010": "MISC. EXP.", "011": "MASJID DONATION", "012": "MISC. RENT EXP",
    "013": "ELECTRIC GOODS", "014": "REPAIR & MAINTINANCE", "015": "TRANSPORTATION",
    "016": "FURNITURE & FIXTURE", "017": "MEDICEN EXP.", "018": "PRINTING & STATIONARY",
    "019": "NEWS PAPERS", "020": "LANDRY", "021": "CLOTH & SHOES EXP.", "022": "CROCRY",
    "023": "AUDIT FEE", "024": "BOOKS", "025": "SALARIES", "026": "SENETARY EXP.",
    "027": "OTHER INCOME", "028": "HABIB BANK A/C NO. 17271-68", "029": "BANK CHARGES",
    "030": "SALES OF HIDE", "031": "CARPETS", "032": "OFFICE EQUIPMENTS", "033": "BUILDING",
    "034": "PRAYER MATS (SAFAIN)", "035": "CLEANLINESS ETC", "036": "WATER PUMP",
    "037": "RECEIVABLE A/C", "038": "ACCOUMULATED FUND", "039": "EXPENSES PAYABLE",
    "040": "WAGES ETC", "041": "COMPUTER", "042": "LAIBRARY BOOKS", "043": "SECURITY DEPOSIT",
    "044": "PRISES TO STUDENT", "045": "STEPENDS", "046": "SOLAR SYSTEM", "047": "TUFF TILES"
}

# 2. ڈیٹا بیس کنکشن
conn = sqlite3.connect('madrasa_v2.db', check_same_thread=False)
c = conn.cursor()

def init_db():
    c.execute('''CREATE TABLE IF NOT EXISTS transactions 
                 (date TEXT, jvno TEXT, code TEXT, name TEXT, description TEXT, income REAL, payment REAL)''')
    conn.commit()

init_db()

# 3. ڈیٹا منتقل کرنے کا فنکشن (DBF to SQLite)
def migrate_data():
    if os.path.exists('JIID2023.DBF'):
        try:
            table = DBF('JIID2023.DBF', charimg='cp850')
            count = 0
            for record in table:
                d = str(record['DATE'])
                f_date = f"{d[:4]}-{d[4:6]}-{d[6:]}" if len(d)==8 else d
                name = ACCOUNT_MASTER.get(record['CODE'].strip(), "Unknown Account")
                c.execute("INSERT INTO transactions VALUES (?,?,?,?,?,?,?)",
                          (f_date, record['JVNO'], record['CODE'], name, record['DESC1'], record['INCOME'], record['PAYMENT']))
            conn.commit()
            st.success(f"مبارک! {len(table)} ریکارڈز منتقل ہو گئے۔")
        except Exception as e:
            st.error(f"ایرر: {e}")
    else:
        st.warning("JIID2023.DBF فائل گٹ ہب پر موجود نہیں ہے۔")

# 4. ویب انٹرفیس ڈیزائن
st.title("جامعہ ملیہ اسلامیہ - جدید اکاؤنٹنگ سسٹم")

menu = st.sidebar.selectbox("مینو", ["ڈیش بورڈ", "نئی انٹری (Voucher)", "لیجر رپورٹ", "ڈیٹا منتقلی"])

if menu == "ڈیش بورڈ":
    df = pd.read_sql_query("SELECT * FROM transactions", conn)
    col1, col2, col3 = st.columns(3)
    col1.metric("کل آمدنی", f"{df['income'].sum():,.2f}")
    col2.metric("کل اخراجات", f"{df['payment'].sum():,.2f}")
    col3.metric("بیلنس", f"{(df['income'].sum()-df['payment'].sum()):,.2f}")
    st.write("### حالیہ ٹرانزیکشنز")
    st.dataframe(df.tail(20), use_container_width=True)

elif menu == "نئی انٹری (Voucher)":
    st.subheader("روزنامچہ واؤچر")
    with st.form("entry_form"):
        v_date = st.date_input("تاریخ")
        v_code = st.selectbox("اکاؤنٹ کوڈ", list(ACCOUNT_MASTER.keys()), format_func=lambda x: f"{x} - {ACCOUNT_MASTER[x]}")
        v_desc = st.text_input("تفصیل")
        v_type = st.radio("رقم کی قسم", ["Income", "Payment"])
        v_amt = st.number_input("رقم", min_value=0.0)
        if st.form_submit_button("محفوظ کریں"):
            inc = v_amt if v_type == "Income" else 0
            pay = v_amt if v_type == "Payment" else 0
            c.execute("INSERT INTO transactions VALUES (?,?,?,?,?,?,?)", 
                      (str(v_date), "NEW", v_code, ACCOUNT_MASTER[v_code], v_desc, inc, pay))
            conn.commit()
            st.success("ریکارڈ محفوظ!")

elif menu == "لیجر رپورٹ":
    code = st.selectbox("کھاتہ منتخب کریں", list(ACCOUNT_MASTER.keys()), format_func=lambda x: ACCOUNT_MASTER[x])
    df = pd.read_sql_query(f"SELECT * FROM transactions WHERE code = '{code}'", conn)
    st.write(f"### رپورٹ برائے: {ACCOUNT_MASTER[code]}")
    st.table(df)

elif menu == "ڈیٹا منتقلی":
    st.write("اگر آپ پرانی DBF فائل کا ڈیٹا یہاں لانا چاہتے ہیں تو نیچے والا بٹن دبائیں۔")
    if st.button("پرانا ڈیٹا لوڈ کریں"):
        migrate_data()
