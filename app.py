import streamlit as st
import pandas as pd
import sqlite3
import os
from dbfread import DBF
import tempfile

# 1. اکاؤنٹ ماسٹر ڈیٹا (آپ کی لسٹ کے مطابق)
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

# 2. ڈیٹا بیس سیٹ اپ
conn = sqlite3.connect('madrasa_final.db', check_same_thread=False)
c = conn.cursor()

def init_db():
    c.execute('''CREATE TABLE IF NOT EXISTS transactions 
                 (date TEXT, jvno TEXT, code TEXT, name TEXT, description TEXT, income REAL, payment REAL)''')
    conn.commit()

init_db()

# 3. خوبصورت ڈیزائن اور RTL اسٹائلنگ
st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Noto+Nastaliq+Urdu&display=swap');
    
    .main {
        direction: rtl;
        text-align: right;
        font-family: 'Noto Sans Arabic', sans-serif;
    }
    div[data-testid="stMetricValue"] {
        text-align: center;
        color: #1f77b4;
    }
    .stButton>button {
        width: 100%;
        border-radius: 10px;
        background-color: #2e7d32;
        color: white;
    }
    /* کالمز کو دائیں سے بائیں کرنے کے لیے */
    [data-testid="column"] {
        direction: rtl;
    }
    </style>
    """, unsafe_allow_html=True)

# 4. سائیڈ بار مینو
with st.sidebar:
    st.image("https://cdn-icons-png.flaticon.com/512/3222/3222672.png", width=100)
    st.title("جامعہ ملیہ اسلامیہ")
    menu = st.radio("انتخاب کریں:", ["ڈیش بورڈ", "نئی انٹری (Voucher)", "لیجر رپورٹ", "فائل اپلوڈ کریں"])

# --- فنکشن: ڈیٹا پروسیسنگ ---
def process_dbf(uploaded_file):
    with tempfile.NamedTemporaryFile(delete=False, suffix=".dbf") as tmp_file:
        tmp_file.write(uploaded_file.getbuffer())
        tmp_path = tmp_file.name
    
    try:
        # یہاں 'ignore_missing_memofile=True' شامل کیا گیا ہے
        table = DBF(tmp_path, ignore_missing_memofile=True) 
        new_records = 0
        for record in table:
            d = str(record.get('DATE', ''))
            f_date = f"{d[:4]}-{d[4:6]}-{d[6:]}" if len(d) == 8 else d
            acc_code = str(record.get('CODE', '')).strip()
            acc_name = ACCOUNT_MASTER.get(acc_code, "نامعلوم کھاتہ")
            
            c.execute("INSERT INTO transactions VALUES (?,?,?,?,?,?,?)",
                      (f_date, record.get('JVNO',''), acc_code, acc_name, 
                       record.get('DESC1',''), record.get('INCOME', 0), record.get('PAYMENT', 0)))
            new_records += 1
        conn.commit()
        os.remove(tmp_path)
        return new_records
    except Exception as e:
        return f"ایرر: {e}"

# --- مینو کے مطابق صفحات ---

if menu == "ڈیش بورڈ":
    st.markdown("<h1 style='text-align: right;'>مالیاتی خلاصہ</h1>", unsafe_allow_html=True)
    df = pd.read_sql_query("SELECT * FROM transactions", conn)
    
    if not df.empty:
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("کل آمدنی", f"{df['income'].sum():,.2f}")
        with col2:
            st.metric("کل اخراجات", f"{df['payment'].sum():,.2f}")
        with col3:
            st.metric("باقی بیلنس", f"{(df['income'].sum() - df['payment'].sum()):,.2f}")
        
        st.write("---")
        st.write("### حالیہ ٹرانزیکشنز")
        st.dataframe(df.tail(15), use_container_width=True)
    else:
        st.info("ابھی کوئی ڈیٹا موجود نہیں ہے۔ براہ کرم فائل اپلوڈ کریں یا انٹری کریں۔")

elif menu == "نئی انٹری (Voucher)":
    st.markdown("<h2 style='text-align: right;'>نیا واؤچر درج کریں</h2>", unsafe_allow_html=True)
    with st.form("entry_form", clear_on_submit=True):
        col1, col2 = st.columns(2)
        with col1:
            v_date = st.date_input("تاریخ")
            v_code = st.selectbox("کھاتہ کوڈ", list(ACCOUNT_MASTER.keys()), 
                                 format_func=lambda x: f"{x} - {ACCOUNT_MASTER[x]}")
        with col2:
            v_type = st.radio("قسم", ["آمدنی (Income)", "ادائیگی (Payment)"])
            v_amount = st.number_input("رقم", min_value=0.0)
        
        v_desc = st.text_area("تفصیل")
        if st.form_submit_button("محفوظ کریں"):
            inc = v_amount if v_type == "آمدنی (Income)" else 0
            pay = v_amount if v_type == "ادائیگی (Payment)" else 0
            c.execute("INSERT INTO transactions VALUES (?,?,?,?,?,?,?)", 
                      (str(v_date), "Manual", v_code, ACCOUNT_MASTER[v_code], v_desc, inc, pay))
            conn.commit()
            st.success("ریکارڈ کامیابی سے محفوظ ہو گیا!")

elif menu == "لیجر رپورٹ":
    st.markdown("<h2 style='text-align: right;'>کھاتہ وار رپورٹ</h2>", unsafe_allow_html=True)
    sel_code = st.selectbox("کھاتہ منتخب کریں:", list(ACCOUNT_MASTER.keys()), 
                           format_func=lambda x: f"{x} - {ACCOUNT_MASTER[x]}")
    
    df_ledger = pd.read_sql_query(f"SELECT * FROM transactions WHERE code = '{sel_code}'", conn)
    if not df_ledger.empty:
        st.table(df_ledger)
        st.download_button("ایکسل فائل ڈاؤن لوڈ کریں", df_ledger.to_csv(index=False), "ledger.csv")
    else:
        st.warning("اس کھاتہ میں کوئی ریکارڈ نہیں ملا۔")

elif menu == "فائل اپلوڈ کریں":
    st.markdown("<h2 style='text-align: right;'>پرانی DBF فائلیں اپلوڈ کریں</h2>", unsafe_allow_html=True)
    st.write("آپ کسی بھی سال کی فائل (جیسے JIID2023.DBF) یہاں اپلوڈ کر کے ڈیٹا منتقل کر سکتے ہیں۔")
    
    up_files = st.file_uploader("فائل منتخب کریں", type=["dbf"], accept_multiple_files=True)
    
    if st.button("ڈیٹا منتقل کریں"):
        if up_files:
            for f in up_files:
                res = process_dbf(f)
                if isinstance(res, int):
                    st.success(f"فائل {f.name}: {res} ریکارڈز شامل ہو گئے۔")
                else:
                    st.error(f"فائل {f.name} میں غلطی: {res}")
        else:
            st.warning("پہلے فائل منتخب کریں۔")
