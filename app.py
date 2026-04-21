import streamlit as st
import pandas as pd
import sqlite3
import os
import tempfile
from dbfread import DBF
import base64
from datetime import datetime

# ---------------------------- ڈیٹا بیس سیٹ اپ ----------------------------
conn = sqlite3.connect('madrasa_accounts.db', check_same_thread=False)
c = conn.cursor()

def init_db():
    # ٹرانزیکشنز ٹیبل
    c.execute('''CREATE TABLE IF NOT EXISTS transactions
                 (date TEXT, receipt_no TEXT, jvno TEXT, code TEXT, name TEXT,
                  description TEXT, income REAL, payment REAL)''')
    # اوپننگ بیلنس ٹیبل
    c.execute('''CREATE TABLE IF NOT EXISTS opening_balances
                 (year TEXT, code TEXT, balance REAL, PRIMARY KEY(year, code))''')
    conn.commit()

init_db()

# ---------------------------- اکاؤنٹ ماسٹر ----------------------------
ACCOUNT_MASTER = {
    "001": "صدقات", "002": "زکوٰۃ", "003": "عام عطیات", "004": "تعمیراتی عطیات",
    "005": "کھانے کے اخراجات", "006": "قرض حسنہ", "007": "بجلی", "008": "ٹیلی فون و ڈاک",
    "009": "سوئی گیس", "010": "متفرق اخراجات", "011": "مسجد عطیات", "012": "کرایہ متفرق",
    "013": "برقی سامان", "014": "مرمت و دیکھ بھال", "015": "نقل و حمل",
    "016": "فرنیچر و فکسچر", "017": "ادویات", "018": "طباعت و اسٹیشنری",
    "019": "اخبارات", "020": "لانڈری", "021": "کپڑے اور جوتے", "022": "کراکری",
    "023": "آڈٹ فیس", "024": "کتب", "025": "تنخواہیں", "026": "صفائی اخراجات",
    "027": "دیگر آمدنی", "028": "حبیب بینک اکاؤنٹ نمبر 17271-68", "029": "بینک چارجز",
    "030": "کھالوں کی فروخت", "031": "قالین", "032": "دفتری سازوسامان", "033": "عمارت",
    "034": "صفائی مصلے", "035": "صفائی وغیرہ", "036": "واٹر پمپ",
    "037": "قابل وصول اکاؤنٹ", "038": "جمع شدہ فنڈ", "039": "قابل ادائیگی اخراجات",
    "040": "اجرت وغیرہ", "041": "کمپیوٹر", "042": "لائبریری کتب", "043": "سیکیورٹی ڈپازٹ",
    "044": "طلبہ انعامات", "045": "وظائف", "046": "سولر سسٹم", "047": "ٹف ٹائلز"
}

# ---------------------------- حسب ضرورت CSS (RTL + پرنٹ) ----------------------------
st.set_page_config(page_title="جامعہ اکاؤنٹس", layout="wide")
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Noto+Nastaliq+Urdu&display=swap');
    html, body, [class*="css"] {
        direction: rtl;
        text-align: right;
        font-family: 'Noto Sans Arabic', sans-serif;
    }
    .stButton>button {
        width: 100%;
        border-radius: 12px;
        background-color: #0d6efd;
        color: white;
        font-weight: bold;
    }
    .reportview-container .main .block-container {
        padding-top: 2rem;
    }
    .css-1d391kg {direction: rtl;}
    .print-only {display: none;}
    @media print {
        .no-print {display: none !important;}
        .print-only {display: block !important;}
    }
</style>
""", unsafe_allow_html=True)

# ---------------------------- معاون فنکشنز ----------------------------
def get_years():
    c.execute("SELECT DISTINCT substr(date,1,4) FROM transactions ORDER BY 1")
    trans_years = [row[0] for row in c.fetchall()]
    c.execute("SELECT DISTINCT year FROM opening_balances ORDER BY 1")
    bal_years = [row[0] for row in c.fetchall()]
    return sorted(set(trans_years + bal_years), reverse=True)

def get_current_year():
    years = get_years()
    return years[0] if years else str(datetime.now().year)

def download_link(df, filename, text):
    csv = df.to_csv(index=False).encode('utf-8')
    b64 = base64.b64encode(csv).decode()
    href = f'<a href="data:file/csv;base64,{b64}" download="{filename}">{text}</a>'
    return href

def print_button():
    st.markdown("""
    <button onclick="window.print();" class="no-print" style="padding:8px 16px; background:#2e7d32; color:white; border:none; border-radius:8px; margin:10px 0;">
        🖨️ پرنٹ کریں
    </button>
    """, unsafe_allow_html=True)

def process_dbf(uploaded_file):
    with tempfile.NamedTemporaryFile(delete=False, suffix=".dbf") as tmp_file:
        tmp_file.write(uploaded_file.getbuffer())
        tmp_path = tmp_file.name
    try:
        table = DBF(tmp_path, ignore_missing_memofile=True)
        new_records = 0
        for record in table:
            d = str(record.get('DATE', ''))
            f_date = f"{d[:4]}-{d[4:6]}-{d[6:]}" if len(d) == 8 else d
            acc_code = str(record.get('CODE', '')).strip()
            acc_name = ACCOUNT_MASTER.get(acc_code, "نامعلوم کھاتہ")
            c.execute("INSERT INTO transactions VALUES (?,?,?,?,?,?,?,?)",
                      (f_date, record.get('RECEIPT',''), record.get('JVNO',''), acc_code, acc_name,
                       record.get('DESC1',''), record.get('INCOME',0), record.get('PAYMENT',0)))
            new_records += 1
        conn.commit()
        os.remove(tmp_path)
        return new_records
    except Exception as e:
        return f"خرابی: {e}"

# ---------------------------- سائیڈ بار ----------------------------
with st.sidebar:
    st.image("https://cdn-icons-png.flaticon.com/512/3222/3222672.png", width=80)
    st.title("📋 جامعہ ملیہ اسلامیہ")
    menu = st.radio("مینو", ["🏠 ڈیش بورڈ", "➕ نئی انٹری", "📒 کیش بک", "📚 لیجر", "⚖️ ٹرائل بیلنس", "🔓 اوپننگ بیلنس", "📤 ڈیٹا اپ لوڈ", "⚙️ بیک اپ/ریسٹور"])
    
    # سال کا انتخاب (گلوبل فلٹر)
    years = get_years()
    if years:
        selected_year = st.selectbox("📅 مالی سال منتخب کریں", years, index=0)
    else:
        selected_year = str(datetime.now().year)
        st.info("کوئی ڈیٹا موجود نہیں، پہلے انٹری کریں")
    st.session_state['selected_year'] = selected_year

# ---------------------------- مینو پیجز ----------------------------
if menu == "🏠 ڈیش بورڈ":
    st.header("مالیاتی خلاصہ")
    year = st.session_state['selected_year']
    df = pd.read_sql_query(f"SELECT * FROM transactions WHERE date LIKE '{year}%'", conn)
    
    if not df.empty:
        total_income = df['income'].sum()
        total_payment = df['payment'].sum()
        col1, col2, col3 = st.columns(3)
        col1.metric("کل آمدنی", f"Rs. {total_income:,.2f}")
        col2.metric("کل اخراجات", f"Rs. {total_payment:,.2f}")
        col3.metric("خالص بیلنس", f"Rs. {total_income - total_payment:,.2f}")
        
        st.subheader("حالیہ اندراجات")
        st.dataframe(df.tail(10).iloc[::-1], use_container_width=True)
    else:
        st.info(f"سال {year} کے لیے کوئی ڈیٹا موجود نہیں")

elif menu == "➕ نئی انٹری":
    st.header("واؤچر اندراج")
    tab1, tab2 = st.tabs(["💰 آمدنی", "💸 ادائیگی"])
    
    with tab1:
        with st.form("income_form", clear_on_submit=True):
            col1, col2 = st.columns(2)
            with col1:
                date = st.date_input("تاریخ", key="inc_date")
                code = st.selectbox("کھاتہ کوڈ", list(ACCOUNT_MASTER.keys()),
                                    format_func=lambda x: f"{x} - {ACCOUNT_MASTER[x]}", key="inc_code")
                receipt = st.text_input("رسید نمبر (اختیاری)", key="inc_receipt")
            with col2:
                amount = st.number_input("رقم", min_value=0.0, step=100.0, key="inc_amt")
                jvno = st.text_input("جرنل واؤچر نمبر", key="inc_jvno")
            desc = st.text_area("تفصیل", key="inc_desc")
            if st.form_submit_button("💰 آمدنی محفوظ کریں"):
                c.execute("INSERT INTO transactions VALUES (?,?,?,?,?,?,?,?)",
                          (str(date), receipt, jvno, code, ACCOUNT_MASTER[code], desc, amount, 0))
                conn.commit()
                st.success("آمدنی ریکارڈ محفوظ ہو گئی")
                st.rerun()
    
    with tab2:
        with st.form("payment_form", clear_on_submit=True):
            col1, col2 = st.columns(2)
            with col1:
                date = st.date_input("تاریخ", key="pay_date")
                code = st.selectbox("کھاتہ کوڈ", list(ACCOUNT_MASTER.keys()),
                                    format_func=lambda x: f"{x} - {ACCOUNT_MASTER[x]}", key="pay_code")
                receipt = st.text_input("رسید نمبر (اختیاری)", key="pay_receipt")
            with col2:
                amount = st.number_input("رقم", min_value=0.0, step=100.0, key="pay_amt")
                jvno = st.text_input("جرنل واؤچر نمبر", key="pay_jvno")
            desc = st.text_area("تفصیل", key="pay_desc")
            if st.form_submit_button("💸 ادائیگی محفوظ کریں"):
                c.execute("INSERT INTO transactions VALUES (?,?,?,?,?,?,?,?)",
                          (str(date), receipt, jvno, code, ACCOUNT_MASTER[code], desc, 0, amount))
                conn.commit()
                st.success("ادائیگی ریکارڈ محفوظ ہو گئی")
                st.rerun()

elif menu == "📒 کیش بک":
    st.header("کیش بک (روزنامچہ)")
    year = st.session_state['selected_year']
    df = pd.read_sql_query(f"""
        SELECT date, receipt_no, jvno, code, name, description, income, payment
        FROM transactions WHERE date LIKE '{year}%'
        ORDER BY date
    """, conn)
    
    if not df.empty:
        df['بیلنس'] = (df['income'] - df['payment']).cumsum()
        st.dataframe(df, use_container_width=True)
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.download_button("📥 CSV ڈاؤن لوڈ", df.to_csv(index=False), f"cashbook_{year}.csv")
        with col2:
            st.markdown(download_link(df, f"cashbook_{year}.xlsx", "📊 ایکسل ڈاؤن لوڈ"), unsafe_allow_html=True)
        with col3:
            print_button()
    else:
        st.warning(f"سال {year} میں کوئی لین دین نہیں")

elif menu == "📚 لیجر":
    st.header("کھاتہ وار لیجر")
    year = st.session_state['selected_year']
    code = st.selectbox("کھاتہ منتخب کریں", list(ACCOUNT_MASTER.keys()),
                        format_func=lambda x: f"{x} - {ACCOUNT_MASTER[x]}")
    
    df = pd.read_sql_query(f"""
        SELECT date, receipt_no, jvno, description, income, payment
        FROM transactions WHERE code = '{code}' AND date LIKE '{year}%'
        ORDER BY date
    """, conn)
    
    if not df.empty:
        # اوپننگ بیلنس شامل کریں
        c.execute("SELECT balance FROM opening_balances WHERE year=? AND code=?", (year, code))
        opening = c.fetchone()
        opening_bal = opening[0] if opening else 0.0
        
        df['بیلنس'] = opening_bal + (df['income'] - df['payment']).cumsum()
        st.subheader(f"اوپننگ بیلنس: Rs. {opening_bal:,.2f}")
        st.dataframe(df, use_container_width=True)
        st.download_button("📥 لیجر ڈاؤن لوڈ", df.to_csv(index=False), f"ledger_{code}_{year}.csv")
        print_button()
    else:
        st.warning("اس کھاتہ میں کوئی اندراج نہیں")

elif menu == "⚖️ ٹرائل بیلنس":
    st.header("ٹرائل بیلنس")
    year = st.session_state['selected_year']
    
    # ہر اکاؤنٹ کے لیے کل آمدنی اور ادائیگی نکالیں
    query = f"""
        SELECT code, name, SUM(income) as total_income, SUM(payment) as total_payment
        FROM transactions WHERE date LIKE '{year}%'
        GROUP BY code, name
    """
    df = pd.read_sql_query(query, conn)
    
    # اوپننگ بیلنس شامل کریں
    ob_df = pd.read_sql_query(f"SELECT code, balance FROM opening_balances WHERE year='{year}'", conn)
    
    if not df.empty or not ob_df.empty:
        # ڈیٹا کو ضم کریں
        df = pd.merge(df, ob_df, on='code', how='outer').fillna(0)
        df['balance'] = df['balance'] + df['total_income'] - df['total_payment']
        df = df[['code', 'name', 'balance']].sort_values('code')
        
        st.dataframe(df, use_container_width=True)
        st.download_button("📥 ٹرائل بیلنس ڈاؤن لوڈ", df.to_csv(index=False), f"trial_balance_{year}.csv")
        print_button()
    else:
        st.warning(f"سال {year} کے لیے کوئی ڈیٹا نہیں")

elif menu == "🔓 اوپننگ بیلنس":
    st.header("اوپننگ بیلنس سیٹ کریں")
    year = st.text_input("سال درج کریں (مثلاً 2025)", max_chars=4)
    code = st.selectbox("کھاتہ منتخب کریں", list(ACCOUNT_MASTER.keys()),
                        format_func=lambda x: f"{x} - {ACCOUNT_MASTER[x]}")
    balance = st.number_input("بیلنس رقم", step=100.0)
    
    if st.button("محفوظ کریں"):
        c.execute("INSERT OR REPLACE INTO opening_balances (year, code, balance) VALUES (?,?,?)",
                  (year, code, balance))
        conn.commit()
        st.success(f"{year} کے لیے اوپننگ بیلنس سیٹ ہو گیا")
    
    # موجودہ اوپننگ بیلنس دکھائیں
    st.subheader("موجودہ اوپننگ بیلنسز")
    df_ob = pd.read_sql_query("SELECT year, code, balance FROM opening_balances ORDER BY year DESC", conn)
    if not df_ob.empty:
        st.dataframe(df_ob)
    else:
        st.info("ابھی تک کوئی اوپننگ بیلنس سیٹ نہیں")

elif menu == "📤 ڈیٹا اپ لوڈ":
    st.header("پرانی DBF فائلیں اپ لوڈ کریں")
    uploaded_files = st.file_uploader("DBF فائلیں منتخب کریں", type=['dbf'], accept_multiple_files=True)
    if st.button("ڈیٹا امپورٹ کریں") and uploaded_files:
        for file in uploaded_files:
            result = process_dbf(file)
            if isinstance(result, int):
                st.success(f"{file.name}: {result} ریکارڈ شامل ہوئے")
            else:
                st.error(f"{file.name}: {result}")
        st.rerun()

elif menu == "⚙️ بیک اپ/ریسٹور":
    st.header("ڈیٹا بیس مینجمنٹ")
    tab1, tab2 = st.tabs(["💾 بیک اپ ڈاؤن لوڈ", "📂 ریسٹور"])
    with tab1:
        st.write("پورے ڈیٹا بیس کی فائل ڈاؤن لوڈ کریں")
        with open('madrasa_accounts.db', 'rb') as f:
            st.download_button("⬇️ ڈیٹا بیس ڈاؤن لوڈ", f, file_name="madrasa_backup.db")
    with tab2:
        uploaded_db = st.file_uploader("بیک اپ فائل منتخب کریں (.db)", type=['db'])
        if uploaded_db and st.button("ریسٹور کریں"):
            with open('madrasa_accounts.db', 'wb') as f:
                f.write(uploaded_db.getbuffer())
            st.success("ڈیٹا بیس کامیابی سے ریسٹور ہو گیا۔ براہ کرم ایپ ری لوڈ کریں۔")
            st.button("🔄 ری لوڈ ایپ", on_click=lambda: st.rerun())

# ---------------------------- فوٹر ----------------------------
st.markdown("---")
st.caption("© جامعہ ملیہ اسلامیہ اکاؤنٹنگ سسٹم | تیار کردہ: Streamlit")
