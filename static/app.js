const state = {
  user: null,
  year: "",
  language: "en",
  years: [],
  dashboard: null,
  ledger: [],
  accounts: [],
  settings: null,
  report: null,
  quickIncome: [],
  quickExpense: [],
  accountMap: new Map(),
  activeView: "incomeView",
};

const I18N = {
  en: {
    loginEyebrow: "Secure Access",
    loginSubtitle: "Please sign in before opening the accounting system.",
    username: "Username",
    password: "Password",
    login: "Login",
    loginHint: "Default login: admin / admin123",
    eyebrow: "Madrasa Accounting",
    systemControls: "System Controls",
    legacyTools: "Legacy Data Tools",
    language: "Language",
    workingYear: "Working Year",
    selectedYear: "Selected Year",
    uploadLegacy: "Upload Old Data",
    uploadFiles: "Upload Files",
    importYear: "Import Current Year",
    reloadYear: "Reload Year",
    openIncomeEntry: "Open Income Entry",
    openExpenseEntry: "Open Expense Entry",
    tabIncome: "Income Entry",
    tabExpense: "Expense Entry",
    tabReports: "Reports",
    tabLedger: "Ledger",
    tabAccounts: "Accounts",
    tabOverview: "Overview",
    tabSettings: "Settings",
    logout: "Logout",
    quickIncomeEntry: "Quick Income Entry",
    quickIncomeNote: "Enter income in the selected year only.",
    recentIncome: "Recent Income",
    recentIncomeManage: "View, edit, and delete previous income entries.",
    quickExpenseEntry: "Quick Expense Entry",
    quickExpenseNote: "Enter payments in the selected year only.",
    recentExpense: "Recent Expense",
    recentExpenseManage: "View, edit, and delete previous expense entries.",
    reportsTitle: "Reports And Printing",
    reportsNote: "Print and download yearly reports like cash book, ledger, trial balance, and opening balance.",
    customReportTitle: "Custom Report View",
    customReportNote: "Choose type and dates, then open print view or download PDF.",
    reportType: "Report Type",
    fromDate: "From Date",
    toDate: "To Date",
    viewReport: "View Report",
    printReport: "Print",
    downloadPdf: "Download PDF",
    downloadCsv: "Download CSV",
    openPrint: "Open Print",
    yearlyPdf: "Yearly PDF",
    yearlyCsv: "Yearly CSV",
    ledgerTitle: "Ledger Browser",
    ledgerNote: "Search and manage entries inside the selected year only.",
    accountHeads: "Account Heads",
    accountHeadsNote: "Code number and head name remain ready for data entry.",
    saveAccountHead: "Save Account Head",
    saveAccountHeadNote: "Create or update code and its head name.",
    yearSettings: "Year Settings",
    yearSettingsNote: "Date range and cash values stay attached to the selected year.",
    date: "Date",
    code: "Code",
    accountHead: "Account Head",
    branch: "Branch",
    category: "Category",
    receiptNo: "Receipt No",
    voucherNo: "Voucher No",
    jvNo: "JV No",
    description: "Description",
    amount: "Amount",
    saveIncome: "Save Income",
    saveExpense: "Save Expense",
    reset: "Reset",
    search: "Search",
    entryMode: "Entry Type",
    refreshLedger: "Refresh Ledger",
    type: "Type",
    accountName: "Account Name",
    saveAccount: "Save Account",
    cashInHand: "Cash In Hand",
    minCash: "Min Cash",
    maxCash: "Max Cash",
    lastJvNo: "Last JV No",
    saveSettings: "Save Settings",
    monthlyFlow: "Monthly Flow",
    monthlyFlowNote: "Monthly income and expense totals.",
    topAccounts: "Top Accounts",
    topAccountsNote: "Strongest balances in the selected year.",
    colDate: "Date",
    colCode: "Code",
    colAccount: "Account",
    colDescription: "Description",
    colIncome: "Income",
    colExpense: "Expense",
    colActions: "Actions",
    colSource: "Source",
    colType: "Type",
    colEntries: "Entries",
    colBalance: "Balance",
    edit: "Edit",
    delete: "Delete",
    totalEntries: "Entries",
    totalIncome: "Total Income",
    totalExpense: "Total Expense",
    netBalance: "Net Balance",
    openingBalance: "Opening Balance",
    cashSummary: "Cash In Hand",
    noData: "No data found.",
    uploadSelectFirst: "Please choose DBF files first.",
    pageIncomeTitle: "Income Entry",
    pageIncomeNote: "Use this page for daily income data entry.",
    pageExpenseTitle: "Expense Entry",
    pageExpenseNote: "Use this page for daily payment data entry.",
    pageReportsTitle: "Reports",
    pageReportsNote: "Open print view or export a yearly PDF.",
    pageLedgerTitle: "Ledger",
    pageLedgerNote: "Filter, inspect, edit, and delete within the selected year.",
    pageAccountsTitle: "Accounts",
    pageAccountsNote: "Code list and account heads for data entry.",
    pageOverviewTitle: "Overview",
    pageOverviewNote: "Monthly movement and top account balances.",
    pageSettingsTitle: "Settings",
    pageSettingsNote: "Year range and cash settings.",
    loadingYear: "Loading year",
    yearReady: "ready",
    accountNotFound: "Head not found",
    codeHint: "As soon as you type the code, its account head appears automatically.",
    reportPreview: "Report Preview",
    loginRequired: "Login required.",
    annualReportSet: "Yearly Report Set",
    annualReportNote: "Direct print and PDF buttons for the selected year.",
    currentUser: "Current User",
    cashBook: "Cash Book",
    ledger: "Ledger",
    trialBalance: "Trial Balance",
    openingBalanceReport: "Opening Balance",
    incomeExpense: "Income And Expense",
  },
  ur: {
    loginEyebrow: "محفوظ رسائی",
    loginSubtitle: "اکاؤنٹنگ سسٹم کھولنے سے پہلے لاگ ان کریں۔",
    username: "یوزر نیم",
    password: "پاس ورڈ",
    login: "لاگ ان",
    loginHint: "ڈیفالٹ لاگ ان: admin / admin123",
    eyebrow: "مدرسہ اکاؤنٹنگ",
    systemControls: "سسٹم کنٹرولز",
    legacyTools: "پرانا ڈیٹا ٹولز",
    language: "زبان",
    workingYear: "کام کا سال",
    selectedYear: "منتخب سال",
    uploadLegacy: "پرانا ڈیٹا اپلوڈ",
    uploadFiles: "فائلیں اپلوڈ کریں",
    importYear: "موجودہ سال امپورٹ کریں",
    reloadYear: "سال دوبارہ لوڈ کریں",
    openIncomeEntry: "انکم انٹری کھولیں",
    openExpenseEntry: "پیمنٹس انٹری کھولیں",
    tabIncome: "انکم انٹری",
    tabExpense: "پیمنٹس انٹری",
    tabReports: "رپورٹس",
    tabLedger: "لیجر",
    tabAccounts: "اکاؤنٹس",
    tabOverview: "جائزہ",
    tabSettings: "سیٹنگز",
    logout: "لاگ آؤٹ",
    quickIncomeEntry: "فوری انکم انٹری",
    quickIncomeNote: "صرف منتخب سال میں انکم درج کریں۔",
    recentIncome: "حالیہ انکم",
    recentIncomeManage: "پچھلی انکم انٹریز دیکھیں، ترمیم کریں، حذف کریں۔",
    quickExpenseEntry: "فوری پیمنٹس انٹری",
    quickExpenseNote: "صرف منتخب سال میں پیمنٹس درج کریں۔",
    recentExpense: "حالیہ پیمنٹس",
    recentExpenseManage: "پچھلی پیمنٹس انٹریز دیکھیں، ترمیم کریں، حذف کریں۔",
    reportsTitle: "رپورٹس اور پرنٹنگ",
    reportsNote: "کیش بک، لیجر، ٹرائل بیلنس اور اوپننگ بیلنس کی سالانہ پرنٹ اور پی ڈی ایف۔",
    customReportTitle: "کسٹم رپورٹ ویو",
    customReportNote: "قسم اور تاریخیں منتخب کریں، پھر پرنٹ یا پی ڈی ایف کھولیں۔",
    reportType: "رپورٹ کی قسم",
    fromDate: "شروع تاریخ",
    toDate: "آخری تاریخ",
    viewReport: "رپورٹ دیکھیں",
    printReport: "پرنٹ",
    downloadPdf: "پی ڈی ایف",
    downloadCsv: "سی ایس وی",
    openPrint: "پرنٹ کھولیں",
    yearlyPdf: "سالانہ پی ڈی ایف",
    yearlyCsv: "سالانہ سی ایس وی",
    ledgerTitle: "لیجر براؤزر",
    ledgerNote: "صرف منتخب سال کے اندر تلاش، ترمیم اور حذف۔",
    accountHeads: "اکاؤنٹ ہیڈز",
    accountHeadsNote: "کوڈ نمبر اور ہیڈ نام ڈیٹا انٹری کے لیے تیار رہتے ہیں۔",
    saveAccountHead: "اکاؤنٹ ہیڈ محفوظ کریں",
    saveAccountHeadNote: "کوڈ اور اس کا ہیڈ نام بنائیں یا تبدیل کریں۔",
    yearSettings: "سال کی سیٹنگز",
    yearSettingsNote: "تاریخ کی حد اور کیش کی قدریں صرف منتخب سال سے منسلک رہتی ہیں۔",
    date: "تاریخ",
    code: "کوڈ",
    accountHead: "اکاؤنٹ ہیڈ",
    branch: "برانچ",
    category: "کیٹیگری",
    receiptNo: "رسید نمبر",
    voucherNo: "واؤچر نمبر",
    jvNo: "جے وی نمبر",
    description: "تفصیل",
    amount: "رقم",
    saveIncome: "انکم محفوظ کریں",
    saveExpense: "پیمنٹ محفوظ کریں",
    reset: "ری سیٹ",
    search: "تلاش",
    entryMode: "انٹری قسم",
    refreshLedger: "لیجر ریفریش",
    type: "قسم",
    accountName: "اکاؤنٹ نام",
    saveAccount: "اکاؤنٹ محفوظ کریں",
    cashInHand: "کیش ان ہینڈ",
    minCash: "کم از کم کیش",
    maxCash: "زیادہ سے زیادہ کیش",
    lastJvNo: "آخری جے وی نمبر",
    saveSettings: "سیٹنگز محفوظ کریں",
    monthlyFlow: "ماہانہ بہاؤ",
    monthlyFlowNote: "ماہانہ انکم اور پیمنٹس کا مجموعہ۔",
    topAccounts: "اہم اکاؤنٹس",
    topAccountsNote: "منتخب سال کے نمایاں بیلنس۔",
    colDate: "تاریخ",
    colCode: "کوڈ",
    colAccount: "اکاؤنٹ",
    colDescription: "تفصیل",
    colIncome: "انکم",
    colExpense: "پیمنٹ",
    colActions: "اقدامات",
    colSource: "سورس",
    colType: "قسم",
    colEntries: "انٹریز",
    colBalance: "بیلنس",
    edit: "ترمیم",
    delete: "حذف",
    totalEntries: "انٹریز",
    totalIncome: "کل انکم",
    totalExpense: "کل پیمنٹ",
    netBalance: "خالص بیلنس",
    openingBalance: "اوپننگ بیلنس",
    cashSummary: "کیش ان ہینڈ",
    noData: "کوئی ڈیٹا نہیں ملا۔",
    uploadSelectFirst: "پہلے DBF فائلیں منتخب کریں۔",
    pageIncomeTitle: "انکم انٹری",
    pageIncomeNote: "روزانہ انکم انٹری کے لیے یہ صفحہ استعمال کریں۔",
    pageExpenseTitle: "پیمنٹس انٹری",
    pageExpenseNote: "روزانہ پیمنٹس انٹری کے لیے یہ صفحہ استعمال کریں۔",
    pageReportsTitle: "رپورٹس",
    pageReportsNote: "پرنٹ ویو کھولیں یا سالانہ پی ڈی ایف ڈاؤن لوڈ کریں۔",
    pageLedgerTitle: "لیجر",
    pageLedgerNote: "منتخب سال کے اندر فلٹر، جائزہ، ترمیم اور حذف۔",
    pageAccountsTitle: "اکاؤنٹس",
    pageAccountsNote: "ڈیٹا انٹری کے لیے کوڈ لسٹ اور ہیڈز۔",
    pageOverviewTitle: "جائزہ",
    pageOverviewNote: "ماہانہ حرکت اور اہم بیلنس۔",
    pageSettingsTitle: "سیٹنگز",
    pageSettingsNote: "سال کی تاریخ اور کیش سیٹنگز۔",
    loadingYear: "سال لوڈ ہو رہا ہے",
    yearReady: "تیار",
    accountNotFound: "ہیڈ نہیں ملا",
    codeHint: "کوڈ لکھتے ہی متعلقہ اکاؤنٹ ہیڈ خود نظر آ جائے گا۔",
    reportPreview: "رپورٹ پری ویو",
    loginRequired: "لاگ ان ضروری ہے۔",
    annualReportSet: "سالانہ رپورٹ سیٹ",
    annualReportNote: "منتخب سال کے لیے direct print اور PDF بٹن۔",
    currentUser: "موجودہ یوزر",
    cashBook: "کیش بک",
    ledger: "لیجر",
    trialBalance: "ٹرائل بیلنس",
    openingBalanceReport: "اوپننگ بیلنس",
    incomeExpense: "انکم اور ایکسپنس",
  },
};

const VIEW_META = {
  incomeView: ["pageIncomeTitle", "pageIncomeNote"],
  expenseView: ["pageExpenseTitle", "pageExpenseNote"],
  reportsView: ["pageReportsTitle", "pageReportsNote"],
  ledgerView: ["pageLedgerTitle", "pageLedgerNote"],
  accountsView: ["pageAccountsTitle", "pageAccountsNote"],
  overviewView: ["pageOverviewTitle", "pageOverviewNote"],
  settingsView: ["pageSettingsTitle", "pageSettingsNote"],
};

const REPORT_META = [
  ["ledger", "ledger", "Yearly ledger with date, code, account, receipt, voucher, and balance movement."],
  ["cashbook", "cashBook", "Full cash book for the selected year with running balance and print-ready export."],
  ["trial-balance", "trialBalance", "Trial balance for the chosen year with income, expense, and closing balance."],
  ["opening-balance", "openingBalanceReport", "Opening balance report for the selected year and starting period."],
  ["income-expense", "incomeExpense", "Income and expense summary from date to date inside the selected year."],
];

function t(key) {
  return I18N[state.language]?.[key] || I18N.en[key] || key;
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}

function money(value) {
  const locale = state.language === "ur" ? "ur-PK" : "en-US";
  return new Intl.NumberFormat(locale, {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  }).format(Number(value || 0));
}

async function api(url, options = {}) {
  const headers = { credentials: "same-origin", ...options };
  if (!(options.body instanceof FormData)) {
    headers.headers = { "Content-Type": "application/json", ...(options.headers || {}) };
  }
  const response = await fetch(url, headers);
  if (!response.ok) {
    let message = `Request failed: ${response.status}`;
    try {
      const payload = await response.json();
      if (payload.error) message = payload.error;
    } catch {}
    throw new Error(message);
  }
  const contentType = response.headers.get("content-type") || "";
  return contentType.includes("application/json") ? response.json() : response.text();
}

function setStatus(message) {
  document.getElementById("statusText").textContent = message;
}

function switchView(viewId) {
  state.activeView = viewId;
  document.querySelectorAll(".nav-button").forEach((button) => {
    button.classList.toggle("is-active", button.dataset.view === viewId);
  });
  document.querySelectorAll(".view").forEach((view) => {
    view.classList.toggle("is-active", view.id === viewId);
  });
  document.querySelectorAll(".mini-switch").forEach((button) => {
    button.classList.toggle("is-active", button.dataset.viewTarget === viewId);
  });
  const [titleKey, noteKey] = VIEW_META[viewId];
  document.getElementById("pageTitle").textContent = t(titleKey);
  document.getElementById("pageNote").textContent = t(noteKey);
}

function applyLanguage() {
  document.documentElement.lang = state.language === "ur" ? "ur" : "en";
  document.documentElement.dir = state.language === "ur" ? "rtl" : "ltr";
  document.body.classList.toggle("rtl", state.language === "ur");
  document.querySelectorAll("[data-i18n]").forEach((node) => {
    node.textContent = t(node.dataset.i18n);
  });
  renderAll();
}

function mapAccounts(rows) {
  state.accountMap = new Map(rows.map((row) => [String(row.code).padStart(3, "0"), row]));
}

function accountLabel(code) {
  const row = state.accountMap.get(String(code || "").padStart(3, "0"));
  if (!row) return t("accountNotFound");
  return `${row.name || ""}${row.atype ? ` (${row.atype})` : ""}`;
}

async function lookupAccountHead(code) {
  const normalized = String(code || "").trim().padStart(3, "0");
  if (!normalized || normalized === "000") return "";
  const local = state.accountMap.get(normalized);
  if (local) return `${local.name || ""}${local.atype ? ` (${local.atype})` : ""}`;
  try {
    const payload = await api(`/api/account-head?code=${encodeURIComponent(normalized)}`);
    const row = payload.account;
    if (!row) return t("accountNotFound");
    state.accountMap.set(normalized, row);
    return `${row.name || ""}${row.atype ? ` (${row.atype})` : ""}`;
  } catch {
    return t("accountNotFound");
  }
}

function bindCodeLookup(formId) {
  const form = document.getElementById(formId);
  if (!form) return;
  const codeInput = form.code;
  const headInput = form.head_name;
  const sync = async () => {
    const code = String(codeInput.value || "").trim();
    if (!code) {
      headInput.value = "";
      return;
    }
    const label = await lookupAccountHead(code);
    if (String(codeInput.value || "").trim().padStart(3, "0") === String(code).padStart(3, "0")) {
      headInput.value = label;
    }
  };
  if (!codeInput.dataset.lookupBound) {
    codeInput.addEventListener("input", sync);
    codeInput.dataset.lookupBound = "1";
  }
  sync();
}

function statCards() {
  const summary = state.dashboard?.summary || {};
  const settings = state.dashboard?.settings || {};
  return [
    [t("totalEntries"), summary.entries_count || 0],
    [t("totalIncome"), money(summary.total_income)],
    [t("totalExpense"), money(summary.total_payment)],
    [t("netBalance"), money(summary.balance)],
    [t("openingBalance"), money(summary.opening_balance)],
    [t("cashSummary"), money(settings.cash_in_hand || 0)],
  ];
}

function renderSummary() {
  const node = document.getElementById("summaryGrid");
  if (!node) return;
  node.innerHTML = statCards()
    .map(([label, value]) => `<article class="stat"><label>${escapeHtml(label)}</label><strong>${escapeHtml(value)}</strong></article>`)
    .join("");
}

function renderMonthlyChart() {
  const chart = document.getElementById("monthlyChart");
  if (!chart) return;
  const rows = state.dashboard?.monthly || [];
  if (!rows.length) {
    chart.innerHTML = `<p>${escapeHtml(t("noData"))}</p>`;
    return;
  }
  const max = Math.max(...rows.flatMap((row) => [Number(row.income || 0), Number(row.payment || 0)]), 1);
  chart.innerHTML = rows
    .map((row) => {
      const incomeHeight = Math.max((Number(row.income || 0) / max) * 180, 4);
      const expenseHeight = Math.max((Number(row.payment || 0) / max) * 180, 4);
      return `<div class="chart-col"><div class="bars"><div class="bar income" style="height:${incomeHeight}px"></div><div class="bar expense" style="height:${expenseHeight}px"></div></div><div class="chart-label">${escapeHtml(row.month)}</div></div>`;
    })
    .join("");
}

function actionButtons(entry, mode) {
  return `<div class="row-actions"><button class="mini-button" data-action="edit" data-mode="${escapeHtml(mode)}" data-id="${escapeHtml(entry.id)}">${escapeHtml(t("edit"))}</button><button class="mini-button danger" data-action="delete" data-mode="${escapeHtml(mode)}" data-id="${escapeHtml(entry.id)}">${escapeHtml(t("delete"))}</button></div>`;
}

function renderRows(targetId, rows, valueKey, mode) {
  const body = document.getElementById(targetId);
  if (!body) return;
  body.innerHTML =
    rows
      .map(
        (entry) => `
    <tr>
      <td>${escapeHtml(entry.entry_date || "-")}</td>
      <td>${escapeHtml(entry.code)}</td>
      <td>${escapeHtml(entry.account_name || "-")}</td>
      <td>${escapeHtml(entry.description || "-")}</td>
      <td>${escapeHtml(money(entry[valueKey]))}</td>
      ${mode ? `<td>${actionButtons(entry, mode)}</td>` : ""}
    </tr>
  `,
      )
      .join("") || `<tr><td colspan="${mode ? 6 : 5}">${escapeHtml(t("noData"))}</td></tr>`;
}

function renderTopAccounts() {
  const body = document.getElementById("topAccountsBody");
  if (!body) return;
  body.innerHTML =
    (state.dashboard?.top_accounts || [])
      .map(
        (row) => `
    <tr>
      <td>${escapeHtml(row.code)}</td>
      <td>${escapeHtml(row.name || "-")}</td>
      <td>${escapeHtml(row.atype || "-")}</td>
      <td class="${Number(row.balance) >= 0 ? "money-positive" : "money-negative"}">${escapeHtml(money(row.balance))}</td>
    </tr>
  `,
      )
      .join("") || `<tr><td colspan="4">${escapeHtml(t("noData"))}</td></tr>`;
}

function renderLedger() {
  const body = document.getElementById("ledgerBody");
  if (!body) return;
  body.innerHTML =
    state.ledger
      .map(
        (entry) => `
    <tr>
      <td>${escapeHtml(entry.entry_date || "-")}</td>
      <td>${escapeHtml(entry.code)}</td>
      <td>${escapeHtml(entry.account_name || "-")}</td>
      <td>${escapeHtml(entry.description || "-")}</td>
      <td>${escapeHtml(money(entry.income))}</td>
      <td>${escapeHtml(money(entry.payment))}</td>
      <td>${escapeHtml(entry.source_file || "-")}</td>
      <td>${actionButtons(entry, entry.income > 0 ? "income" : "expense")}</td>
    </tr>
  `,
      )
      .join("") || `<tr><td colspan="8">${escapeHtml(t("noData"))}</td></tr>`;
}

function renderAccounts() {
  const body = document.getElementById("accountsBody");
  if (!body) return;
  body.innerHTML =
    state.accounts
      .map(
        (row) => `
    <tr data-code="${escapeHtml(row.code)}" data-name="${escapeHtml(row.name || "")}" data-atype="${escapeHtml(row.atype || "")}">
      <td>${escapeHtml(row.code)}</td>
      <td>${escapeHtml(row.name || "-")}</td>
      <td>${escapeHtml(row.atype || "-")}</td>
      <td>${escapeHtml(row.entries_count)}</td>
      <td class="${Number(row.balance) >= 0 ? "money-positive" : "money-negative"}">${escapeHtml(money(row.balance))}</td>
    </tr>
  `,
      )
      .join("") || `<tr><td colspan="5">${escapeHtml(t("noData"))}</td></tr>`;
}

function reportLinkSet(reportType, useCurrentFilters = false) {
  const dateFrom = useCurrentFilters ? document.getElementById("reportFromDate").value : state.settings?.start_date || "";
  const dateTo = useCurrentFilters ? document.getElementById("reportToDate").value : state.settings?.end_date || "";
  const params = new URLSearchParams({ type: reportType, year: state.year });
  if (dateFrom) params.set("date_from", dateFrom);
  if (dateTo) params.set("date_to", dateTo);
  const query = params.toString();
  return {
    print: `/report?${query}`,
    pdf: `/api/report.pdf?${query}`,
    csv: `/api/report.csv?${query}`,
  };
}

function renderAnnualReports() {
  const container = document.getElementById("annualReportCards");
  if (!container) return;
  container.innerHTML = REPORT_META.map(([type, titleKey, note]) => {
    const links = reportLinkSet(type, false);
    return `
      <article class="report-card">
        <div>
          <h4>${escapeHtml(t(titleKey))}</h4>
          <p>${escapeHtml(state.language === "ur" ? t("annualReportNote") : note)}</p>
        </div>
        <div class="report-card-actions">
          <button class="action-button action-muted" type="button" data-report-open="${escapeHtml(type)}">${escapeHtml(t("viewReport"))}</button>
          <a class="action-button action-muted" href="${links.print}" target="_blank" rel="noopener">${escapeHtml(t("openPrint"))}</a>
          <a class="action-button" href="${links.pdf}">${escapeHtml(t("yearlyPdf"))}</a>
          <a class="action-button action-muted" href="${links.csv}">${escapeHtml(t("yearlyCsv"))}</a>
        </div>
      </article>
    `;
  }).join("");
}

function renderReport() {
  const report = state.report;
  document.getElementById("reportPreviewTitle").textContent = report?.title || t("reportPreview");
  document.getElementById("reportPreviewMeta").textContent = report
    ? `${t("selectedYear")}: ${report.year} | ${t("fromDate")}: ${report.date_from || "-"} | ${t("toDate")}: ${report.date_to || "-"}`
    : "-";
  const head = document.getElementById("reportHead");
  const body = document.getElementById("reportBody");
  const summary = document.getElementById("reportSummary");
  if (!report) {
    head.innerHTML = "";
    body.innerHTML = "";
    summary.innerHTML = "";
    return;
  }
  head.innerHTML = `<tr>${report.columns.map((column) => `<th>${escapeHtml(column)}</th>`).join("")}</tr>`;
  body.innerHTML =
    report.rows
      .map((row) => `<tr>${report.columns.map((column) => `<td>${escapeHtml(row[column] ?? "")}</td>`).join("")}</tr>`)
      .join("") || `<tr><td colspan="${report.columns.length}">${escapeHtml(t("noData"))}</td></tr>`;
  summary.innerHTML = [
    [t("totalIncome"), money(report.summary.total_income)],
    [t("totalExpense"), money(report.summary.total_payment)],
    [t("netBalance"), money(report.summary.net_balance)],
  ]
    .map(([label, value]) => `<article class="stat"><label>${escapeHtml(label)}</label><strong>${escapeHtml(value)}</strong></article>`)
    .join("");
  document.getElementById("reportPdfLink").href = report.pdf_url;
  document.getElementById("reportCsvLink").href = report.csv_url;
}

function populateSettings() {
  const form = document.getElementById("settingsForm");
  const settings = state.settings || {};
  form.start_date.value = settings.start_date || "";
  form.end_date.value = settings.end_date || "";
  form.cash_in_hand.value = settings.cash_in_hand ?? 0;
  form.min_cash.value = settings.min_cash ?? 0;
  form.max_cash.value = settings.max_cash ?? 0;
  form.last_jvno.value = settings.last_jvno ?? 0;
  document.getElementById("reportFromDate").value = settings.start_date || "";
  document.getElementById("reportToDate").value = settings.end_date || "";
  document.getElementById("ledgerFromDate").value = settings.start_date || "";
  document.getElementById("ledgerToDate").value = settings.end_date || "";
}

function fillQuickForm(formId, entry = null) {
  const form = document.getElementById(formId);
  form.id.value = entry?.id || "";
  form.entry_date.value = entry?.entry_date || "";
  form.code.value = entry?.code || "";
  form.branch.value = entry?.branch || "G";
  form.category.value = entry?.category || "GENERAL";
  if (form.receipt_no) form.receipt_no.value = entry?.receipt_no ?? "";
  if (form.voucher_no) form.voucher_no.value = entry?.voucher_no ?? "";
  form.jv_no.value = entry?.jv_no ?? "";
  form.description.value = entry?.description || "";
  form.amount.value = formId === "incomeForm" ? entry?.income ?? "" : entry?.payment ?? "";
  form.head_name.value = entry?.code ? accountLabel(entry.code) : "";
}

function resetEntryForms() {
  fillQuickForm("incomeForm");
  fillQuickForm("expenseForm");
}

function rowById(id) {
  const all = [...state.ledger, ...state.quickIncome, ...state.quickExpense];
  return all.find((row) => String(row.id) === String(id));
}

async function loadYears() {
  const payload = await api("/api/years");
  document.getElementById("brandName").textContent = payload.brand_name;
  document.getElementById("loginBrand").textContent = payload.brand_name;
  state.years = payload.years;
  if (!state.year) state.year = payload.years.at(-1)?.year || "";
  const yearSelect = document.getElementById("yearSelect");
  yearSelect.innerHTML = payload.years
    .map((item) => `<option value="${escapeHtml(item.year)}">${escapeHtml(item.year)} (${escapeHtml(item.imported_entries)})</option>`)
    .join("");
  yearSelect.value = state.year;
}

async function loadDashboard() {
  state.dashboard = await api(`/api/dashboard?year=${encodeURIComponent(state.year)}`);
}

async function loadLedger() {
  const params = new URLSearchParams({
    year: state.year,
    query: document.getElementById("ledgerSearch").value.trim(),
    code: document.getElementById("ledgerCode").value.trim(),
    date_from: document.getElementById("ledgerFromDate").value,
    date_to: document.getElementById("ledgerToDate").value,
    mode: document.getElementById("ledgerMode").value,
    limit: "300",
  });
  const payload = await api(`/api/entries?${params.toString()}`);
  state.ledger = payload.entries;
}

async function loadQuickEntries() {
  const income = await api(`/api/quick-entries?year=${encodeURIComponent(state.year)}&mode=income&limit=20`);
  const expense = await api(`/api/quick-entries?year=${encodeURIComponent(state.year)}&mode=expense&limit=20`);
  state.quickIncome = income.entries;
  state.quickExpense = expense.entries;
}

async function loadAccounts() {
  const payload = await api(`/api/accounts?year=${encodeURIComponent(state.year)}`);
  state.accounts = payload.accounts;
  mapAccounts(payload.accounts);
}

async function loadSettings() {
  const payload = await api(`/api/settings?year=${encodeURIComponent(state.year)}`);
  state.settings = payload.settings;
}

async function loadReport() {
  const params = new URLSearchParams({
    year: state.year,
    type: document.getElementById("reportTypeSelect").value,
    date_from: document.getElementById("reportFromDate").value,
    date_to: document.getElementById("reportToDate").value,
  });
  const payload = await api(`/api/report-data?${params.toString()}`);
  state.report = payload.report;
}

async function refreshAll() {
  setStatus(`${t("loadingYear")} ${state.year}...`);
  await Promise.all([loadDashboard(), loadLedger(), loadQuickEntries(), loadAccounts(), loadSettings(), loadReport()]);
  populateSettings();
  renderAll();
  setStatus(`${t("selectedYear")}: ${state.year} ${t("yearReady")}.`);
}

function renderAll() {
  document.getElementById("selectedYearBadge").textContent = state.year || "-";
  document.getElementById("userCaption").textContent = state.user ? `${t("currentUser")}: ${state.user.display_name || state.user.username}` : "-";
  renderSummary();
  renderMonthlyChart();
  renderTopAccounts();
  renderRows("incomeQuickBody", state.quickIncome, "income", "income");
  renderRows("expenseQuickBody", state.quickExpense, "payment", "expense");
  renderLedger();
  renderAccounts();
  renderAnnualReports();
  renderReport();
  switchView(state.activeView);
  bindCodeLookup("incomeForm");
  bindCodeLookup("expenseForm");
}

async function saveQuickEntry(formId, mode) {
  const form = document.getElementById(formId);
  const payload = {
    year: state.year,
    entry_date: form.entry_date.value,
    code: form.code.value,
    branch: form.branch.value,
    category: form.category.value,
    description: form.description.value,
    receipt_no: form.receipt_no ? form.receipt_no.value : "",
    voucher_no: form.voucher_no ? form.voucher_no.value : "",
    jv_no: form.jv_no.value,
    jv_ext: "",
    entry_kind: "C",
    income: mode === "income" ? form.amount.value : 0,
    payment: mode === "expense" ? form.amount.value : 0,
    group_no: "",
    checked_flag: false,
  };
  if (form.id.value) {
    await api(`/api/entries/${form.id.value}`, { method: "PUT", body: JSON.stringify(payload) });
  } else {
    await api("/api/entries", { method: "POST", body: JSON.stringify(payload) });
  }
  fillQuickForm(formId);
  await refreshAll();
}

async function uploadLegacyFiles() {
  const input = document.getElementById("legacyUploadInput");
  const files = Array.from(input.files || []);
  if (!files.length) {
    setStatus(t("uploadSelectFirst"));
    return;
  }
  const formData = new FormData();
  files.forEach((file) => formData.append("files", file, file.name));
  const response = await fetch("/api/upload-legacy", { method: "POST", body: formData, credentials: "same-origin" });
  if (!response.ok) {
    const payload = await response.json();
    throw new Error(payload.error || "Upload failed.");
  }
  input.value = "";
  await loadYears();
  await refreshAll();
}

async function checkSession() {
  const payload = await api("/api/me");
  if (payload.authenticated) {
    state.user = payload.user;
    document.getElementById("loginScreen").classList.add("is-hidden");
    document.getElementById("appShell").classList.remove("is-hidden");
    await loadYears();
    await refreshAll();
    return;
  }
  state.user = null;
  document.getElementById("loginScreen").classList.remove("is-hidden");
  document.getElementById("appShell").classList.add("is-hidden");
}

function bindEvents() {
  document.getElementById("languageSelect").addEventListener("change", (event) => {
    state.language = event.target.value;
    localStorage.setItem("jamia-language", state.language);
    applyLanguage();
  });

  document.getElementById("loginForm").addEventListener("submit", async (event) => {
    event.preventDefault();
    const form = event.currentTarget;
    try {
      await api("/api/login", {
        method: "POST",
        body: JSON.stringify({ username: form.username.value, password: form.password.value }),
      });
      document.getElementById("loginStatus").textContent = "";
      await checkSession();
    } catch (error) {
      document.getElementById("loginStatus").textContent = error.message;
    }
  });

  document.getElementById("logoutButton").addEventListener("click", async () => {
    await api("/api/logout", { method: "POST", body: JSON.stringify({}) });
    state.user = null;
    await checkSession();
  });

  document.querySelectorAll(".nav-button").forEach((button) => {
    button.addEventListener("click", () => switchView(button.dataset.view));
  });

  document.querySelectorAll("[data-view-target]").forEach((button) => {
    button.addEventListener("click", () => switchView(button.dataset.viewTarget));
  });

  document.getElementById("yearSelect").addEventListener("change", async (event) => {
    state.year = event.target.value;
    resetEntryForms();
    await refreshAll();
  });

  document.getElementById("reloadYearButton").addEventListener("click", refreshAll);
  document.getElementById("uploadLegacyButton").addEventListener("click", async () => {
    try {
      await uploadLegacyFiles();
    } catch (error) {
      setStatus(error.message);
    }
  });

  document.getElementById("importYearButton").addEventListener("click", async () => {
    await api("/api/import", { method: "POST", body: JSON.stringify({ year: state.year }) });
    await refreshAll();
  });

  document.getElementById("incomeForm").addEventListener("submit", async (event) => {
    event.preventDefault();
    await saveQuickEntry("incomeForm", "income");
  });

  document.getElementById("expenseForm").addEventListener("submit", async (event) => {
    event.preventDefault();
    await saveQuickEntry("expenseForm", "expense");
  });

  document.querySelectorAll("[data-reset]").forEach((button) => {
    button.addEventListener("click", () => fillQuickForm(button.dataset.reset));
  });

  document.getElementById("ledgerRefreshButton").addEventListener("click", async () => {
    await loadLedger();
    renderLedger();
  });

  document.getElementById("reportTypeSelect").addEventListener("change", async () => {
    await loadReport();
    renderReport();
  });

  document.getElementById("reportLoadButton").addEventListener("click", async () => {
    await loadReport();
    renderReport();
  });

  document.getElementById("reportPrintButton").addEventListener("click", () => {
    if (state.report?.print_url) window.open(state.report.print_url, "_blank", "noopener");
  });

  document.getElementById("annualReportCards").addEventListener("click", async (event) => {
    const target = event.target.closest("[data-report-open]");
    if (!target) return;
    document.getElementById("reportTypeSelect").value = target.dataset.reportOpen;
    document.getElementById("reportFromDate").value = state.settings?.start_date || "";
    document.getElementById("reportToDate").value = state.settings?.end_date || "";
    await loadReport();
    renderReport();
    switchView("reportsView");
  });

  document.getElementById("accountForm").addEventListener("submit", async (event) => {
    event.preventDefault();
    const form = event.currentTarget;
    await api("/api/accounts", {
      method: "POST",
      body: JSON.stringify({ code: form.code.value, name: form.name.value, atype: form.atype.value }),
    });
    form.reset();
    await loadAccounts();
    renderAccounts();
  });

  document.getElementById("accountsBody").addEventListener("click", (event) => {
    const row = event.target.closest("tr[data-code]");
    if (!row) return;
    const form = document.getElementById("accountForm");
    form.code.value = row.dataset.code;
    form.name.value = row.dataset.name;
    form.atype.value = row.dataset.atype;
  });

  document.getElementById("settingsForm").addEventListener("submit", async (event) => {
    event.preventDefault();
    const form = event.currentTarget;
    await api(`/api/settings/${encodeURIComponent(state.year)}`, {
      method: "PUT",
      body: JSON.stringify({
        start_date: form.start_date.value,
        end_date: form.end_date.value,
        cash_in_hand: form.cash_in_hand.value,
        min_cash: form.min_cash.value,
        max_cash: form.max_cash.value,
        last_jvno: form.last_jvno.value,
      }),
    });
    await loadSettings();
    populateSettings();
    renderAnnualReports();
    await loadReport();
    renderReport();
  });

  document.body.addEventListener("click", async (event) => {
    const target = event.target.closest("[data-action]");
    if (!target) return;
    const entry = rowById(target.dataset.id);
    if (!entry) return;
    if (target.dataset.action === "edit") {
      const formId = target.dataset.mode === "income" ? "incomeForm" : "expenseForm";
      fillQuickForm(formId, entry);
      switchView(target.dataset.mode === "income" ? "incomeView" : "expenseView");
      return;
    }
    if (target.dataset.action === "delete") {
      await api(`/api/entries/${target.dataset.id}?year=${encodeURIComponent(state.year)}`, { method: "DELETE" });
      await refreshAll();
    }
  });
}

(async function init() {
  state.language = localStorage.getItem("jamia-language") || "en";
  document.getElementById("languageSelect").value = state.language;
  bindEvents();
  applyLanguage();
  await checkSession();
})();
