# Madrasa Accounts Modern

یہ ایک modern web-based version ہے جو legacy FoxPro DOS accounting files سے data import کرتا ہے اور اسے ایک نئی UI میں دکھاتا ہے۔

## کیا شامل ہے

- `C:\vDos\acc` سے legacy DBF data import
- browser سے DBF files upload کر کے import
- year selector
- dashboard with totals, opening balance, cash in hand
- ledger search
- account heads summary
- yearly control settings
- نئی entries اور account updates کے لیے SQLite-based modern layer
- CSV export

## Legacy files جن سے app data اٹھاتا ہے

- `JIIDYYYY.DBF` ledger entries
- `JIICYYYY.DBF` year control settings
- `JIICODED.DBF` account heads

## Run

```powershell
python .\legacy_madrasa_app.py
```

پھر browser میں یہ URL کھولیں:

```text
http://127.0.0.1:8765
```

اگر یہ port استعمال ہو رہی ہو تو app اگلی available port لے لے گی۔

## Important

- legacy DBF files کو app read کرتا ہے، overwrite نہیں کرتا
- uploaded files `uploaded_legacy` folder میں محفوظ ہو جاتی ہیں
- modern edits `madrasa_modern.sqlite3` میں محفوظ ہوتے ہیں
- دوبارہ import کرنے سے legacy rows refresh ہوں گی، لیکن modern manually-added rows برقرار رہیں گی
