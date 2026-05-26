# Dataflow Manager

通用資料清洗工具，自動建立真實 SQL 欄位，支援 CSV、Excel、JSON、Parquet、Feather 格式。

每個 Dataset 匯入時自動建立**真實 DB table**，欄位使用真正的 SQL 型別（IntegerField / FloatField / DateField / CharField），Django Admin 可看到所有欄位。

---

## 環境初始化

```bash
pip install django pandas openpyxl pyarrow   # 安裝依賴
python manage.py migrate                      # 建立資料庫
```

**Admin 帳號（選用）**

```bash
PYTHONPATH=. python -c "
import os; os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
import django; django.setup()
from django.contrib.auth.models import User
User.objects.create_superuser('admin', 'admin@example.com', 'admin123')
"
python manage.py runserver                    # http://localhost:8000/admin
```

---

## 四大操作

> 所有操作統一使用 `pipeline.py`。輸出檔案自動放入 `output/`，規則檔自動放入 `rules/`。
> 匯入時自動建立真實 SQL table，欄位具備正確型別。

### 1. 輸出數據

先匯出資料庫中現有資料（備份）。匯出時直接從動態 SQL table 讀取，型別保留。

```bash
# 匯出到 output/ 目錄
python pipeline.py export "資料集名稱" -o result.csv
python pipeline.py export "資料集名稱" -o result.xlsx
python pipeline.py export "資料集名稱" -o result.json

# 查看有哪些資料集可匯出
python pipeline.py list
```

---

### 2. 清洗資料庫中的舊數據

使用新規則重新清洗已儲存的資料，無需重新指定原始檔案。

```bash
# 使用新規則重新清洗已儲存的資料
python pipeline.py clean "資料集名稱" --rules rules/strict_rules.json

# 刪除不需要的資料集（含動態 table）
python pipeline.py delete "資料集名稱"

# 查看資料集摘要（含欄位型別、筆數、清洗記錄）
python pipeline.py report "資料集名稱"

# 列出所有資料集
python pipeline.py list
```

---

### 3. 格式化 Dataset

自動推斷或套用規則後，資料會被標準化，並寫入對應的 SQL 型別欄位：

| 原始值 | 格式化後 | SQL 型別 |
|--------|---------|----------|
| `" S001 "` | `"S001"` | `CharField` |
| `" YES "` | `true` | `BooleanField` |
| `" 85.5 "` | `85.5` | `FloatField` |
| `"2023/09/02"` | `2023-09-02` | `DateField` |
| `"105"`（score, max=100） | `100.0` | `FloatField` |

**手動產生規則檔：**

```bash
python pipeline.py rules data.csv -o movie_rules.json
# → rules/movie_rules.json 生成後可編輯
```

規則檔格式（與動態 table 欄位型別直接對應）：

```json
{
    "id":       {"type": "int", "required": true},
    "name":     {"type": "string", "required": true},
    "email":    {"type": "email", "required": true},
    "score":    {"type": "float", "min": 0, "max": 100, "default": 0.0},
    "reg_date": {"type": "date"},
    "is_vip":   {"type": "boolean", "default": false}
}
```

---

### 4. 輸入新數據

匯入時自動：推斷型別 → 建立 `DatasetSchema` → 建立真實 SQL table → 寫入資料。

```bash
# 自動推斷規則（最簡單）— 每個欄位建立對應型別的 SQL column
python pipeline.py run data.csv --name "資料集名稱"

# 使用自訂規則檔 — 欄位型別由規則決定
python pipeline.py run data.csv --name "資料集名稱" --rules rules/movie_rules.json

# 取代同名舊資料（自動 drop + recreate 動態 table）
python pipeline.py run data.csv --name "資料集名稱" --replace

# 匯入同時匯出清洗結果
python pipeline.py run data.csv --name "資料集" --export cleaned.csv
```

| 參數 | 說明 |
|------|------|
| `file` | 資料檔案（`.csv` `.xlsx` `.json` `.parquet` `.feather`） |
| `--name`, `-n` | 資料集名稱（必填） |
| `--rules`, `-r` | 規則檔路徑（省略則自動推斷） |
| `--replace` | 取代同名資料集（含動態 table） |
| `--export`, `-o` | 同時匯出清洗結果到 `output/` |

**流程：** `LOAD → RULES → CLEAN → FORMAT → STORE (dual-write)`

```
LOAD       讀取檔案，自動識別格式
  ↓
RULES      取樣推斷每欄型別（int/float/date/email/boolean/string）
  ↓
CLEAN      逐行驗證：型別轉換、範圍限制、必填檢查
  ↓
FORMAT     統一字串 trim、日期轉 ISO、數值 clamp
  ↓
STORE      ├─ DataRecord（JSON 備份）
           └─ 動態 SQL table（真實型別，含 IntegerField / FloatField / DateField / BooleanField）
```

---

## Admin 查看

啟動 `python manage.py runserver`，打開 `http://localhost:8001/admin`：

| 表格 | 內容 |
|------|------|
| **Datasets** | 每次匯入建立一筆，記錄名稱和時間 |
| **DatasetSchemas** | 每筆 Dataset 的欄位定義（名稱、型別、限制） |
| **DynamicDataset_X** | 每個 Dataset 的**真實 SQL table**，欄位具備正確型別 |
| **DataRecords** | JSON 備份（向後相容） |
| **CleaningLogs** | 清洗統計（總筆數、成功/失敗數、每行錯誤明細） |

> 動態 table 在 server 重啟後自動重建，不影響已存資料。

---

## 內建清洗型別

| 型別 | SQL 型別 | 說明 | 可用參數 |
|------|---------|------|------|
| `string` | `CharField(500)` | 去除前後空白 | — |
| `upper` | `CharField(500)` | 轉大寫 | — |
| `lower` | `CharField(500)` | 轉小寫 | — |
| `email` | `CharField(255)` | 驗證 email 格式 | — |
| `phone` | `CharField(20)` | 只保留數字，最多 15 碼 | — |
| `int` | `IntegerField` | 整數 | `default`, `min`, `max` |
| `float` | `FloatField` | 浮點數 | `default`, `min`, `max` |
| `decimal` | `DecimalField` | 精確小數 | `default`, `min` |
| `date` | `DateField` | 日期轉 date 物件 | `formats` |
| `datetime` | `DateTimeField` | 日期時間轉 datetime | `formats` |
| `boolean` | `BooleanField` | true/false, yes/no, 1/0 | `default` |

---

## 目錄結構

```
Dataflow_manager/
├── pipeline.py            # 一鍵 ETL（主要工具）
├── auto_rules.py          # 自動推斷規則
├── core/
│   ├── data_processor.py  # 清洗引擎 + 底層 API
│   ├── schema_manager.py  # 動態 Model 工廠 + SchemaEditor DDL
│   ├── models.py          # Dataset / DataRecord / CleaningLog / DatasetSchema
│   └── admin.py           # Admin（含動態 model 註冊）
├── rules/                 # 規則檔自動存放處
├── output/                # 匯出檔案自動存放處
└── config/                # Django 設定
```
