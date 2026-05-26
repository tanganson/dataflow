# CLI Reference Manual

所有操作統一使用 `pipeline.py`。

```
python pipeline.py {run,clean,export,report,list,rules} [options]
```

---

## `run` — 輸入新數據

讀取檔案、推斷型別、清洗、儲存到資料庫。

```bash
python pipeline.py run <file> [--name NAME] [--rules RULES] [--replace] [--export EXPORT]
```

| 參數 | 簡寫 | 必填 | 說明 |
|------|------|------|------|
| `file` | — | 是 | 資料檔案（`.csv` `.xlsx` `.json` `.parquet` `.feather`） |
| `--name` | `-n` | 否 | 資料集名稱（省略則自動從檔名推導） |
| `--rules` | `-r` | 否 | 規則檔路徑（省略則自動推斷型別） |
| `--replace` | — | 否 | 取代同名舊資料集（含動態 SQL table） |
| `--export` | `-o` | 否 | 同時匯出清洗結果到 `output/` |

**範例：**

```bash
python pipeline.py run data.csv                          # 最簡單：自動推斷 + 自動命名
python pipeline.py run data.csv --name "用戶資料"         # 指定資料集名稱
python pipeline.py run data.csv -n "電影" -r rules/movie_rules.json  # 使用自訂規則
python pipeline.py run data.csv -n "電影" --replace       # 取代舊資料
python pipeline.py run data.csv -n "電影" -o cleaned.csv  # 匯入同時匯出
```

---

## `clean` — 清洗資料庫中的舊數據

從資料庫取出原始資料，套用新規則重新清洗並取代舊資料。無需原始檔案。

```bash
python pipeline.py clean <name> --rules <RULES>
```

| 參數 | 簡寫 | 必填 | 說明 |
|------|------|------|------|
| `name` | — | 是 | 要重新清洗的資料集名稱 |
| `--rules` | `-r` | 是 | 新的規則檔路徑 |

**範例：**

```bash
python pipeline.py clean "電影" --rules rules/strict_rules.json
```

---

## `export` — 輸出數據

從資料庫匯出已儲存的資料集到 `output/` 目錄。直接從動態 SQL table 讀取，型別保留。

```bash
python pipeline.py export <name> --output <OUTPUT>
```

| 參數 | 簡寫 | 必填 | 說明 |
|------|------|------|------|
| `name` | — | 是 | 要匯出的資料集名稱 |
| `--output` | `-o` | 是 | 輸出檔名（支援 `.csv` `.xlsx` `.json` `.parquet`） |

**範例：**

```bash
python pipeline.py export "電影" -o movies.csv
python pipeline.py export "電影" -o movies.xlsx
python pipeline.py export "電影" -o movies.json
```

--- 

## `delete` — 刪除資料集

刪除資料集、動態 SQL table 及所有相關記錄。

```bash
python pipeline.py delete <name>
```

| 參數 | 簡寫 | 必填 | 說明 |
|------|------|------|------|
| `name` | — | 是 | 要刪除的資料集名稱 |

**範例：**

```bash
python pipeline.py delete "電影"
```

---

## `report` — 查看資料集摘要

顯示資料集的欄位型別、筆數、清洗記錄。

```bash
python pipeline.py report <name>
```

| 參數 | 簡寫 | 必填 | 說明 |
|------|------|------|------|
| `name` | — | 是 | 資料集名稱 |

**範例：**

```bash
python pipeline.py report "電影"
```

---

## `list` — 列出所有資料集

```bash
python pipeline.py list
```

顯示 ID、名稱、記錄數、更新時間。

---

## `rules` — 產生規則檔

從 CSV 自動推斷每欄型別，產生可編輯的規則 JSON 檔到 `rules/` 目錄。

```bash
python pipeline.py rules <file> [--output OUTPUT] [--sample SAMPLE]
```

| 參數 | 簡寫 | 必填 | 說明 |
|------|------|------|------|
| `file` | — | 是 | CSV 檔案路徑 |
| `--output` | `-o` | 否 | 輸出規則檔名（省略則自動命名為 `<檔名>_rules.json`） |
| `--sample` | `-s` | 否 | 用於型別推斷的取樣筆數（預設 200） |

**範例：**

```bash
python pipeline.py rules data.csv                         # → rules/data_rules.json
python pipeline.py rules data.csv -o movie_rules.json     # → rules/movie_rules.json
python pipeline.py rules data.csv -s 500                  # 取樣 500 筆
```

---

## 規則檔格式

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

### 支援型別

| 型別 | SQL 型別 | 可用參數 |
|------|---------|------|
| `string` | `CharField(500)` | — |
| `upper` | `CharField(500)` | — |
| `lower` | `CharField(500)` | — |
| `email` | `CharField(255)` | — |
| `phone` | `CharField(20)` | — |
| `int` | `IntegerField` | `default`, `min`, `max` |
| `float` | `FloatField` | `default`, `min`, `max` |
| `decimal` | `DecimalField` | `default`, `min` |
| `date` | `DateField` | `formats` |
| `datetime` | `DateTimeField` | `formats` |
| `boolean` | `BooleanField` | `default` |

---

## 四大操作對應

| 操作 | CLI 指令 |
|------|----------|
| 1. 輸出數據 | `export`, `list` |
| 2. 清洗舊數據 | `clean`, `report`, `list`, `delete` |
| 3. 格式化 Dataset | `rules` |
| 4. 輸入新數據 | `run` |
