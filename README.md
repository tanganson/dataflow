# Dataflow Manager

通用資料清洗工具，支援 CSV、Excel、JSON、Parquet、Feather，自動建立真實 SQL 欄位。

---

## 安裝

```bash
pip install -e /Users/anson/Dataflow_manager
```

`settings.py`：
```python
INSTALLED_APPS = [..., 'dataflow']
MIDDLEWARE = [..., 'dataflow.middleware.RefreshDynamicAdminsMiddleware']
```

`urls.py`：
```python
path('dataflow/', include('dataflow.urls'))
```

```bash
python manage.py migrate
```

---

## CLI

```bash
python manage.py dataflow <指令> [選項]
```

| 指令 | 說明 | 常用選項 |
|------|------|----------|
| `run <file>` | 匯入檔案到 DB | `--name`, `--rules`, `--replace`, `--dry-run` |
| `clean <name>` | 重新清洗已匯入資料 | `--rules` |
| `export <name>` | 匯出資料集 | `-o result.csv/xlsx/json` |
| `delete <name>` | 刪除資料集 | — |
| `report <name>` | 查看摘要 | — |
| `list` | 列出所有資料集 | — |
| `rules <file>` | 從 CSV 產生規則檔 | `-o rules.json`, `-s 500` |

**常見用法：**
```bash
python manage.py dataflow run data.csv --name "課程" --replace
python manage.py dataflow run data.csv --name "課程" --rules rules.json
python manage.py dataflow export "課程" -o result.csv
python manage.py dataflow rules data.csv -o rules.json
```

---

## 規則檔

```json
{
    "id":       {"type": "int", "required": true},
    "name":     {"type": "string"},
    "email":    {"type": "email"},
    "score":    {"type": "float", "min": 0, "max": 100},
    "reg_date": {"type": "date"},
    "is_vip":   {"type": "boolean", "default": false}
}
```

支援型別：`string` `upper` `lower` `email` `phone` `url` `image` `int` `float` `decimal` `date` `datetime` `boolean`
