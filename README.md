# Dataflow Manager

通用資料清洗工具，自動建立真實 SQL 欄位，支援 CSV、Excel、JSON、Parquet、Feather 格式。

每個 Dataset 匯入時自動建立**真實 DB table**，欄位使用真正的 SQL 型別（IntegerField / FloatField / DateField / CharField），Django Admin 可看到所有欄位。

---

## 安裝（在其他 Django 專案中使用）

```bash
pip install -e /path/to/dataflow_manager
```

在 `settings.py` 加入：

```python
INSTALLED_APPS = [
    ...
    'dataflow',
]

MIDDLEWARE = [
    ...
    'dataflow.middleware.RefreshDynamicAdminsMiddleware',
]
```

在 `urls.py` 中加入路由（掛在子路徑下，不影響宿主專案首頁）：

```python
from django.urls import path, include

urlpatterns = [
    path('admin/', admin.site.urls),
    path('dataflow/', include('dataflow.urls')),  # UI 在 /dataflow/
]
```

```bash
python manage.py migrate
```

---

## CLI 指令

> 所有操作統一使用 `manage.py`。

### 在本專案中（Demo）

```bash
python pipeline.py run data.csv --name "DatasetName"
```

或使用 Django management command：

```bash
python manage.py dataflow run data.csv --name "DatasetName"
```

### 在其他專案中（pip install）

統一使用 `manage.py`：

```bash
python manage.py dataflow run data.csv --name "DatasetName"
python manage.py dataflow export "Name" -o result.csv
python manage.py dataflow report "Name"
python manage.py dataflow list
python manage.py dataflow clean "Name" --rules rules.json
python manage.py dataflow delete "Name"
python manage.py dataflow rules data.csv -o rules.json
```

---

## 四大操作

### 1. 輸出數據

```bash
python manage.py dataflow export "DatasetName" -o result.csv
python manage.py dataflow export "DatasetName" -o result.xlsx
python manage.py dataflow export "DatasetName" -o result.json
python manage.py dataflow list
```

### 2. 清洗資料庫中的舊數據

```bash
python manage.py dataflow clean "DatasetName"
python manage.py dataflow clean "DatasetName" --rules rules/strict_rules.json
python manage.py dataflow delete "DatasetName"
python manage.py dataflow report "DatasetName"
python manage.py dataflow list
```

### 3. 格式化 Dataset

```bash
python manage.py dataflow rules data.csv -o movie_rules.json
# → rules/movie_rules.json 生成後可編輯
```

### 4. 輸入新數據

```bash
python manage.py dataflow run data.csv --name "DatasetName"
python manage.py dataflow run data.csv --name "Name" --rules rules/movie_rules.json
python manage.py dataflow run data.csv --name "Name" --replace
python manage.py dataflow run data.csv --name "Name" --export cleaned.csv
python manage.py dataflow run data.csv --dry-run
python manage.py dataflow run data.csv --verbose
```

---

## Admin 查看

啟動 `python manage.py runserver`，打開 `http://localhost:8000/admin`：

| 表格 | 內容 |
|------|------|
| **Datasets** | 每次匯入建立一筆 |
| **DatasetSchemas** | 欄位定義 |
| **DynamicDataset_X** | 真實 SQL table |
| **DataRecords** | JSON 備份 |
| **CleaningLogs** | 清洗統計 |

---

## 內建清洗型別

| 型別 | SQL 型別 | 可用參數 |
|------|---------|------|
| `string` | `CharField(500)` | — |
| `upper` | `CharField(500)` | — |
| `lower` | `CharField(500)` | — |
| `email` | `CharField(255)` | — |
| `phone` | `CharField(20)` | — |
| `url` | `URLField(500)` | 驗證 URL，自動補 https:// |
| `image` | `URLField(500)` | 圖片 URL，同 url 驗證 |
| `int` | `IntegerField` | `default`, `min`, `max` |
| `float` | `FloatField` | `default`, `min`, `max` |
| `decimal` | `DecimalField` | `default`, `min` |
| `date` | `DateField` | `formats` |
| `datetime` | `DateTimeField` | `formats` |
| `boolean` | `BooleanField` | `default` |
