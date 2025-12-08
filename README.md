# SQL Server to MySQL Migration Tool with CDC

A robust, GUI-based ETL application written in Python (PyQt5) designed to migrate data from Microsoft SQL Server to MySQL in real-time. This tool handles both the initial full load of data and continuous incremental updates using SQL Server's Change Data Capture (CDC) mechanism.

[![Latest Release](https://img.shields.io/github/v/release/mhesham2000/migration-tool-sqlserver-to-mysql)](https://github.com/mhesham2000/migration-tool-sqlserver-to-mysql/releases/latest)

<img width="1198" height="835" alt="Image" src="https://github.com/user-attachments/assets/66c4a821-bb0b-4e00-a0e3-70e340cc7665" />

## 🚀 Features

* **Hybrid Migration Strategy:**
    * **Full Load:** Efficiently moves existing data using pagination (`TOP 100`) and handles duplicates via `ON DUPLICATE KEY UPDATE`.
    * **Incremental Load:** Uses SQL Server CDC (Change Data Capture) to replicate Inserts, Updates, and Deletes in near real-time.
* **Resilience & State Management:**
    * Automatically tracks progress (Last Primary Key & Last LSN) in a MySQL `migration_log` table.
    * Resumes from where it left off after a restart.
    * Auto-reconnect logic for handling network drops.
* **Modern GUI (PyQt5):**
    * **Database Browser:** Browse SQL Server schemas, tables, and columns dynamically.
    * **Column Mapping:** Select specific columns to include or exclude.
    * **Themes:** Toggle between Dark and Light modes.
* **Admin & Sync Tools:**
    * **CDC Management:** Enable/Disable CDC on the database or specific tables directly from the UI.
    * **Data Sync:** Compare row counts between Source and Destination and trigger "repair" syncs for missing data based on Primary Keys.

## 📋 Prerequisites

* **OS:** Windows (preferred) or Linux.
* **Python:** 3.8+
* **Database Drivers:**
    * [ODBC Driver 17 for SQL Server](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server).
* **Source Database:** SQL Server (Standard/Enterprise/Developer).
* **Destination Database:** MySQL 5.7 or 8.0+.

## 🛠️ Installation

1.  **Clone the repository:**
    ```bash
    git clone github.com/mhesham2000/migration-tool-sqlserver-to-mysql.git
    cd sql-migration-tool
    ```

2.  **Install Python dependencies:**
    ```bash
    pip install PyQt5 pyodbc mysql-connector-python
    ```

## 📖 Usage

### 1. Configuration & Connection
Connect to your SQL Server instance, browse the object tree, and select the tables you wish to migrate. You can also select specific columns if you don't need the entire table.

Configure your destination MySQL credentials on the right panel. The tool will automatically create the database and tables if they don't exist.

### 2. Monitoring Process
Once started, switch to the **Log** tab to watch the migration in real-time. You will see detailed logs of inserted rows, primary keys processing, and any connection retries.

<img width="1204" height="841" alt="Image" src="https://github.com/user-attachments/assets/7daa9cad-639a-4c98-acc1-2fdeb723ff68" />

## ⚙️ Administration & Tools

### Admin Tab (CDC Management)
This tab allows you to manage the Change Data Capture status on the SQL Server without needing to open SSMS.
* **Check CDC Status:** Verifies if the database is CDC-enabled.
* **Activate/Deactivate CDC:** Toggles CDC on the DB and selected tables.
* **Show CDC Table Sizes:** Displays the storage footprint of the change tables.
  
<img width="1202" height="838" alt="Image" src="https://github.com/user-attachments/assets/34c485f4-bf83-40dd-a527-6cc362f43118" />

### Sync Tab (Data Integrity)
Use this tab to ensure data consistency between Source and Destination.
* **Get Row Counts:** Compares `COUNT(*)` on both sides. Discrepancies are highlighted in **Red**.
* **Sync Button:** Clicking 'Sync' on a mismatched table performs a Primary Key diff (Set Difference) to identify and fetch only the missing rows.


<img width="1199" height="844" alt="Image" src="https://github.com/user-attachments/assets/03b351e3-3172-48c4-9c59-7105c3e6360d" />

## 📂 Configuration File

The tool automatically generates a `config.json` file. You can back this up or edit it manually if needed:

```json
{
    "sql_server": "LOCALHOST",
    "mysql_host": "127.0.0.1",
    "mysql_db_name": "migration_dest",
    "mysql_user": "root",
    "mysql_password": "password",
    "source_db": "SourceDB",
    "tables": "Users,Orders",
    "Users": {
        "columns": "id,name,email"
    }
}
