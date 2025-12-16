import sys
import json
import os
from PyQt5.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, 
                             QLabel, QLineEdit, QPushButton, QTextEdit, QTabWidget, 
                             QListWidget, QGroupBox, QCheckBox, QMessageBox, QProgressBar,
                             QTableWidget, QTableWidgetItem, QHeaderView, QSplitter, QComboBox,
                             QTreeWidget, QTreeWidgetItem, QAbstractItemView, QGridLayout, QToolBar,QSystemTrayIcon, QMenu)
from PyQt5.QtCore import Qt, QThread, pyqtSignal, QObject, QTimer
from PyQt5.QtGui import QFont, QPalette, QColor, QIcon,QBrush 
import pyodbc
import mysql.connector
from decimal import Decimal
from datetime import datetime, date, time
import time as t
import msvcrt

LOG_FILE = "migration.log"
CONFIG_FILE = "config.json"
RECONNECT_INTERVAL = 4  # 30 minutes

# Create a QObject-based logger that can properly handle signals
class Logger(QObject):
    log_signal = pyqtSignal(str)
    
    def __init__(self):
        super().__init__()
    
    def log(self, message):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_message = f"[{timestamp}] {message}"
        print(log_message)
        self.log_signal.emit(log_message)
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(log_message + "\n")

# Global logger instance (will be initialized after QApplication is created)
logger = None

def load_config():
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def save_config(config):
    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(config, f, indent=4, ensure_ascii=False)

def resolve_columns(all_columns, selection):
    if selection.strip() == "*":
        return all_columns
    if selection.startswith("*-"):
        excluded = [c.strip() for c in selection[2:].split(",")]
        return [c for c in all_columns if c not in excluded]
    included = [c.strip() for c in selection.split(",")]
    return [c for c in all_columns if c in included]

def convert_value(val):
    if isinstance(val, Decimal):
        return float(val)
    if isinstance(val, (datetime, date, time)):
        return str(val)
    if isinstance(val, bool):
        return int(val)
    return val

def map_type(sql_type):
    sql_type = sql_type.lower()
    if "int" in sql_type:
        return "INT"
    if "decimal" in sql_type or "numeric" in sql_type:
        return "DECIMAL(18,4)"
    if "float" in sql_type or "real" in sql_type:
        return "DOUBLE"
    if "bit" in sql_type:
        return "TINYINT(1)"
    if "date" in sql_type:
        return "DATE"
    if "time" in sql_type:
        return "TIME"
    if "datetime" in sql_type or "smalldatetime" in sql_type:
        return "DATETIME"
    if "char" in sql_type or "text" in sql_type or "nchar" in sql_type or "nvarchar" in sql_type:
        return "VARCHAR(255)"
    return "TEXT"

def connect_sql_server(driver, server, database=None):
    while True:
        try:
            connection_string = f'DRIVER={{{driver}}};' \
                               f'SERVER={server};'
            
            if database:
                connection_string += f'DATABASE={database};'
                
            connection_string += 'Trusted_Connection=yes;'
            
            conn = pyodbc.connect(connection_string, timeout=5)
            logger.log(f"Connected to SQL Server{' database ' + database if database else ''}")
            return conn
        except Exception as e:
            logger.log(f"SQL Server connection failed: {e}. Retrying in 5 sec...")
            t.sleep(5)


# def connect_mysql(mysql_db_name, mysqlhost, mysqluser, mysqlpassword):
#     max_retries = 3
#     for attempt in range(max_retries):
#         try:
#             conn = mysql.connector.connect(
#                 host=mysqlhost,
#                 user=mysqluser,
#                 password=mysqlpassword,
#                 database=mysql_db_name,
#             )
#             logger.log("Connected to MySQL")
#             cur = conn.cursor()
#             cur.execute(f"CREATE DATABASE IF NOT EXISTS `{mysql_db_name}`")
#             cur.execute(f"USE `{mysql_db_name}`")
#             return conn, cur
#         except Exception as e:
#             logger.log(f"MySQL connection failed: {e}. Retrying... (Attempt {attempt + 1}/{max_retries})")
#             t.sleep(5)

def connect_mysql(mysql_db_name, mysqlhost, mysqluser, mysqlpassword):
    while True: # Changed from 'for attempt in range(max_retries):'
        try:
            conn = mysql.connector.connect(
                host=mysqlhost,
                user=mysqluser,
                password=mysqlpassword,
                database=mysql_db_name,
                pool_reset_session=True, 
            )
            logger.log("Connected to MySQL")
            cur = conn.cursor()
            cur.execute(f"CREATE DATABASE IF NOT EXISTS `{mysql_db_name}`")
            cur.execute(f"USE `{mysql_db_name}`")
            return conn, cur
        except Exception as e:
            logger.log(f"MySQL connection failed: {e}. Retrying in 5 sec...") # Simplified log message
            t.sleep(5)
    
    # If all attempts fail, log and return None
    logger.log("Failed to connect to MySQL after multiple attempts. Please check your credentials.")
    return None, None

def ensure_migration_log_table(mysql_cursor, mysql_conn):
    mysql_cursor.execute("""
        CREATE TABLE IF NOT EXISTS `migration_log` (
            `table_name` VARCHAR(255) PRIMARY KEY,
            `last_lsn` BINARY(10),
            `last_pk` VARCHAR(255),
            `full_load_done` TINYINT DEFAULT 0
        )
    """)
    mysql_conn.commit()

def get_primary_keys(sql_cursor, table, columns):
    sql_cursor.execute(f"""
            SELECT k.COLUMN_NAME
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS t
            JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE k
            ON t.CONSTRAINT_NAME = k.CONSTRAINT_NAME
            AND t.TABLE_NAME = k.TABLE_NAME
            WHERE t.TABLE_NAME = '{table}'
            AND t.CONSTRAINT_TYPE = 'PRIMARY KEY';
    """)
    pk_cols = [row[0] for row in sql_cursor.fetchall()]
    if not pk_cols:
        pk_cols = [columns[0][0]]
    return pk_cols

def create_mysql_table_if_not_exists(mysql_cursor, mysql_conn, sql_cursor, sql_server_conn, table, columns, pk_cols, col_names):
    mysql_cursor.execute("SHOW TABLES LIKE %s", (table,))
    if mysql_cursor.fetchone():
        return
    
    # --- MODIFICATION START ---
    
    # 1. Create a dictionary for quick lookup of column metadata (name -> sql_type)
    #    This uses the full 'columns' list fetched from SQL Server
    col_metadata = {name: sql_type for name, sql_type in columns}
    
    cols_def = []
    
    # 2. Iterate ONLY over the selected column names (col_names)
    for name in col_names:
        # Get the type from the metadata dictionary
        sql_type = col_metadata.get(name)
        
        if sql_type is None:
            # Should not happen if col_names is correctly derived from all_columns, 
            # but acts as a safety check.
            logger.log(f"Warning: Selected column '{name}' not found in SQL Server metadata for table '{table}'. Skipping.")
            continue
            
        mysql_type = map_type(sql_type)
        null_def = "NULL" if name not in pk_cols else "NOT NULL"
        cols_def.append(f"`{name}` {mysql_type} {null_def}")
        
    # --- MODIFICATION END ---
    
    # Check if any columns were successfully processed
    if not cols_def:
        logger.log(f"Error: No valid columns selected or processed for table {table}. Skipping table creation.")
        return

    # The rest of the logic remains the same
    pk_def = f", PRIMARY KEY ({', '.join(f'`{c}`' for c in pk_cols if c in col_names)})" 
    
    # Ensure primary keys used in PK definition are actually in the selected columns
    
    create_table_sql = f"CREATE TABLE `{table}` ({', '.join(cols_def)}{pk_def}) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
    mysql_cursor.execute(create_table_sql)
    mysql_conn.commit()

    # Enable CDC with captured_column_list (uses col_names)
    try:
        captured_columns = ",".join([f"[{c}]" for c in col_names])
        sql_cursor.execute(f"""EXEC sys.sp_cdc_enable_table
            @source_schema = 'dbo',
            @source_name = '{table}',
            @role_name = NULL,
            @captured_column_list = '{captured_columns}'
        """)
        sql_server_conn.commit()
        logger.log(f"CDC enabled for table {table} with captured columns: {captured_columns}")
    except Exception as e:
        logger.log(f"CDC already enabled or failed for table {table}: {e}")

    # Enable CDC with captured_column_list
    try:
        captured_columns = ",".join([f"[{c}]" for c in col_names])
        sql_cursor.execute(f"""EXEC sys.sp_cdc_enable_table
            @source_schema = 'dbo',
            @source_name = '{table}',
            @role_name = NULL,
            @captured_column_list = '{captured_columns}'
        """)
        sql_server_conn.commit()
        logger.log(f"CDC enabled for table {table} with captured columns: {captured_columns}")
    except Exception as e:
        logger.log(f"CDC already enabled or failed for table {table}: {e}")

def do_full_load(sql_cursor, mysql_cursor, mysql_conn, table, col_names, pk_cols, last_pk, thread):
    # 1. زيادة حجم الدفعة لزيادة السرعة
    chunk_size = 1000

    applied_changes = 0

    # Get total count for full load progress
    sql_cursor.execute(f"SELECT COUNT(*) FROM {table}")
    total_rows = sql_cursor.fetchone()[0]
    thread.set_progress_range_signal.emit(0, total_rows)

    # 2. تجهيز جملة SQL مرة واحدة خارج الحلقة (Optimization)
    placeholders = ','.join(['%s'] * len(col_names))
    update_clause = ','.join([f"`{c}`=VALUES(`{c}`)" for c in col_names if c not in pk_cols])
    
    # التعامل مع حالة الجداول التي تحتوي على Primary Key فقط (لا يوجد أعمدة للتحديث)
    if update_clause:
        insert_sql = (
            f"INSERT INTO `{table}` ({', '.join(f'`{c}`' for c in col_names)}) "
            f"VALUES ({placeholders}) "
            f"ON DUPLICATE KEY UPDATE {update_clause}"
        )
    else:
         insert_sql = (
            f"INSERT IGNORE INTO `{table}` ({', '.join(f'`{c}`' for c in col_names)}) "
            f"VALUES ({placeholders}) "
        )

    while True:
        # Check stop flag
        if not thread.running: 
            break

        if last_pk is None:
            sql_cursor.execute(f"SELECT TOP {chunk_size} * FROM {table} ORDER BY {pk_cols[0]} ASC")
        else:
            sql_cursor.execute(
                f"SELECT TOP {chunk_size} * FROM {table} WHERE {pk_cols[0]} > ? ORDER BY {pk_cols[0]} ASC",
                (last_pk,)
            )

        rows = sql_cursor.fetchall()
        if not rows:
            break

        # 3. تجهيز البيانات في قائمة (List of Tuples) للدفعات
        batch_data = []
        last_row_obj = None

        for row in rows:
            if not thread.running: break
            
            # تحويل القيم (Decimal, Date, etc.)
            row_values = tuple(convert_value(getattr(row, col)) for col in col_names if hasattr(row, col))
            batch_data.append(row_values)
            last_row_obj = row

        if not batch_data:
            break

        # 4. تنفيذ الدفعة كاملة (Batch Execution)
        try:
            mysql_cursor.executemany(insert_sql, batch_data)
            mysql_conn.commit() # Commit after each batch
        except Exception as e:
            logger.log(f"Error executing batch for table {table}: {e}")
            # في حالة الخطأ، يمكن هنا إضافة منطق للمحاولة مرة أخرى أو التوقف
            thread.error_signal.emit(f"Batch Error: {e}")
            break

        # تحديث العدادات
        batch_len = len(batch_data)
        applied_changes += batch_len
        
        # 5. تحديث الواجهة مرة واحدة فقط لكل دفعة (توفيرًا للموارد)
        thread.progress_signal.emit(applied_changes)

        # تحديث السجلات وحالة آخر PK
        if last_row_obj:
            last_pk = getattr(last_row_obj, pk_cols[0])
            mysql_cursor.execute(
                "UPDATE `migration_log` SET `last_pk`=%s WHERE `table_name`=%s",
                (last_pk, table)
            )
            mysql_conn.commit()
            
            # لوغ مختصر لكل دفعة بدلاً من كل صف
            logger.log(f"Table {table}: Migrated batch of {batch_len} rows. Total: {applied_changes}")

    # Finalize
    if thread.running: 
        mysql_cursor.execute(
            "UPDATE `migration_log` SET `full_load_done`=1 WHERE `table_name`=%s",
            (table,)
        )
        mysql_conn.commit()
        logger.log(f"Table {table} full load COMPLETED. Total rows migrated: {applied_changes}")

    # Ensure progress bar reaches 100%
    thread.progress_signal.emit(total_rows)

def do_incremental_load(sql_cursor, mysql_cursor, mysql_conn, table, col_names, pk_cols, thread):
    # 1. التحقق من تفعيل CDC
    sql_cursor.execute(
        f"SELECT capture_instance FROM cdc.change_tables WHERE source_object_id = OBJECT_ID('{table}')"
    )
    result = sql_cursor.fetchall()
    if not result:
        # logger.log(f"CDC not enabled for table {table}. Skipping incremental...")
        return False

    capture_instance = result[0][0]

    # 2. الحصول على LSNs
    sql_cursor.execute("SELECT sys.fn_cdc_get_max_lsn()")
    max_lsn = sql_cursor.fetchone()[0]

    mysql_cursor.execute("SELECT `last_lsn` FROM `migration_log` WHERE `table_name`=%s", (table,))
    row = mysql_cursor.fetchone()
    last_lsn = row[0] if row else None

    if not last_lsn:
        mysql_cursor.execute("""
            INSERT INTO `migration_log` (`table_name`, `last_lsn`)
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE `last_lsn`=%s
        """, (table, max_lsn, max_lsn))
        mysql_conn.commit()
        return False

    if last_lsn == max_lsn:
        # لا توجد تغييرات جديدة
        return False

    # 3. جلب التغييرات
    cdc_query = f"SELECT * FROM cdc.fn_cdc_get_all_changes_{capture_instance}(?, ?, ?)"
    try:
        sql_cursor.execute(cdc_query, (last_lsn, max_lsn, 'all'))
        rows = sql_cursor.fetchall()
    except Exception as e:
        # أحياناً يكون LSN القديم قد تم حذفه من SQL Server (Cleaned up)
        logger.log(f"CDC Error (LSN mismatch?): {e}. Reseting LSN to current max.")
        mysql_cursor.execute("UPDATE `migration_log` SET `last_lsn`=%s WHERE `table_name`=%s", (max_lsn, table))
        mysql_conn.commit()
        return False

    total_changes = len(rows)
    thread.set_progress_range_signal.emit(0, total_changes)

    if not rows:
        return False # No rows returned

    # 4. تحضير جمل SQL
    placeholders = ','.join(['%s'] * len(col_names))
    update_clause = ','.join([f"`{c}`=VALUES(`{c}`)" for c in col_names if c not in pk_cols])
    
    # جملة الإضافة/التعديل
    upsert_sql = (
        f"INSERT INTO `{table}` ({', '.join(f'`{c}`' for c in col_names)}) "
        f"VALUES ({placeholders}) "
        f"ON DUPLICATE KEY UPDATE {update_clause}"
    )
    
    # جملة الحذف
    where_clause = ' AND '.join([f"`{col}`=%s" for col in pk_cols])
    delete_sql = f"DELETE FROM `{table}` WHERE {where_clause}"

    # 5. معالجة الدفعات بذكاء (Smart Batching)
    upsert_batch = []
    delete_batch = []
    processed_count = 0

    def flush_upserts():
        if upsert_batch:
            try:
                mysql_cursor.executemany(upsert_sql, upsert_batch)
                upsert_batch.clear()
            except Exception as e:
                logger.log(f"Error executing upsert batch: {e}")

    def flush_deletes():
        if delete_batch:
            try:
                mysql_cursor.executemany(delete_sql, delete_batch)
                delete_batch.clear()
            except Exception as e:
                logger.log(f"Error executing delete batch: {e}")

    for i, row in enumerate(rows):
        if not thread.running: break

        operation = getattr(row, '__$operation', 2)
        
        # تحويل البيانات
        row_dict = {col: convert_value(getattr(row, col)) for col in col_names if hasattr(row, col)}

        if operation == 1:  # Delete
            # يجب تنفيذ أي إضافات معلقة أولاً للحفاظ على الترتيب
            flush_upserts()
            
            # تجهيز مفاتيح الحذف
            pk_values = tuple(row_dict[col] for col in pk_cols)
            delete_batch.append(pk_values)
            
        else:  # Insert (2) or Update (4)
            # يجب تنفيذ أي حذوفات معلقة أولاً
            flush_deletes()
            
            # تجهيز بيانات الصف
            row_values = tuple(row_dict[c] for c in col_names)
            upsert_batch.append(row_values)

        processed_count += 1
        
        # تنفيذ الدفعات عند الوصول لحجم معين (مثلاً كل 1000 صف) لتفادي استهلاك الذاكرة
        if len(upsert_batch) >= 1:
            flush_upserts()
            thread.progress_signal.emit(processed_count)
            logger.log(f"Table {table} Insert row with PK={row_dict[pk_cols[0]]}")

            mysql_conn.commit()
            
        if len(delete_batch) >= 1:
            flush_deletes()
            thread.progress_signal.emit(processed_count)
            mysql_conn.commit()

    # تنفيذ المتبقي
    flush_upserts()
    flush_deletes()
    mysql_conn.commit()

    if processed_count > 0:
        logger.log(f"{table} Incremental: Processed {processed_count} changes (Batch Mode).")

    # تحديث LSN في النهاية
    mysql_cursor.execute("""
        INSERT INTO `migration_log` (`table_name`, `last_lsn`)
        VALUES (%s, %s)
        ON DUPLICATE KEY UPDATE `last_lsn`=%s
    """, (table, max_lsn, max_lsn))
    mysql_conn.commit()

    thread.progress_signal.emit(total_changes)
    return True

def migrate_table(sql_cursor, mysql_cursor, mysql_conn, sql_server_conn, table, config):
    logger.log(f"Starting migration for table {table}...")
    
    # Use the table name directly (database context is already set in the connection)
    sql_cursor.execute(f"SELECT TOP 0 * FROM {table}")
    columns = [(col[0], col[1].__name__) for col in sql_cursor.description]

    if table not in config:
        # If we don't have a saved config, use all columns
        col_names = [col[0] for col in columns]
    else:
        # Use the saved column selection
        col_names = resolve_columns([col[0] for col in columns], config[table]["columns"])

    pk_cols = get_primary_keys(sql_cursor, table, columns)
    create_mysql_table_if_not_exists(mysql_cursor, mysql_conn, sql_cursor, sql_server_conn, table, columns, pk_cols, col_names)

    mysql_cursor.execute("SELECT `last_pk`, `full_load_done` FROM `migration_log` WHERE `table_name`=%s", (table,))
    row = mysql_cursor.fetchone()
    if row:
        last_pk, full_load_done = row
    else:
        last_pk, full_load_done = None, 0
        mysql_cursor.execute(
            "INSERT INTO `migration_log` (`table_name`, `last_pk`, `full_load_done`) VALUES (%s, %s, %s)",
            (table, None, 0)
        )
        mysql_conn.commit()

    if full_load_done == 0:
        logger.log(f"Table {table} created in MySQL. Performing full load...")
        do_full_load(sql_cursor, mysql_cursor, mysql_conn, table, col_names, pk_cols, last_pk)

    return do_incremental_load(sql_cursor, mysql_cursor, mysql_conn, table, col_names, pk_cols)


class MigrationThread(QThread):
    log_signal = pyqtSignal(str)
    progress_signal = pyqtSignal(int)
    set_progress_range_signal = pyqtSignal(int, int) # New signal
    finished_signal = pyqtSignal()
    error_signal = pyqtSignal(str)
    
    def __init__(self, config, parent=None):
        super().__init__(parent)
        self.config = config
        self.running = True
        
    def run(self):
        try:
            source_db = self.config['source_db']
            sql_server = self.config['sql_server']
            sql_driver = self.config['sql_driver'] # <--- MUST BE PRESENT
            mysqlhost = self.config['mysql_host']
            mysqluser = self.config['mysql_user']
            mysqlpassword = self.config['mysql_password']
            tables = [t.strip() for t in self.config['tables'].split(',')]
            mysql_db_name = self.config['mysql_db_name']
            
            self.log_signal.emit("Starting migration process...")
            
            # Connect to the specific database
            sql_server_conn = connect_sql_server(sql_driver, sql_server, source_db)
            sql_cursor = sql_server_conn.cursor()
            mysql_conn, mysql_cursor = connect_mysql(mysql_db_name, mysqlhost, mysqluser, mysqlpassword)
            ensure_migration_log_table(mysql_cursor, mysql_conn)
            
            config_data = load_config()
            last_reconnect = t.time()
            attempt = 0
            
            while self.running:
                any_changes = False
                for table in tables:
                    if not self.running:
                        break
                        
                    # try:
                    #     # Use just the table name (database context is already set)
                    #     changed = migrate_table(sql_cursor, mysql_cursor, mysql_conn, sql_server_conn, table, config_data)
                    #     if changed:
                    #         any_changes = True

                    try:
                        # Get table information from the migration log
                        mysql_cursor.execute("SELECT `last_pk`, `full_load_done` FROM `migration_log` WHERE `table_name`=%s", (table,))
                        row = mysql_cursor.fetchone()
                        if row:
                            last_pk, full_load_done = row
                        else:
                            last_pk, full_load_done = None, 0
                            mysql_cursor.execute(
                                "INSERT INTO `migration_log` (`table_name`, `last_pk`, `full_load_done`) VALUES (%s, %s, %s)",
                                (table, None, 0)
                            )
                            mysql_conn.commit()
                            
                        # Get column names for migration
                        sql_cursor.execute(f"SELECT TOP 0 * FROM {table}")
                        columns = [(col[0], col[1].__name__) for col in sql_cursor.description]
                        if table not in config_data:
                            col_names = [col[0] for col in columns]
                        else:
                            col_names = resolve_columns([col[0] for col in columns], config_data[table]["columns"])
                        
                        pk_cols = get_primary_keys(sql_cursor, table, columns)
                        
                        # Perform full load if not completed
                        if full_load_done == 0:
                            logger.log(f"Table {table} created in MySQL. Performing full load...")
                            create_mysql_table_if_not_exists(mysql_cursor, mysql_conn, sql_cursor, sql_server_conn, table, columns, pk_cols, col_names)
                            do_full_load(sql_cursor, mysql_cursor, mysql_conn, table, col_names, pk_cols, last_pk, self)
                        
                        # Always perform incremental load
                        changed = do_incremental_load(sql_cursor, mysql_cursor, mysql_conn, table, col_names, pk_cols, self)
                        if changed:
                            any_changes = True
                            
                    except (pyodbc.Error, mysql.connector.Error) as e:
                        error_msg = f"Error with table {table}: {e}. Retrying connections..."
                        self.log_signal.emit(error_msg)
                        try: 
                            sql_server_conn.close()
                        except: 
                            pass
                        try: 
                            mysql_conn.close()
                        except: 
                            pass
                        # Reconnect to the specific database
                        sql_server_conn = connect_sql_server(sql_driver, sql_server, source_db) # <--- PASS DRIVER
                        sql_cursor = sql_server_conn.cursor()
                        mysql_conn, mysql_cursor = connect_mysql(mysql_db_name, mysqlhost, mysqluser, mysqlpassword)
                        ensure_migration_log_table(mysql_cursor, mysql_conn)
                        continue

                # periodic reconnect
                if t.time() - last_reconnect >= RECONNECT_INTERVAL * 60:  # minutes → seconds
                    self.log_signal.emit("Reconnecting to servers (4 min interval reached)...")
                    try: 
                        sql_server_conn.close()
                    except: 
                        pass
                    try: 
                        mysql_conn.close()
                    except: 
                        pass
                    # Reconnect to the specific database
                    sql_server_conn = connect_sql_server(sql_driver, sql_server, source_db) # <--- PASS DRIVER
                    sql_cursor = sql_server_conn.cursor()
                    self.log_signal.emit("Reconnected to SQL Server")
                    mysql_conn, mysql_cursor = connect_mysql(mysql_db_name, mysqlhost, mysqluser, mysqlpassword)
                    self.log_signal.emit("Reconnected to MySQL")
                    ensure_migration_log_table(mysql_cursor, mysql_conn)
                    last_reconnect = t.time()

                self.log_signal.emit("Cycle complete.")

                if any_changes:
                    attempt = 0
                    self.log_signal.emit("Sleeping 5 seconds (changes applied)...")
                    self.sleep(5)
                else:
                    attempt = self.backoff_sleep(attempt)
                    
        except Exception as e:
            self.error_signal.emit(str(e))
        finally:
            self.finished_signal.emit()
            
    def backoff_sleep(self, attempt, base=15, cap=240):
        wait = min(base * (2 ** attempt), cap)
        self.log_signal.emit(f"Backoff sleeping for {wait} seconds...")
        self.sleep(wait)
        return attempt + 1
        
    def sleep(self, seconds):
        for i in range(seconds * 10):
            if not self.running:
                break
            self.msleep(100)
            
    def stop(self):
        self.running = False

class DatabaseBrowser(QTreeWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setHeaderLabel("SQL Server Objects")
        self.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.itemSelectionChanged.connect(self.on_selection_changed)
        self.sql_conn = None
        self.sql_cursor = None
        
    def connect_to_sql_server(self, driver, server): 
        try:
            self.sql_conn = pyodbc.connect(
                f'DRIVER={{{driver}}};'
                f'SERVER={server};'
                'Trusted_Connection=yes;',
                timeout=5
            )
            self.sql_cursor = self.sql_conn.cursor()
            self.load_databases()
            return True
        except Exception as e:
            QMessageBox.critical(self, "Connection Error", f"Failed to connect to SQL Server: {str(e)}")
            return False
            
    def load_databases(self):
        self.clear()
        self.sql_cursor.execute("SELECT name FROM sys.databases WHERE database_id > 4")  # Skip system databases
        databases = self.sql_cursor.fetchall()
        
        for db in databases:
            db_item = QTreeWidgetItem(self, [db[0]])
            db_item.setData(0, Qt.UserRole, "database")
            # Add a placeholder child to make it expandable
            QTreeWidgetItem(db_item, ["Loading..."])
            
    def load_tables(self, db_item):
        db_name = db_item.text(0)
        # Remove the placeholder child
        db_item.takeChild(0)
        
        try:
            self.sql_cursor.execute(f"USE [{db_name}]")
            self.sql_cursor.execute("""
                SELECT TABLE_NAME 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_TYPE = 'BASE TABLE'
                ORDER BY TABLE_NAME ASC
            """)
            tables = self.sql_cursor.fetchall()
            
            for table in tables:
                table_item = QTreeWidgetItem(db_item, [table[0]])
                table_item.setData(0, Qt.UserRole, "table")
                table_item.setData(0, Qt.UserRole + 1, db_name)  # Store database name
                # Add a placeholder child to make it expandable
                QTreeWidgetItem(table_item, ["Loading..."])
                
        except Exception as e:
            QMessageBox.warning(self, "Error", f"Failed to load tables: {str(e)}")
            
    def load_columns(self, table_item):
        table_name = table_item.text(0)
        db_name = table_item.data(0, Qt.UserRole + 1)  # Get database name from parent
        
        # Remove the placeholder child
        table_item.takeChild(0)
        
        try:
            self.sql_cursor.execute(f"USE [{db_name}]")
            self.sql_cursor.execute(f"""
                SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE 
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_NAME = '{table_name}'
                ORDER BY ORDINAL_POSITION
            """)
            columns = self.sql_cursor.fetchall()
            
            for column in columns:
                col_info = f"{column[0]} ({column[1]}, {'NULL' if column[2] == 'YES' else 'NOT NULL'})"
                col_item = QTreeWidgetItem(table_item, [col_info])
                col_item.setData(0, Qt.UserRole, "column")
                col_item.setData(0, Qt.UserRole + 1, column[0])  # Store column name
                col_item.setCheckState(0, Qt.Checked)
                
        except Exception as e:
            QMessageBox.warning(self, "Error", f"Failed to load columns: {str(e)}")
            
    def on_item_expanded(self, item):
        item_type = item.data(0, Qt.UserRole)
        
        if item_type == "database":
            self.load_tables(item)
        elif item_type == "table":
            self.load_columns(item)
            
    def on_selection_changed(self):
        selected_tables = []
        
        for item in self.selectedItems():
            item_type = item.data(0, Qt.UserRole)
            if item_type == "table":
                db_name = item.data(0, Qt.UserRole + 1)
                table_name = item.text(0)
                selected_tables.append(f"{db_name}.{table_name}")
        
        # Emit signal to parent if it has the method
        parent = self.parent()
        if hasattr(parent, 'on_tables_selected'):
            parent.on_tables_selected(selected_tables)
                
    def get_selected_columns(self, table_path):
        """
        Retrieves a list of selected column names for a given table path (db.table).
        Guaranteed to return a list, not None.
        """
        if '.' not in table_path:
            # Should not happen if tables are added correctly, but as a fallback
            return []

        db_name, table_name = table_path.split('.')
        
        # 1. Find the table item
        for i in range(self.topLevelItemCount()):
            db_item = self.topLevelItem(i)
            if db_item.text(0) == db_name:
                for j in range(db_item.childCount()):
                    table_item = db_item.child(j)
                    
                    # Check if this is the correct table item
                    if table_item.text(0) == table_name:
                        columns = []
                        
                        # Check if columns are loaded (if childCount is 1 and the child is not 'column', it's still loading)
                        if table_item.childCount() == 1 and table_item.child(0).data(0, Qt.UserRole) != "column":
                            # The table was not expanded, or only the "Loading..." placeholder is present.
                            # We cannot determine selection, so we return an empty list, implying default behavior ("*")
                            return []

                        # 2. Iterate through loaded columns and check state
                        for k in range(table_item.childCount()):
                            col_item = table_item.child(k)
                            
                            # Ensure we are dealing with a column item, not a placeholder
                            if col_item.data(0, Qt.UserRole) == "column":
                                if col_item.checkState(0) == Qt.Checked:
                                    # Get the stored column name
                                    columns.append(col_item.data(0, Qt.UserRole + 1)) 
                                    
                        return columns
        
        # 3. If the database or table item was never found in the browser tree, return empty list
        return []
    
    def select_all_columns(self, table_item):
        """Select all columns in a table"""
        for i in range(table_item.childCount()):
            col_item = table_item.child(i)
            col_item.setCheckState(0, Qt.Checked)
    
    def deselect_all_columns(self, table_item):
        """Deselect all columns in a table"""
        for i in range(table_item.childCount()):
            col_item = table_item.child(i)
            col_item.setCheckState(0, Qt.Unchecked)
    
    def get_selected_table_items(self):
        """Get all selected table items"""
        selected_tables = []
        for item in self.selectedItems():
            if item.data(0, Qt.UserRole) == "table":
                selected_tables.append(item)
        return selected_tables

class MigrationGUI(QMainWindow):
    def __init__(self):
        super().__init__()
        self.migration_thread = None
        self.sql_conn = None
        self.sql_cursor = None
        self.auto_start = False
        
        # Define the dark and light palettes here, accessible by all methods
        self.dark_palette = QPalette()
        self.dark_palette.setColor(QPalette.Window, QColor(53, 53, 53))
        self.dark_palette.setColor(QPalette.WindowText, Qt.white)
        self.dark_palette.setColor(QPalette.Base, QColor(25, 25, 25))
        self.dark_palette.setColor(QPalette.AlternateBase, QColor(53, 53, 53))
        self.dark_palette.setColor(QPalette.ToolTipBase, Qt.white)
        self.dark_palette.setColor(QPalette.ToolTipText, Qt.white)
        self.dark_palette.setColor(QPalette.Text, Qt.white)
        self.dark_palette.setColor(QPalette.Button, QColor(53, 53, 53))
        self.dark_palette.setColor(QPalette.ButtonText, Qt.white)
        self.dark_palette.setColor(QPalette.BrightText, Qt.red)
        self.dark_palette.setColor(QPalette.Highlight, QColor(42, 130, 218))
        self.dark_palette.setColor(QPalette.HighlightedText, Qt.black)

        self.light_palette = QPalette()
        self.light_palette.setColor(QPalette.Window, QColor(240, 240, 240))
        self.light_palette.setColor(QPalette.WindowText, QColor(0, 0, 0))
        self.light_palette.setColor(QPalette.Base, QColor(255, 255, 255))
        self.light_palette.setColor(QPalette.AlternateBase, QColor(220, 220, 220))
        self.light_palette.setColor(QPalette.ToolTipBase, QColor(255, 255, 220))
        self.light_palette.setColor(QPalette.ToolTipText, QColor(0, 0, 0))
        self.light_palette.setColor(QPalette.Text, QColor(0, 0, 0))
        self.light_palette.setColor(QPalette.Button, QColor(240, 240, 240))
        self.light_palette.setColor(QPalette.ButtonText, QColor(0, 0, 0))
        self.light_palette.setColor(QPalette.BrightText, QColor(255, 0, 0))
        self.light_palette.setColor(QPalette.Highlight, QColor(100, 180, 255))
        self.light_palette.setColor(QPalette.HighlightedText, QColor(0, 0, 0))

        # Set default theme
        QApplication.instance().setPalette(self.light_palette)

        self.initUI()
        self.load_saved_config()
        self.initTrayIcon()
        
        # Connect the global logger to the GUI log output
        global logger
        logger.log_signal.connect(self.log)
        
        # Set up auto-start timer
        self.auto_start_timer = QTimer(self)
        self.auto_start_timer.setSingleShot(True)
        self.auto_start_timer.timeout.connect(self.auto_start_migration)
        self.auto_start_timer.start(1000)  # Start after 1 second

        # --- NEW: Daily CDC Activation Scheduler ---   
        self.daily_scheduler_timer = QTimer(self)
        self.daily_scheduler_timer.timeout.connect(self.run_daily_cdc_schedule)
        
        # Start the timer to check every minute (60,000 ms)
        self.daily_scheduler_timer.start(60 * 1000) 
        self.log("Started daily CDC schedule timer (checks every minute).")
        # --- END NEW ---

    def initTrayIcon(self):
        # 2. إنشاء أيقونة شريط المهام
        self.tray_icon = QSystemTrayIcon(self)
        self.tray_icon.setIcon(QIcon("icon.ico")) # تأكد من وجود ملف أيقونة
        self.tray_icon.setToolTip("Migration Tool")
        
        # 3. إعداد القائمة
        menu = QMenu()
        show_action = menu.addAction("Show")
        quit_action = menu.addAction("Quit")
        
        show_action.triggered.connect(self.show)
        quit_action.triggered.connect(QApplication.instance().quit)
        
        self.tray_icon.setContextMenu(menu)
        self.tray_icon.show()
        
        # 5. التعامل مع النقرات
        self.tray_icon.activated.connect(self.onTrayIconActivated)
        
    def onTrayIconActivated(self, reason):
        # عند النقر المزدوج على الأيقونة
        if reason == QSystemTrayIcon.DoubleClick:
            self.show()

    # 4. تخصيص حدث الإغلاق
    def closeEvent(self, event):
        # قم بإخفاء النافذة بدلاً من إغلاقها
        event.ignore()
        self.hide()
        # # إظهار رسالة للمستخدم
        # QMessageBox.information(self, "Migration Tool", 
        #                         "The application is running in the background. "
        #                         "To exit, right-click the tray icon and select 'Quit'.")
        
    def initUI(self):
        self.setWindowTitle('Migration Tool')
        self.setGeometry(100, 100, 1200, 800)
        
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        
        layout = QVBoxLayout(central_widget)
        
        # Create a splitter for the main area
        splitter = QSplitter(Qt.Horizontal)
        layout.addWidget(splitter)
        
        # Left panel - Database browser
        left_widget = QWidget()
        left_layout = QVBoxLayout(left_widget)
        
        # SQL Server connection group
        sql_group = QGroupBox("SQL Server Connection")
        sql_group.setObjectName("SQL Server Connection")
        sql_layout = QGridLayout(sql_group)

        # --- MODIFIED DRIVER INPUT ---
        sql_layout.addWidget(QLabel("ODBC Driver:"), 0, 0)
        
        driver_version_layout = QHBoxLayout()
        self.sql_driver_prefix = QLabel("ODBC Driver")
        self.sql_driver_prefix.setStyleSheet("font-weight: bold;")
        driver_version_layout.addWidget(self.sql_driver_prefix)
        
        self.sql_driver_version_combo = QComboBox()
        self.sql_driver_version_combo.addItems(["11", "13", "13.1", "14", "17", "18"])
        self.sql_driver_version_combo.setCurrentText("17") # Set default
        driver_version_layout.addWidget(self.sql_driver_version_combo)
        
        self.sql_driver_suffix = QLabel("for SQL Server")
        self.sql_driver_suffix.setStyleSheet("font-weight: bold;")
        driver_version_layout.addWidget(self.sql_driver_suffix)
        
        driver_version_layout.addStretch(1) # Push elements to the left
        
        sql_layout.addLayout(driver_version_layout, 0, 1, 1, 2)
        # --- END MODIFIED DRIVER INPUT ---
        
        sql_layout.addWidget(QLabel("SQL Server:"), 1, 0)
        self.sql_server_input = QLineEdit()
        sql_layout.addWidget(self.sql_server_input, 1, 1)
        
        self.connect_btn = QPushButton("Connect")
        self.connect_btn.clicked.connect(self.connect_to_sql_server)
        sql_layout.addWidget(self.connect_btn, 1, 2)
        
        left_layout.addWidget(sql_group)
        
        # Database browser toolbar
        browser_toolbar = QHBoxLayout()
        
        self.select_all_btn = QPushButton("Select All Columns")
        self.select_all_btn.clicked.connect(self.select_all_columns)
        self.select_all_btn.setEnabled(False)
        browser_toolbar.addWidget(self.select_all_btn)
        
        self.deselect_all_btn = QPushButton("Deselect All Columns")
        self.deselect_all_btn.clicked.connect(self.deselect_all_columns)
        self.deselect_all_btn.setEnabled(False)
        browser_toolbar.addWidget(self.deselect_all_btn)
        
        self.add_table_btn = QPushButton("Add Selected Table")
        self.add_table_btn.clicked.connect(self.add_selected_table)
        self.add_table_btn.setEnabled(False)
        browser_toolbar.addWidget(self.add_table_btn)
        
        left_layout.addLayout(browser_toolbar)
        
        # Database browser
        self.db_browser = DatabaseBrowser()
        self.db_browser.itemExpanded.connect(self.db_browser.on_item_expanded)
        self.db_browser.itemSelectionChanged.connect(self.on_browser_selection_changed)
        left_layout.addWidget(self.db_browser)
        
        splitter.addWidget(left_widget)
        
        # Right panel - Configuration and log
        right_widget = QWidget()
        right_layout = QVBoxLayout(right_widget)
        
        # Create tabs for right panel
        self.tabs = QTabWidget()
        right_layout.addWidget(self.tabs)
        
        # Configuration tab
        config_tab = QWidget()
        config_layout = QVBoxLayout(config_tab)
        
        # Selected tables group
        selected_tables_group = QGroupBox("Selected Tables")
        selected_tables_group.setObjectName("Selected Tables")
        selected_tables_layout = QVBoxLayout(selected_tables_group)
        
        # Selected tables toolbar
        selected_toolbar = QHBoxLayout()
        
        self.remove_table_btn = QPushButton("Remove Selected")
        self.remove_table_btn.clicked.connect(self.remove_selected_table)
        selected_toolbar.addWidget(self.remove_table_btn)
        
        self.clear_tables_btn = QPushButton("Clear All")
        self.clear_tables_btn.clicked.connect(self.clear_all_tables)
        selected_toolbar.addWidget(self.clear_tables_btn)
        
        selected_tables_layout.addLayout(selected_toolbar)
        
        self.selected_tables_list = QListWidget()
        selected_tables_layout.addWidget(self.selected_tables_list)
        
        config_layout.addWidget(selected_tables_group)
        
        # MySQL connection group
        mysql_group = QGroupBox("MySQL Connection")
        mysql_group.setObjectName("MySQL Connection")
        mysql_layout = QGridLayout(mysql_group)
        
        mysql_layout.addWidget(QLabel("Host:"), 0, 0)
        self.mysql_host_input = QLineEdit()
        mysql_layout.addWidget(self.mysql_host_input, 0, 1)
        
        mysql_layout.addWidget(QLabel("Database:"), 1, 0)
        self.mysql_db_input = QLineEdit()
        mysql_layout.addWidget(self.mysql_db_input, 1, 1)
        
        mysql_layout.addWidget(QLabel("User:"), 2, 0)
        self.mysql_user_input = QLineEdit()
        mysql_layout.addWidget(self.mysql_user_input, 2, 1)
        
        mysql_layout.addWidget(QLabel("Password:"), 3, 0)
        self.mysql_pass_input = QLineEdit()
        self.mysql_pass_input.setEchoMode(QLineEdit.Password)
        mysql_layout.addWidget(self.mysql_pass_input, 3, 1)
        
        config_layout.addWidget(mysql_group)
        
        # Auto-start status
        self.auto_start_status = QLabel("Auto-start: Not configured")
        config_layout.addWidget(self.auto_start_status)
        
        # Buttons
        button_layout = QHBoxLayout()
        self.save_config_btn = QPushButton("Save Configuration")
        self.save_config_btn.clicked.connect(self.save_config)
        button_layout.addWidget(self.save_config_btn)
        
        self.test_connections_btn = QPushButton("Test Connections")
        self.test_connections_btn.clicked.connect(self.test_connections)
        button_layout.addWidget(self.test_connections_btn)
        
        config_layout.addLayout(button_layout)
        
        self.tabs.addTab(config_tab, "Configuration")
        
        # Log tab
        log_tab = QWidget()
        log_layout = QVBoxLayout(log_tab)
        
        self.log_output = QTextEdit()
        self.log_output.setReadOnly(True)
        log_layout.addWidget(self.log_output)
        
        self.tabs.addTab(log_tab, "Log")
        
        # New Admin tab
        admin_tab = QWidget()
        admin_layout = QVBoxLayout(admin_tab)

        db_selection_layout = QHBoxLayout()
        db_selection_layout.addWidget(QLabel("Select Database:"))
        self.db_dropdown = QComboBox()
        self.db_dropdown.currentIndexChanged.connect(self.save_selected_db)
        db_selection_layout.addWidget(self.db_dropdown)

        self.refresh_db_btn = QPushButton("Refresh Databases")
        self.refresh_db_btn.clicked.connect(self.populate_db_dropdown)
        db_selection_layout.addWidget(self.refresh_db_btn)

        admin_layout.addLayout(db_selection_layout)

        self.show_cdc_size_btn = QPushButton("Show CDC Table Sizes")
        self.show_cdc_size_btn.clicked.connect(self.show_cdc_table_sizes)
        admin_layout.addWidget(self.show_cdc_size_btn)

        self.cdc_status_label = QLabel("CDC Status: Unknown")
        self.cdc_status_label.setStyleSheet("color: gray; font-weight: bold;")
        admin_layout.addWidget(self.cdc_status_label)

        check_btn = QPushButton("Check CDC Status")
        check_btn.clicked.connect(self.check_cdc_status)
        admin_layout.addWidget(check_btn)

        activate_btn = QPushButton("Activate CDC")
        activate_btn.clicked.connect(self.activate_cdc)
        admin_layout.addWidget(activate_btn)

        deactivate_btn = QPushButton("Deactivate CDC")
        deactivate_btn.clicked.connect(self.deactivate_cdc)
        admin_layout.addWidget(deactivate_btn)

        self.tabs.addTab(admin_tab, "Admin")

        # New Config Viewer tab
        config_viewer_tab = QWidget()
        config_viewer_layout = QVBoxLayout(config_viewer_tab)

        self.config_text = QTextEdit()
        self.config_text.setReadOnly(True)
        config_viewer_layout.addWidget(self.config_text)

        refresh_btn = QPushButton("Refresh Config")
        refresh_btn.clicked.connect(self.load_config_viewer)
        config_viewer_layout.addWidget(refresh_btn)

        self.tabs.addTab(config_viewer_tab, "Config Viewer")

        # New Sync tab
        sync_tab = QWidget()
        sync_layout = QVBoxLayout(sync_tab)
        
        self.get_row_counts_btn = QPushButton("Get Row Counts")
        self.get_row_counts_btn.clicked.connect(self.get_row_counts)
        sync_layout.addWidget(self.get_row_counts_btn)

        self.row_counts_table = QTableWidget()
        self.row_counts_table.setColumnCount(5)
        self.row_counts_table.setHorizontalHeaderLabels(["Table Name", "SQL Server Count", "MySQL Count", "Difference", "Action"])
        header = self.row_counts_table.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.Stretch)
        header.setSectionResizeMode(1, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(2, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(3, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(4, QHeaderView.ResizeToContents)

        sync_layout.addWidget(self.row_counts_table)

        self.tabs.addTab(sync_tab, "Sync")
        
        splitter.addWidget(right_widget)
        
        # Set splitter sizes
        splitter.setSizes([400, 800])

        # --- إنشاء علامة تبويب سجل الترحيل الجديدة (Migration Log Viewer) ---
        log_viewer_tab = QWidget()
        log_viewer_layout = QVBoxLayout(log_viewer_tab)
        
        # 1. زر التحديث
        refresh_log_btn = QPushButton("Refresh Migration Log")
        refresh_log_btn.clicked.connect(self.load_migration_log)
        log_viewer_layout.addWidget(refresh_log_btn)
        
        # 2. جدول عرض البيانات
        self.migration_log_table = QTableWidget()
        self.migration_log_table.setColumnCount(4) # table_name, last_lsn, full_load_done
        self.migration_log_table.setHorizontalHeaderLabels([
            "Table Name", "Last LSN", "Last PK", "Full Load Done"
        ])
        self.migration_log_table.horizontalHeader().setStretchLastSection(True)
        log_viewer_layout.addWidget(self.migration_log_table)
        
        # --- إضافة علامة التبويب إلى الـ QTabWidget ---
        self.tabs.addTab(log_viewer_tab, "Migration Log Viewer") 
        
        # Control buttons
        control_layout = QHBoxLayout()
        
        self.start_btn = QPushButton("Start Migration")
        self.start_btn.clicked.connect(self.start_migration)
        control_layout.addWidget(self.start_btn, 1)
        
        self.stop_btn = QPushButton("Stop Migration")
        self.stop_btn.clicked.connect(self.stop_migration)
        self.stop_btn.setEnabled(False)
        control_layout.addWidget(self.stop_btn, 1)

        # Theme toggle button
        self.theme_btn = QPushButton("Theme")
        self.theme_btn.clicked.connect(self.toggle_theme)
        self.theme_btn.setFixedWidth(120)
        control_layout.addWidget(self.theme_btn)
        
        layout.addLayout(control_layout)
        
        # Progress bar
        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)
        layout.addWidget(self.progress_bar)

        # Apply new enhanced styles
        self.apply_styles()

    def apply_styles(self):
        # QSS for a flat, modern button
        button_style = """
            QPushButton {
                background-color: #4CAF50; /* Green */
                color: white;
                border: none;
                padding: 10px 20px;
                border-radius: 5px;
                font-weight: bold;
            }
            QPushButton:hover {
                background-color: #45a049;
            }
            QPushButton:pressed {
                background-color: #3e8e41;
            }
            QPushButton:disabled {
                background-color: #a0a0a0;
            }
        """
        # Specific styles for different buttons
        self.start_btn.setStyleSheet(button_style.replace("#4CAF50", "#4CAF50"))
        self.stop_btn.setStyleSheet(button_style.replace("#4CAF50", "#f44336"))
        self.connect_btn.setStyleSheet(button_style.replace("#4CAF50", "#3F51B5"))
        self.add_table_btn.setStyleSheet(button_style.replace("#4CAF50", "#FFC107"))
        self.remove_table_btn.setStyleSheet(button_style.replace("#4CAF50", "#9E9E9E"))
        self.clear_tables_btn.setStyleSheet(button_style.replace("#4CAF50", "#9E9E9E"))
        self.save_config_btn.setStyleSheet(button_style.replace("#4CAF50", "#009688"))
        self.test_connections_btn.setStyleSheet(button_style.replace("#4CAF50", "#03A9F4"))
        self.get_row_counts_btn.setStyleSheet(button_style.replace("#4CAF50", "#FF9800"))
        self.show_cdc_size_btn.setStyleSheet(button_style.replace("#4CAF50", "#673AB7"))
        self.theme_btn.setStyleSheet(button_style.replace("#4CAF50", "#795548"))


        # Progress bar style - text color depends on theme
        is_dark_theme = QApplication.instance().palette().color(QPalette.Window).value() < 128
        progress_text_color = "white" if is_dark_theme else "black"
        progress_bar_style = f"""
            QProgressBar {{
                border: 2px solid #555;
                border-radius: 5px;
                text-align: center;
                color: {progress_text_color};
            }}
            QProgressBar::chunk {{
                background-color: #2196F3;
                width: 20px;
            }}
        """
        self.progress_bar.setStyleSheet(progress_bar_style)

        # Global font and color settings
        font = QFont("Segoe UI", 10)
        self.setFont(font)
        
        # Group box styling
        groupbox_style = """
            QGroupBox {
                border: 1px solid #555;
                border-radius: 5px;
                margin-top: 2ex;
            }
            QGroupBox::title {
                subcontrol-origin: margin;
                subcontrol-position: top left;
                padding: 0 3px;
                color: #BDBDBD;
            }
        """
        # We need to adapt the groupbox styling for the light theme
        if not is_dark_theme:
            groupbox_style = """
                QGroupBox {
                    border: 1px solid #DEDEDE;
                    border-radius: 5px;
                    margin-top: 2ex;
                }
                QGroupBox::title {
                    subcontrol-origin: margin;
                    subcontrol-position: top left;
                    padding: 0 3px;
                    color: #555;
                }
            """

        self.findChild(QGroupBox, "SQL Server Connection").setStyleSheet(groupbox_style)
        self.findChild(QGroupBox, "Selected Tables").setStyleSheet(groupbox_style)
        self.findChild(QGroupBox, "MySQL Connection").setStyleSheet(groupbox_style)
        
        # Tab widget styling - dynamically change for theme
        if is_dark_theme:
            tabwidget_style = """
                QTabWidget::pane {
                    border-top: 2px solid #555;
                }
                QTabWidget::tab-bar {
                    left: 5px;
                }
                QTabBar::tab {
                    background: #444;
                    border: 1px solid #555;
                    border-bottom-color: #444;
                    border-top-left-radius: 4px;
                    border-top-right-radius: 4px;
                    padding: 5px;
                    min-width: 80px;
                }
                QTabBar::tab:selected {
                    background: #555;
                    border-color: #555;
                    border-bottom-color: #555;
                }
                QTabBar::tab:hover {
                    background: #666;
                }
            """
        else:
            tabwidget_style = """
                QTabWidget::pane {
                    border-top: 2px solid #DEDEDE;
                }
                QTabWidget::tab-bar {
                    left: 5px;
                }
                QTabBar::tab {
                    background: #E8E8E8;
                    border: 1px solid #DEDEDE;
                    border-bottom-color: #E8E8E8;
                    border-top-left-radius: 4px;
                    border-top-right-radius: 4px;
                    padding: 5px;
                    min-width: 80px;
                    color: #000000;
                }
                QTabBar::tab:selected {
                    background: #F0F0F0;
                    border-color: #DEDEDE;
                    border-bottom-color: #F0F0F0;
                }
                QTabBar::tab:hover {
                    background: #DEDEDE;
                }
            """
        self.tabs.setStyleSheet(tabwidget_style)
        
    def toggle_theme(self):
        """Toggles between dark and light themes."""
        current_color = QApplication.instance().palette().color(QPalette.Window)
        # A simple check to see if we are currently in a dark theme
        if current_color.value() < 128:
            QApplication.instance().setPalette(self.light_palette)
        else:
            QApplication.instance().setPalette(self.dark_palette)
        
        self.apply_styles() # Re-apply QSS to update dynamic styles

    def has_valid_config(self):
        """Check if we have a valid configuration for auto-start"""
        config = load_config()
        
        # Check required fields
        required_fields = ['sql_server', 'mysql_host', 'mysql_db_name', 'mysql_user', 'tables']
        for field in required_fields:
            if field not in config or not config[field]:
                return False
                
        # Check if we have at least one table
        tables = config.get('tables', '').split(',')
        if not tables or not tables[0]:
            return False
            
        return True
        
    def auto_start_migration(self):
        """Automatically start migration if we have a valid config"""
        if self.has_valid_config():
            self.auto_start = True
            self.auto_start_status.setText("Auto-start: Starting migration...")
            self.log("Auto-start: Valid configuration found, starting migration...")
            self.start_migration()
        else:
            self.auto_start_status.setText("Auto-start: No valid configuration found")
            self.log("Auto-start: No valid configuration found for auto-start")
        
    def on_browser_selection_changed(self):
        # Enable/disable buttons based on selection
        selected_items = self.db_browser.selectedItems()
        has_table_selection = any(item.data(0, Qt.UserRole) == "table" for item in selected_items)
        
        self.select_all_btn.setEnabled(has_table_selection)
        self.deselect_all_btn.setEnabled(has_table_selection)
        self.add_table_btn.setEnabled(has_table_selection)


    def get_full_driver_name(self):
            """Assembles the full ODBC driver string from the prefix, version, and suffix."""
            prefix = self.sql_driver_prefix.text().strip()
            version = self.sql_driver_version_combo.currentText().strip()
            suffix = self.sql_driver_suffix.text().strip()
            
            # Reconstructs the string: "ODBC Driver 11 for SQL Server"
            return f"{prefix} {version} {suffix}"
        
    def select_all_columns(self):
        """Select all columns in the currently selected table"""
        selected_items = self.db_browser.selectedItems()
        for item in selected_items:
            if item.data(0, Qt.UserRole) == "table":
                self.db_browser.select_all_columns(item)
                
    def deselect_all_columns(self):
        """Deselect all columns in the currently selected table"""
        selected_items = self.db_browser.selectedItems()
        for item in selected_items:
            if item.data(0, Qt.UserRole) == "table":
                self.db_browser.deselect_all_columns(item)
                
    def add_selected_table(self):
        """Add the selected table to the migration list"""
        selected_tables = self.db_browser.get_selected_table_items()
        for table_item in selected_tables:
            db_name = table_item.data(0, Qt.UserRole + 1)
            table_name = table_item.text(0)
            table_path = f"{db_name}.{table_name}"
            
            # Check if table is already in the list
            existing_items = self.selected_tables_list.findItems(table_path, Qt.MatchExactly)
            if not existing_items:
                self.selected_tables_list.addItem(table_path)
                
    def remove_selected_table(self):
        """Remove selected tables from the migration list"""
        for item in self.selected_tables_list.selectedItems():
            self.selected_tables_list.takeItem(self.selected_tables_list.row(item))
            
    def clear_all_tables(self):
        """Clear all tables from the migration list"""
        self.selected_tables_list.clear()
        
    def connect_to_sql_server(self):
        server = self.sql_server_input.text()
        driver = self.get_full_driver_name() # USE HELPER

        if not server or not driver:
            QMessageBox.warning(self, "Input Error", "Please enter a SQL Server name and select an ODBC Driver version.")
            return
            
        if self.db_browser.connect_to_sql_server(driver, server):
            self.log(f"Connected to SQL Server: {server} using driver: {driver}")
            self.populate_db_dropdown()
            
    def on_tables_selected(self, tables):
        # This method is called when tables are selected in the browser
        # We don't automatically add them to the list anymore
        pass
            
    def load_saved_config(self):
        try:
            if os.path.exists(CONFIG_FILE):
                with open(CONFIG_FILE, 'r') as f:
                    config = json.load(f)

                # --- NEW: Load Driver Version ---
                saved_version = config.get('sql_driver_version', '17')
                index = self.sql_driver_version_combo.findText(saved_version)
                if index != -1:
                    self.sql_driver_version_combo.setCurrentIndex(index)
                    
                self.sql_server_input.setText(config.get('sql_server', ''))
                self.mysql_host_input.setText(config.get('mysql_host', ''))
                self.mysql_db_input.setText(config.get('mysql_db_name', ''))
                self.mysql_user_input.setText(config.get('mysql_user', ''))
                self.mysql_pass_input.setText(config.get('mysql_password', ''))
                
                # Read source database name directly from config
                source_db = config.get('source_db', '')
                
                # Load tables using the correct source database name
                tables = config.get('tables', '').split(',')
                if tables and tables[0]:
                    for table_name in tables:
                        # Reconstruct the full path using the saved database name
                        table_path = f"{source_db}.{table_name}"
                        self.selected_tables_list.addItem(table_path)

                self.log("Loaded saved configuration")
                
        except Exception as e:
            self.log(f"Error loading configuration: {str(e)}")
            
    def save_config(self):
        try:
            # Get selected tables and determine source database
            tables = []
            source_db = ""
            if self.selected_tables_list.count() > 0:
                first_table_path = self.selected_tables_list.item(0).text()
                if '.' in first_table_path:
                    source_db = first_table_path.split('.')[0]

            for i in range(self.selected_tables_list.count()):
                tables.append(self.selected_tables_list.item(i).text())
                
            # Create the configuration dictionary
            config = {
                'sql_driver_version': self.sql_driver_version_combo.currentText(),
                'sql_server': self.sql_server_input.text(),
                'mysql_host': self.mysql_host_input.text(),
                'mysql_db_name': self.mysql_db_input.text(),
                'mysql_user': self.mysql_user_input.text(),
                'mysql_password': self.mysql_pass_input.text(),
                'source_db': source_db, 
                # Store only the table names (without database prefix)
                'tables': ','.join([t.split('.')[1] if '.' in t else t for t in tables])
            }
            
            # Add column selections for each table (using just table name as key)
            for table_path in tables:
                # Extract just the table name (remove database prefix if present)
                table_name = table_path.split('.')[1] if '.' in table_path else table_path
                
                # Retrieve columns. This MUST return a list (empty or populated).
                columns = self.db_browser.get_selected_columns(table_path)
                
                # Save the list of selected columns as a comma-separated string
                if columns:
                    config[table_name] = {"columns": ",".join(columns)}
                else:
                    # If columns is empty (either none were checked, or metadata wasn't loaded),
                    # save "*" to indicate all columns should be used by default.
                    config[table_name] = {"columns": "*"}
            
            with open(CONFIG_FILE, 'w', encoding="utf-8") as f:
                json.dump(config, f, indent=4, ensure_ascii=False)
                
            self.log("Configuration saved successfully")
            QMessageBox.information(self, "Success", "Configuration saved successfully")
        except Exception as e:
            self.log(f"Error saving configuration: {str(e)}")
            QMessageBox.critical(self, "Error", f"Error saving configuration: {str(e)}")
            
    def test_connections(self):
        server = self.sql_server_input.text()
        driver = self.get_full_driver_name()
        # Test SQL Server connection
        try:
            sql_conn = connect_sql_server(driver, server)
            sql_conn.close()
            self.log("SQL Server connection test: SUCCESS")
        except Exception as e:
            self.log(f"SQL Server connection test: FAILED - {str(e)}")
            QMessageBox.warning(self, "Connection Test", f"SQL Server connection failed: {str(e)}")
            return
            
        # Test MySQL connection
        try:
            mysql_conn = mysql.connector.connect(
                host=self.mysql_host_input.text(),
                user=self.mysql_user_input.text(),
                password=self.mysql_pass_input.text(),
                database=self.mysql_db_input.text(),
            )
            mysql_conn.close()
            self.log("MySQL connection test: SUCCESS")
            QMessageBox.information(self, "Connection Test", "Both connections tested successfully!")
        except Exception as e:
            self.log(f"MySQL connection test: FAILED - {str(e)}")
            QMessageBox.warning(self, "Connection Test", f"MySQL connection failed: {str(e)}")
            
    def start_migration(self):
        # 1. Get configuration details from GUI
        server = self.sql_server_input.text()
        driver = self.get_full_driver_name()
        mysqlhost = self.mysql_host_input.text()
        mysqluser = self.mysql_user_input.text()
        mysqlpassword = self.mysql_pass_input.text()
        mysql_db_name = self.mysql_db_input.text()
        
        tables = []
        source_db = ""

        # 2. Extract selected tables and source database
        if self.selected_tables_list.count() > 0:
            first_table_path = self.selected_tables_list.item(0).text()
            if '.' in first_table_path:
                source_db = first_table_path.split('.')[0]
                
            db_names = set()
            for i in range(self.selected_tables_list.count()):
                table_path = self.selected_tables_list.item(i).text()
                if '.' in table_path:
                    db_name, table_name = table_path.split('.')
                    db_names.add(db_name)
                    tables.append(table_name)
                else:
                    tables.append(table_path) # Should not happen if browser is used correctly
        
        # 3. Validation Checks
        if not tables:
            QMessageBox.warning(self, "Validation Error", "Please select at least one table to migrate")
            return
            
        if len(db_names) > 1:
            QMessageBox.warning(self, "Validation Error", "All tables must be from the same database")
            return
            
        if not all([driver, server, mysqlhost, mysqluser, mysql_db_name]):
            QMessageBox.warning(self, "Validation Error", "Please fill in all required connection fields")
            return
        
        # 4. Test Connections
        try:
            # Test SQL Server connection (using the new driver parameter)
            sql_server_conn = connect_sql_server(driver, server, source_db)
            sql_cursor = sql_server_conn.cursor()
            sql_server_conn.close() # Close test connection

            # Test MySQL connection
            mysql_conn, mysql_cursor = connect_mysql(mysql_db_name, mysqlhost, mysqluser, mysqlpassword)
            if not mysql_conn:
                QMessageBox.critical(self, "Connection Error", "Failed to connect to MySQL. Please check your credentials.")
                return
            mysql_conn.close() # Close test connection

        except Exception as e:
            QMessageBox.critical(self, "Connection Error", f"Failed to establish initial connections: {str(e)}")
            return

        # 5. Assemble Configuration for Thread
        config = {
            'sql_driver': driver,
            'sql_server': server,
            'source_db': source_db,
            'mysql_host': mysqlhost,
            'mysql_db_name': mysql_db_name,
            'mysql_user': mysqluser,
            'mysql_password': mysqlpassword,
            'tables': ','.join(tables)
        }
        
        # Add column selections to config_data
        config_data = load_config()
        for i in range(self.selected_tables_list.count()):
            table_path = self.selected_tables_list.item(i).text()
            table_name = table_path.split('.')[1] if '.' in table_path else table_path
            columns = self.db_browser.get_selected_columns(table_path)
            if columns:
                config_data[table_name] = {"columns": ",".join(columns)}
        save_config(config_data)
        
        # 6. Start Migration Thread
        self.migration_thread = MigrationThread(config)
        self.migration_thread.log_signal.connect(self.log)
        self.migration_thread.finished_signal.connect(self.migration_finished)
        self.migration_thread.error_signal.connect(self.migration_error)

        # Connect new signals for progress bar
        self.migration_thread.set_progress_range_signal.connect(self.set_progress_range)
        self.migration_thread.progress_signal.connect(self.update_progress)
        
        self.start_btn.setEnabled(False)
        self.stop_btn.setEnabled(True)
        self.progress_bar.setVisible(True)

        self.progress_bar.setValue(0)
        
        self.migration_thread.start()
        self.log("Migration started...")
        
        if self.auto_start:
            self.auto_start_status.setText("Auto-start: Migration running")

#1111
# Add new methods to MigrationGUI class
    def set_progress_range(self, minimum, maximum):
        self.progress_bar.setMinimum(minimum)
        self.progress_bar.setMaximum(maximum)

#1111
    def update_progress(self, value):
        self.progress_bar.setValue(value)
        
    def stop_migration(self):
        if self.migration_thread:
            self.migration_thread.stop()
            self.migration_thread.wait()
            self.log("Migration stopped by user")
            
    def migration_finished(self):
        self.start_btn.setEnabled(True)
        self.stop_btn.setEnabled(False)
        self.progress_bar.setVisible(False)
        self.log("Migration process finished")
        
        # Update auto-start status if this was an auto-start
        if self.auto_start:
            self.auto_start_status.setText("Auto-start: Migration finished")
        
    def migration_error(self, error_msg):
        self.log(f"Migration error: {error_msg}")
        self.migration_finished()
        QMessageBox.critical(self, "Migration Error", f"An error occurred during migration: {error_msg}")
        
        # Update auto-start status if this was an auto-start
        if self.auto_start:
            self.auto_start_status.setText("Auto-start: Migration failed")
        
    def log(self, message):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_message = f"[{timestamp}] {message}"
        self.log_output.append(log_message)
        
        # Also write to log file
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(log_message + "\n")


    def get_config_db_name(self):
        config = load_config()
        return config.get("source_db", "")
    

    def populate_db_dropdown(self):
        server = self.sql_server_input.text()
        driver = self.get_full_driver_name() # Fetch the driver name
        if not server:
            QMessageBox.warning(self, "Input Error", "Please enter a SQL Server name first.")
            return

        try:
            conn = connect_sql_server(driver, server)  # Connect without a specific database
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sys.databases WHERE database_id > 4")
            databases = [row[0] for row in cursor.fetchall()]
            conn.close()

            self.db_dropdown.clear()
            self.db_dropdown.addItems(databases)
            
            # Set the current selection based on the loaded config
            config = load_config()
            source_db = config.get("source_db", "")
            if source_db in databases:
                self.db_dropdown.setCurrentText(source_db)

        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed to load databases: {e}")
            self.db_dropdown.clear()

    def save_selected_db(self):
        selected_db = self.db_dropdown.currentText()
        if not selected_db:
            return

        config = load_config()
        config["source_db"] = selected_db
        save_config(config)
        self.log(f"Config updated: source_db set to {selected_db}")

        # After saving the database, update the status label for CDC
        self.check_cdc_status()

    def check_cdc_status(self):
        try:
            config = load_config()
            server = config.get("sql_server")
            db_name = config.get("source_db")

            if not server or not db_name:
                QMessageBox.warning(self, "Error", "SQL Server or Database not set in config.json")
                return
            
            driver = self.get_full_driver_name()

            conn = connect_sql_server(driver,server, db_name)
            cursor = conn.cursor()
            cursor.execute("SELECT is_cdc_enabled FROM sys.databases WHERE name = ?", (db_name,))
            result = cursor.fetchone()
            conn.close()

            if result and result[0] == 1:
                self.cdc_status_label.setText("CDC Status: Activated")
                self.cdc_status_label.setStyleSheet("color: green; font-weight: bold;")
            else:
                self.cdc_status_label.setText("CDC Status: Deactivated")
                self.cdc_status_label.setStyleSheet("color: red; font-weight: bold;")
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed to check CDC status: {e}")

    def activate_cdc(self, show_message=True): # New argument
            try:
                config = load_config()
                server = config.get("sql_server")
                db_name = config.get("source_db")
                mysql_db_name = config.get("mysql_db_name")
                mysqlhost = config.get("mysql_host")
                mysqluser = config.get("mysql_user")
                mysqlpassword = config.get("mysql_password")

                if not server or not db_name:
                    QMessageBox.warning(self, "Error", "SQL Server or Database not set in config.json")
                    return
                
                driver = self.get_full_driver_name()

                sql_conn = connect_sql_server(driver,server, db_name)
                sql_cursor = sql_conn.cursor()

                # Connect to MySQL
                mysql_conn, mysql_cursor = connect_mysql(mysql_db_name, mysqlhost, mysqluser, mysqlpassword)
                if not mysql_conn:
                    raise Exception("Failed to connect to MySQL for schema check.")

                self.log(f"Starting column schema synchronization for tables in {mysql_db_name}...")
                
                tables = [t.strip() for t in config.get("tables", "").split(",") if t.strip()]
                for table in tables:
                    self.log(f"Checking schema for table: {table}")
                    
                    # 1. Get the list of ALL columns from SQL Server (to get data types)
                    sql_cursor.execute(f"SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table}' ORDER BY ORDINAL_POSITION")
                    sql_all_columns = {row[0]: row[1] for row in sql_cursor.fetchall()}
                    
                    # 2. Get the list of REQUIRED columns from config.json (resolved list)
                    table_conf = config.get(table, {})
                    col_selection = table_conf.get("columns", "*") 
                    
                    # Resolve the final list of columns to be in the MySQL table
                    required_col_names = resolve_columns(list(sql_all_columns.keys()), col_selection)
                    
                    # 3. Get the list of CURRENT columns in the MySQL table
                    mysql_cursor.execute(f"SHOW COLUMNS FROM `{table}`")
                    mysql_current_columns = {row[0] for row in mysql_cursor.fetchall()}
                    
                    # 4. Determine missing columns
                    missing_cols = [
                        col for col in required_col_names 
                        if col not in mysql_current_columns and col in sql_all_columns
                    ]
                    
                    if missing_cols:
                        self.log(f"Found {len(missing_cols)} missing columns in MySQL for table {table}.")
                        
                        # 5. Add missing columns
                        for col_name in missing_cols:
                            sql_type = sql_all_columns[col_name]
                            mysql_type = map_type(sql_type)
                            
                            # Use ALTER TABLE ADD COLUMN
                            alter_sql = f"ALTER TABLE `{table}` ADD COLUMN `{col_name}` {mysql_type} NULL"
                            
                            try:
                                mysql_cursor.execute(alter_sql)
                                self.log(f"Successfully added column `{col_name}` ({mysql_type}) to MySQL table `{table}`.")
                            except Exception as e:
                                self.log(f"Failed to add column `{col_name}` to MySQL table `{table}`: {e}")
                        
                        mysql_conn.commit()
                    else:
                        self.log(f"MySQL table {table} has all required columns.")
                
                self.log("Column schema synchronization complete.")

                # --- Now proceed with enabling CDC ---
                
                # Close MySQL connection (will be reopened by the loop below)
                mysql_conn.close()

                # Enable CDC on database
                sql_cursor.execute("EXEC sys.sp_cdc_enable_db")
                sql_conn.commit()
                self.log(f"CDC enabled on database {db_name}")

                # loop on tables and enable CDC for each
                for table in tables:
                    # determine captured columns (must be the final required list)
                    table_conf = config.get(table, {})
                    col_selection = table_conf.get("columns", "*")
                    
                    # Re-fetch all columns to resolve selection
                    sql_cursor.execute(f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table}'")
                    cols = [row[0] for row in sql_cursor.fetchall()]
                    required_cols = resolve_columns(cols, col_selection)
                    
                    captured_cols = ",".join([f"[{c}]" for c in required_cols])

                    try:
                        sql_cursor.execute(f"""
                            EXEC sys.sp_cdc_enable_table
                                @source_schema = 'dbo',
                                @source_name   = '{table}',
                                @role_name     = NULL,
                                @captured_column_list = '{captured_cols}'
                        """)
                        sql_conn.commit()
                        self.log(f"CDC enabled for table {table} with columns: {captured_cols}")
                    except Exception as e:
                        self.log(f"CDC enable failed or already active for table {table}: {e}")

                if show_message: # Only show message if called manually
                    QMessageBox.information(self, "Success", f"Schema synchronized and CDC enabled on database {db_name} and all configured tables")
                
                self.check_cdc_status()

            except Exception as e:
                self.log(f"Failed to activate CDC or synchronize schema: {e}")
                if show_message: # Only show error message if called manually
                    QMessageBox.critical(self, "Error", f"Failed to activate CDC or synchronize schema: {e}")
                if sql_conn: sql_conn.close()
                if mysql_conn: mysql_conn.close()


    def deactivate_cdc(self, show_message=True): # New argument
            try:
                config = load_config()
                server = config.get("sql_server")
                db_name = config.get("source_db")

                if not server or not db_name:
                    QMessageBox.warning(self, "Error", "SQL Server or Database not set in config.json")
                    return
                
                driver = self.get_full_driver_name()

                # Connect to SQL Server
                sql_conn = connect_sql_server(driver,server, db_name)
                sql_cursor = sql_conn.cursor()

                # --- Attempt to Connect to MySQL and Check for migration_log table ---
                mysql_conn = None
                mysql_cursor = None
                log_table_exists = False
                
                try:
                    # connect_mysql ensures a database is created and used
                    mysql_conn, mysql_cursor = connect_mysql(
                        config.get("mysql_db_name"),
                        config.get("mysql_host"),
                        config.get("mysql_user"),
                        config.get("mysql_password")
                    )
                    
                    if mysql_conn and mysql_cursor:
                        # Check if migration_log table exists
                        mysql_cursor.execute("SHOW TABLES LIKE 'migration_log'")
                        if mysql_cursor.fetchone():
                            log_table_exists = True
                    
                except Exception as e:
                    self.log(f"MySQL connection or check for migration_log failed: {e}. Proceeding with CDC disable only.")
                    # mysql_conn remains None or closed if the error was connection-related
                    

                # loop on all tables and save last_pk ONLY if MySQL log is accessible
                if log_table_exists and mysql_conn and mysql_cursor:
                    tables = [t.strip() for t in config.get("tables", "").split(",") if t.strip()]
                    for table in tables:
                        try:
                            # get primary key
                            sql_cursor.execute(f"""
                                SELECT COLUMN_NAME 
                                FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                                WHERE TABLE_NAME='{table}'
                            """)
                            pk_cols = [row[0] for row in sql_cursor.fetchall()]
                            
                            if not pk_cols:
                                # fallback to the first column
                                sql_cursor.execute(f"""
                                    SELECT COLUMN_NAME 
                                    FROM INFORMATION_SCHEMA.COLUMNS
                                    WHERE TABLE_NAME='{table}' ORDER BY ORDINAL_POSITION
                                """)
                                pk_cols = [sql_cursor.fetchone()[0]]

                            pk = pk_cols[0]
                            
                            # get max(pk) from SQL Server table
                            sql_cursor.execute(f"SELECT MAX([{pk}]) FROM [{table}]")
                            last_pk = sql_cursor.fetchone()[0]

                            # update migration_log
                            mysql_cursor.execute("""
                                INSERT INTO migration_log (table_name, last_pk, full_load_done, last_lsn)
                                VALUES (%s, %s, 0, null)
                                ON DUPLICATE KEY UPDATE last_pk=%s, full_load_done=0, last_lsn=null
                            """, (table, last_pk, last_pk))

                            self.log(f"Table {table}: saved last_pk={last_pk}, set full_load_done=0")
                            
                        except Exception as e:
                            self.log(f"Warning: Failed to process table {table} for last_pk save: {e}")

                    mysql_conn.commit()
                    mysql_conn.close()
                    self.log("MySQL log update complete.")
                else:
                    self.log("Skipping last_pk save and full_load reset due to inaccessible MySQL log table.")


                # disable CDC on SQL Server database (Always attempt to do this)
                sql_cursor.execute("EXEC sys.sp_cdc_disable_db")
                sql_conn.commit()
                sql_conn.close()

                status_msg = f"CDC disabled on SQL Server database {db_name}."
                if log_table_exists:
                    status_msg += " Last PK saved and full load reset in MySQL."
                
                if show_message: # Only show message if called manually
                    QMessageBox.information(self, "Success", status_msg)
                    
                self.check_cdc_status()

            except Exception as e:
                self.log(f"Failed to deactivate CDC: {e}")
                if show_message: # Only show error message if called manually
                    QMessageBox.critical(self, "Error", f"Failed to deactivate CDC: {e}")
                if sql_conn:
                    sql_conn.close()
                if mysql_conn:
                    mysql_conn.close()


    def show_cdc_table_sizes(self):
        try:
            config = load_config()
            server = config.get("sql_server")
            db_name = config.get("source_db")

            if not server or not db_name:
                QMessageBox.warning(self, "Error", "SQL Server or Database not set in config.json")
                return

            driver = self.get_full_driver_name()

            sql_conn = connect_sql_server(driver,server, db_name)
            sql_cursor = sql_conn.cursor()

            query = """
            SELECT t.name AS ChangeTable,
                SUM(ps.reserved_page_count) * 8 / 1024 AS SizeMB
            FROM sys.dm_db_partition_stats ps
            JOIN sys.tables t ON ps.object_id = t.object_id
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = 'cdc' AND t.name LIKE '%[_]CT'
            GROUP BY t.name
            ORDER BY SizeMB DESC;
            """

            sql_cursor.execute(query)
            results = sql_cursor.fetchall()
            sql_conn.close()

            if not results:
                QMessageBox.information(self, "Information", "No CDC tables found or no data available.")
                return

            message = "CDC Table Sizes:\n\n"
            for row in results:
                table_name = row[0]
                size_mb = row[1]
                message += f"• {table_name}: {size_mb} MB\n"

            QMessageBox.information(self, "CDC Table Sizes", message)

        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed to get CDC table sizes: {e}")

    def load_config_viewer(self):
        try:
            if os.path.exists(CONFIG_FILE):
                with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                    content = f.read()
                self.config_text.setPlainText(content)
            else:
                self.config_text.setPlainText("config.json not found.")
        except Exception as e:
            self.config_text.setPlainText(f"Error loading config.json: {e}")

    def get_row_counts(self):
            self.row_counts_table.setRowCount(0) # Clear existing data
            self.log("Counting rows...")
            
            try:
                config = load_config()
                sql_server = config.get("sql_server")
                source_db = config.get("source_db")
                mysql_host = config.get("mysql_host")
                mysql_user = config.get("mysql_user")
                mysql_pass = config.get("mysql_password")
                mysql_db = config.get("mysql_db_name")
                tables = [t.strip() for t in config.get("tables", "").split(",") if t.strip()]

                if not all([sql_server, source_db, mysql_host, mysql_user, mysql_pass, mysql_db]):
                    QMessageBox.warning(self, "Error", "Please complete all connection settings in the Config tab.")
                    return

                driver = self.get_full_driver_name()

                sql_conn = connect_sql_server(driver, sql_server, source_db)
                sql_cursor = sql_conn.cursor()

                mysql_conn = mysql.connector.connect(
                    host=mysql_host,
                    user=mysql_user,
                    password=mysql_pass,
                    database=mysql_db
                )
                mysql_cursor = mysql_conn.cursor()
                
                self.log("Connection successful. Getting counts...")
                
                # --- تعديل 1: زيادة عدد الأعمدة إلى 6 ---
                self.row_counts_table.setColumnCount(6)
                self.row_counts_table.setHorizontalHeaderLabels(["Table Name", "SQL Server Count", "MySQL Count", "Difference", "Sync", "Reset Log"])
                
                header = self.row_counts_table.horizontalHeader()
                header.setSectionResizeMode(0, QHeaderView.Stretch)
                # ضبط أحجام باقي الأعمدة لتكون مناسبة للمحتوى
                for k in range(1, 6):
                    header.setSectionResizeMode(k, QHeaderView.ResizeToContents)

                self.row_counts_table.setRowCount(len(tables))
                
                for i, table in enumerate(tables):
                    # SQL Server count
                    sql_cursor.execute(f"SELECT COUNT(*) FROM {table}")
                    sql_count = sql_cursor.fetchone()[0]

                    # MySQL count
                    mysql_cursor.execute(f"SELECT COUNT(*) FROM {table}")
                    mysql_count = mysql_cursor.fetchone()[0]

                    difference = abs(sql_count - mysql_count)

                    # Populate the table widget
                    self.row_counts_table.setItem(i, 0, QTableWidgetItem(table))
                    self.row_counts_table.setItem(i, 1, QTableWidgetItem(f"{sql_count:,}"))
                    self.row_counts_table.setItem(i, 2, QTableWidgetItem(f"{mysql_count:,}"))

                    # Difference Item
                    diff_item = QTableWidgetItem(f"{difference:,}")
                    if difference > 0:
                        diff_item.setForeground(QBrush(QColor("red")))
                        # Sync Button (Column 4)
                        sync_button = QPushButton("Sync")
                        sync_button.setStyleSheet("background-color: #FF9800; color: white; font-weight: bold;")
                        sync_button.clicked.connect(lambda _, t=table: self.sync_rows_by_pk(t))
                        self.row_counts_table.setCellWidget(i, 4, sync_button)
                    
                    self.row_counts_table.setItem(i, 3, diff_item)

                    # --- تعديل 2: إضافة زر Reset Log (Column 5) ---
                    reset_btn = QPushButton("Reset Log")
                    reset_btn.setStyleSheet("background-color: #f44336; color: white; font-weight: bold;") # لون أحمر
                    # نستخدم lambda لتمرير اسم الجدول عند الضغط
                    reset_btn.clicked.connect(lambda _, t=table: self.reset_migration_log(t))
                    self.row_counts_table.setCellWidget(i, 5, reset_btn)

                sql_conn.close()
                mysql_conn.close()

                self.log("Row counts updated successfully.")
                # QMessageBox.information(self, "Success", "Row counts updated successfully.")

            except Exception as e:
                self.row_counts_table.setRowCount(0) # Clear table on error
                QMessageBox.critical(self, "Error", f"An error occurred: {e}")

    def run_daily_cdc_schedule(self):
            """
            Runs daily CDC deactivation and reactivation at specific times.
            - Deactivate: 03:00 AM (03:00)
            - Activate: 10:00 AM (22:00) - Assuming 1 hour downtime for maintenance, adjust as needed.
            """
            now = datetime.now()
            
            # Time for CDC Deactivation (03:00 AM)
            if now.hour == 3 and now.minute == 0:
                self.log("--- Daily CDC Deactivation triggered at 03:00 ---")
                # Stop the continuous migration thread if it's running before disabling CDC
                if self.migration_thread and self.migration_thread.running:
                    self.stop_migration()
                    
                self.deactivate_cdc(show_message=False)
                self.log("--- Daily CDC Deactivation complete ---")
                
            # Time for CDC Reactivation (03:05 AM - allowing 5 minutes for deactivation process)
            # We perform activation shortly after deactivation to minimize downtime.
            if now.hour == 3 and now.minute == 5: 
                self.log("--- Daily CDC Reactivation triggered at 03:05 ---")
                self.activate_cdc(show_message=False)
                self.log("--- Daily CDC Reactivation complete ---")
                
                # Restart the migration thread if it was running previously
                if self.start_btn.isEnabled(): # Check if migration is not currently running
                    self.start_migration() # This restarts the data sync


    def run_periodic_cdc_sync(self):
            """
            Executes the activate_cdc logic periodically to ensure schema consistency 
            and CDC is active on all configured tables.
            """
            self.log("--- Starting periodic CDC sync and schema check (5-minute interval) ---")
            
            try:
                # We call the existing activate_cdc method
                # It already contains the logic to:
                # 1. Check MySQL connection.
                # 2. Check current MySQL schema against config.json.
                # 3. ALTER TABLE to add any missing columns.
                # 4. Enable CDC on the database.
                # 5. Enable CDC on all tables with the correct column list.
                self.activate_cdc()
                
                self.log("--- Periodic CDC sync and schema check completed ---")

            except Exception as e:
                # Note: The activate_cdc function already has internal error handling (QMessageBox, log).
                # This outer try/except is mainly for catching unexpected timer execution failures.
                self.log(f"CRITICAL ERROR during periodic CDC sync: {e}")


    def sync_rows_by_pk(self, table_name):
                self.log(f"Starting re-sync for table: {table_name}")
                self.progress_bar.setVisible(True)
                try:
                    # --- 1. Load Configuration and Establish Connections ---
                    config = load_config()
                    sql_server = config.get("sql_server")
                    source_db = config.get("source_db")
                    mysql_host = config.get("mysql_host")
                    mysql_user = config.get("mysql_user")
                    mysql_pass = config.get("mysql_password")
                    mysql_db = config.get("mysql_db_name")

                    driver = self.get_full_driver_name()

                    sql_conn = connect_sql_server(driver, sql_server, source_db)
                    sql_cursor = sql_conn.cursor()
                    mysql_conn, mysql_cursor = connect_mysql(mysql_db, mysql_host, mysql_user, mysql_pass)

                    # Get the primary key column name
                    pk_cols = get_primary_keys(sql_cursor, table_name, [])
                    if not pk_cols:
                        QMessageBox.warning(self, "Error", f"No primary key found for table {table_name}.")
                        return
                    pk_col = pk_cols[0]

                    # --- 2. Determine Selected Columns from Config ---
                    config_data = load_config()
                    
                    # Fetch all possible columns from SQL Server
                    sql_cursor.execute(f"SELECT TOP 0 * FROM {table_name}")
                    all_columns = [col[0] for col in sql_cursor.description]
                    
                    # Determine the final list of columns based on config.json
                    table_config = config_data.get(table_name, {})
                    col_selection = table_config.get("columns", "*") # Default to all if not set
                    col_names = resolve_columns(all_columns, col_selection)
                    
                    if not col_names:
                        self.log(f"Error: No columns selected for table {table_name}.")
                        QMessageBox.critical(self, "Error", f"No columns selected for table {table_name}.")
                        return
                    
                    # Create the comma-separated list of columns for the SQL Server SELECT clause
                    col_list_sql = ",".join(f"[{c}]" for c in col_names)
                    
                    # --- 3. Key Comparison (Finding Differences) ---
                    self.log("Fetching Primary Keys from SQL Server...")
                    sql_cursor.execute(f"SELECT [{pk_col}] FROM {table_name}")
                    sql_pks = {row[0] for row in sql_cursor.fetchall()}

                    self.log("Fetching Primary Keys from MySQL...")
                    mysql_cursor.execute(f"SELECT `{pk_col}` FROM `{table_name}`")
                    mysql_pks = {row[0] for row in mysql_cursor.fetchall()}

                    # Find differences
                    pks_to_insert = list(sql_pks - mysql_pks)
                    pks_to_delete = list(mysql_pks - sql_pks)

                    total_rows_to_sync = len(pks_to_insert) + len(pks_to_delete)
                    self.progress_bar.setMinimum(0)
                    self.progress_bar.setMaximum(total_rows_to_sync)
                    current_progress = 0

                    # --- 4. Handle Inserts / Updates (Batch Optimized Upsert) ---
                    if pks_to_insert:
                        self.log(f"Found {len(pks_to_insert)} rows to insert/update...")
                        
                        chunk_size = 1000 
                        
                        # Prepare MySQL INSERT/UPSERT SQL using the configured columns
                        placeholders_mysql = ','.join(['%s'] * len(col_names))
                        update_clause = ','.join([f"`{c}`=VALUES(`{c}`)" for c in col_names if c not in pk_cols])
                        
                        if update_clause:
                            insert_sql = (
                                f"INSERT INTO `{table_name}` ({', '.join(f'`{c}`' for c in col_names)}) "
                                f"VALUES ({placeholders_mysql}) "
                                f"ON DUPLICATE KEY UPDATE {update_clause}"
                            )
                        else:
                            insert_sql = (
                                f"INSERT IGNORE INTO `{table_name}` ({', '.join(f'`{c}`' for c in col_names)}) "
                                f"VALUES ({placeholders_mysql})"
                            )

                        for i in range(0, len(pks_to_insert), chunk_size):
                            QApplication.processEvents()
                            
                            chunk = pks_to_insert[i:i + chunk_size]
                            
                            # --- MODIFICATION: Use selected columns in SQL Server SELECT ---
                            placeholders = ','.join(['?'] * len(chunk))
                            # Use col_list_sql (which contains only selected columns)
                            sql_query = f"SELECT {col_list_sql} FROM {table_name} WHERE [{pk_col}] IN ({placeholders})"
                            sql_cursor.execute(sql_query, chunk)
                            rows_to_sync = sql_cursor.fetchall()

                            # Prepare the data for Batch Insert
                            batch_data = []
                            for row in rows_to_sync:
                                # Note: The conversion must match the order of col_names
                                # Since we SELECTed only col_names, we can use the cursor description
                                row_data = tuple(convert_value(col_val) for col_val in row) 
                                batch_data.append(row_data)

                            # Execute the Batch Insert/Upsert
                            if batch_data:
                                try:
                                    mysql_cursor.executemany(insert_sql, batch_data)
                                    mysql_conn.commit()
                                except Exception as e:
                                    self.log(f"Error in sync batch insert: {e}")

                            # Update progress
                            current_progress += len(chunk)
                            self.progress_bar.setValue(current_progress)
                            self.log(f"Synced batch of {len(chunk)} rows (Total: {current_progress}/{total_rows_to_sync})")
                        
                    # --- 5. Handle Deletes (Batch Optimized) ---
                    if pks_to_delete:
                        self.log(f"Found {len(pks_to_delete)} rows to delete...")
                        
                        chunk_size = 1000
                        
                        for i in range(0, len(pks_to_delete), chunk_size):
                            QApplication.processEvents()

                            chunk = pks_to_delete[i:i + chunk_size]
                            placeholders_mysql = ','.join(['%s'] * len(chunk))
                            delete_sql = f"DELETE FROM `{table_name}` WHERE `{pk_col}` IN ({placeholders_mysql})"
                            
                            try:
                                mysql_cursor.execute(delete_sql, chunk)
                                mysql_conn.commit()
                            except Exception as e:
                                self.log(f"Error in sync batch delete: {e}")
                                
                            current_progress += len(chunk)
                            self.progress_bar.setValue(current_progress)

                    # --- 6. Advance LSN to prevent redundant incremental load ---
                    self.log(f"Fetching current Max LSN for table: {table_name}")
                    
                    # Fetch the absolute maximum LSN from SQL Server
                    sql_cursor.execute("SELECT sys.fn_cdc_get_max_lsn()")
                    max_lsn = sql_cursor.fetchone()[0]

                    # Update the migration_log table with the new Max LSN
                    mysql_cursor.execute("""
                        INSERT INTO `migration_log` (`table_name`, `last_lsn`)
                        VALUES (%s, %s)
                        ON DUPLICATE KEY UPDATE `last_lsn`=%s
                    """, (table_name, max_lsn, max_lsn))
                    mysql_conn.commit()
                    
                    self.log(f"Successfully updated LSN for {table_name} to prevent repeated changes.")
                    # --- END LSN ADVANCEMENT ---

                    # --- 6. Finalization ---
                    sql_conn.close()
                    mysql_conn.close()
                    
                    self.log(f"Re-sync for {table_name} completed successfully.")
                    QMessageBox.information(self, "Success", f"Re-sync for {table_name} completed.")
                    self.get_row_counts() # Refresh the table
                    
                    self.progress_bar.setValue(total_rows_to_sync)
                    self.progress_bar.setVisible(False)

                except Exception as e:
                    self.log(f"Error during re-sync: {e}")
                    QMessageBox.critical(self, "Error", f"Failed to re-sync table {table_name}: {e}")
                    self.progress_bar.setVisible(False)


    def reset_migration_log(self, table_name):
            # تأكيد الحذف من المستخدم أولاً
            reply = QMessageBox.question(self, 'Confirm Reset', 
                                        f"Are you sure you want to delete the migration log for table '{table_name}'?\n\n"
                                        "This will force the tool to restart migration from the beginning (Full Load) for this table next time you start.",
                                        QMessageBox.Yes | QMessageBox.No, QMessageBox.No)

            if reply == QMessageBox.Yes:
                try:
                    config = load_config()
                    mysql_conn, mysql_cursor = connect_mysql(
                        config.get("mysql_db_name"),
                        config.get("mysql_host"),
                        config.get("mysql_user"),
                        config.get("mysql_password")
                    )
                    
                    if mysql_conn:
                        # تنفيذ الحذف
                        mysql_cursor.execute("DELETE FROM migration_log WHERE table_name = %s", (table_name,))
                        mysql_conn.commit()
                        
                        self.log(f"Migration log reset for table: {table_name}")
                        QMessageBox.information(self, "Success", f"Log cleared for {table_name}. You can now restart migration to perform a fresh Full Load.")
                        
                        mysql_conn.close()
                except Exception as e:
                    self.log(f"Error resetting log for {table_name}: {e}")
                    QMessageBox.critical(self, "Error", f"Failed to reset log: {e}")

    def load_migration_log(self):
        self.log("Fetching Migration Log from MySQL...")
        try:
            config = load_config()
            mysql_host = config.get("mysql_host")
            mysql_user = config.get("mysql_user")
            mysql_pass = config.get("mysql_password")
            mysql_db = config.get("mysql_db_name")
            
            if not all([mysql_host, mysql_user, mysql_db]):
                QMessageBox.warning(self, "Error", "Please fill in MySQL connection settings first.")
                return

            # استخدام الدالة العامة للاتصال بـ MySQL
            mysql_conn, mysql_cursor = connect_mysql(mysql_db, mysql_host, mysql_user, mysql_pass)
            
            # جلب البيانات من جدول migration_log
            mysql_cursor.execute("SELECT table_name, last_lsn, last_pk, full_load_done FROM migration_log")
            results = mysql_cursor.fetchall()
            
            mysql_conn.close()

            # --- عرض البيانات في الجدول ---
            self.migration_log_table.setRowCount(len(results))
            
            for row_index, row_data in enumerate(results):
                table_name, last_lsn, last_pk, full_load_done = row_data
                
                # عرض اسم الجدول
                self.migration_log_table.setItem(row_index, 0, QTableWidgetItem(str(table_name)))
                
                # عرض LSN (نقوم بتحويله إلى تنسيق Hex/String لقراءة أسهل)
                # LSN هو نوع BINARY(10) في MySQL، يجب التعامل معه حسب كيفية تخزينه
                if last_lsn:
                    # افتراضًا أن LSN يتم تخزينه كـ HEX في MySQL (يجب التحقق من تنسيق التخزين)
                    lsn_display = last_lsn.hex() if isinstance(last_lsn, bytes) else str(last_lsn)
                else:
                    lsn_display = "N/A"
                    
                self.migration_log_table.setItem(row_index, 1, QTableWidgetItem(lsn_display))
                
                # 3. عرض قيمة last_pk في العمود 2 (العمود الجديد)
                pk_display = str(last_pk) if last_pk is not None else "N/A"
                self.migration_log_table.setItem(row_index, 2, QTableWidgetItem(pk_display)) # <-- العمود 2
                
                # 4. نقل عرض Full Load Done إلى العمود 3
                full_load_status = "Done (1)" if full_load_done == 1 else "Pending (0)"
                self.migration_log_table.setItem(row_index, 3, QTableWidgetItem(full_load_status)) # <-- العمود 3

            self.log("Migration Log loaded successfully.")
            
        except Exception as e:
            self.log(f"Error loading migration log: {e}")
            QMessageBox.critical(self, "Error", f"Failed to load migration log: {e}")


def main():
    app = QApplication(sys.argv)

    lock_file_path = "app.lock"

    try:
        # Open the lock file in write mode
        lock_file = open(lock_file_path, 'w')
        
        # Try to acquire an exclusive lock on the file
        if sys.platform == "win32":
            # Windows-specific locking
            msvcrt.locking(lock_file.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            # Unix-like systems locking
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)

    except (IOError, OSError):
        # If locking fails, another instance is running
        QMessageBox.warning(None, "Application Already Running",
                            "The migration tool is already running. Please check your taskbar.")
        sys.exit(0) # Exit the new instance
        
    
    # Initialize the global logger after QApplication is created
    global logger
    logger = Logger()

    # Set the application icon
    app_icon = QIcon('icon.ico')
    app.setWindowIcon(app_icon)
    
    # Set a modern dark theme
    app.setStyle("Fusion")
    
    gui = MigrationGUI()
    gui.show()

        # Ensure the lock file is released on exit
    def release_lock_and_exit():
        try:
            if sys.platform == "win32":
                msvcrt.locking(lock_file.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
            lock_file.close()
            os.remove(lock_file_path)
        except Exception as e:
            print(f"Failed to release lock or delete file: {e}")
        finally:
            sys.exit(0)

    app.aboutToQuit.connect(release_lock_and_exit)

    sys.exit(app.exec_())

if __name__ == "__main__":
    main()