# setup.py (Corrected)
import sys
# Import the constants module from cx_Freeze (optional but clearer)
from cx_Freeze import setup, Executable # No need to import constants explicitly if using string

# Options for building the executable.
build_exe_options = {
    "packages": ["os", "sys", "json", "pyodbc", "mysql.connector", "PyQt5", "msvcrt"],
    "excludes": ["tkinter", "PyQt5.QtDeclarative", "PyQt5.QtQuick", "PyQt5.QtQml", "PyQt5.Enginio"],
    "include_files": ["icon.ico"]
}

# 🚨 THE FIX IS HERE: Set 'base' to 'Win32GUI' explicitly, 
# which is the standard base name for a GUI application without a console window.
base = None
if sys.platform == "win32":
    # 💡 Use the string 'Win32GUI'
    base = "Win32GUI" 

setup(
    name="Migration Tool.exe",
    version="2.2.4.1",
    description="A tool to migrate data from SQL Server to MySQL.",
    options={"build_exe": build_exe_options},
    executables=[Executable("newgui.py", base=base, target_name="Migration Tool.exe", icon="icon.ico")]
)
