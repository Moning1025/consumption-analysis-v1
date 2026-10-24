@echo off
chcp 65001 >nul
cd /d "%~dp0"
echo ===== 多源数据融合的本地消费分析系统 =====
"..\venv_p3\Scripts\python.exe" main.py
if errorlevel 1 (
  echo.
  echo [启动失败] 请把上面的报错截图发给开发助手。
  pause
)
