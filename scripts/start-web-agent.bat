@echo off

rem Use local variables
setlocal

@call "setenv.bat"

start "WebUI"  /D "%WEB_CONSOLE_HOME%\web-agent" /B gridgain-web-console-agent.bat -n http://127.0.0.1:7080

endlocal
title Command Prompt
if "%OS%"=="Windows_NT" @endlocal
if "%OS%"=="WINNT" @endlocal
exit /B 0
