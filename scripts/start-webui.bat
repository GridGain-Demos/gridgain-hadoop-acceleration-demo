@echo off

rem Use local variables
setlocal

@call "setenv.bat"


start "WebUI"  /D "%WEB_CONSOLE_HOME%\" /B web-console.bat --server:port 3000

endlocal
title Command Prompt
if "%OS%"=="Windows_NT" @endlocal
if "%OS%"=="WINNT" @endlocal
exit /B 0
