REM Copyright 2026 Alibaba Group Holding Ltd.
REM
REM Licensed under the Apache License, Version 2.0 (the "License");
REM you may not use this file except in compliance with the License.
REM You may obtain a copy of the License at
REM
REM     http://www.apache.org/licenses/LICENSE-2.0
REM
REM Unless required by applicable law or agreed to in writing, software
REM distributed under the License is distributed on an "AS IS" BASIS,
REM WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
REM See the License for the specific language governing permissions and
REM limitations under the License.

@echo off
setlocal enableextensions

REM install.bat for dockur/windows OEM hook.
REM It prefers local execd.exe from C:\OEM and starts it.

set "EXECD_INSTALL_DIR=%EXECD_INSTALL_DIR%"
if "%EXECD_INSTALL_DIR%"=="" set "EXECD_INSTALL_DIR=C:\OpenSandbox"

set "EXECD_BIN=%EXECD_BIN%"
if "%EXECD_BIN%"=="" set "EXECD_BIN=%EXECD_INSTALL_DIR%\execd.exe"

set "EXECD_LOG_FILE=%EXECD_LOG_FILE%"
if "%EXECD_LOG_FILE%"=="" set "EXECD_LOG_FILE=%EXECD_INSTALL_DIR%\install.log"

set "EXECD_OEM_BIN=C:\OEM\execd.exe"
if not exist "%EXECD_OEM_BIN%" if exist "C:\OEM\execd_windows_amd64.exe" set "EXECD_OEM_BIN=C:\OEM\execd_windows_amd64.exe"
if not exist "%EXECD_OEM_BIN%" if exist "C:\OEM\execd_windows_arm64.exe" set "EXECD_OEM_BIN=C:\OEM\execd_windows_arm64.exe"

if not exist "%EXECD_INSTALL_DIR%" mkdir "%EXECD_INSTALL_DIR%"
if errorlevel 1 (
    echo [install.bat] WARN: failed to create install dir: %EXECD_INSTALL_DIR%
    exit /b 0
)

call :log "startup begin"
call :log "install dir: %EXECD_INSTALL_DIR%"
call :log "target bin: %EXECD_BIN%"
call :log "oem bin candidate: %EXECD_OEM_BIN%"

if exist "%EXECD_OEM_BIN%" (
    call :log "copying local binary from %EXECD_OEM_BIN%"
    copy /Y "%EXECD_OEM_BIN%" "%EXECD_BIN%" >nul
    if errorlevel 1 (
        call :log "WARN: failed to copy local binary"
        exit /b 0
    )
    call :log "copy local binary completed"
)

if not exist "%EXECD_BIN%" (
    call :log "WARN: execd binary not found at %EXECD_OEM_BIN%"
    call :log "WARN: execd binary not found at %EXECD_BIN%"
    exit /b 0
)

call :log "starting %EXECD_BIN%"
start "opensandbox-execd" /B "%EXECD_BIN%"
if errorlevel 1 (
    call :log "WARN: failed to start execd.exe"
    exit /b 0
)

call :log "execd started in background"
exit /b 0

:log
echo [install.bat] %~1
>>"%EXECD_LOG_FILE%" echo [install.bat] %~1
exit /b 0
