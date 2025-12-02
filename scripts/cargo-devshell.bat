@echo off
rem Initializes MSVC build environment and forwards args to cargo.
call "C:\Program Files (x86)\Microsoft Visual Studio\2022\BuildTools\VC\Auxiliary\Build\vcvars64.bat"
if %errorlevel% neq 0 (
  echo Failed to init vcvars64.bat
  exit /b %errorlevel%
)
"%USERPROFILE%\.cargo\bin\cargo.exe" %*
