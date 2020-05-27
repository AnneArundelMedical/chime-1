@echo on

set SRC_DIR=\\dataviz.aahs.org\L$\CovidLogs
set DST_DIR=%~dp0

copy %SRC_DIR%\CovidCensusSnapshot*.csv %DST_DIR%\input\
