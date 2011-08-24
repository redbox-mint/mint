@echo off

REM show usage if no parameters given
if "%1" == "" goto usage

set PROGRAM_DIR=%~dp0
call "%PROGRAM_DIR%tf_env.bat"

set LOG_FILE=%FASCINATOR_HOME%/logs/geo-harvest.out
echo "Geonames Solr Index building."
echo "Logging to: '%LOG_FILE%'"

set CLASSPATH=%PROJECT_HOME%/home/geonames/solr/conf;%CLASSPATH%
echo bob > "%LOG_FILE%bob"
REM call java %JAVA_OPTS% -cp %CLASSPATH% com.googlecode.solrgeonames.harvester.Harvester "%PROGRAM_DIR%%1" > "%LOG_FILE%"
goto end

:usage
echo This script requires the first parameter to be in the input file to ingest.
echo NOTE: This input file is expected to be a tab delimited geonames data dump.
echo Usage: %0 inputFile.txt

:end