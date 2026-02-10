@echo off
echo ============================================
echo VOLLAND TEST SCRAPER - WITH DOM DETECTION
echo ============================================
echo.
echo Starting test...
echo Browser will open - you can interact with widgets!
echo.

cd /d "%~dp0"
python test_scraper.py

echo.
echo ============================================
echo TEST COMPLETE - Check the captures folder
echo ============================================
pause
