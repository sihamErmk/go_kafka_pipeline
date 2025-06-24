@echo off
echo Starting Kafka Stress Test...
echo.

REM Run stress test
echo === Kafka Stress Test ===
python stress_test.py
echo.

echo Stress test completed!
pause