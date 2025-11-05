echo "===== MyPy ======="
poetry run mypy focus_validator/
echo "=================="
echo ""

echo "===== iSort ======"
poetry run isort focus_validator/ -c
echo "=================="
echo ""

echo "===== Black ======"
poetry run black focus_validator/ --check
echo "=================="
echo ""

echo "==== Flake8 ======"
poetry run flake8 focus_validator/
echo "=================="
echo ""

