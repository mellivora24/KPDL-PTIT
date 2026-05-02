!bin/bash

python --version

if [ ! -d ".venv" ]; then
    python -m venv .venv
fi

if [[ "$OSTYPE" == "linux-gnu"* ]]; then
    source .venv/bin/activate
elif [[ "$OSTYPE" == "msys"* ]]; then
    .venv\Scripts\activate
else
    echo "Unsupported OS: $OSTYPE"
    exit 1
fi

pip install -r requirements.txt

echo "Kiểm tra kết nối đến SQL Server..."
python -c "import pyodbc; conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost;DATABASE=master;Trusted_Connection=yes;'); cursor = conn.cursor(); cursor.execute('SELECT @@VERSION'); print(cursor.fetchone()[0])"
