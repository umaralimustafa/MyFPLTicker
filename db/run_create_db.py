import os
import subprocess
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '..', '.env'))

# Read variables
db_name = os.getenv('DB_NAME')
db_user = os.getenv('DB_USER')
db_password = os.getenv('DB_PASSWORD')
db_host = os.getenv('DB_HOST')
db_port = os.getenv('DB_PORT')

# Prepare psql variable setting commands
psql_vars = f"\\set db_name '{db_name}'\n\\set db_user '{db_user}'\n\\set db_password '{db_password}'\n\\set db_host '{db_host}'\n\\set db_port '{db_port}'\n"

# Read the SQL script
with open(os.path.join(os.path.dirname(__file__), 'create_db.sql'), 'r') as f:
    sql_script = f.read()

# Combine variable settings and SQL script
full_script = psql_vars + sql_script

# Write to a temporary file
tmp_sql_path = os.path.join(os.path.dirname(__file__), 'tmp_create_db.sql')
with open(tmp_sql_path, 'w') as f:
    f.write(full_script)

# Run the script using psql
psql_command = [
    'psql',
    '-U', db_user,
    '-h', db_host,
    '-p', db_port,
    '-f', tmp_sql_path
]

# Set PGPASSWORD for authentication
env = os.environ.copy()
if db_password is None:
    raise ValueError("DB_PASSWORD environment variable is not set.")
env['PGPASSWORD'] = db_password

subprocess.run(psql_command, env=env)

# Clean up temporary file
os.remove(tmp_sql_path)
