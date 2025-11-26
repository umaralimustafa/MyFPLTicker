import requests
import psycopg2
import os
from dotenv import load_dotenv

FPL_API_URL = "https://fantasy.premierleague.com/api/bootstrap-static/"

# Load environment variables from .env file
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '..', '.env'))

DB_PARAMS = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT", 5432))
}

def insert_teams_to_postgres(teams):
    conn = psycopg2.connect(**DB_PARAMS)
    cur = conn.cursor()
    for team in teams:
        cur.execute("""
            INSERT INTO teams (
                code, draw, form, id, loss, name, played, points, position, short_name,
                strength, team_division, unavailable, win, strength_overall_home,
                strength_overall_away, strength_attack_home, strength_attack_away,
                strength_defence_home, strength_defence_away, pulse_id
            ) VALUES (
                %(code)s, %(draw)s, %(form)s, %(id)s, %(loss)s, %(name)s, %(played)s, %(points)s, %(position)s, %(short_name)s,
                %(strength)s, %(team_division)s, %(unavailable)s, %(win)s, %(strength_overall_home)s,
                %(strength_overall_away)s, %(strength_attack_home)s, %(strength_attack_away)s,
                %(strength_defence_home)s, %(strength_defence_away)s, %(pulse_id)s
            )
            ON CONFLICT (code) DO NOTHING;
        """, team)
    conn.commit()
    cur.close()
    conn.close()

def fetch_and_insert_teams():
    response = requests.get(FPL_API_URL)
    response.raise_for_status()
    data = response.json()
    teams = data.get("teams", [])
    insert_teams_to_postgres(teams)

if __name__ == "__main__":
    fetch_and_insert_teams()
