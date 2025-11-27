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

def insert_teams_to_postgres(teams, cur):
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

def insert_elements_to_postgres(elements, cur):
    for el in elements:
        cur.execute("""
            INSERT INTO elements (
                id, code, first_name, second_name, web_name, team, team_code, element_type, now_cost,
                points_per_game, total_points, minutes, goals_scored, assists, clean_sheets, goals_conceded,
                own_goals, penalties_saved, penalties_missed, yellow_cards, red_cards, saves, bonus, bps,
                influence, creativity, threat, ict_index, form, status, birth_date, region, team_join_date
            ) VALUES (
                %(id)s, %(code)s, %(first_name)s, %(second_name)s, %(web_name)s, %(team)s, %(team_code)s, %(element_type)s, %(now_cost)s,
                %(points_per_game)s, %(total_points)s, %(minutes)s, %(goals_scored)s, %(assists)s, %(clean_sheets)s, %(goals_conceded)s,
                %(own_goals)s, %(penalties_saved)s, %(penalties_missed)s, %(yellow_cards)s, %(red_cards)s, %(saves)s, %(bonus)s, %(bps)s,
                %(influence)s, %(creativity)s, %(threat)s, %(ict_index)s, %(form)s, %(status)s, %(birth_date)s, %(region)s, %(team_join_date)s
            )
            ON CONFLICT (id) DO NOTHING;
        """, el)

def fetch_and_insert_data():
    response = requests.get(FPL_API_URL)
    response.raise_for_status()
    data = response.json()
    teams = data.get("teams", [])
    elements = data.get("elements", [])
    conn = psycopg2.connect(**DB_PARAMS)
    cur = conn.cursor()
    insert_teams_to_postgres(teams, cur)
    insert_elements_to_postgres(elements, cur)
    conn.commit()
    cur.close()
    conn.close()

if __name__ == "__main__":
    fetch_and_insert_data()
