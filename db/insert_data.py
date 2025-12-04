import requests
import psycopg2
import os
from dotenv import load_dotenv

FPL_API_URL = "https://fantasy.premierleague.com/api/bootstrap-static/"
FPL_FIXTURES_URL = "https://fantasy.premierleague.com/api/fixtures/"

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
            ON CONFLICT (id) DO NOTHING;
        """, team)

def insert_elements_to_postgres(elements, cur):
    float_fields = [
        "ep_next", "ep_this", "form", "points_per_game", "selected_by_percent", "value_form", "value_season",
        "influence", "creativity", "threat", "ict_index", "expected_goals", "expected_assists", "expected_goal_involvements",
        "expected_goals_conceded", "expected_goals_per_90", "saves_per_90", "expected_assists_per_90",
        "expected_goal_involvements_per_90", "expected_goals_conceded_per_90", "goals_conceded_per_90",
        "starts_per_90", "clean_sheets_per_90", "defensive_contribution_per_90"
    ]
    for el in elements:
        # Convert string fields to float if present
        for f in float_fields:
            if f in el and el[f] not in (None, ""):
                try:
                    el[f] = float(el[f])
                except (ValueError, TypeError):
                    el[f] = None
        cur.execute("""
            INSERT INTO elements (
                id, can_transact, can_select, chance_of_playing_next_round, chance_of_playing_this_round, code,
                cost_change_event, cost_change_event_fall, cost_change_start, cost_change_start_fall, dreamteam_count,
                element_type, ep_next, ep_this, event_points, first_name, form, in_dreamteam, news, news_added,
                now_cost, photo, points_per_game, removed, second_name, selected_by_percent, special, squad_number,
                status, team, team_code, total_points, transfers_in, transfers_in_event, transfers_out, transfers_out_event,
                value_form, value_season, web_name, region, team_join_date, birth_date, has_temporary_code, opta_code,
                minutes, goals_scored, assists, clean_sheets, goals_conceded, own_goals, penalties_saved, penalties_missed,
                yellow_cards, red_cards, saves, bonus, bps, influence, creativity, threat, ict_index,
                clearances_blocks_interceptions, recoveries, tackles, defensive_contribution, starts, expected_goals,
                expected_assists, expected_goal_involvements, expected_goals_conceded, influence_rank, influence_rank_type,
                creativity_rank, creativity_rank_type, threat_rank, threat_rank_type, ict_index_rank, ict_index_rank_type,
                corners_and_indirect_freekicks_order, corners_and_indirect_freekicks_text, direct_freekicks_order,
                direct_freekicks_text, penalties_order, penalties_text, expected_goals_per_90, saves_per_90,
                expected_assists_per_90, expected_goal_involvements_per_90, expected_goals_conceded_per_90,
                goals_conceded_per_90, now_cost_rank, now_cost_rank_type, form_rank, form_rank_type,
                points_per_game_rank, points_per_game_rank_type, selected_rank, selected_rank_type,
                starts_per_90, clean_sheets_per_90, defensive_contribution_per_90
            ) VALUES (
                %(id)s, %(can_transact)s, %(can_select)s, %(chance_of_playing_next_round)s, %(chance_of_playing_this_round)s, %(code)s,
                %(cost_change_event)s, %(cost_change_event_fall)s, %(cost_change_start)s, %(cost_change_start_fall)s, %(dreamteam_count)s,
                %(element_type)s, %(ep_next)s, %(ep_this)s, %(event_points)s, %(first_name)s, %(form)s, %(in_dreamteam)s, %(news)s, %(news_added)s,
                %(now_cost)s, %(photo)s, %(points_per_game)s, %(removed)s, %(second_name)s, %(selected_by_percent)s, %(special)s, %(squad_number)s,
                %(status)s, %(team)s, %(team_code)s, %(total_points)s, %(transfers_in)s, %(transfers_in_event)s, %(transfers_out)s, %(transfers_out_event)s,
                %(value_form)s, %(value_season)s, %(web_name)s, %(region)s, %(team_join_date)s, %(birth_date)s, %(has_temporary_code)s, %(opta_code)s,
                %(minutes)s, %(goals_scored)s, %(assists)s, %(clean_sheets)s, %(goals_conceded)s, %(own_goals)s, %(penalties_saved)s, %(penalties_missed)s,
                %(yellow_cards)s, %(red_cards)s, %(saves)s, %(bonus)s, %(bps)s, %(influence)s, %(creativity)s, %(threat)s, %(ict_index)s,
                %(clearances_blocks_interceptions)s, %(recoveries)s, %(tackles)s, %(defensive_contribution)s, %(starts)s, %(expected_goals)s,
                %(expected_assists)s, %(expected_goal_involvements)s, %(expected_goals_conceded)s, %(influence_rank)s, %(influence_rank_type)s,
                %(creativity_rank)s, %(creativity_rank_type)s, %(threat_rank)s, %(threat_rank_type)s, %(ict_index_rank)s, %(ict_index_rank_type)s,
                %(corners_and_indirect_freekicks_order)s, %(corners_and_indirect_freekicks_text)s, %(direct_freekicks_order)s,
                %(direct_freekicks_text)s, %(penalties_order)s, %(penalties_text)s, %(expected_goals_per_90)s, %(saves_per_90)s,
                %(expected_assists_per_90)s, %(expected_goal_involvements_per_90)s, %(expected_goals_conceded_per_90)s,
                %(goals_conceded_per_90)s, %(now_cost_rank)s, %(now_cost_rank_type)s, %(form_rank)s, %(form_rank_type)s,
                %(points_per_game_rank)s, %(points_per_game_rank_type)s, %(selected_rank)s, %(selected_rank_type)s,
                %(starts_per_90)s, %(clean_sheets_per_90)s, %(defensive_contribution_per_90)s
            )
            ON CONFLICT (id) DO NOTHING;
        """, el)

def insert_fixtures_to_postgres(fixtures, cur):
    for fixture in fixtures:
        cur.execute("""
            INSERT INTO fixtures (
                id, code, event, finished, finished_provisional, kickoff_time, minutes,
                provisional_start_time, started, team_a, team_a_score, team_h, team_h_score,
                team_h_difficulty, team_a_difficulty, pulse_id
            ) VALUES (
                %(id)s, %(code)s, %(event)s, %(finished)s, %(finished_provisional)s, %(kickoff_time)s, %(minutes)s,
                %(provisional_start_time)s, %(started)s, %(team_a)s, %(team_a_score)s, %(team_h)s, %(team_h_score)s,
                %(team_h_difficulty)s, %(team_a_difficulty)s, %(pulse_id)s
            )
            ON CONFLICT (id) DO NOTHING;
        """, fixture)
        
        # Insert fixture stats
        stats = fixture.get("stats", [])
        for stat in stats:
            stat_identifier = stat.get("identifier")
            
            # Process away team stats
            for player_stat in stat.get("a", []):
                cur.execute("""
                    INSERT INTO fixture_stats (
                        fixture_id, stat_identifier, element, value, is_home
                    ) VALUES (
                        %s, %s, %s, %s, %s
                    )
                """, (fixture["id"], stat_identifier, player_stat["element"], player_stat["value"], False))
            
            # Process home team stats
            for player_stat in stat.get("h", []):
                cur.execute("""
                    INSERT INTO fixture_stats (
                        fixture_id, stat_identifier, element, value, is_home
                    ) VALUES (
                        %s, %s, %s, %s, %s
                    )
                """, (fixture["id"], stat_identifier, player_stat["element"], player_stat["value"], True))

def fetch_and_insert_data():
    # Fetch bootstrap-static data (teams and elements)
    response = requests.get(FPL_API_URL)
    response.raise_for_status()
    data = response.json()
    teams = data.get("teams", [])
    elements = data.get("elements", [])
    
    # Fetch fixtures data
    fixtures_response = requests.get(FPL_FIXTURES_URL)
    fixtures_response.raise_for_status()
    fixtures = fixtures_response.json()
    
    conn = psycopg2.connect(**DB_PARAMS)
    cur = conn.cursor()
    
    insert_teams_to_postgres(teams, cur)
    insert_elements_to_postgres(elements, cur)
    insert_fixtures_to_postgres(fixtures, cur)
    
    conn.commit()
    cur.close()
    conn.close()

if __name__ == "__main__":
    fetch_and_insert_data()
