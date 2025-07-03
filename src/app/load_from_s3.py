import os
import json
import psycopg2
import boto3
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


# === Load secure parameters from AWS SSM Parameter Store ===
ssm = boto3.client("ssm", region_name=os.getenv("AWS_REGION", "us-west-1"))

def get_ssm_param(name):
    response = ssm.get_parameter(Name=name, WithDecryption=True)
    return response["Parameter"]["Value"]   

# Load secrets from SSM (fallback to env if not found)
DATABASE_URL = os.getenv("DATABASE_URL") or get_ssm_param("/football/DATABASE_URL")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME") or get_ssm_param("/football/S3_BUCKET_NAME")
S3_PREFIX = os.getenv("S3_PREFIX", "open-data/data/")
AWS_REGION = os.getenv("AWS_REGION", "us-west-1")

# === Use instance role OR .env credentials ===
if os.getenv("AWS_ACCESS_KEY_ID") and os.getenv("AWS_SECRET_ACCESS_KEY"):
    s3 = boto3.client(
        "s3",
        region_name=AWS_REGION,
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
    )
else:
    s3 = boto3.client("s3", region_name=AWS_REGION)


def load_competitions():
    print("Loading competitions.json...")
    obj = s3.get_object(Bucket=S3_BUCKET_NAME, Key=f"{S3_PREFIX}competitions.json")
    data = json.load(obj["Body"])

    with psycopg2.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS competitions (
                    competition_id INT,
                    season_id INT,
                    country_name TEXT,
                    competition_name TEXT,
                    season_name TEXT
                )
            """)

            for row in data:
                cur.execute("""
                    INSERT INTO competitions (competition_id, season_id, country_name, competition_name, season_name)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                """, (
                    row["competition_id"],
                    row["season_id"],
                    row["country_name"],
                    row["competition_name"],
                    row["season_name"]
                ))

    print("Done: competitions.json loaded.")

def load_matches():
    print("Loading matches...")
    with psycopg2.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS matches (
                    match_id BIGINT PRIMARY KEY,
                    competition_id INT,
                    season_id INT,
                    match_date DATE,
                    home_team TEXT,
                    away_team TEXT
                )
            """)

            competitions = json.load(s3.get_object(Bucket=S3_BUCKET_NAME, Key=f"{S3_PREFIX}competitions.json")["Body"])

            for comp in competitions:
                comp_id = comp["competition_id"]
                season_id = comp["season_id"]
                key = f"{S3_PREFIX}matches/{comp_id}/{season_id}.json"

                print(f"Loading matches for competition {comp_id} season {season_id}...")
                try:
                    data = json.load(s3.get_object(Bucket=S3_BUCKET_NAME, Key=key)["Body"])
                    for match in data:
                        cur.execute("""
                            INSERT INTO matches (match_id, competition_id, season_id, match_date, home_team, away_team)
                            VALUES (%s, %s, %s, %s, %s, %s)
                            ON CONFLICT (match_id) DO NOTHING
                        """, (
                            match["match_id"],
                            comp_id,
                            season_id,
                            match["match_date"],
                            match["home_team"]["home_team_name"],
                            match["away_team"]["away_team_name"]
                        ))
                        print(f"Inserted match {match['match_id']} for competition {comp_id} season {season_id}")
                except Exception as e:
                    print(f"Failed to load matches from {key}: {e}")

    print("Done: matches loaded.")

def load_lineups():
    print("Loading lineups...")
    with psycopg2.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS lineups (
                    match_id BIGINT,
                    team_name TEXT,
                    player_name TEXT
                )
            """)

            competitions = json.load(s3.get_object(Bucket=S3_BUCKET_NAME, Key=f"{S3_PREFIX}competitions.json")["Body"])

            for comp in competitions:
                comp_id = comp["competition_id"]
                season_id = comp["season_id"]
                match_key = f"{S3_PREFIX}matches/{comp_id}/{season_id}.json"

                print(f"Loading lineups for competition {comp_id} season {season_id}...")

                try:
                    matches = json.load(s3.get_object(Bucket=S3_BUCKET_NAME, Key=match_key)["Body"])
                    for match in matches:
                        match_id = match["match_id"]
                        key = f"{S3_PREFIX}lineups/{match_id}.json"
                        data = json.load(s3.get_object(Bucket=S3_BUCKET_NAME, Key=key)["Body"])

                        print(f"Loading lineups for match {match_id}...")
                        for team in data:
                            team_name = team["team_name"]
                            for player in team["lineup"]:
                                cur.execute("""
                                    INSERT INTO lineups (match_id, team_name, player_name)
                                    VALUES (%s, %s, %s)
                                """, (match_id, team_name, player["player_name"]))
                except Exception as e:
                    print(f"Failed to load lineups for match {match_id}: {e}")

    print("Done: lineups loaded.")

def load_events():
    print("Loading events...")
    with psycopg2.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS events (
                    id SERIAL PRIMARY KEY,
                    match_id BIGINT,
                    index INT,
                    timestamp TEXT,
                    type TEXT
                )
            """)

            competitions = json.load(s3.get_object(Bucket=S3_BUCKET_NAME, Key=f"{S3_PREFIX}competitions.json")["Body"])
            total_comps = len(competitions)

            for comp_index, comp in enumerate(competitions):
                comp_id = comp["competition_id"]
                season_id = comp["season_id"]
                print(f"Loading competition {comp_id} season {season_id} ({comp_index + 1}/{total_comps})...")
                match_key = f"{S3_PREFIX}matches/{comp_id}/{season_id}.json"

                try:
                    matches = json.load(s3.get_object(Bucket=S3_BUCKET_NAME, Key=match_key)["Body"])
                    total_matches = len(matches)

                    for match_index, match in enumerate(matches):
                        match_id = match["match_id"]
                        key = f"{S3_PREFIX}events/{match_id}.json"
                        data = json.load(s3.get_object(Bucket=S3_BUCKET_NAME, Key=key)["Body"])

                        for event in data:
                            cur.execute("""
                                INSERT INTO events (match_id, index, timestamp, type)
                                VALUES (%s, %s, %s, %s)
                            """, (
                                match_id,
                                event.get("index"),
                                event.get("timestamp"),
                                event.get("type", {}).get("name")
                            ))

                        percent = ((match_index + 1) / total_matches) * 100
                        print(f"Inserted events for match {match_id} ({match_index + 1}/{total_matches}) - {percent:.2f}% complete")

                except Exception as e:
                    print(f"Failed to load events for match {match_id}: {e}")

    print("Done: events loaded.")
# === CLI loader selector ===
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--type", choices=["competitions", "matches", "lineups", "events"], default="competitions")
    args = parser.parse_args()

    if args.type == "competitions":
        load_competitions()
    elif args.type == "matches":
        load_matches()
    elif args.type == "lineups":
        load_lineups()
    elif args.type == "events":
        load_events()