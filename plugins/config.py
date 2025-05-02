"""Configuration variables for ETL process"""
from typing import List

TABLE_NAMES: List[str] = [
    "staging_events",
    "staging_songs",
    "songplays",
    "users",
    "songs",
    "artists",
    "time",
]
