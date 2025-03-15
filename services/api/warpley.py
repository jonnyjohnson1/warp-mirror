import requests
import re
from collections import defaultdict
from typing import List, Dict, Any
from datetime import datetime

import pandas as pd

from tqdm import tqdm

from services.nlp.nlp_processor import NLPProcessor

class WarpcastAPI:
    def __init__(self, url: str):
        self.url = url
        self.data: List[Dict[str, Any]] = []
        self.user_database: Dict[str, Dict[str, Any]] = {}
        self.edge_database: List[Dict[str, Any]] = []
        self.engagement_metrics: Dict[str, Dict[str, int]] = defaultdict(lambda: {"likes": 0, "recasts": 0, "replies": 0, "total": 0})
    
    def format_timestamp(self, timestamp) -> str:
        timestamp_sec = timestamp / 1000
        return datetime.fromtimestamp(timestamp_sec).strftime("%B %d, %I:%M %p")

        
    def fetch_data(self) -> None:
        """Fetches data from the API and stores it in a list."""
        try:
            response = requests.get(self.url)
            response.raise_for_status()
            
            json_data = response.json()
            self.data = json_data.get("casts", [])
            print(f"Fetched {len(self.data)} casts.")
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data: {e}")
    
    def parse_casts(self) -> List[Dict[str, Any]]:
        """Parses relevant fields from each cast."""
        parsed_data = []
        for cast in tqdm(self.data):
            sentiment = NLPProcessor.analyze_sentiment(cast["text"])
            emotion = NLPProcessor.detect_emotion(cast["text"])
            topics = NLPProcessor.extract_topics(cast["text"])
            
            parsed_cast = {
                "id": cast["id"],
                "author": {
                    "username": cast["author"]["username"],
                    "displayName": cast["author"]["displayName"],
                    "profileImage": cast["author"]["profileImage"],
                },
                "text": cast["text"],
                "timestamp": cast["timestamp"],
                "engagement": cast["engagement"],
                "sentiment": sentiment,
                "emotion": emotion,
                "topics": topics,
                "embeds": cast.get("embeds", {})
            }
            parsed_data.append(parsed_cast)
            self.save_to_user_database(parsed_cast)
            self.process_edge_relationships(parsed_cast)
            self.track_engagement(parsed_cast)
        return parsed_data
    
    def save_to_user_database(self, cast: Dict[str, Any]) -> None:
        """Saves user data to the user database."""
        author = cast["author"]
        username = author["username"]
        
        if username not in self.user_database:
            self.user_database[username] = {
                "fid": cast["id"],
                "name": username,
                "display_name": author["displayName"],
                "msg_count": 0,
                "messages": []
            }
        
        self.user_database[username]["messages"].append(cast["text"])
        self.user_database[username]["msg_count"] += 1
    
    def process_edge_relationships(self, cast: Dict[str, Any]) -> None:
        """Creates edge relationships based on mentions in text."""
        author = cast["author"]["username"]
        text = cast["text"]
        timestamp = cast['timestamp']
        mentions = re.findall(r"@([a-zA-Z0-9_.]+)", text)
        
        for mentioned_user in mentions:
            relationship_type = "mention"
            if text.startswith(f"@{mentioned_user}"):
                relationship_type = "reply"
            elif "callout" in text.lower():
                relationship_type = "callout"
            
            self.edge_database.append({
                "timestamp": timestamp,
                "from": author,
                "to": mentioned_user,
                "type": relationship_type
            })
    
    def track_engagement(self, cast: Dict[str, Any]) -> None:
        """Tracks engagement metrics for each user."""
        username = cast["author"]["username"]
        for key in ["likes", "recasts", "replies", "total"]:
            self.engagement_metrics[username][key] += cast["engagement"].get(key, 0)
    
    def display_casts(self) -> None:
        """Displays the casts in a nicely formatted CLI text block."""
        for cast in self.parse_casts():
            print("=" * 50)
            print(f"Author: {cast['author']['displayName']} (@{cast['author']['username']})")
            print(f"Profile Image: {cast['author']['profileImage']}")
            print(f"Text: {cast['text']}")
            print(f"Timestamp: {self.format_timestamp(cast['timestamp'])}")
            print(f"Sentiment: {cast['sentiment']}")
            print(f"Emotion: {cast['emotion']}")
            print(f"Topics: {', '.join(cast['topics'])}")
            print("Engagement:")
            print(f"  Likes: {cast['engagement']['likes']}")
            print(f"  Recasts: {cast['engagement']['recasts']}")
            print(f"  Replies: {cast['engagement']['replies']}")
            print(f"  Total: {cast['engagement']['total']}")
            print("=" * 50 + "\n")

    def show_users_dataframe(self):
        """Displays the users as a Pandas DataFrame."""
        users_df = pd.DataFrame([
            {
                "fid": data["fid"],
                "username": username,
                "display_name": data["display_name"],
                "messages": "\n".join(data["messages"])
            }
            for username, data in self.user_database.items()
        ])
        print("Users Data")
        print(users_df)

    def show_edge_dataframe(self):
        """Displays the edges as a Pandas DataFrame."""
        edges_df = pd.DataFrame(self.edge_database)
        print("Edge Data")
        print(edges_df)

    def show_engagement_metrics_dataframe(self):
        """Displays the engagement metrics as a Pandas DataFrame."""
        engagement_df = pd.DataFrame.from_dict(self.engagement_metrics, orient="index").reset_index()
        engagement_df.rename(columns={"index": "username"}, inplace=True)
        print("Engagement Metrics")
        print(engagement_df)
