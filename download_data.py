import requests
import sqlite3
import json
from abc import ABC, abstractmethod
import sys
import time

class WarpAPI:
    def __init__(self, base_url):
        """
        Initialize the WarpAPI instance.
        
        Args:
            base_url (str): The base URL for the WarpCast API endpoint.
        """
        self.base_url = base_url
        self.channels = []
        self.channel_dict = {}
    
    def get_all_channels(self, limit=100):
        """
        Fetch all channels and store them in the channels attribute.
        
        Args:
            limit (int): The number of channels to retrieve.
            
        Returns:
            list: A list of channel dictionaries.
        """
        url = f"{self.base_url}?allChannels=true&limit={limit}"
        try:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            self.channels = data.get("channels", [])
            # Sort channels by followerCount in descending order
            self.channels.sort(key=lambda channel: channel.get('followerCount', 0), reverse=True)
            return self.channels
        except requests.RequestException as error:
            print(f"Error fetching channels: {error}")
            return []
    
    def get_channel_followers(self, channel_id, cursor=None):
        """
        Fetch a page of channel followers, with a retry mechanism.
        
        Args:
            channel_id (str): The ID of the channel.
            cursor (str): Pagination cursor, if available.
        
        Returns:
            tuple: (followers list, next cursor)
            
        Raises:
            requests.RequestException: If all retries fail.
        """
        url = f'https://api.warpcast.com/v1/channel-followers?channelId={channel_id}'
        if cursor:
            url += f"&cursor={cursor}"
        
        # Retry delays in seconds: 3, 5, 10, 60
        delays = [3, 5, 10, 60]
        attempt = 0
        
        while True:
            try:
                response = requests.get(url)
                response.raise_for_status()
                data = response.json()
                # Expecting follower data to look like:
                # {"fid": 588495, "followedAt": 1742090574}
                followers = data.get("result", {}).get("users", [])
                next_cursor = data.get("next", {}).get("cursor")
                return followers, next_cursor
            except requests.RequestException as error:
                if attempt < len(delays):
                    wait_time = delays[attempt]
                    print(f"Error fetching followers for channel {channel_id}: {error}. Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                    attempt += 1
                else:
                    print("Failed after multiple retries.")
                    raise error

    def fetch_and_insert_followers_in_batches(self, channel_id, db, batch_size=100):
        """
        Fetch followers for a given channel and insert them in batches into the database.
        
        Args:
            channel_id (str): The ID of the channel.
            db (Database): An instance of a Database (or subclass) to insert data.
            batch_size (int): The number of followers per batch.
        """
        cursor = None
        batch = []
        while True:
            followers, cursor = self.get_channel_followers(channel_id, cursor)
            if followers:
                batch.extend(followers)
                # Overwrite the same line with the current batch size
                sys.stdout.write(f"\rDownloading followers for channel {channel_id}: batch size is now {len(batch)}")
                sys.stdout.flush()
                # Once we've collected enough followers for a batch, insert them
                if len(batch) >= batch_size:
                    # Clear the line and print the insertion message
                    sys.stdout.write("\r" + " " * 80 + "\r")
                    print(f"Inserting batch of {len(batch)} followers for channel {channel_id}...")
                    db.insert_followers_batch(channel_id, batch)
                    batch = []  # Reset the batch
            if not cursor:
                # Insert any remaining followers in the final batch
                if batch:
                    sys.stdout.write("\r" + " " * 80 + "\r")
                    print(f"Inserting final batch of {len(batch)} followers for channel {channel_id}...")
                    db.insert_followers_batch(channel_id, batch)
                break

# ------------------------------------------
# Database Classes using OOP and Abstraction
# ------------------------------------------

class Database(ABC):
    """
    Abstract base class defining the interface for a database.
    """
    @abstractmethod
    def connect(self):
        pass

    @abstractmethod
    def create_tables(self):
        pass

    @abstractmethod
    def insert_channel(self, channel):
        pass

    @abstractmethod
    def insert_follower(self, channel_id, follower):
        pass

    @abstractmethod
    def insert_followers_batch(self, channel_id, followers):
        pass

class SQLDatabase(Database):
    """
    SQL Database implementation using SQLite.
    
    This version creates:
      - A 'channels' table for channel details.
      - For each channel, a separate followers table named "followers_<channel_id>".
    """
    def __init__(self, db_path="channels.db"):
        self.db_path = db_path
        self.conn = None
        self.connect()
        self.create_tables()

    def connect(self):
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row

    def create_tables(self):
        # Create the channels table with fields corresponding to channel JSON structure
        create_channels_query = """
        CREATE TABLE IF NOT EXISTS channels (
            id TEXT PRIMARY KEY,
            url TEXT,
            name TEXT,
            description TEXT,
            descriptionMentions TEXT,
            descriptionMentionsPositions TEXT,
            imageUrl TEXT,
            headerImageUrl TEXT,
            leadFid INTEGER,
            moderatorFids TEXT,
            createdAt INTEGER,
            followerCount INTEGER,
            memberCount INTEGER,
            publicCasting INTEGER
        );
        """
        self.conn.execute(create_channels_query)
        self.conn.commit()

    def insert_channel(self, channel):
        """
        Insert a channel record if it does not already exist.
        """
        channel_id = channel.get("id")
        cur = self.conn.execute("SELECT COUNT(*) FROM channels WHERE id=?", (channel_id,))
        if cur.fetchone()[0] > 0:
            print(f"Channel {channel_id} already exists. Skipping insertion.")
            return
        # Extract individual fields from the channel JSON
        url = channel.get("url")
        name = channel.get("name")
        description = channel.get("description")
        descriptionMentions = json.dumps(channel.get("descriptionMentions", []))
        descriptionMentionsPositions = json.dumps(channel.get("descriptionMentionsPositions", []))
        imageUrl = channel.get("imageUrl")
        headerImageUrl = channel.get("headerImageUrl")
        leadFid = channel.get("leadFid")
        moderatorFids = json.dumps(channel.get("moderatorFids", []))
        createdAt = channel.get("createdAt")
        followerCount = channel.get("followerCount")
        memberCount = channel.get("memberCount")
        publicCasting = 1 if channel.get("publicCasting") else 0

        query = """
        INSERT INTO channels 
            (id, url, name, description, descriptionMentions, descriptionMentionsPositions, 
             imageUrl, headerImageUrl, leadFid, moderatorFids, createdAt, followerCount, 
             memberCount, publicCasting)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        self.conn.execute(query, (channel_id, url, name, description, descriptionMentions,
                                    descriptionMentionsPositions, imageUrl, headerImageUrl,
                                    leadFid, moderatorFids, createdAt, followerCount,
                                    memberCount, publicCasting))
        self.conn.commit()
        print(f"Inserted channel {channel_id} into channels table.")

    def create_followers_table(self, channel_id):
        """
        Create a followers table for the specified channel with columns matching the follower JSON.
        """
        table_name = f"followers_{channel_id}"
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            fid INTEGER PRIMARY KEY,
            followedAt INTEGER
        );
        """
        self.conn.execute(create_table_query)
        self.conn.commit()

    def insert_follower(self, channel_id, follower):
        """
        Insert a single follower record if it does not already exist.
        """
        table_name = f"followers_{channel_id}"
        self.create_followers_table(channel_id)
        fid = follower.get("fid")
        followedAt = follower.get("followedAt")
        cur = self.conn.execute(f"SELECT COUNT(*) FROM {table_name} WHERE fid=?", (fid,))
        if cur.fetchone()[0] > 0:
            print(f"Follower {fid} already exists in table {table_name}. Skipping insertion.")
            return
        query = f"INSERT INTO {table_name} (fid, followedAt) VALUES (?, ?)"
        self.conn.execute(query, (fid, followedAt))
        self.conn.commit()
        print(f"Inserted follower {fid} into table {table_name}.")

    def insert_followers_batch(self, channel_id, followers):
        """
        Insert a batch of followers into the followers table for the channel.
        Uses 'INSERT OR IGNORE' to avoid duplicate records.
        """
        table_name = f"followers_{channel_id}"
        self.create_followers_table(channel_id)
        data_to_insert = []
        for follower in followers:
            fid = follower.get("fid")
            followedAt = follower.get("followedAt")
            data_to_insert.append((fid, followedAt))
        query = f"INSERT OR IGNORE INTO {table_name} (fid, followedAt) VALUES (?, ?)"
        self.conn.executemany(query, data_to_insert)
        self.conn.commit()
        print(f"Inserted a batch of {len(data_to_insert)} followers into table {table_name}.")

# ------------------------------------------
# Example usage:
# ------------------------------------------
if __name__ == '__main__':
    # Instantiate the API client with your base URL
    api = WarpAPI("https://warpley.netlify.app/.netlify/functions/warpcast-api")
    
    # Retrieve channels (limit 50 for example)
    channels = api.get_all_channels(limit=50)
    print("Retrieved Channels:")
    print(channels)

    # Create SQL database instance
    db = SQLDatabase("channels.db")
    
    # Insert channels and their followers in batches
    for channel in channels[2:]:
        db.insert_channel(channel)
        channel_id = channel.get("id")
        follower_count = channel.get("followerCount")
        print(f"CHANNEL {channel_id} :: downloading followers :: {follower_count}")
        # For each channel, fetch and insert followers in batches of 100
        api.fetch_and_insert_followers_in_batches(channel_id, db, batch_size=1500)
