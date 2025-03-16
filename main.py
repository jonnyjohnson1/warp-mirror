import requests

class WarpAPI:
    def __init__(self, base_url):
        """
        Initialize the WarpAPI instance.
        
        Args:
            base_url (str): The base URL for the WarpCast API endpoint.
                            For example: "https://https://warpley.netlify.app/.netlify/functions/warpcast-api"
        """
        self.base_url = base_url
        self.channels = []          # This attribute will hold the list of channels retrieved
        self.channel_dict = {}

    def get_all_channels(self, limit=100):
        """
        Fetch all channels from the Farcaster API and store them in the 'channels' attribute.
        
        Args:
            limit (int): Number of channels to retrieve (Default: 100, Maximum: 100).
        
        Returns:
            list: A list of channel dictionaries if the request is successful; otherwise, an empty list.
        """
        # Construct the request URL with parameters
        url = f"{self.base_url}?allChannels=true&limit={limit}"
    
        try:
            response = requests.get(url)
            response.raise_for_status()  # Raises an HTTPError if the response code was unsuccessful
            data = response.json()
            
            # Store channels from the response in the class attribute
            self.channels = data.get("channels", [])
            
            # Sort the channels list by memberCount in ascending order
            self.channels.sort(key=lambda channel: channel.get('followerCount', 0), reverse=True)
            
            return self.channels
        
        except requests.RequestException as error:
            print(f"Error fetching channels: {error}")
            return []
    
    def get_channel_followers(self, channel_id, cursor=None):
        url = f'https://api.warpcast.com/v1/channel-followers?channelId={channel_id}'
        # params = {"channelId": channel_id}
        if cursor:
            url += f"&cursor={cursor}"
            # params["cursor"] = cursor
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        # Get the list of followers
        followers = data.get("result", {}).get("users", [])
        # Get the next cursor, if available
        next_cursor = data.get("next", {}).get("cursor")
        return followers, next_cursor

    def fetch_all_followers(self, channel_id):
        all_followers = []
        cursor = None
        while True:
            followers, cursor = self.get_channel_followers(channel_id, cursor)
            print(followers, cursor)
            all_followers.extend(followers)
            if not cursor:  # No further pages
                break
        # The API returns followers in descending order based on followedAt.
        # If you need to be certain, you can sort them:
        all_followers.sort(key=lambda f: f.get("followedAt", 0), reverse=True)
        return all_followers
    
    
    def download_all_channel_followers(self, channel_id):
        """
        Fetch all channel followers.
    
        """
        try:
            followers = self.fetch_all_followers(channel_id)
    
        except requests.RequestException as error:
            print(f"Error fetching channels: {error}")
            return []

# Example usage:
if __name__ == '__main__':
    # Replace the URL below with the actual API endpoint.
    api = WarpAPI("https://warpley.netlify.app/.netlify/functions/warpcast-api")
    
    # Retrieve channels (using a limit of 50 in this example)
    # channels = api.get_all_channels(limit=1)
    # print(channels)
    # print("Retrieved Channels:")
    channel_id = "dev"
    api.download_all_channel_followers(channel_id)
    
        
        