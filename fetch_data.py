# Usage example
from services.api.warpley import WarpcastAPI


# Usage example
if __name__ == "__main__":
    CHANNEL_ID = "page"
    FOLLOWER_LIMIT = 30
    CAST_LIMIT = 50
    TOTAL_CAST_LIMIT = 100
    url = f"https://warpley.netlify.app/.netlify/functions/warpcast-api?followerFeed=true&channelId={CHANNEL_ID}&followerLimit={FOLLOWER_LIMIT}&castLimit={CAST_LIMIT}&totalCastLimit={TOTAL_CAST_LIMIT}"
    
    api = WarpcastAPI(url)
    api.fetch_data()
    print(api.data)
    api.display_casts()
    api.show_users_dataframe()
    api.show_edge_dataframe()
    api.show_engagement_metrics_dataframe()