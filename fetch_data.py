# Usage example
from services.api.warpley import WarpcastAPI


# Usage example
if __name__ == "__main__":
    api = WarpcastAPI() # <---  Fix this api; migrate to python code
    api.fetch_data()
    print(api.data)
    api.display_casts()
    api.show_users_dataframe()
    api.show_edge_dataframe()
    api.show_engagement_metrics_dataframe()