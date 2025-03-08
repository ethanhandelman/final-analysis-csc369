import duckdb
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

def main():
    # List of usernames to include (after removing 'FuckPutinBot' and 'kanadianbest')
    usernames = [
        "UlfaniaEda",
        "Hkjhgc2",
        "rogue_corq"
    ]
    
    # Define the time range
    start_date = pd.Timestamp("2022-02-01")
    end_date = pd.Timestamp("2023-06-30")
    # Create monthly bins: from Feb 2022 to July 2023 to capture all tweets in June 2023
    bins = pd.date_range(start=start_date, end="2023-07-01", freq='MS')
    
    # Dictionary to store monthly tweet counts for each username
    user_counts = {}
    
    db_file = './data/combined_tweets3.duckdb'
    con = duckdb.connect(database=db_file, read_only=True)
    
    for username in usernames:
        query = f"""
        SELECT tweetcreatedts
        FROM combined_tweets
        WHERE username = '{username}'
          AND tweetcreatedts >= '2022-02-01'
          AND tweetcreatedts <= '2023-06-30'
        ORDER BY tweetcreatedts ASC;
        """
        df = con.execute(query).fetchdf()
        
        if df.empty:
            # If no tweets are found, use an array of zeros for each month
            counts = np.zeros(len(bins)-1, dtype=int)
        else:
            # Convert the tweetcreatedts column to datetime
            df['tweetcreatedts'] = pd.to_datetime(df['tweetcreatedts'], errors='coerce')
            df = df.dropna(subset=['tweetcreatedts'])
            # Convert the tweet times to numpy datetime64 with seconds precision
            times = df['tweetcreatedts'].values.astype('datetime64[s]')
            # Convert bins to numpy datetime64
            bin_edges = np.array(bins.values, dtype='datetime64[s]')
            counts, _ = np.histogram(times, bins=bin_edges)
        
        user_counts[username] = counts
    
    con.close()
    
    # Now, prepare to plot a grouped bar chart
    num_bins = len(bins) - 1       # number of monthly bins
    num_users = len(usernames)     # number of users
    indices = np.arange(num_bins)  # x positions for the groups
    bar_width = 0.8 / num_users    # width for each bar within a group
    
    # Choose distinct colors for each user (using matplotlib's tab10 colormap)
    colors = plt.cm.tab10(np.linspace(0, 1, num_users))
    
    plt.figure(figsize=(12, 6))
    for i, username in enumerate(usernames):
        counts = user_counts[username]
        # Calculate offset positions for each user's bar in the group
        x_positions = indices - 0.4 + i * bar_width + bar_width/2
        plt.bar(x_positions, counts, width=bar_width, color=colors[i], label=username)
    
    # Set x-axis tick positions and labels using the left edges of each monthly bin
    xtick_labels = [pd.to_datetime(bins[i]).strftime("%Y-%m") for i in range(num_bins)]
    plt.xticks(indices, xtick_labels, rotation=45)
    
    plt.xlabel("Month")
    plt.ylabel("Number of Tweets")
    plt.title("Monthly Distribution of Tweets (Feb 2022 - Jun 2023) for Selected Users")
    # Move the legend to the top left
    plt.legend(loc='upper left')
    plt.tight_layout()
    plt.show()

if __name__ == '__main__':
    main()
