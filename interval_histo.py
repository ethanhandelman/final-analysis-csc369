import duckdb
import pandas as pd
import matplotlib.pyplot as plt
import sys

def main():
    # Ensure the user id is provided as a command-line argument
    if len(sys.argv) < 2:
        print("Usage: python script.py <user_id>")
        sys.exit(1)
    
    user_id = sys.argv[1]
    
    # Path to the DuckDB database
    db_file = './data/combined_tweets.duckdb'
    
    # Connect to the DuckDB database in read-only mode
    con = duckdb.connect(database=db_file, read_only=True)
    
    # Retrieve tweet timestamps for the specified user, ordered by tweet creation time
    query = f"""
    SELECT tweetcreatedts
    FROM combined_tweets
    WHERE userid = '{user_id}'
      AND tweetcreatedts IS NOT NULL
    ORDER BY tweetcreatedts ASC;
    """
    
    # Load results into a DataFrame
    df = con.execute(query).fetchdf()
    con.close()
    
    # Convert tweetcreatedts to datetime; non-convertible rows will be set as NaT
    df['tweetcreatedts'] = pd.to_datetime(df['tweetcreatedts'], errors='coerce')
    
    # Drop any rows where conversion failed
    df = df.dropna(subset=['tweetcreatedts'])
    
    # Check if there are enough tweets to compute time differences
    if len(df) < 2:
        print(f"Not enough tweets for user {user_id} to compute time differences.")
        sys.exit(0)
    
    # Calculate the time differences (in seconds) between consecutive tweets
    df['time_diff'] = df['tweetcreatedts'].diff().dt.total_seconds()
    
    # Drop the first row (NaN resulting from diff())
    time_diffs = df['time_diff'].dropna()
    
    # Create a histogram of the time intervals between tweets
    plt.figure(figsize=(10, 6))
    plt.hist(time_diffs, bins=5, edgecolor='black')
    plt.xlabel('Time Interval Between Tweets (seconds)')
    plt.ylabel('Frequency')
    plt.title(f'Histogram of Time Intervals for User {user_id}')
    plt.tight_layout()
    plt.show()

if __name__ == '__main__':
    main()
