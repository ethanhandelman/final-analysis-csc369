import duckdb
import pandas as pd
import sys

def main():
    # Ensure the user id is provided as a command-line argument
    if len(sys.argv) < 2:
        print("Usage: python script.py <user_id>")
        sys.exit(1)
    
    user_id = sys.argv[1]
    
    # Path to the DuckDB database
    db_file = './data/combined_tweets2.duckdb'
    
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
        print("Not enough tweets for user {} to compute time differences.".format(user_id))
        sys.exit(0)
    
    # Calculate the time differences (in seconds) between consecutive tweets
    df['time_diff'] = df['tweetcreatedts'].diff().dt.total_seconds()
    
    # Drop the first row (NaN resulting from diff())
    time_diffs = df['time_diff'].dropna()
    
    # Compute statistics on the time differences
    mean_diff = time_diffs.mean()
    median_diff = time_diffs.median()
    std_diff = time_diffs.std()
    
    # Print the computed statistics
    print(f"Statistics for user {user_id}:")
    print(f"Mean time between tweets: {mean_diff:.2f} seconds")
    print(f"Median time between tweets: {median_diff:.2f} seconds")
    print(f"Standard deviation: {std_diff:.2f} seconds")

if __name__ == '__main__':
    main()
