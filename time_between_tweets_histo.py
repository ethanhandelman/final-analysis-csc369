import duckdb
import pandas as pd
import matplotlib.pyplot as plt

def main():
    # Path to the DuckDB database
    db_file = './data/combined_tweets2.duckdb'
    
    # Connect to the DuckDB database (read-only)
    con = duckdb.connect(database=db_file, read_only=True)
    
    # SQL query to calculate average interval in seconds between tweets for each user
    query = """
    SELECT 
        userid,
        (EXTRACT(EPOCH FROM MAX(tweetcreatedts)) - EXTRACT(EPOCH FROM MIN(tweetcreatedts))) / (COUNT(*) - 1) AS avg_interval_seconds
    FROM combined_tweets
    WHERE tweetcreatedts BETWEEN '2022-02-01' AND '2023-06-30'
    GROUP BY userid
    HAVING COUNT(*) > 100;
    """
    
    # Execute the query and load results into a DataFrame
    df = con.execute(query).fetchdf()
    con.close()
    
    # Convert average interval from seconds to days
    df['avg_interval_days'] = df['avg_interval_seconds'] / (3600 * 24)
    
    # Create a histogram of the average time between tweets in days
    plt.figure(figsize=(10, 6))
    plt.hist(df['avg_interval_days'], bins=50, edgecolor='black')
    plt.xlabel('Average Time Between Tweets (days)')
    plt.ylabel('Number of Users')
    plt.title('Histogram of Average Time Between Tweets (for Users with >100 Tweets)')
    plt.tight_layout()
    plt.show()

if __name__ == '__main__':
    main()
