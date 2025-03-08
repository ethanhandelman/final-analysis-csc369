import duckdb
import pandas as pd
import numpy as np

def main():
    # Path to the DuckDB database
    db_file = './data/combined_tweets.duckdb'
    
    # Connect to the DuckDB database in read-only mode
    con = duckdb.connect(database=db_file, read_only=True)
    
    # SQL query: group by userid, count tweets, and filter for users with >100 tweets
    query = """
    SELECT 
        userid, 
        COUNT(*) AS tweet_count
    FROM combined_tweets
    GROUP BY userid
    HAVING COUNT(*) > 100;
    """
    
    # Execute the query and load the result into a DataFrame
    df = con.execute(query).fetchdf()
    con.close()
    
    # Extract tweet counts as a numpy array
    tweet_counts = df['tweet_count'].values
    
    # Define the desired percentiles
    percentiles = [25, 50, 75, 90, 95, 99, 99.9]
    
    # Calculate the percentile values using numpy
    percentile_values = np.percentile(tweet_counts, percentiles)
    
    # Print the results
    print("Percentiles of number of tweets per user (only users with >100 tweets):")
    for p, value in zip(percentiles, percentile_values):
        print(f"{p}th percentile: {value}")

if __name__ == '__main__':
    main()
