import duckdb
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

def main():
    # Path to the DuckDB database
    db_file = './data/combined_tweets.duckdb'
    
    # Connect to the DuckDB database in read-only mode
    con = duckdb.connect(database=db_file, read_only=True)
    
    # Retrieve the retweet count column, filtering out any NULL values
    query = "SELECT retweetcount FROM combined_tweets WHERE retweetcount IS NOT NULL"
    df = con.execute(query).fetchdf()
    con.close()

    # Convert retweet counts to a numpy array
    retweets = df['retweetcount'].values

    # Define percentiles from 0 to 100
    percentiles = np.arange(0, 101)
    
    # Compute the retweet count corresponding to each percentile
    retweet_percentile_values = np.percentile(retweets, percentiles)

    # Plot the percentiles of retweet counts
    plt.figure(figsize=(10, 6))
    plt.plot(percentiles, retweet_percentile_values, marker='o', linestyle='-')
    plt.xlabel('Percentile')
    plt.ylabel('Retweet Count')
    plt.title('Percentiles of Number of Retweets for Tweets')
    plt.grid(True)
    plt.tight_layout()
    plt.show()

if __name__ == '__main__':
    main()
