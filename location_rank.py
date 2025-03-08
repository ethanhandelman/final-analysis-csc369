import duckdb
import pandas as pd

def main():
    # Path to the DuckDB database
    db_file = './data/combined_tweets.duckdb'
    
    # Connect to the DuckDB database in read-only mode
    con = duckdb.connect(database=db_file, read_only=True)
    
    # SQL query to count tweets per location, filtering out NULL locations,
    # and then ordering by count in descending order
    query = """
    SELECT location, COUNT(*) AS tweet_count
    FROM combined_tweets
    WHERE location IS NOT NULL
    GROUP BY location
    ORDER BY tweet_count DESC
    LIMIT 20;
    """
    
    # Execute the query and fetch the result as a DataFrame
    df = con.execute(query).fetchdf()
    con.close()
    
    # Print the results
    print("Top 20 Locations:")
    print(df)

if __name__ == '__main__':
    main()
