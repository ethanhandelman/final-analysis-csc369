import duckdb

def main():
    # Path to the DuckDB database
    db_file = './data/combined_tweets2.duckdb'
    
    # Connect to the DuckDB database (read-only)
    con = duckdb.connect(database=db_file, read_only=True)
    
    # SQL query: Group by userid, count tweets, and filter for users with >50,000 tweets
    query = """
    SELECT userid, COUNT(*) AS tweet_count
    FROM combined_tweets
    GROUP BY userid
    HAVING COUNT(*) > 50000;
    """
    
    # Execute the query and fetch all rows
    results = con.execute(query).fetchall()
    con.close()
    
    # Print the user IDs (and tweet counts) for accounts with >50,000 tweets
    print("User IDs with more than 50,000 tweets:")
    for userid, tweet_count in results:
        print(f"UserID: {userid} (Tweet Count: {tweet_count})")

if __name__ == '__main__':
    main()
