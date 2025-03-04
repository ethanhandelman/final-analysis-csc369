import duckdb
import pandas as pd

def main():
    # Path to the DuckDB database
    db_file = './data/combined_tweets.duckdb'
    
    # Connect to the DuckDB database in read-only mode
    con = duckdb.connect(database=db_file, read_only=True)
    
    # SQL query to get min and max of the timestamp columns
    query = """
    SELECT 
        MIN(usercreatedts) AS min_usercreatedts,
        MAX(usercreatedts) AS max_usercreatedts,
        MIN(tweetcreatedts) AS min_tweetcreatedts,
        MAX(tweetcreatedts) AS max_tweetcreatedts
    FROM combined_tweets;
    """
    
    # Execute the query and fetch the result as a DataFrame
    result = con.execute(query).fetchdf()
    
    # Close the connection
    con.close()
    
    # Print the results
    print("Minimum and Maximum Timestamp Values:")
    print(result)

if __name__ == '__main__':
    main()
