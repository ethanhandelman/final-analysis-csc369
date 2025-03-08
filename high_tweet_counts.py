import duckdb
import pandas as pd

def main():
    # Path to the DuckDB database
    db_file = './data/combined_tweets.duckdb'
    
    # Connect to the DuckDB database in read-only mode
    con = duckdb.connect(database=db_file, read_only=True)
    
    # SQL query to aggregate tweet counts per user and count users above given thresholds
    query = """
    SELECT
      SUM(CASE WHEN tweet_count > 1000 THEN 1 ELSE 0 END) AS users_gt_1000,
      SUM(CASE WHEN tweet_count > 10000 THEN 1 ELSE 0 END) AS users_gt_10000,
      SUM(CASE WHEN tweet_count > 50000 THEN 1 ELSE 0 END) AS users_gt_50000
    FROM (
        SELECT userid, COUNT(*) AS tweet_count
        FROM combined_tweets
        GROUP BY userid
    ) AS user_counts;
    """
    
    # Execute the query and load the result into a DataFrame
    result = con.execute(query).fetchdf()
    con.close()
    
    # Print the results
    print("Number of users with >1000 tweets:", result.loc[0, "users_gt_1000"])
    print("Number of users with >10000 tweets:", result.loc[0, "users_gt_10000"])
    print("Number of users with >50000 tweets:", result.loc[0, "users_gt_50000"])

if __name__ == '__main__':
    main()
