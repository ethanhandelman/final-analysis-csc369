import duckdb
import pandas as pd

def main():
    # Path to the DuckDB database
    db_file = './data/combined_tweets.duckdb'
    
    # Connect to the DuckDB database in read-only mode
    con = duckdb.connect(database=db_file, read_only=True)
    
    # SQL query that computes the total number of users, the count of users created before 2005,
    # and the corresponding percentage
    query = """
    SELECT 
        COUNT(*) AS total_users,
        SUM(CASE WHEN CAST(usercreatedts AS TIMESTAMP) < '2005-01-01' THEN 1 ELSE 0 END) AS users_before_2005,
        (SUM(CASE WHEN CAST(usercreatedts AS TIMESTAMP) < '2005-01-01' THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) AS percentage_before_2005
    FROM combined_tweets
    WHERE usercreatedts IS NOT NULL;
    """
    
    # Execute the query and fetch the result as a DataFrame
    result = con.execute(query).fetchdf()
    con.close()
    
    # Display the results
    print("User Account Creation Statistics:")
    print(result)

if __name__ == '__main__':
    main()
