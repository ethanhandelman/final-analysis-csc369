import duckdb
import pandas as pd

def main():
    # Path to the DuckDB database
    db_file = './data/combined_tweets2.duckdb'
    
    # Connect to the DuckDB database in read-only mode
    con = duckdb.connect(database=db_file, read_only=True)
    
    # SQL query to compute the averages:
    # - AVG(EXTRACT(EPOCH FROM usercreatedts)) returns the average creation time in seconds.
    query = """
    SELECT 
        AVG(followers) AS mean_followers,
        AVG(following) AS mean_following,
        AVG(CASE WHEN following = 0 THEN NULL ELSE CAST(followers AS DOUBLE) / following END) AS mean_ratio,
        AVG(EXTRACT(EPOCH FROM usercreatedts)) AS avg_user_epoch,
        AVG(totaltweets) AS avg_totaltweets,
        AVG(retweetcount) AS avg_retweetcount
    FROM combined_tweets
    WHERE usercreatedts IS NOT NULL 
      AND followers IS NOT NULL 
      AND following IS NOT NULL
      AND totaltweets IS NOT NULL 
      AND retweetcount IS NOT NULL;
    """
    
    # Execute the query and fetch the result into a DataFrame
    result = con.execute(query).fetchdf()
    con.close()
    
    # Convert the average user creation epoch seconds to a datetime
    avg_user_epoch = result.loc[0, 'avg_user_epoch']
    avg_user_date = pd.to_datetime(avg_user_epoch, unit='s')
    
    # Print the computed statistics
    print("Mean number of followers:", result.loc[0, "mean_followers"])
    print("Mean number of following:", result.loc[0, "mean_following"])
    print("Mean ratio of followers to following:", result.loc[0, "mean_ratio"])
    print("Average user account creation date:", avg_user_date)
    print("Mean number of total tweets:", result.loc[0, "avg_totaltweets"])
    print("Mean retweet count:", result.loc[0, "avg_retweetcount"])

if __name__ == '__main__':
    main()
