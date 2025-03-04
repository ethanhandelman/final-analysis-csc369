import duckdb
import pandas as pd
import matplotlib.pyplot as plt

def main():
    # Path to the DuckDB database
    db_file = './data/combined_tweets.duckdb'
    
    # Connect to the DuckDB database in read-only mode
    con = duckdb.connect(database=db_file, read_only=True)
    
    # Retrieve the tweetcreatedts column, filtering out any null values
    query = "SELECT tweetcreatedts FROM combined_tweets WHERE tweetcreatedts IS NOT NULL"
    df = con.execute(query).fetchdf()
    con.close()
    
    # Convert tweetcreatedts to datetime, invalid parsing will be set as NaT
    df['tweetcreatedts'] = pd.to_datetime(df['tweetcreatedts'], errors='coerce')
    
    # Drop any rows where the conversion failed
    df = df.dropna(subset=['tweetcreatedts'])
    
    # Filter tweets between January 1, 2022 and July 31, 2023 (inclusive)
    start_date = pd.Timestamp("2022-01-01")
    end_date = pd.Timestamp("2023-07-31")
    df_filtered = df[(df['tweetcreatedts'] >= start_date) & (df['tweetcreatedts'] <= end_date)]
    
    # Create monthly bins: We generate bin edges from Jan 1, 2022 to Aug 1, 2023 (so July is included)
    bins = pd.date_range(start="2022-01-01", end="2023-08-01", freq='MS')
    
    # Create a histogram of tweetcreatedts with the monthly bins
    plt.figure(figsize=(12, 6))
    plt.hist(df_filtered['tweetcreatedts'], bins=bins, edgecolor='black')
    plt.xlabel('Tweet Created Timestamp')
    plt.ylabel('Number of Tweets')
    plt.title('Histogram of Tweet Created Timestamps by Month (Jan 2022 - Jul 2023)')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

if __name__ == '__main__':
    main()
