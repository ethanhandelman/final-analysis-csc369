import duckdb
import pandas as pd
import matplotlib.pyplot as plt

def main():
    # Path to the DuckDB database
    db_file = './data/combined_tweets.duckdb'
    
    # Connect to the DuckDB database (read-only)
    con = duckdb.connect(database=db_file, read_only=True)
    
    # Retrieve the usercreatedts column (ignoring any null values)
    query = "SELECT usercreatedts FROM combined_tweets WHERE usercreatedts IS NOT NULL"
    df = con.execute(query).fetchdf()
    con.close()
    
    # Convert the usercreatedts column to datetime; errors will be set as NaT
    df['usercreatedts'] = pd.to_datetime(df['usercreatedts'], errors='coerce')
    
    # Remove rows where the conversion failed
    df = df.dropna(subset=['usercreatedts'])
    
    # Extract the year from the datetime values
    df['year'] = df['usercreatedts'].dt.year
    
    # Define bin edges for every 5 years between 1975 and 2025.
    # We set the rightmost edge to 2030 so that tweets from 2025 are included in the last bin [2025, 2030).
    bins = list(range(1975, 2030, 5))
    
    # Create a histogram
    plt.figure(figsize=(10, 6))
    plt.hist(df['year'], bins=bins, edgecolor='black')
    plt.xlabel('Year')
    plt.ylabel('Number of Users')
    plt.title('Histogram of User Created Timestamps (1975-2025)')
    plt.xticks(bins)
    plt.tight_layout()
    plt.show()

if __name__ == '__main__':
    main()
