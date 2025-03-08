import duckdb
import sys
import re

def main():
    # Ensure a username is provided
    if len(sys.argv) < 2:
        print("Usage: python script.py <username>")
        sys.exit(1)
    
    username = sys.argv[1]
    
    # Path to the DuckDB database
    db_file = './data/combined_tweets3.duckdb'
    
    # Connect to the DuckDB database (read-only)
    con = duckdb.connect(database=db_file, read_only=True)
    
    # Query to retrieve tweet text for the specified username
    query = f"""
    SELECT text 
    FROM combined_tweets
    WHERE username = '{username}'
      AND text IS NOT NULL;
    """
    
    # Execute the query and fetch results
    results = con.execute(query).fetchall()
    con.close()
    
    if not results:
        print(f"No tweets found for username: {username}")
        sys.exit(0)
    
    # Create a filename based on the username
    output_filename = f"{username}_tweets.txt"
    
    # Open the file for writing the cleaned tweet texts
    with open(output_filename, "w", encoding="utf-8") as f:
        for (tweet_text,) in results:
            # Remove all words that start with a '#' using regex.
            # This regex matches any word that starts with '#' preceded by either start-of-string or whitespace.
            cleaned_text = re.sub(r'(?:^|\s)#\S+', '', tweet_text).strip()
            f.write(cleaned_text + "\n")
    
    print(f"Tweet texts for username '{username}' have been written to {output_filename}")

if __name__ == '__main__':
    main()
