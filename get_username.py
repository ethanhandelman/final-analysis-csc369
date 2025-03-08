import duckdb
import sys

def main():
    # Check if a user id is provided as a command-line argument
    if len(sys.argv) < 2:
        print("Usage: python script.py <user_id>")
        sys.exit(1)
    
    user_id = sys.argv[1]
    
    # Path to the DuckDB database
    db_file = './data/combined_tweets3.duckdb'
    
    # Connect to the DuckDB database (read-only)
    con = duckdb.connect(database=db_file, read_only=True)
    
    # SQL query to fetch the username and usercreatedts for the given user id.
    # Using DISTINCT in case multiple rows exist for the same user id.
    query = f"""
    SELECT DISTINCT username, usercreatedts 
    FROM combined_tweets
    WHERE userid = '{user_id}'
    LIMIT 1;
    """
    
    result = con.execute(query).fetchall()
    con.close()
    
    if result:
        username, usercreatedts = result[0]
        print(f"Username for user id {user_id}: {username}")
        print(f"User account creation timestamp: {usercreatedts}")
    else:
        print(f"No data found for user id {user_id}")

if __name__ == '__main__':
    main()
