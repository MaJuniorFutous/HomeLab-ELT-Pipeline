import subprocess, csv, time, io, os
import psycopg2

def wait_for_postgres(host, max_retries=5, delay_seconds=5):
    """Wait for PostgreSQL to become available."""
    retries = 0
    while retries < max_retries:
        try:
            result = subprocess.run(["pg_isready", "-h", host], check=True, capture_output=True, text=True)
            if "accepting connections" in result.stdout:
                print("Successfully connected to PostgreSQL!")
                return True
        except subprocess.CalledProcessError as e:
            print(f"Error connecting to PostgreSQL: {e}")
            retries += 1
            print(f"Retrying in {delay_seconds} seconds... (Attempt {retries}/{max_retries})")
            time.sleep(delay_seconds)

    print("Max retries reached. Exiting.")
    return False

def main():
    try:
        source_conn = psycopg2.connect(
            dbname='source_pg', 
            user='postgres', 
            password='pg_pass', 
            host='192.168.1.11'
        )
    except Exception as ex:
        print(f"Error connecting to source database: {ex}")
        exit(1)
    else:   
        print("Successfully connected to source database.")
        source_cur = source_conn.cursor()
        source_cur.execute("SELECT * FROM public.events_sample;")
        data = source_cur.fetchall()
        source_conn.commit()

    if data:
        # Use the function before running the ELT process
        if not wait_for_postgres(host="dest_postgres"):
            print("Destination (local) DB never came up after all retries. Exiting.")
            exit(1)
        try:# local postgres db
            dest_conn = psycopg2.connect(
                dbname='demo', 
                user='dest_postgres_user', 
                password=os.getenv('DEST_PG_USER_PASSWORD'), 
                host='dest_postgres'
            )
        except Exception as ex:
            print(f"Error connecting to destination database: {ex}")
            exit(1)
        else:
            print("Successfully connected to destination database.")
            dest_cur = dest_conn.cursor()
            buffer = io.StringIO()
            writer = csv.writer(buffer)
            writer.writerows(data)
            buffer.seek(0)

            dest_cur.copy_expert(
                "COPY dev.bronze__dest_events_sample FROM STDIN WITH (FORMAT CSV);",
                buffer
            )
            dest_conn.commit()


    else:
        print("No data found in source database.")

if __name__ == "__main__":
    main()