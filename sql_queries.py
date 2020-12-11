# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id SERIAL PRIMARY KEY,
        start_time TIMESTAMP,
        user_id INT NOT NULL,
        level VARCHAR,
        song_id VARCHAR,
        artist_id VARCHAR,
        session_id INT,
        location VARCHAR,
        user_agent VARCHAR
    )
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id INT PRIMARY KEY,
        first_name VARCHAR NOT NULL,
        last_name VARCHAR NOT NULL,
        gender VARCHAR,
        level VARCHAR
    )
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id VARCHAR PRIMARY KEY,
        title VARCHAR NOT NULL,
        artist_id VARCHAR NOT NULL,
        year INT,
        duration DECIMAL
    )
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id VARCHAR PRIMARY KEY,
        name VARCHAR NOT NULL,
        location VARCHAR,
        latitude DECIMAL,
        longitude DECIMAL
    )
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time TIMESTAMP PRIMARY KEY,
        hour INT NOT NULL,
        day INT NOT NULL,
        week INT NOT NULL,
        month INT NOT NULL,
        year INT NOT NULL,
        weekday INT NOT NULL
    )
""")

# INSERT RECORDS

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
        VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT ON CONSTRAINT users_pkey
        DO UPDATE SET first_name = EXCLUDED.first_name, gender = EXCLUDED.gender, level = EXCLUDED.level;
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
        VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT ON CONSTRAINT songs_pkey
        DO UPDATE SET title = EXCLUDED.title, artist_id = EXCLUDED.artist_id, year = EXCLUDED.year, duration = EXCLUDED.duration;
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
        VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT ON CONSTRAINT artists_pkey
        DO UPDATE SET name = EXCLUDED.name, location = EXCLUDED.location, latitude = EXCLUDED.latitude, longitude = EXCLUDED.longitude;
""")


time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT ON CONSTRAINT time_pkey
        DO NOTHING;
""")

time_table_copy_insert = ("""
    CREATE TEMP TABLE tmp_table (
        start_time TIMESTAMP,
        hour INT NOT NULL,
        day INT NOT NULL,
        week INT NOT NULL,
        month INT NOT NULL,
        year INT NOT NULL,
        weekday INT NOT NULL
    )
    ON COMMIT DROP;

    COPY tmp_table FROM %s With CSV;

    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT *
    FROM tmp_table
    ON CONFLICT ON CONSTRAINT time_pkey
        DO NOTHING;
""")

users_table_copy_insert = ("""
    CREATE TEMP TABLE users_tmp_table (
        user_id INT,
        first_name VARCHAR NOT NULL,
        last_name VARCHAR NOT NULL,
        gender VARCHAR,
        level VARCHAR
    )
    ON COMMIT DROP;

    COPY users_tmp_table FROM %s With CSV;

    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT *
    FROM users_tmp_table
    ON CONFLICT ON CONSTRAINT users_pkey
        DO NOTHING;
""")

songplay_table_copy_insert = ("""
    CREATE TEMP TABLE songplays_tmp_table (
        start_time TIMESTAMP,
        user_id INT NOT NULL,
        level VARCHAR,
        song_id VARCHAR,
        artist_id VARCHAR,
        session_id INT,
        location VARCHAR,
        user_agent VARCHAR
    )
    ON COMMIT DROP;

    COPY songplays_tmp_table FROM %s With CSV;

    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT *
    FROM songplays_tmp_table
""")

# FIND SONGS

song_select = ("""
    SELECT s.song_id, a.artist_id
    FROM songs s JOIN artists a ON s.artist_id = a.artist_id
    WHERE (s.title = %s) AND (a.name = %s) AND (s.duration = %s)
""")

# DASHBOARD QUERIES

songplays_by_location_select = ("""
    SELECT COUNT(songplay_id) plays, location FROM songplays GROUP BY location ORDER BY plays DESC
""")

songplays_by_level_select = ("""
    SELECT COUNT(*) plays, level FROM songplays GROUP BY level
""")

songplays_by_day_select = ("""
    SELECT COUNT(songplay_id) plays, day FROM songplays INNER JOIN time ON songplays.start_time = time.start_time WHERE month = %s AND year = %s GROUP BY day, month, year ORDER by day
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]