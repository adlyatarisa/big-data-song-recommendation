def format_song_data(song):
    # Function to format song data for display or processing
    return {
        "title": song.get("title", "Unknown Title"),
        "artist": song.get("artist", "Unknown Artist"),
        "album": song.get("album", "Unknown Album"),
        "year": song.get("year", "Unknown Year"),
    }

def log_message(message):
    # Function to log messages to the console
    print(f"[LOG] {message}")