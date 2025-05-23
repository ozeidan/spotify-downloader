from spotdl.utils.spotify import SpotifyClient, SpotifyError
from typing import List
from spotdl.types.playlist import Playlist
from spotdl.types.album import Album
from spotdl.types.artist import Artist
from spotdl.utils.ytm import get_ytm_client
from spotdl.types.song import Song
from spotdl.utils.errors import QueryError


__all__ = [
    "get_all_user_playlists",
    "get_user_followed_artists",
    "get_all_saved_playlists",
    "get_user_saved_albums",
    "create_ytm_album",
    "create_ytm_playlist",
    "get_ytm_client",
    "QueryError"
]


def get_all_user_playlists(user_url: str = "") -> List[Playlist]:
    """
    Get all user playlists.

    ### Args (optional)
    - user_url: Spotify user profile url.
        If a url is mentioned, get all public playlists of that specific user.

    ### Returns
    - List of all user playlists
    """

    spotify_client = SpotifyClient()
    if spotify_client.user_auth is False:  # type: ignore
        raise SpotifyError("You must be logged in to use this function")

    if user_url and not user_url.startswith("https://open.spotify.com/user/"):
        raise ValueError(f"Invalid user profile url: {user_url}")

    user_id = user_url.split("https://open.spotify.com/user/")[-1].replace("/", "")

    if user_id:
        user_playlists_response = spotify_client.user_playlists(user_id)
    else:
        user_playlists_response = spotify_client.current_user_playlists()
        user_resp = spotify_client.current_user()
        if user_resp is None:
            raise SpotifyError("Couldn't get user info")

        user_id = user_resp["id"]

    if user_playlists_response is None:
        raise SpotifyError("Couldn't get user playlists")

    user_playlists = user_playlists_response["items"]

    # Fetch all saved tracks
    while user_playlists_response and user_playlists_response["next"]:
        response = spotify_client.next(user_playlists_response)
        if response is None:
            break

        user_playlists_response = response
        user_playlists.extend(user_playlists_response["items"])

    return [
        Playlist.from_url(playlist["external_urls"]["spotify"], fetch_songs=False)
        for playlist in user_playlists
        if playlist["owner"]["id"] == user_id
    ]


def get_user_followed_artists() -> List[Artist]:
    """
    Get all user playlists

    ### Returns
    - List of all user playlists
    """

    spotify_client = SpotifyClient()
    if spotify_client.user_auth is False:  # type: ignore
        raise SpotifyError("You must be logged in to use this function")

    user_followed_response = spotify_client.current_user_followed_artists()
    if user_followed_response is None:
        raise SpotifyError("Couldn't get user followed artists")

    user_followed_response = user_followed_response["artists"]
    user_followed = user_followed_response["items"]

    # Fetch all artists
    while user_followed_response and user_followed_response["next"]:
        response = spotify_client.next(user_followed_response)
        if response is None:
            break

        user_followed_response = response["artists"]
        user_followed.extend(user_followed_response["items"])

    return [
        Artist.from_url(followed_artist["external_urls"]["spotify"], fetch_songs=False)
        for followed_artist in user_followed
    ]


def get_user_saved_albums() -> List[Album]:
    """
    Get all user saved albums

    ### Returns
    - List of all user saved albums
    """

    spotify_client = SpotifyClient()
    if spotify_client.user_auth is False:  # type: ignore
        raise SpotifyError("You must be logged in to use this function")

    user_saved_albums_response = spotify_client.current_user_saved_albums()
    if user_saved_albums_response is None:
        raise SpotifyError("Couldn't get user saved albums")

    user_saved_albums = user_saved_albums_response["items"]

    # Fetch all saved tracks
    while user_saved_albums_response and user_saved_albums_response["next"]:
        response = spotify_client.next(user_saved_albums_response)
        if response is None:
            break

        user_saved_albums_response = response
        user_saved_albums.extend(user_saved_albums_response["items"])

    return [
        Album.from_url(item["album"]["external_urls"]["spotify"], fetch_songs=False)
        for item in user_saved_albums
    ]


def get_all_saved_playlists() -> List[Playlist]:
    """
    Get all user playlists.

    ### Args (optional)
    - user_url: Spotify user profile url.
        If a url is mentioned, get all public playlists of that specific user.

    ### Returns
    - List of all user playlists
    """

    spotify_client = SpotifyClient()
    if spotify_client.user_auth is False:  # type: ignore
        raise SpotifyError("You must be logged in to use this function")

    user_playlists_response = spotify_client.current_user_playlists()

    if user_playlists_response is None:
        raise SpotifyError("Couldn't get user playlists")

    user_playlists = user_playlists_response["items"]
    user_id = user_playlists_response["href"].split("users/")[-1].split("/")[0]

    # Fetch all saved tracks
    while user_playlists_response and user_playlists_response["next"]:
        response = spotify_client.next(user_playlists_response)
        if response is None:
            break

        user_playlists_response = response
        user_playlists.extend(user_playlists_response["items"])

    return [
        Playlist.from_url(playlist["external_urls"]["spotify"], fetch_songs=False)
        for playlist in user_playlists
        if playlist["owner"]["id"] != user_id
    ]


def create_ytm_album(url: str, fetch_songs: bool = True) -> Album:
    """
    Creates a list of Song objects from an album query.

    ### Arguments
    - album_query: the url of the album

    ### Returns
    - a list of Song objects
    """

    if "?list=" not in url or not url.startswith("https://music.youtube.com/"):
        raise ValueError(f"Invalid album url: {url}")

    browse_id = get_ytm_client().get_album_browse_id(
        url.split("?list=")[1].split("&")[0]
    )
    if browse_id is None:
        raise ValueError(f"Invalid album url: {url}")

    album = get_ytm_client().get_album(browse_id)

    if album is None:
        raise ValueError(f"Couldn't fetch album: {url}")

    metadata = {
        "artist": album["artists"][0]["name"],
        "name": album["title"],
        "url": url,
    }

    songs = []
    for track in album["tracks"]:
        artists = [artist["name"] for artist in track["artists"]]

        song = Song.from_missing_data(
            name=track["title"],
            artists=artists,
            artist=artists[0],
            album_name=metadata["name"],
            album_artist=metadata["artist"],
            duration=track["duration_seconds"],
            download_url=f"https://music.youtube.com/watch?v={track['videoId']}",
        )

        if fetch_songs:
            song = Song.from_search_term(f"{song.artist} - {song.name}")

        songs.append(song)

    return Album(**metadata, songs=songs, urls=[song.url for song in songs])


def create_ytm_playlist(url: str, fetch_songs: bool = True) -> Playlist:
    """
    Returns a playlist object from a youtube playlist url

    ### Arguments
    - url: the url of the playlist

    ### Returns
    - a Playlist object
    """

    if not ("?list=" in url or "/browse/VLPL" in url) or not url.startswith(
        "https://music.youtube.com/"
    ):
        raise ValueError(f"Invalid playlist url: {url}")

    if "/browse/VLPL" in url:
        playlist_id = url.split("/browse/")[1]
    else:
        playlist_id = url.split("?list=")[1]
    playlist = get_ytm_client().get_playlist(playlist_id, None)  # type: ignore

    if playlist is None:
        raise ValueError(f"Couldn't fetch playlist: {url}")

    metadata = {
        "description": (
            playlist["description"] if playlist["description"] is not None else ""
        ),
        "author_url": (
            f"https://music.youtube.com/channel/{playlist['author']['id']}"
            if playlist.get("author") is not None
            else "Missing author url"
        ),
        "author_name": (
            playlist["author"]["name"]
            if playlist.get("author") is not None
            else "Missing author"
        ),
        "cover_url": (
            playlist["thumbnails"][0]["url"]
            if playlist.get("thumbnails") is not None
            else "Missing thumbnails"
        ),
        "name": playlist["title"],
        "url": url,
    }

    songs = []
    for track in playlist["tracks"]:
        if track["videoId"] is None or track["isAvailable"] is False:
            continue

        song = Song.from_missing_data(
            name=track["title"],
            artists=(
                [artist["name"] for artist in track["artists"]]
                if track.get("artists") is not None
                else []
            ),
            artist=(
                track["artists"][0]["name"]
                if track.get("artists") is not None
                else None
            ),
            album_name=(
                track.get("album", {}).get("name")
                if track.get("album") is not None
                else None
            ),
            duration=track.get("duration_seconds"),
            explicit=track.get("isExplicit"),
            download_url=f"https://music.youtube.com/watch?v={track['videoId']}",
        )

        if fetch_songs:
            song = reinit_song(song)

        songs.append(song)

    return Playlist(**metadata, songs=songs, urls=[song.url for song in songs])


def reinit_song(song: Song) -> Song:
    """
    Update song object with new data
    from Spotify

    ### Arguments
    - song: Song object

    ### Returns
    - Updated song object
    """

    data = song.json
    if data.get("url"):
        new_data = Song.from_url(data["url"]).json
    elif data.get("song_id"):
        new_data = Song.from_url(
            "https://open.spotify.com/track/" + data["song_id"]
        ).json
    elif data.get("name") and data.get("artist"):
        new_data = Song.from_search_term(f"{data['artist']} - {data['name']}").json
    else:
        raise QueryError("Song object is missing required data to be reinitialized")

    for key in Song.__dataclass_fields__:  # type: ignore # pylint: disable=E1101
        val = data.get(key)
        new_val = new_data.get(key)
        if new_val is not None and val is None:
            data[key] = new_val
        elif new_val is not None and val is not None:
            data[key] = val

    # return reinitialized song object
    return Song(**data)
