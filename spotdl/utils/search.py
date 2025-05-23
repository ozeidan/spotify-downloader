"""
Module for creating Song objects by interacting with Spotify API
or by parsing a query.

To use this module you must first initialize the SpotifyClient.
"""

import concurrent.futures
import logging
import re
import requests
from pathlib import Path
from typing import Dict, List, Optional

from spotdl.types.album import Album
from spotdl.types.playlist import Playlist
from spotdl.types.song import Song, SongList
from spotdl.utils.metadata import get_file_metadata
from spotdl.utils.queryparsers import get_query_parsers
from spotdl.utils.songfetchers import reinit_song

__all__ = [
    "get_search_results",
    "parse_query",
    "get_simple_songs",
    "get_song_from_file_metadata",
    "gather_known_songs",
]

logger = logging.getLogger(__name__)


def get_search_results(search_term: str) -> List[Song]:
    """
    Creates a list of Song objects from a search term.

    ### Arguments
    - search_term: the search term to use

    ### Returns
    - a list of Song objects
    """

    return Song.list_from_search_term(search_term)


def parse_query(
    query: List[str],
    threads: int = 1,
    use_ytm_data: bool = False,
    playlist_numbering: bool = False,
    album_type=None,
    playlist_retain_track_cover: bool = False,
) -> List[Song]:
    """
    Parse query and return list containing song object

    ### Arguments
    - query: List of strings containing query
    - threads: Number of threads to use

    ### Returns
    - List of song objects
    """

    songs: List[Song] = get_simple_songs(
        query,
        use_ytm_data=use_ytm_data,
        playlist_numbering=playlist_numbering,
        album_type=album_type,
        playlist_retain_track_cover=playlist_retain_track_cover,
    )

    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
        future_to_song = {executor.submit(reinit_song, song): song for song in songs}
        for future in concurrent.futures.as_completed(future_to_song):
            song = future_to_song[future]
            try:
                results.append(future.result())
            except Exception as exc:
                logger.error("%s generated an exception: %s", song.display_name, exc)

    return results


def resolve_spotify_share_link(share_link: str) -> str:
    resp = requests.head(share_link, allow_redirects=True, timeout=10)
    return resp.url


def get_simple_songs(
    query: List[str],
    use_ytm_data: bool = False,
    playlist_numbering: bool = False,
    albums_to_ignore=None,
    album_type=None,
    playlist_retain_track_cover: bool = False,
) -> List[Song]:
    """
    Parse query and return list containing simple song objects

    ### Arguments
    - query: List of strings containing query

    ### Returns
    - List of simple song objects
    """

    songs: List[Song] = []
    lists: List[SongList] = []

    parsers = get_query_parsers()

    for request in query:
        logger.info("Processing query: %s", request)

        if "https://spotify.link/" in request:
            request = resolve_spotify_share_link(request)

        # Remove /intl-xxx/ from Spotify URLs with regex
        request = re.sub(r"\/intl-\w+\/", "/", request)

        for parser in parsers:
            if parser.can_handle(request):
                song, song_list = parser.parse(request, use_ytm_data)
                if song:
                    songs.append(song)
                if song_list:
                    lists.append(song_list)
                break

    for song_list in lists:
        songs.extend(
            process_song_list(
                song_list,
                playlist_numbering=playlist_numbering,
                playlist_retain_track_cover=playlist_retain_track_cover,
            )
        )

    # removing songs for --ignore-albums
    original_length = len(songs)
    if albums_to_ignore:
        songs = [
            song
            for song in songs
            if all(
                keyword not in song.album_name.lower() for keyword in albums_to_ignore
            )
        ]
        logger.info("Skipped %s songs (Ignored albums)", (original_length - len(songs)))

    if album_type:
        songs = [song for song in songs if song.album_type == album_type]

        logger.info(
            "Skipped %s songs for Album Type %s",
            (original_length - len(songs)),
            album_type,
        )

    logger.debug("Found %s songs in %s lists", len(songs), len(lists))

    return songs


def process_song_list(
    song_list: SongList,
    playlist_numbering: bool = False,
    playlist_retain_track_cover: bool = False,
) -> List[Song]:
    songs = []
    logger.info(
        "Found %s songs in %s (%s)",
        len(song_list.urls),
        song_list.name,
        song_list.__class__.__name__,
    )

    for song in song_list.songs:
        song_data = song.json
        song_data["list_name"] = song_list.name
        song_data["list_url"] = song_list.url
        song_data["list_position"] = song.list_position
        song_data["list_length"] = song_list.length

        if playlist_numbering:
            song_data["track_number"] = song_data["list_position"]
            song_data["tracks_count"] = song_data["list_length"]
            song_data["album_name"] = song_data["list_name"]
            song_data["disc_number"] = 1
            song_data["disc_count"] = 1
            if isinstance(song_list, Playlist):
                song_data["album_artist"] = song_list.author_name
                song_data["cover_url"] = song_list.cover_url

        if playlist_retain_track_cover:
            song_data["track_number"] = song_data["list_position"]
            song_data["tracks_count"] = song_data["list_length"]
            song_data["album_name"] = song_data["list_name"]
            song_data["disc_number"] = 1
            song_data["disc_count"] = 1
            song_data["cover_url"] = song_data["cover_url"]
            if isinstance(song_list, Playlist):
                song_data["album_artist"] = song_list.author_name

        songs.append(Song.from_dict(song_data))

    return songs


def songs_from_albums(albums: List[str]):
    """
    Get all songs from albums ids/urls/etc.

    ### Arguments
    - albums: List of albums ids

    ### Returns
    - List of songs
    """

    songs: List[Song] = []
    for album_id in albums:
        album = Album.from_url(album_id, fetch_songs=False)

        songs.extend([Song.from_missing_data(**song.json) for song in album.songs])

    return songs


def get_song_from_file_metadata(file: Path, id3_separator: str = "/") -> Optional[Song]:
    """
    Get song based on the file metadata or file name

    ### Arguments
    - file: Path to file

    ### Returns
    - Song object
    """

    file_metadata = get_file_metadata(file, id3_separator)

    if file_metadata is None:
        return None

    return Song.from_missing_data(**file_metadata)


def gather_known_songs(output: str, output_format: str) -> Dict[str, List[Path]]:
    """
    Gather all known songs from the output directory

    ### Arguments
    - output: Output path template
    - output_format: Output format

    ### Returns
    - Dictionary containing all known songs and their paths
    """

    # Get the base directory from the path template
    # Path("/Music/test/{artist}/{artists} - {title}.{output-ext}") -> "/Music/test"
    base_dir = output.split("{", 1)[0]
    paths = Path(base_dir).glob(f"**/*.{output_format}")

    known_songs: Dict[str, List[Path]] = {}
    for path in paths:
        # Try to get the song from the metadata
        song = get_song_from_file_metadata(path)

        # If the songs doesn't have metadata, try to get it from the filename
        if song is None or song.url is None:
            search_results = get_search_results(path.stem)
            if len(search_results) == 0:
                continue

            song = search_results[0]

        known_paths = known_songs.get(song.url)
        if known_paths is None:
            known_songs[song.url] = [path]
        else:
            known_songs[song.url].append(path)

    return known_songs
