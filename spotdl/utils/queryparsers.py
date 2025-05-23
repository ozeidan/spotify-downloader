import json
import requests
from abc import ABC, abstractmethod
from typing import Optional, Tuple, List

from spotdl.types.album import Album
from spotdl.types.artist import Artist
from spotdl.types.playlist import Playlist
from spotdl.types.saved import Saved
from spotdl.types.song import Song, SongList
from spotdl.utils.search import (
    get_all_user_playlists,
    get_user_followed_artists,
    get_user_saved_albums,
    get_all_saved_playlists,
)
from spotdl.utils.search import (
    create_ytm_album,
    create_ytm_playlist,
    get_ytm_client,
    get_simple_songs,
)
from spotdl.utils.search import QueryError

__all__ = ["get_query_parsers"]


class QueryParser(ABC):
    @abstractmethod
    def can_handle(self, request: str) -> bool:
        pass

    @abstractmethod
    def parse(
        self, request: str, use_ytm_data: bool = False
    ) -> Tuple[Optional[Song], Optional[SongList]]:
        pass


class YouTubeSpotifyTrackParser(QueryParser):
    def can_handle(self, request: str) -> bool:
        return (
            (
                "watch?v=" in request
                or "youtu.be/" in request
                or "soundcloud.com/" in request
                or "bandcamp.com/" in request
            )
            and "open.spotify.com" in request
            and "track" in request
            and "|" in request
        )

    def parse(
        self, request: str, use_ytm_data: bool = False
    ) -> Tuple[Optional[Song], Optional[SongList]]:
        split_urls = request.split("|")
        if (
            len(split_urls) <= 1
            or not (
                "watch?v=" in split_urls[0]
                or "youtu.be" in split_urls[0]
                or "soundcloud.com/" in split_urls[0]
                or "bandcamp.com/" in split_urls[0]
            )
            or "spotify" not in split_urls[1]
        ):
            raise QueryError(
                'Incorrect format used, please use "YouTubeURL|SpotifyURL"'
            )

        return (
            Song.from_missing_data(url=split_urls[1], download_url=split_urls[0]),
            None,
        )


class YouTubeMusicTrackParser(QueryParser):
    def can_handle(self, request: str) -> bool:
        return "music.youtube.com/watch?v" in request

    def parse(
        self, request: str, use_ytm_data: bool = False
    ) -> Tuple[Optional[Song], Optional[SongList]]:
        track_data = get_ytm_client().get_song(request.split("?v=", 1)[1])

        yt_song = Song.from_search_term(
            f"{track_data['videoDetails']['author']} - {track_data['videoDetails']['title']}"
        )

        if use_ytm_data:
            yt_song.name = track_data["title"]
            yt_song.artist = track_data["author"]
            yt_song.artists = [track_data["author"]]
            yt_song.duration = track_data["lengthSeconds"]

        yt_song.download_url = request
        return yt_song, None


class YouTubePlaylistParser(QueryParser):
    def can_handle(self, request: str) -> bool:
        return (
            "youtube.com/playlist?list=" in request
            or "youtube.com/browse/VLPL" in request
        )

    def parse(
        self, request: str, use_ytm_data: bool = False
    ) -> Tuple[Optional[Song], Optional[SongList]]:
        request = request.replace(
            "https://www.youtube.com/", "https://music.youtube.com/"
        )
        request = request.replace("https://youtube.com/", "https://music.youtube.com/")

        split_urls = request.split("|")
        if len(split_urls) == 1:
            if "?list=OLAK5uy_" in request:
                return None, create_ytm_album(request, fetch_songs=False)
            elif "?list=PL" in request or "browse/VLPL" in request:
                return None, create_ytm_playlist(request, fetch_songs=False)
            else:
                return None, None
        else:
            if ("spotify" not in split_urls[1]) or not any(
                x in split_urls[0]
                for x in ["?list=PL", "?list=OLAK5uy_", "browse/VLPL"]
            ):
                raise QueryError(
                    'Incorrect format used, please use "YouTubeMusicURL|SpotifyURL". '
                    "Currently only supports YouTube Music playlists and albums."
                )

            if ("open.spotify.com" in request and "album" in request) and (
                "?list=OLAK5uy_" in request
            ):
                ytm_list = create_ytm_album(split_urls[0], fetch_songs=False)
                spot_list = Album.from_url(split_urls[1], fetch_songs=False)
            elif ("open.spotify.com" in request and "playlist" in request) and (
                "?list=PL" in request or "browse/VLPL" in request
            ):
                ytm_list = create_ytm_playlist(split_urls[0], fetch_songs=False)
                spot_list = Playlist.from_url(split_urls[1], fetch_songs=False)
            else:
                raise QueryError(
                    f"URLs are not of the same type, {split_urls[0]} is not "
                    f"the same type as {split_urls[1]}."
                )

            if ytm_list.length != spot_list.length:
                raise QueryError(
                    f"The YouTube Music ({ytm_list.length}) "
                    f"and Spotify ({spot_list.length}) lists have different lengths. "
                )

            if use_ytm_data:
                for index, song in enumerate(ytm_list.songs):
                    song.url = spot_list.songs[index].url
                return None, ytm_list
            else:
                for index, song in enumerate(spot_list.songs):
                    song.download_url = ytm_list.songs[index].download_url
                return None, spot_list


class SpotifyTrackParser(QueryParser):
    def can_handle(self, request: str) -> bool:
        return "open.spotify.com" in request and "track" in request

    def parse(
        self, request: str, use_ytm_data: bool = False
    ) -> Tuple[Optional[Song], Optional[SongList]]:
        return Song.from_url(url=request), None


class SpotifyLinkParser(QueryParser):
    def can_handle(self, request: str) -> bool:
        return "https://spotify.link/" in request

    def parse(
        self, request: str, use_ytm_data: bool = False
    ) -> Tuple[Optional[Song], Optional[SongList]]:
        resp = requests.head(request, allow_redirects=True, timeout=10)
        full_url = resp.url
        full_lists = get_simple_songs(
            [full_url],
            use_ytm_data=use_ytm_data,
            playlist_numbering=False,
            album_type=None,
            playlist_retain_track_cover=False,
        )
        return full_lists[0] if full_lists else None, None


class SpotifyPlaylistParser(QueryParser):
    def can_handle(self, request: str) -> bool:
        return "open.spotify.com" in request and "playlist" in request

    def parse(
        self, request: str, use_ytm_data: bool = False
    ) -> Tuple[Optional[Song], Optional[SongList]]:
        return None, Playlist.from_url(request, fetch_songs=False)


class SpotifyAlbumParser(QueryParser):
    def can_handle(self, request: str) -> bool:
        return "open.spotify.com" in request and "album" in request

    def parse(
        self, request: str, use_ytm_data: bool = False
    ) -> Tuple[Optional[Song], Optional[SongList]]:
        return None, Album.from_url(request, fetch_songs=False)


class SpotifyArtistParser(QueryParser):
    def can_handle(self, request: str) -> bool:
        return "open.spotify.com" in request and "artist" in request

    def parse(
        self, request: str, use_ytm_data: bool = False
    ) -> Tuple[Optional[Song], Optional[SongList]]:
        return None, Artist.from_url(request, fetch_songs=False)


class SpotifyUserParser(QueryParser):
    def can_handle(self, request: str) -> bool:
        return "open.spotify.com" in request and "user" in request

    def parse(
        self, request: str, use_ytm_data: bool = False
    ) -> Tuple[Optional[Song], Optional[SongList]]:
        return None, (
            get_all_user_playlists(request)[0]
            if get_all_user_playlists(request)
            else None
        )


class AlbumSearchParser(QueryParser):
    def can_handle(self, request: str) -> bool:
        return "album:" in request

    def parse(
        self, request: str, use_ytm_data: bool = False
    ) -> Tuple[Optional[Song], Optional[SongList]]:
        return None, Album.from_search_term(request, fetch_songs=False)


class PlaylistSearchParser(QueryParser):
    def can_handle(self, request: str) -> bool:
        return "playlist:" in request

    def parse(
        self, request: str, use_ytm_data: bool = False
    ) -> Tuple[Optional[Song], Optional[SongList]]:
        return None, Playlist.from_search_term(request, fetch_songs=False)


class ArtistSearchParser(QueryParser):
    def can_handle(self, request: str) -> bool:
        return "artist:" in request

    def parse(
        self, request: str, use_ytm_data: bool = False
    ) -> Tuple[Optional[Song], Optional[SongList]]:
        return None, Artist.from_search_term(request, fetch_songs=False)


class SavedParser(QueryParser):
    def can_handle(self, request: str) -> bool:
        return request == "saved"

    def parse(
        self, request: str, use_ytm_data: bool = False
    ) -> Tuple[Optional[Song], Optional[SongList]]:
        return None, Saved.from_url(request, fetch_songs=False)


class AllUserPlaylistsParser(QueryParser):
    def can_handle(self, request: str) -> bool:
        return request == "all-user-playlists"

    def parse(
        self, request: str, use_ytm_data: bool = False
    ) -> Tuple[Optional[Song], Optional[SongList]]:
        return None, get_all_user_playlists()[0] if get_all_user_playlists() else None


class AllUserFollowedArtistsParser(QueryParser):
    def can_handle(self, request: str) -> bool:
        return request == "all-user-followed-artists"

    def parse(
        self, request: str, use_ytm_data: bool = False
    ) -> Tuple[Optional[Song], Optional[SongList]]:
        return None, (
            get_user_followed_artists()[0] if get_user_followed_artists() else None
        )


class AllUserSavedAlbumsParser(QueryParser):
    def can_handle(self, request: str) -> bool:
        return request == "all-user-saved-albums"

    def parse(
        self, request: str, use_ytm_data: bool = False
    ) -> Tuple[Optional[Song], Optional[SongList]]:
        return None, get_user_saved_albums()[0] if get_user_saved_albums() else None


class AllSavedPlaylistsParser(QueryParser):
    def can_handle(self, request: str) -> bool:
        return request == "all-saved-playlists"

    def parse(
        self, request: str, use_ytm_data: bool = False
    ) -> Tuple[Optional[Song], Optional[SongList]]:
        return None, get_all_saved_playlists()[0] if get_all_saved_playlists() else None


class SpotdlFileParser(QueryParser):
    def can_handle(self, request: str) -> bool:
        return request.endswith(".spotdl")

    def parse(
        self, request: str, use_ytm_data: bool = False
    ) -> Tuple[Optional[Song], Optional[SongList]]:
        with open(request, "r", encoding="utf-8") as save_file:
            tracks = json.load(save_file)
            return Song.from_dict(tracks[0]) if tracks else None, None


class DefaultParser(QueryParser):
    def can_handle(self, request: str) -> bool:
        return True

    def parse(
        self, request: str, use_ytm_data: bool = False
    ) -> Tuple[Optional[Song], Optional[SongList]]:
        return Song.from_search_term(request), None


def get_query_parsers() -> List[QueryParser]:
    return [
        YouTubeSpotifyTrackParser(),
        YouTubeMusicTrackParser(),
        YouTubePlaylistParser(),
        SpotifyTrackParser(),
        SpotifyLinkParser(),
        SpotifyPlaylistParser(),
        SpotifyAlbumParser(),
        SpotifyArtistParser(),
        SpotifyUserParser(),
        AlbumSearchParser(),
        PlaylistSearchParser(),
        ArtistSearchParser(),
        SavedParser(),
        AllUserPlaylistsParser(),
        AllUserFollowedArtistsParser(),
        AllUserSavedAlbumsParser(),
        AllSavedPlaylistsParser(),
        SpotdlFileParser(),
        DefaultParser(),
    ]
