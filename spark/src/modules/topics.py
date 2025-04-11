from enum import Enum


class RealTimeStreamType(Enum):
    VIEWS_BY_PAGE = "views_by_page"
    VIEWS_IN_TIME = "views_in_time"
    LISTENS_BY_ARTIST = "listens_by_artist"
    LISTENS_IN_TIME = "listens_in_time"


class KafkaTopic(Enum):
    """
    Enum class to define Kafka topics.
    """

    PAGE_VIEW_EVENTS = "page_view_events"
    AUTH_EVENTS = "auth_events"
    STATUS_CHANGE_EVENTS = "status_change_events"
    LISTEN_EVENTS = "listen_events"
