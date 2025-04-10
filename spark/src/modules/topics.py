from enum import Enum


class RealTimeKafkaTopic(Enum):
    VIEWS_BY_PAGE = "rt_views_by_page"
    LISTENS_BY_ARTIST = "rt_listens_by_artist"


class KafkaTopic(Enum):
    """
    Enum class to define Kafka topics.
    """

    PAGE_VIEW_EVENTS = "page_view_events"
    AUTH_EVENTS = "auth_events"
    STATUS_CHANGE_EVENTS = "status_change_events"
    LISTEN_EVENTS = "listen_events"
