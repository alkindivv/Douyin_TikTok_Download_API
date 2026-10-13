"""Feed analytics remain valid without a downloadable video URL.

Variants use the existing captured/redacted fixture, not a claimed new live
capture. Historical diagnostics identify this missing path but omit the body.
"""

from copy import deepcopy

import pytest

from dtk.core.errors import UpstreamChanged
from dtk.platforms.tiktok import parser
from tests.replay.support import FETCHED_AT, load


@pytest.mark.parametrize("absent", ["missing", "null", "empty"])
def test_feed_without_primary_video_urls_preserves_identity_and_metrics(absent):
    payload = load("tiktok", "user_posts_page1")
    baseline = parser.parse_author_posts(deepcopy(payload), fetched_at=FETCHED_AT)
    video = payload["itemList"][0]["video"]
    for field in ("playAddr", "downloadAddr"):
        if absent == "missing":
            video.pop(field, None)
        else:
            video[field] = None if absent == "null" else ""
    result = parser.parse_author_posts(payload, fetched_at=FETCHED_AT)
    assert len(result.items) == len(baseline.items)
    assert result.has_more == baseline.has_more
    assert result.cursor == baseline.cursor
    for actual, expected in zip(result.items, baseline.items, strict=True):
        assert actual.content_id == expected.content_id
        assert actual.author == expected.author
        assert actual.stats == expected.stats
        assert actual.created_at == expected.created_at
        assert actual.web_url == expected.web_url
    assert result.items[0].media.video is None
    assert result.items[0].media.covers == baseline.items[0].media.covers
    assert result.items[0].media.streams == baseline.items[0].media.streams


def test_feed_without_any_video_url_keeps_unknown_metrics_unknown():
    payload = load("tiktok", "user_posts_page1")
    item = payload["itemList"][0]
    for field in ("playAddr", "downloadAddr", "bitrateInfo"):
        item["video"].pop(field, None)
    item["stats"] = {}
    item["statsV2"] = {}
    result = parser.parse_author_posts(payload, fetched_at=FETCHED_AT)
    first = result.items[0]
    assert first.media.video is None
    assert first.media.streams == []
    assert first.content_id == item["id"]
    assert first.author.unique_id == item["author"]["uniqueId"]
    assert first.stats.play_count is None
    assert first.stats.digg_count is None
    assert first.stats.comment_count is None
    assert first.stats.share_count is None
    assert len(result.items) == len(payload["itemList"])


@pytest.mark.parametrize("required", ["author", "id", "video"])
def test_feed_still_rejects_missing_identity_or_video_structure(required):
    payload = load("tiktok", "user_posts_page1")
    del payload["itemList"][0][required]
    with pytest.raises(UpstreamChanged):
        parser.parse_author_posts(payload, fetched_at=FETCHED_AT)


def test_feed_missing_item_list_is_not_an_empty_success():
    payload = load("tiktok", "user_posts_page1")
    del payload["itemList"]
    with pytest.raises(UpstreamChanged):
        parser.parse_author_posts(payload, fetched_at=FETCHED_AT)


def test_detail_download_contract_remains_strict():
    payload = load("tiktok", "video_normal")
    video = payload["itemInfo"]["itemStruct"]["video"]
    video.pop("playAddr", None)
    video.pop("downloadAddr", None)
    with pytest.raises(UpstreamChanged):
        parser.parse_content(payload, fetched_at=FETCHED_AT)
