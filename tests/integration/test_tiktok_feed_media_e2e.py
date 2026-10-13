"""Controlled mutations of the redacted captured TikTok feed, not a live replay.

Only the provider socket is replaced. ASGI submission/polling, native signing,
parser, FetchService, scheduler, identity pool, worker, PostgreSQL and Redis are
real. This module refuses anything except the dedicated loopback QA services
before the shared support fixtures can DELETE tables or FLUSHDB.
"""

from __future__ import annotations

import copy
import json
import os
import uuid
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlsplit

import pytest
from sqlalchemy import select

from dtk.core.config import Config
from dtk.core.db import session_scope
from dtk.core.redis import get_redis
from dtk.core.types import BrowserFamily, IdentitySource, Outcome, Platform, TaskState
from dtk.db.models import ArchivedContent, ContentSnapshot, Identity, RequestLog, Task
from dtk.identity.pool import IdentityPool
from dtk.platforms import get_adapter
from dtk.scheduler.leases import INFLIGHT_KEY
from dtk.scheduler.scheduler import Scheduler, SchedulerConfig
from dtk.services.fetch import FetchService
from dtk.signing.base import RequestSpec, SigningSession, StaticFingerprint
from dtk.signing.native.signer import NativeSigner
from dtk.transport.base import Fingerprint, RawResponse
from dtk.transport.classify import classify_detailed
from dtk.worker.main import ATTEMPTS_KEY, DatabaseTaskStore, TaskWorker, WorkerOptions
from dtk.worker.runtime import PoolCandidates
from tests.integration import test_api_support as support
from tests.integration.conftest import TEST_DATABASE_URL, TEST_REDIS_URL
from tests.integration.test_worker import _run_worker_until

api_app = support.api_app
client = support.client
pytestmark = [pytest.mark.integration, pytest.mark.asyncio]
FIXTURE = Path(__file__).parents[1] / "fixtures/tiktok/user_posts_page1.json"
METRICS = ("play_count", "digg_count", "comment_count", "share_count", "collect_count")


@pytest.fixture(scope="session", autouse=True)
def qa_services_only():
    """Fail closed before any destructive support fixture is entered."""
    assert os.environ.get("DTK_TEST_DATABASE_URL") == TEST_DATABASE_URL
    assert os.environ.get("DTK_TEST_REDIS_URL") == TEST_REDIS_URL
    database = urlsplit(TEST_DATABASE_URL)
    redis = urlsplit(TEST_REDIS_URL)
    assert database.scheme == "postgresql+asyncpg"
    assert database.hostname == "127.0.0.1" and database.port == 55432
    assert database.path == "/dtk_test" and database.username == "dtk"
    assert redis.scheme == "redis" and redis.hostname == "127.0.0.1"
    assert redis.port == 56379 and redis.path == "/0"
    assert os.environ.get("DTK_SECRET_KEY") == support.TEST_SECRET_KEY


class CapturedTransport:
    """No socket implementation exists; classify using the real DTK ruleset."""

    def __init__(self, payload):
        self.body = json.dumps(payload).encode()
        self.requests = []

    def classify(self, response=None, exception=None, *, empty_body_is_normal=False):
        return classify_detailed(response, exception)

    async def request(self, identity, spec, timeout=None):
        assert identity.platform is Platform.TIKTOK
        assert urlsplit(spec.url).hostname == "www.tiktok.com"
        self.requests.append((identity.id, spec))
        return RawResponse(
            status=200,
            headers={"content-type": "application/json"},
            body=self.body,
            final_url=spec.url,
            elapsed_ms=12,
        )

    async def evict(self, identity_id):
        pass

    async def close(self):
        pass


async def pipeline(app, payload):
    pool = IdentityPool(app.state.cipher)
    async with session_scope() as session:
        identity_id = await pool.add(
            session,
            platform=Platform.TIKTOK,
            cookies={"msToken": "qa-controlled-fixture-token"},
            fingerprint=Fingerprint(
                browser_family=BrowserFamily.CHROME,
                browser_major=130,
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/130.0.0.0 Safari/537.36",
            ),
            source=IdentitySource.IMPORTED,
        )
    transport = CapturedTransport(payload)
    native = NativeSigner(Platform.TIKTOK)

    async def sign(platform, url, params, sender):
        assert platform is Platform.TIKTOK
        return await native.sign(
            RequestSpec.get(url, {k: str(v) for k, v in params.items()}),
            StaticFingerprint.of(sender.fingerprint),
            SigningSession(cookies=sender.cookies, identity_id=str(sender.id)),
        )

    fetch = FetchService(
        scheduler=Scheduler(PoolCandidates(pool), SchedulerConfig(max_wait_seconds=2)),
        pool=pool,
        transport=transport,
        sign=sign,
    )
    app.state.config = Config({"archive.enabled": True, "snapshot.min_interval_seconds": 0})
    worker = TaskWorker(
        fetch=fetch,
        store=DatabaseTaskStore(),
        config=app.state.config,
        options=WorkerOptions(concurrency=1, claim_timeout_seconds=1, drain_timeout_seconds=5),
    )
    return worker, transport, identity_id


async def submit_and_finish(client, worker, path, params):
    response = await client.get(path, params={**params, "wait": 0, "refresh": True})
    assert response.status_code == 202, response.text
    task_id = uuid.UUID(support.envelope(response)["data"]["task_id"])
    queued = support.envelope(await client.get(f"/api/v1/tasks/{task_id}"))["data"]
    assert queued["state"] == TaskState.QUEUED.value

    async def finished():
        async with session_scope() as session:
            row = await session.get(Task, task_id)
            assert row is not None
            return row.state in (TaskState.DONE.value, TaskState.FAILED.value)

    await _run_worker_until(worker, finished)
    response = await client.get(f"/api/v1/tasks/{task_id}")
    assert response.status_code == 200
    return task_id, support.envelope(response)["data"]


async def assert_attempt(task_id, transport, identity_id, endpoint):
    assert len(transport.requests) == 1
    assert transport.requests[0][0] == str(identity_id)
    async with session_scope() as session:
        logs = (
            await session.scalars(select(RequestLog).where(RequestLog.task_id == task_id))
        ).all()
        assert len(logs) == 1
        log = logs[0]
        assert log.endpoint == endpoint and log.platform == "tiktok"
        assert log.identity_id == identity_id
        assert log.outcome == Outcome.OK.value and log.http_status == 200
        assert log.cache_hit is False and log.signer == "native"
        identity = await session.get(Identity, identity_id)
        assert identity is not None and identity.state == "active"
    redis = get_redis()
    assert await redis.get(INFLIGHT_KEY.format(identity_id=identity_id)) is None
    assert await redis.get(ATTEMPTS_KEY.format(task_id=task_id)) is None


@pytest.mark.parametrize(
    "missing_primary_urls",
    [False, True],
    ids=["captured-baseline", "controlled-missing-primary-urls"],
)
async def test_feed_survives_missing_primary_video_urls(client, api_app, missing_primary_urls):
    captured = json.loads(FIXTURE.read_text())
    payload = copy.deepcopy(captured)
    if missing_primary_urls:
        video = payload["itemList"][0]["video"]
        del video["playAddr"]
        del video["downloadAddr"]
        # Keep bitrateInfo, counters, author, and the rest of the page untouched.
        assert video["bitrateInfo"] == captured["itemList"][0]["video"]["bitrateInfo"]
    baseline = get_adapter(Platform.TIKTOK).parse_author_posts(
        captured, fetched_at=datetime.now(UTC)
    )
    await support.signed_in(client)
    worker, transport, identity_id = await pipeline(api_app, payload)
    task_id, result = await submit_and_finish(
        client,
        worker,
        "/api/v1/tiktok/user/posts",
        {"sec_user_id": captured["itemList"][0]["author"]["secUid"]},
    )
    assert result["state"] == TaskState.DONE.value, result.get("error")
    page = result["data"]
    assert len(page["items"]) == len(captured["itemList"]) == len(baseline.items)
    assert page["cursor"] == captured["cursor"]
    assert page["has_more"] == captured["hasMore"]
    for actual, expected in zip(page["items"], baseline.items, strict=True):
        assert actual["content_id"] == expected.content_id
        assert actual["author"]["uid"] == expected.author.uid
        assert actual["stats"] == expected.stats.model_dump(mode="json")
    first = page["items"][0]
    assert (first["media"]["video"] is None) == missing_primary_urls
    await assert_attempt(task_id, transport, identity_id, "tiktok.author_posts")
    async with session_scope() as session:
        stored = await session.get(Task, task_id)
        assert stored.error is None and stored.result["data"] == page
        archives = (
            await session.scalars(
                select(ArchivedContent).where(ArchivedContent.platform == "tiktok")
            )
        ).all()
        snapshots = (
            await session.scalars(
                select(ContentSnapshot).where(ContentSnapshot.platform == "tiktok")
            )
        ).all()
        assert {row.content_id for row in archives} == {item.content_id for item in baseline.items}
        assert len(snapshots) == len(baseline.items)
        for expected in baseline.items:
            snapshot = next(row for row in snapshots if row.content_id == expected.content_id)
            assert {key: getattr(snapshot, key) for key in METRICS} == {
                key: getattr(expected.stats, key) for key in METRICS
            }
            archived = next(row for row in archives if row.content_id == expected.content_id)
            assert archived.author_uid == expected.author.uid
            if expected.content_id == first["content_id"]:
                assert archived.media == first["media"]


@pytest.mark.parametrize(
    "mutation", ["missing-author", "missing-item-list", "detail-missing-primary-urls"]
)
async def test_structural_guards_and_detail_media_remain_strict(client, api_app, mutation):
    payload = json.loads(FIXTURE.read_text())
    author_id = payload["itemList"][0]["author"]["secUid"]
    params = {"sec_user_id": author_id}
    path = "/api/v1/tiktok/user/posts"
    endpoint = "tiktok.author_posts"
    if mutation == "missing-author":
        del payload["itemList"][0]["author"]
    elif mutation == "missing-item-list":
        del payload["itemList"]
    else:
        item = payload["itemList"][0]
        del item["video"]["playAddr"]
        del item["video"]["downloadAddr"]
        payload = {"statusCode": 0, "itemInfo": {"itemStruct": item}}
        path, endpoint = "/api/v1/tiktok/video", "tiktok.content_detail"
        params = {"url": f"https://www.tiktok.com/@synthetic.creator/video/{item['id']}"}
    await support.signed_in(client)
    worker, transport, identity_id = await pipeline(api_app, payload)
    task_id, result = await submit_and_finish(client, worker, path, params)
    assert result["state"] == TaskState.FAILED.value
    assert result["error"]["code"] == "UPSTREAM_CHANGED"
    if mutation == "detail-missing-primary-urls":
        assert result["error"]["details"]["path"] == "itemInfo.itemStruct.video.playAddr"
    await assert_attempt(task_id, transport, identity_id, endpoint)
    async with session_scope() as session:
        assert (await session.scalars(select(ArchivedContent))).all() == []
        assert (await session.scalars(select(ContentSnapshot))).all() == []
