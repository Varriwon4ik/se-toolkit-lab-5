"""ETL pipeline: fetch data from the autochecker API and load it into the database.

The autochecker dashboard API provides two endpoints:
- GET /api/items — lab/task catalog
- GET /api/logs  — anonymized check results (supports ?since= and ?limit= params)

Both require HTTP Basic Auth (email + password from settings).
"""

from datetime import datetime

import httpx
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession

from app.models.item import ItemRecord
from app.settings import settings


# ---------------------------------------------------------------------------
# Extract — fetch data from the autochecker API
# ---------------------------------------------------------------------------


async def fetch_items() -> list[dict]:
    """Fetch the lab/task catalog from the autochecker API.

    - Use httpx.AsyncClient to GET {settings.autochecker_api_url}/api/items
    - Pass HTTP Basic Auth using settings.autochecker_email and
      settings.autochecker_password
    - The response is a JSON array of objects with keys:
      lab (str), task (str | null), title (str), type ("lab" | "task")
    - Return the parsed list of dicts
    - Raise an exception if the response status is not 200
    """
    url = f"{settings.autochecker_api_url}/api/items"
    auth = (settings.autochecker_email, settings.autochecker_password)

    async with httpx.AsyncClient() as client:
        response = await client.get(url, auth=auth)
        response.raise_for_status()  # Raises HTTPError if status != 200
        return response.json()


async def fetch_logs(since: datetime | None = None) -> list[dict]:
    """Fetch check results from the autochecker API.

    - Use httpx.AsyncClient to GET {settings.autochecker_api_url}/api/logs
    - Pass HTTP Basic Auth using settings.autochecker_email and
      settings.autochecker_password
    - Query parameters:
      - limit=500 (fetch in batches)
      - since={iso timestamp} if provided (for incremental sync)
    - The response JSON has shape:
      {"logs": [...], "count": int, "has_more": bool}
    - Handle pagination: keep fetching while has_more is True
      - Use the submitted_at of the last log as the new "since" value
    - Return the combined list of all log dicts from all pages
    """
    url = f"{settings.autochecker_api_url}/api/logs"
    auth = (settings.autochecker_email, settings.autochecker_password)
    limit = 500

    all_logs: list[dict] = []
    current_since = since

    while True:
        params: dict[str, str | int] = {"limit": limit}
        if current_since is not None:
            params["since"] = current_since.isoformat()

        async with httpx.AsyncClient() as client:
            response = await client.get(url, auth=auth, params=params)
            response.raise_for_status()
            data = response.json()

        logs = data.get("logs", [])
        all_logs.extend(logs)

        # Check if there are more pages
        if not data.get("has_more", False):
            break

        # Get the submitted_at of the last log as the new "since" value
        last_log = logs[-1] if logs else None
        if last_log is None:
            break

        current_since = datetime.fromisoformat(last_log["submitted_at"])

    return all_logs


# ---------------------------------------------------------------------------
# Load — insert fetched data into the local database
# ---------------------------------------------------------------------------


async def load_items(items: list[dict], session: AsyncSession) -> int:
    """Load items (labs and tasks) into the database.

    - Import ItemRecord from app.models.item
    - Process labs first (items where type="lab"):
      - For each lab, check if an item with type="lab" and matching title
        already exists (SELECT)
      - If not, INSERT a new ItemRecord(type="lab", title=lab_title)
      - Build a dict mapping the lab's short ID (the "lab" field, e.g.
        "lab-01") to the lab's database record, so you can look up
        parent IDs when processing tasks
    - Then process tasks (items where type="task"):
      - Find the parent lab item using the task's "lab" field (e.g.
        "lab-01") as the key into the dict you built above
      - Check if a task with this title and parent_id already exists
      - If not, INSERT a new ItemRecord(type="task", title=task_title,
        parent_id=lab_item.id)
    - Commit after all inserts
    - Return the number of newly created items
    """
    new_count = 0
    lab_short_id_to_record: dict[str, ItemRecord] = {}

    # Process labs first
    for item_data in items:
        if item_data.get("type") != "lab":
            continue

        title = item_data["title"]
        lab_short_id = item_data["lab"]

        # Check if lab already exists
        result = await session.exec(
            select(ItemRecord).where(
                ItemRecord.type == "lab",
                ItemRecord.title == title,
            )
        )
        existing = result.first()

        if existing is None:
            # Create new lab record
            new_lab = ItemRecord(type="lab", title=title)
            session.add(new_lab)
            await session.flush()  # Get the ID
            lab_short_id_to_record[lab_short_id] = new_lab
            new_count += 1
        else:
            lab_short_id_to_record[lab_short_id] = existing

    # Process tasks
    for item_data in items:
        if item_data.get("type") != "task":
            continue

        title = item_data["title"]
        lab_short_id = item_data["lab"]
        parent_lab = lab_short_id_to_record.get(lab_short_id)

        if parent_lab is None:
            # Skip task if parent lab not found (shouldn't happen if data is consistent)
            continue

        # Check if task already exists with this title and parent_id
        result = await session.exec(
            select(ItemRecord).where(
                ItemRecord.type == "task",
                ItemRecord.title == title,
                ItemRecord.parent_id == parent_lab.id,
            )
        )
        existing = result.first()

        if existing is None:
            # Create new task record
            new_task = ItemRecord(type="task", title=title, parent_id=parent_lab.id)
            session.add(new_task)
            new_count += 1

    await session.commit()
    return new_count


async def load_logs(
    logs: list[dict], items_catalog: list[dict], session: AsyncSession
) -> int:
    """Load interaction logs into the database.

    Args:
        logs: Raw log dicts from the API (each has lab, task, student_id, etc.)
        items_catalog: Raw item dicts from fetch_items() — needed to map
            short IDs (e.g. "lab-01", "setup") to item titles stored in the DB.
        session: Database session.

    - Import Learner from app.models.learner
    - Import InteractionLog from app.models.interaction
    - Import ItemRecord from app.models.item
    - Build a lookup from (lab_short_id, task_short_id) to item title
      using items_catalog. For labs, the key is (lab, None). For tasks,
      the key is (lab, task). The value is the item's title.
    - For each log dict:
      1. Find or create a Learner by external_id (log["student_id"])
         - If creating, set student_group from log["group"]
      2. Find the matching item in the database:
         - Use the lookup to get the title for (log["lab"], log["task"])
         - Query the DB for an ItemRecord with that title
         - Skip this log if no matching item is found
      3. Check if an InteractionLog with this external_id already exists
         (for idempotent upsert — skip if it does)
      4. Create InteractionLog with:
         - external_id = log["id"]
         - learner_id = learner.id
         - item_id = item.id
         - kind = "attempt"
         - score = log["score"]
         - checks_passed = log["passed"]
         - checks_total = log["total"]
         - created_at = parsed log["submitted_at"]
    - Commit after all inserts
    - Return the number of newly created interactions
    """
    from app.models.interaction import InteractionLog
    from app.models.item import ItemRecord
    from app.models.learner import Learner

    new_count = 0

    # Build lookup: (lab_short_id, task_short_id_or_none) -> title
    short_id_to_title: dict[tuple[str, str | None], str] = {}
    for item_data in items_catalog:
        lab_short_id = item_data["lab"]
        task_short_id = item_data.get("task")  # None for labs
        title = item_data["title"]
        key = (lab_short_id, task_short_id)
        short_id_to_title[key] = title

    for log in logs:
        # 1. Find or create learner
        student_id = log["student_id"]
        student_group = log.get("group", "")

        result = await session.exec(
            select(Learner).where(Learner.external_id == student_id)
        )
        learner = result.first()

        if learner is None:
            learner = Learner(external_id=student_id, student_group=student_group)
            session.add(learner)
            await session.flush()

        # 2. Find the matching item by title
        lab_short_id = log["lab"]
        task_short_id = log.get("task")  # None for lab-level logs
        item_title = short_id_to_title.get((lab_short_id, task_short_id))

        if item_title is None:
            # Skip if no matching item found
            continue

        result = await session.exec(
            select(ItemRecord).where(ItemRecord.title == item_title)
        )
        item = result.first()

        if item is None:
            # Skip if item not in database
            continue

        # 3. Check if interaction already exists (idempotent upsert)
        log_external_id = log["id"]
        result = await session.exec(
            select(InteractionLog).where(InteractionLog.external_id == log_external_id)
        )
        existing_interaction = result.first()

        if existing_interaction is not None:
            # Skip if already exists
            continue

        # 4. Create new interaction log
        interaction = InteractionLog(
            external_id=log_external_id,
            learner_id=learner.id,
            item_id=item.id,
            kind="attempt",
            score=log.get("score"),
            checks_passed=log.get("passed"),
            checks_total=log.get("total"),
            created_at=datetime.fromisoformat(log["submitted_at"]),
        )
        session.add(interaction)
        new_count += 1

    await session.commit()
    return new_count


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


async def sync(session: AsyncSession) -> dict:
    """Run the full ETL pipeline."""
    from app.models.interaction import InteractionLog
    from sqlmodel import func  # Add this import

    # Step 1: Fetch and load items
    raw_items = await fetch_items()
    await load_items(raw_items, session)

    # Step 2: Get the last synced timestamp
    result = await session.exec(
        select(InteractionLog).order_by(InteractionLog.created_at.desc()).limit(1)
    )
    latest = result.first()
    since = latest.created_at if latest else None

    # Step 3: Fetch and load logs since that timestamp
    raw_logs = await fetch_logs(since=since)
    new_records = await load_logs(raw_logs, raw_items, session)

    # Step 4: Get total count - FIXED
    count_result = await session.exec(select(func.count(InteractionLog.id)))
    total = count_result.one()

    return {"new_records": new_records, "total_records": total}