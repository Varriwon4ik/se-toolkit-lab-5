"""Router for analytics endpoints.

Each endpoint performs SQL aggregation queries on the interaction data
populated by the ETL pipeline. All endpoints require a `lab` query
parameter to filter results by lab (e.g., "lab-01").
"""

from fastapi import APIRouter, Depends, Query
from sqlmodel import func, select, col
from sqlmodel.ext.asyncio.session import AsyncSession
from datetime import date

from app.database import get_session
from app.models.item import ItemRecord
from app.models.interaction import InteractionLog
from app.models.learner import Learner

router = APIRouter()


@router.get("/scores")
async def get_scores(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Score distribution histogram for a given lab.
    
    Returns the distribution of scores in four buckets:
    0-25, 26-50, 51-75, 76-100
    
    Always returns all four buckets, even if count is 0.
    """
    # First, find all tasks that belong to this lab
    # The lab parameter is like "lab-04", and we need to find items where
    # the title contains something like "Lab 04" or "Lab 4"
    
    # Extract the lab number (e.g., "04" -> "4")
    lab_number = lab.replace('lab-', '').lstrip('0')
    
    # Find the lab item - titles might be like "Lab 4: Introduction to Git"
    lab_query = select(ItemRecord).where(
        ItemRecord.type == "lab",
        ItemRecord.title.ilike(f"%Lab {lab_number}%")
    )
    lab_result = await session.exec(lab_query)
    lab_item = lab_result.first()
    
    if not lab_item:
        # Try without stripping zeros (e.g., "Lab 04")
        lab_query = select(ItemRecord).where(
            ItemRecord.type == "lab",
            ItemRecord.title.ilike(f"%Lab {lab.replace('lab-', '')}%")
        )
        lab_result = await session.exec(lab_query)
        lab_item = lab_result.first()
    
    if not lab_item:
        # Return empty buckets if lab not found
        return [
            {"bucket": "0-25", "count": 0},
            {"bucket": "26-50", "count": 0},
            {"bucket": "51-75", "count": 0},
            {"bucket": "76-100", "count": 0}
        ]
    
    # Find all tasks that belong to this lab
    tasks_query = select(ItemRecord.id).where(
        ItemRecord.parent_id == lab_item.id,
        ItemRecord.type == "task"
    )
    tasks_result = await session.exec(tasks_query)
    task_ids = list(tasks_result.all())
    
    if not task_ids:
        # Return empty buckets if no tasks found
        return [
            {"bucket": "0-25", "count": 0},
            {"bucket": "26-50", "count": 0},
            {"bucket": "51-75", "count": 0},
            {"bucket": "76-100", "count": 0}
        ]
    
    # Get all scores for these tasks
    scores_query = select(InteractionLog.score).where(
        InteractionLog.item_id.in_(task_ids),
        InteractionLog.score.isnot(None)
    )
    scores_result = await session.exec(scores_query)
    scores = list(scores_result.all())
    
    # Manually bucket the scores
    buckets = {
        "0-25": 0,
        "26-50": 0,
        "51-75": 0,
        "76-100": 0
    }
    
    for score in scores:
        if score <= 25:
            buckets["0-25"] += 1
        elif score <= 50:
            buckets["26-50"] += 1
        elif score <= 75:
            buckets["51-75"] += 1
        else:
            buckets["76-100"] += 1
    
    # Return in the required order
    return [
        {"bucket": "0-25", "count": buckets["0-25"]},
        {"bucket": "26-50", "count": buckets["26-50"]},
        {"bucket": "51-75", "count": buckets["51-75"]},
        {"bucket": "76-100", "count": buckets["76-100"]}
    ]


@router.get("/pass-rates")
async def get_pass_rates(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Per-task pass rates for a given lab.
    
    For each task, computes:
    - avg_score: average of interaction scores (rounded to 1 decimal)
    - attempts: total number of interactions
    """
    # Extract the lab number
    lab_number = lab.replace('lab-', '').lstrip('0')
    
    # Find the lab item
    lab_query = select(ItemRecord).where(
        ItemRecord.type == "lab",
        ItemRecord.title.ilike(f"%Lab {lab_number}%")
    )
    lab_result = await session.exec(lab_query)
    lab_item = lab_result.first()
    
    if not lab_item:
        # Try without stripping zeros
        lab_query = select(ItemRecord).where(
            ItemRecord.type == "lab",
            ItemRecord.title.ilike(f"%Lab {lab.replace('lab-', '')}%")
        )
        lab_result = await session.exec(lab_query)
        lab_item = lab_result.first()
    
    if not lab_item:
        return []
    
    # Find all tasks that belong to this lab
    tasks_query = select(ItemRecord).where(
        ItemRecord.parent_id == lab_item.id,
        ItemRecord.type == "task"
    ).order_by(ItemRecord.title)
    tasks_result = await session.exec(tasks_query)
    tasks = list(tasks_result.all())
    
    result = []
    for task in tasks:
        # Get all interactions for this task with scores
        interactions_query = select(InteractionLog.score).where(
            InteractionLog.item_id == task.id,
            InteractionLog.score.isnot(None)
        )
        interactions_result = await session.exec(interactions_query)
        scores = list(interactions_result.all())
        
        if scores:
            avg_score = round(sum(scores) / len(scores), 1)
        else:
            avg_score = 0
        
        # Count all interactions (including those without scores)
        count_query = select(func.count(InteractionLog.id)).where(
            InteractionLog.item_id == task.id
        )
        count_result = await session.exec(count_query)
        attempts = count_result.one()
        
        result.append({
            "task": task.title,
            "avg_score": avg_score,
            "attempts": attempts
        })
    
    return result


@router.get("/timeline")
async def get_timeline(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Submissions per day for a given lab.
    
    Groups interactions by date and counts submissions per day.
    """
    # Extract the lab number
    lab_number = lab.replace('lab-', '').lstrip('0')
    
    # Find the lab item
    lab_query = select(ItemRecord).where(
        ItemRecord.type == "lab",
        ItemRecord.title.ilike(f"%Lab {lab_number}%")
    )
    lab_result = await session.exec(lab_query)
    lab_item = lab_result.first()
    
    if not lab_item:
        # Try without stripping zeros
        lab_query = select(ItemRecord).where(
            ItemRecord.type == "lab",
            ItemRecord.title.ilike(f"%Lab {lab.replace('lab-', '')}%")
        )
        lab_result = await session.exec(lab_query)
        lab_item = lab_result.first()
    
    if not lab_item:
        return []
    
    # Find all tasks that belong to this lab
    tasks_query = select(ItemRecord.id).where(
        ItemRecord.parent_id == lab_item.id,
        ItemRecord.type == "task"
    )
    tasks_result = await session.exec(tasks_query)
    task_ids = list(tasks_result.all())
    
    if not task_ids:
        return []
    
    # Get all interactions for these tasks
    interactions_query = select(
        InteractionLog.created_at,
        InteractionLog.id
    ).where(
        InteractionLog.item_id.in_(task_ids)
    ).order_by(InteractionLog.created_at)
    
    interactions_result = await session.exec(interactions_query)
    interactions = list(interactions_result.all())
    
    # Group by date manually
    daily_counts = {}
    for interaction in interactions:
        interaction_date = interaction.created_at.date().isoformat()
        daily_counts[interaction_date] = daily_counts.get(interaction_date, 0) + 1
    
    # Convert to list and sort by date
    result = [
        {"date": date_str, "submissions": count}
        for date_str, count in sorted(daily_counts.items())
    ]
    
    return result


@router.get("/groups")
async def get_groups(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Per-group performance for a given lab.
    
    For each student group, computes:
    - avg_score: average score (rounded to 1 decimal)
    - students: count of distinct learners
    """
    # Extract the lab number
    lab_number = lab.replace('lab-', '').lstrip('0')
    
    # Find the lab item
    lab_query = select(ItemRecord).where(
        ItemRecord.type == "lab",
        ItemRecord.title.ilike(f"%Lab {lab_number}%")
    )
    lab_result = await session.exec(lab_query)
    lab_item = lab_result.first()
    
    if not lab_item:
        # Try without stripping zeros
        lab_query = select(ItemRecord).where(
            ItemRecord.type == "lab",
            ItemRecord.title.ilike(f"%Lab {lab.replace('lab-', '')}%")
        )
        lab_result = await session.exec(lab_query)
        lab_item = lab_result.first()
    
    if not lab_item:
        return []
    
    # Find all tasks that belong to this lab
    tasks_query = select(ItemRecord.id).where(
        ItemRecord.parent_id == lab_item.id,
        ItemRecord.type == "task"
    )
    tasks_result = await session.exec(tasks_query)
    task_ids = list(tasks_result.all())
    
    if not task_ids:
        return []
    
    # Get all learners with their groups and interactions for these tasks
    # Join learners with interactions
    from sqlalchemy import join
    
    query = select(
        Learner.student_group,
        Learner.id.label("learner_id"),
        InteractionLog.score
    ).join(
        InteractionLog, InteractionLog.learner_id == Learner.id
    ).where(
        InteractionLog.item_id.in_(task_ids),
        Learner.student_group != "",  # Exclude empty groups
        InteractionLog.score.isnot(None)
    )
    
    result = await session.exec(query)
    rows = list(result.all())
    
    # Group by student_group manually
    groups = {}
    for row in rows:
        if row.student_group not in groups:
            groups[row.student_group] = {
                "scores": [],
                "learners": set()
            }
        groups[row.student_group]["scores"].append(row.score)
        groups[row.student_group]["learners"].add(row.learner_id)
    
    # Calculate averages and format response
    response = []
    for group_name, data in sorted(groups.items()):
        avg_score = round(sum(data["scores"]) / len(data["scores"]), 1) if data["scores"] else 0
        response.append({
            "group": group_name,
            "avg_score": avg_score,
            "students": len(data["learners"])
        })
    
    return response