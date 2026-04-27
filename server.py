from __future__ import annotations

import json
import os
import re
import sqlite3
import threading
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, unquote, urlparse


BASE_DIR = Path(__file__).resolve().parent
DATA_FILE = BASE_DIR / "data" / "store.json"
DB_ROOT = Path(
    os.getenv(
        "SMART_ALLOCATION_DATA_DIR",
        str(Path(os.getenv("LOCALAPPDATA", str(BASE_DIR / "data"))) / "SmartAllocationBackend"),
    )
)
DB_FILE = Path(os.getenv("DB_FILE", str(DB_ROOT / "smart_allocation.sqlite3")))
HOST = os.getenv("HOST", "127.0.0.1")
PORT = int(os.getenv("PORT", "8000"))
ALLOWED_ORIGIN = os.getenv("ALLOWED_ORIGIN", "*")
WRITE_LOCK = threading.Lock()


SEVERITY_WEIGHT = {
    "Urgent": 45,
    "High": 32,
    "Normal": 18,
    "Low": 8,
}

KNOWN_DISTANCE_KM = {
    frozenset(("Dharavi", "Sion Circle")): 4.5,
    frozenset(("Dharavi", "Mankhurd")): 8.0,
    frozenset(("Dharavi", "Kurla West")): 5.5,
    frozenset(("Dharavi", "Andheri East")): 10.0,
    frozenset(("Kurla West", "Sion Circle")): 6.0,
    frozenset(("Kurla West", "Mankhurd")): 7.0,
    frozenset(("Kurla West", "Andheri East")): 9.0,
    frozenset(("Andheri East", "Sion Circle")): 11.0,
    frozenset(("Andheri East", "Mankhurd")): 13.0,
    frozenset(("Mankhurd", "Sion Circle")): 8.0,
}


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def connect_db() -> sqlite3.Connection:
    DB_FILE.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(DB_FILE, timeout=10)
    connection.row_factory = sqlite3.Row
    return connection


def initialize_database() -> None:
    with connect_db() as connection:
        connection.execute("PRAGMA journal_mode=DELETE")
        connection.execute("PRAGMA busy_timeout=10000")
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS needs (
                id TEXT PRIMARY KEY,
                category TEXT NOT NULL,
                location TEXT NOT NULL,
                severity TEXT NOT NULL,
                status TEXT NOT NULL,
                score INTEGER NOT NULL DEFAULT 0,
                payload TEXT NOT NULL,
                created_at TEXT,
                updated_at TEXT
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS volunteers (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                location TEXT NOT NULL,
                availability TEXT NOT NULL,
                reliability_score INTEGER NOT NULL DEFAULT 0,
                payload TEXT NOT NULL,
                created_at TEXT,
                updated_at TEXT
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS assignments (
                id TEXT PRIMARY KEY,
                need_id TEXT NOT NULL,
                volunteer_id TEXT NOT NULL,
                status TEXT NOT NULL,
                match_score INTEGER NOT NULL DEFAULT 0,
                payload TEXT NOT NULL,
                assigned_at TEXT,
                completed_at TEXT,
                FOREIGN KEY (need_id) REFERENCES needs(id),
                FOREIGN KEY (volunteer_id) REFERENCES volunteers(id)
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS notifications (
                id TEXT PRIMARY KEY,
                type TEXT NOT NULL,
                title TEXT NOT NULL,
                message TEXT NOT NULL,
                severity TEXT NOT NULL DEFAULT 'info',
                is_read INTEGER NOT NULL DEFAULT 0,
                payload TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )
        connection.execute("CREATE INDEX IF NOT EXISTS idx_needs_status_score ON needs(status, score DESC)")
        connection.execute("CREATE INDEX IF NOT EXISTS idx_needs_location ON needs(location)")
        connection.execute("CREATE INDEX IF NOT EXISTS idx_volunteers_location ON volunteers(location)")
        connection.execute("CREATE INDEX IF NOT EXISTS idx_assignments_status ON assignments(status)")
        connection.commit()

        has_data = connection.execute("SELECT COUNT(*) FROM needs").fetchone()[0]
        if has_data == 0:
            if DATA_FILE.exists():
                with DATA_FILE.open("r", encoding="utf-8") as file:
                    store = json.load(file)
            else:
                store = default_store()
            store.setdefault("notifications", default_notifications())
            save_store_to_sqlite(store, connection)


def save_store_to_sqlite(store: dict[str, Any], connection: sqlite3.Connection) -> None:
    connection.execute("DELETE FROM notifications")
    connection.execute("DELETE FROM assignments")
    connection.execute("DELETE FROM volunteers")
    connection.execute("DELETE FROM needs")

    for need in store.get("needs", []):
        connection.execute(
            """
            INSERT INTO needs (
                id, category, location, severity, status, score, payload, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                need.get("id"),
                need.get("category", ""),
                need.get("location", ""),
                need.get("severity", "Normal"),
                need.get("status", "Unassigned"),
                safe_int(need.get("score"), 0),
                json.dumps(need, ensure_ascii=False),
                need.get("createdAt"),
                need.get("updatedAt"),
            ),
        )

    for volunteer in store.get("volunteers", []):
        connection.execute(
            """
            INSERT INTO volunteers (
                id, name, location, availability, reliability_score, payload, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                volunteer.get("id"),
                volunteer.get("name", ""),
                volunteer.get("location", ""),
                volunteer.get("availability", "Available Now"),
                safe_int(volunteer.get("reliabilityScore"), 0),
                json.dumps(volunteer, ensure_ascii=False),
                volunteer.get("createdAt"),
                volunteer.get("updatedAt"),
            ),
        )

    for assignment in store.get("assignments", []):
        connection.execute(
            """
            INSERT INTO assignments (
                id, need_id, volunteer_id, status, match_score, payload, assigned_at, completed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                assignment.get("id"),
                assignment.get("needId", ""),
                assignment.get("volunteerId", ""),
                assignment.get("status", "Assigned"),
                safe_int(assignment.get("matchScore"), 0),
                json.dumps(assignment, ensure_ascii=False),
                assignment.get("assignedAt"),
                assignment.get("completedAt"),
            ),
        )

    for notification in store.get("notifications", []):
        connection.execute(
            """
            INSERT INTO notifications (
                id, type, title, message, severity, is_read, payload, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                notification.get("id"),
                notification.get("type", "system"),
                notification.get("title", ""),
                notification.get("message", ""),
                notification.get("severity", "info"),
                1 if notification.get("isRead") else 0,
                json.dumps(notification.get("payload", {}), ensure_ascii=False),
                notification.get("createdAt") or now_iso(),
            ),
        )


def load_store() -> dict[str, Any]:
    initialize_database()
    with connect_db() as connection:
        needs = [
            json.loads(row["payload"])
            for row in connection.execute("SELECT payload FROM needs ORDER BY score DESC, id")
        ]
        volunteers = [
            json.loads(row["payload"])
            for row in connection.execute("SELECT payload FROM volunteers ORDER BY id")
        ]
        assignments = [
            json.loads(row["payload"])
            for row in connection.execute("SELECT payload FROM assignments ORDER BY assigned_at DESC, id DESC")
        ]
        notifications = [
            {
                "id": row["id"],
                "type": row["type"],
                "title": row["title"],
                "message": row["message"],
                "severity": row["severity"],
                "isRead": bool(row["is_read"]),
                "payload": json.loads(row["payload"] or "{}"),
                "createdAt": row["created_at"],
            }
            for row in connection.execute("SELECT * FROM notifications ORDER BY created_at DESC, id DESC")
        ]

    return {
        "needs": needs,
        "volunteers": volunteers,
        "assignments": assignments,
        "notifications": notifications,
    }


def save_store(store: dict[str, Any]) -> None:
    initialize_database()
    with connect_db() as connection:
        save_store_to_sqlite(store, connection)
        connection.commit()


def next_id(items: list[dict[str, Any]], prefix: str, start: int) -> str:
    highest = start - 1
    for item in items:
        item_id = str(item.get("id", ""))
        if item_id.startswith(prefix):
            number = re.sub(r"\D", "", item_id)
            if number:
                highest = max(highest, int(number))
    return f"{prefix}{highest + 1}"


def default_notifications() -> list[dict[str, Any]]:
    return [
        {
            "id": "NT-1001",
            "type": "priority",
            "title": "Urgent needs require review",
            "message": "Two urgent community needs are waiting for volunteer assignment.",
            "severity": "urgent",
            "isRead": False,
            "payload": {"view": "tasks"},
            "createdAt": "2026-04-27T08:35:00Z",
        },
        {
            "id": "NT-1002",
            "type": "assignment",
            "title": "Medical response assigned",
            "message": "Riya Deshmukh was assigned to the dengue medical supplies task in Dharavi.",
            "severity": "info",
            "isRead": False,
            "payload": {"assignmentId": "A-5001"},
            "createdAt": "2026-04-27T07:10:00Z",
        },
        {
            "id": "NT-1003",
            "type": "impact",
            "title": "Water support completed",
            "message": "Clean drinking water coordination in Mankhurd was marked completed.",
            "severity": "success",
            "isRead": True,
            "payload": {"needId": "N-1045"},
            "createdAt": "2026-04-26T12:00:00Z",
        },
    ]


def default_store() -> dict[str, Any]:
    return {
        "needs": [
            {
                "id": "N-1042",
                "category": "Monsoon Flood Relief",
                "location": "Kurla West",
                "severity": "Urgent",
                "reportedBy": "BMC Ward L",
                "date": "2 hours ago",
                "status": "Unassigned",
                "score": 98,
                "peopleAffected": 250,
                "description": "Flood water has entered low-lying homes. Families need evacuation support, first aid, and dry supplies.",
                "requiredSkills": ["Logistics", "Driving", "First Aid"],
                "createdAt": "2026-04-27T08:30:00Z",
                "updatedAt": "2026-04-27T08:30:00Z",
            },
            {
                "id": "N-1043",
                "category": "Medical Supplies (Dengue)",
                "location": "Dharavi",
                "severity": "High",
                "reportedBy": "Local Clinic",
                "date": "4 hours ago",
                "status": "Assigned",
                "score": 85,
                "peopleAffected": 90,
                "description": "Clinic reports a spike in dengue cases and needs basic medical supplies and trained first-aid volunteers.",
                "requiredSkills": ["Medical", "First Aid"],
                "assignedVolunteerId": "V-001",
                "createdAt": "2026-04-27T06:30:00Z",
                "updatedAt": "2026-04-27T07:10:00Z",
            },
            {
                "id": "N-1044",
                "category": "Shelter & Tarpaulins",
                "location": "Andheri East",
                "severity": "Urgent",
                "reportedBy": "Community Leader",
                "date": "1 day ago",
                "status": "Unassigned",
                "score": 95,
                "peopleAffected": 180,
                "description": "Temporary shelter material is needed for families affected by heavy rain damage.",
                "requiredSkills": ["Construction", "Heavy Lifting", "Logistics"],
                "createdAt": "2026-04-26T10:00:00Z",
                "updatedAt": "2026-04-26T10:00:00Z",
            },
            {
                "id": "N-1045",
                "category": "Clean Drinking Water",
                "location": "Mankhurd",
                "severity": "Normal",
                "reportedBy": "Field Team Alpha",
                "date": "2 days ago",
                "status": "Completed",
                "score": 45,
                "peopleAffected": 60,
                "description": "Community water points need refill coordination and basic sanitation checks.",
                "requiredSkills": ["Logistics", "Water Sanitation"],
                "createdAt": "2026-04-25T10:00:00Z",
                "updatedAt": "2026-04-26T12:00:00Z",
            },
            {
                "id": "N-1046",
                "category": "Clearing Debris",
                "location": "Sion Circle",
                "severity": "High",
                "reportedBy": "Traffic Police",
                "date": "1 hour ago",
                "status": "Unassigned",
                "score": 78,
                "peopleAffected": 45,
                "description": "Roadside debris is blocking emergency access and needs a small heavy-lifting team.",
                "requiredSkills": ["Construction", "Heavy Lifting"],
                "createdAt": "2026-04-27T09:30:00Z",
                "updatedAt": "2026-04-27T09:30:00Z",
            },
        ],
        "volunteers": [
            {
                "id": "V-001",
                "name": "Riya Deshmukh",
                "skills": ["Medical", "First Aid"],
                "location": "Dharavi",
                "availability": "Unavailable",
                "reliabilityScore": 99,
                "avatar": "https://i.pravatar.cc/150?img=1",
                "languages": ["Hindi", "Marathi", "English"],
                "certifications": ["First Aid"],
                "activeAssignments": ["A-5001"],
            },
            {
                "id": "V-002",
                "name": "Rahul Patil",
                "skills": ["Logistics", "Driving"],
                "location": "Kurla West",
                "availability": "Available Now",
                "reliabilityScore": 88,
                "avatar": "https://i.pravatar.cc/150?img=11",
                "languages": ["Hindi", "Marathi"],
                "certifications": ["LMV Driving"],
                "activeAssignments": [],
            },
            {
                "id": "V-003",
                "name": "Priya Sharma",
                "skills": ["Translation", "Admin"],
                "location": "Andheri East",
                "availability": "Available Now",
                "reliabilityScore": 92,
                "avatar": "https://i.pravatar.cc/150?img=5",
                "languages": ["Hindi", "English"],
                "certifications": [],
                "activeAssignments": [],
            },
            {
                "id": "V-004",
                "name": "Arjun Kadam",
                "skills": ["Construction", "Heavy Lifting"],
                "location": "Mankhurd",
                "availability": "Available Tomorrow",
                "reliabilityScore": 75,
                "avatar": "https://i.pravatar.cc/150?img=8",
                "languages": ["Hindi", "Marathi"],
                "certifications": ["Safety Training"],
                "activeAssignments": [],
            },
            {
                "id": "V-005",
                "name": "Aisha Malik",
                "skills": ["Medical", "Counseling"],
                "location": "Sion Circle",
                "availability": "Available Now",
                "reliabilityScore": 95,
                "avatar": "https://i.pravatar.cc/150?img=9",
                "languages": ["Hindi", "Urdu", "English"],
                "certifications": ["Counseling"],
                "activeAssignments": [],
            },
        ],
        "assignments": [
            {
                "id": "A-5001",
                "needId": "N-1043",
                "volunteerId": "V-001",
                "status": "Assigned",
                "assignedBy": "Program Manager",
                "matchScore": 96,
                "assignedAt": "2026-04-27T07:10:00Z",
                "completedAt": None,
                "auditLog": [
                    {
                        "action": "ASSIGNED",
                        "actor": "Program Manager",
                        "timestamp": "2026-04-27T07:10:00Z",
                        "note": "Initial seeded assignment for dengue medical supply response.",
                    }
                ],
            }
        ],
        "notifications": default_notifications(),
    }


def add_notification(
    store: dict[str, Any],
    notification_type: str,
    title: str,
    message: str,
    severity: str = "info",
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    notification = {
        "id": next_id(store.setdefault("notifications", []), "NT-", 1001),
        "type": notification_type,
        "title": title,
        "message": message,
        "severity": severity,
        "isRead": False,
        "payload": payload or {},
        "createdAt": now_iso(),
    }
    store["notifications"].insert(0, notification)
    return notification


def find_by_id(items: list[dict[str, Any]], item_id: str) -> dict[str, Any] | None:
    return next((item for item in items if str(item.get("id")) == item_id), None)


def infer_required_skills(category: str, description: str = "") -> list[str]:
    text = f"{category} {description}".lower()
    if any(word in text for word in ("medical", "dengue", "first aid", "clinic")):
        return ["Medical", "First Aid"]
    if any(word in text for word in ("water", "sanitation")):
        return ["Logistics", "Water Sanitation"]
    if any(word in text for word in ("shelter", "housing", "tarpaulin")):
        return ["Construction", "Heavy Lifting", "Logistics"]
    if any(word in text for word in ("food", "nutrition")):
        return ["Logistics", "Nutrition"]
    if any(word in text for word in ("flood", "relief", "monsoon")):
        return ["Logistics", "Driving", "First Aid"]
    if any(word in text for word in ("debris", "construction", "clearing")):
        return ["Construction", "Heavy Lifting"]
    if any(word in text for word in ("transport", "delivery")):
        return ["Logistics", "Driving"]
    return ["Community Outreach"]


def calculate_need_score(need: dict[str, Any], all_needs: list[dict[str, Any]]) -> int:
    severity = str(need.get("severity", "Normal")).title()
    people_affected = safe_int(need.get("peopleAffected"), 0)
    category = str(need.get("category", "")).lower()
    location = str(need.get("location", "")).lower()

    same_area_count = sum(
        1
        for item in all_needs
        if str(item.get("category", "")).lower() == category
        and str(item.get("location", "")).lower() == location
        and str(item.get("status", "")).lower() != "completed"
    )

    score = SEVERITY_WEIGHT.get(severity, 18)
    score += min(25, people_affected // 10)
    score += min(18, max(0, same_area_count - 1) * 6)
    score += 12
    return max(0, min(100, int(score)))


def safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def distance_between(left: str, right: str) -> float:
    if left == right:
        return 1.0
    return KNOWN_DISTANCE_KM.get(frozenset((left, right)), 12.0)


def active_assignment_for(volunteer: dict[str, Any], assignments: list[dict[str, Any]]) -> dict[str, Any] | None:
    volunteer_id = str(volunteer.get("id"))
    return next(
        (
            assignment
            for assignment in assignments
            if str(assignment.get("volunteerId")) == volunteer_id
            and str(assignment.get("status")) in {"Assigned", "In Progress"}
        ),
        None,
    )


def match_volunteer_to_need(
    volunteer: dict[str, Any],
    need: dict[str, Any],
    assignments: list[dict[str, Any]],
) -> dict[str, Any]:
    required_skills = set(need.get("requiredSkills") or [])
    volunteer_skills = set(volunteer.get("skills") or [])
    overlap = sorted(required_skills.intersection(volunteer_skills))
    missing = sorted(required_skills.difference(volunteer_skills))

    skill_score = 18
    if required_skills:
        skill_score = round(35 * (len(overlap) / len(required_skills)))

    distance_km = distance_between(str(need.get("location", "")), str(volunteer.get("location", "")))
    distance_score = max(0, round(25 - (distance_km * 1.5)))

    availability = str(volunteer.get("availability", "")).lower()
    if "available now" in availability:
        availability_score = 25
    elif "available tomorrow" in availability:
        availability_score = 12
    elif "available" in availability and "unavailable" not in availability:
        availability_score = 15
    else:
        availability_score = 0

    reliability_score = round(safe_int(volunteer.get("reliabilityScore"), 70) * 0.15)
    conflict = active_assignment_for(volunteer, assignments)
    score = skill_score + distance_score + availability_score + reliability_score
    if conflict:
        score -= 30
    score = max(0, min(100, score))

    reasons: list[str] = []
    if overlap:
        reasons.append(f"Skill match: {', '.join(overlap)}")
    if missing:
        reasons.append(f"Missing skill: {', '.join(missing)}")
    if distance_km <= 2:
        reasons.append("Very close to need location")
    elif distance_km <= 6:
        reasons.append(f"Nearby location: {distance_km:g} km")
    else:
        reasons.append(f"Further away: {distance_km:g} km")
    if availability_score:
        reasons.append(str(volunteer.get("availability")))
    else:
        reasons.append("Not currently available")
    if conflict:
        reasons.append(f"Conflict: already assigned to {conflict.get('needId')}")

    return {
        "volunteer": volunteer,
        "matchScore": score,
        "distanceKm": distance_km,
        "skillMatches": overlap,
        "missingSkills": missing,
        "hasConflict": conflict is not None,
        "reasons": reasons,
    }


def build_matches(need: dict[str, Any], store: dict[str, Any]) -> list[dict[str, Any]]:
    matches = [
        match_volunteer_to_need(volunteer, need, store.get("assignments", []))
        for volunteer in store.get("volunteers", [])
    ]
    return sorted(matches, key=lambda item: item["matchScore"], reverse=True)


def dashboard_payload(store: dict[str, Any]) -> dict[str, Any]:
    needs = store.get("needs", [])
    volunteers = store.get("volunteers", [])
    assignments = store.get("assignments", [])
    notifications = store.get("notifications", [])
    active_assignments = [
        assignment
        for assignment in assignments
        if assignment.get("status") in {"Assigned", "In Progress"}
    ]
    completed_needs = [need for need in needs if need.get("status") == "Completed"]
    total_needs = len(needs)

    metrics = {
        "activeNeeds": sum(1 for need in needs if need.get("status") != "Completed"),
        "unmetUrgent": sum(
            1
            for need in needs
            if need.get("status") == "Unassigned" and need.get("severity") == "Urgent"
        ),
        "activeVolunteers": sum(
            1
            for volunteer in volunteers
            if "available" in str(volunteer.get("availability", "")).lower()
            and "unavailable" not in str(volunteer.get("availability", "")).lower()
        ),
        "avgDeploymentTime": "24 mins",
        "activeAssignments": len(active_assignments),
        "completionRate": round((len(completed_needs) / total_needs) * 100) if total_needs else 0,
        "peopleReachedEstimate": sum(safe_int(need.get("peopleAffected"), 0) for need in completed_needs),
        "unreadNotifications": sum(1 for notification in notifications if not notification.get("isRead")),
    }

    priority_needs = sorted(needs, key=lambda need: safe_int(need.get("score"), 0), reverse=True)

    return {
        "metrics": metrics,
        "needs": priority_needs,
        "volunteers": volunteers,
        "assignments": assignments,
        "notifications": notifications[:8],
        "priorityNeeds": priority_needs[:3],
        "analytics": analytics_payload(store),
    }


def analytics_payload(store: dict[str, Any]) -> dict[str, Any]:
    needs = store.get("needs", [])
    volunteers = store.get("volunteers", [])
    assignments = store.get("assignments", [])
    open_needs = [need for need in needs if need.get("status") == "Unassigned"]
    completed_needs = [need for need in needs if need.get("status") == "Completed"]

    skill_demand: dict[str, int] = {}
    for need in open_needs:
        for skill in need.get("requiredSkills", []):
            skill_demand[str(skill)] = skill_demand.get(str(skill), 0) + 1

    volunteer_supply: dict[str, int] = {}
    for volunteer in volunteers:
        if "unavailable" in str(volunteer.get("availability", "")).lower():
            continue
        for skill in volunteer.get("skills", []):
            volunteer_supply[str(skill)] = volunteer_supply.get(str(skill), 0) + 1

    status_counts = {
        "unassigned": sum(1 for need in needs if need.get("status") == "Unassigned"),
        "assigned": sum(1 for need in needs if need.get("status") == "Assigned"),
        "completed": len(completed_needs),
    }

    high_risk_locations = [
        {
            "location": row["name"],
            "open": row["open"],
            "assigned": row["assigned"],
            "completed": row["completed"],
            "total": row["total"],
        }
        for row in summarize_by(needs, "location")
        if row["open"] or row["assigned"]
    ][:5]

    return {
        "statusCounts": status_counts,
        "skillDemand": sorted(
            [{"skill": skill, "openNeeds": count} for skill, count in skill_demand.items()],
            key=lambda item: item["openNeeds"],
            reverse=True,
        ),
        "volunteerSupply": sorted(
            [{"skill": skill, "availableVolunteers": count} for skill, count in volunteer_supply.items()],
            key=lambda item: item["availableVolunteers"],
            reverse=True,
        ),
        "highRiskLocations": high_risk_locations,
        "averageMatchScore": round(
            sum(safe_int(assignment.get("matchScore"), 0) for assignment in assignments) / len(assignments)
        )
        if assignments
        else 0,
    }


def apply_filters(items: list[dict[str, Any]], query: dict[str, list[str]]) -> list[dict[str, Any]]:
    results = items
    for key in ("status", "severity", "location", "category"):
        value = first_query_value(query, key)
        if value:
            results = [
                item
                for item in results
                if str(item.get(key, "")).lower() == value.lower()
            ]
    search = first_query_value(query, "search")
    if search:
        needle = search.lower()
        results = [
            item
            for item in results
            if needle in json.dumps(item, ensure_ascii=False).lower()
        ]
    return results


def first_query_value(query: dict[str, list[str]], key: str) -> str:
    values = query.get(key) or []
    return values[0].strip() if values else ""


def error_payload(message: str, details: Any = None) -> dict[str, Any]:
    payload: dict[str, Any] = {"error": {"message": message}}
    if details is not None:
        payload["error"]["details"] = details
    return payload


class SmartAllocationHandler(BaseHTTPRequestHandler):
    server_version = "SmartAllocationAPI/1.0"

    def log_message(self, format: str, *args: Any) -> None:
        print(f"{self.address_string()} - {format % args}")

    def end_headers(self) -> None:
        self.send_header("Access-Control-Allow-Origin", ALLOWED_ORIGIN)
        self.send_header("Access-Control-Allow-Methods", "GET, POST, PATCH, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, Authorization")
        self.send_header("Access-Control-Max-Age", "86400")
        super().end_headers()

    def do_OPTIONS(self) -> None:
        self.send_response(204)
        self.end_headers()

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/") or "/"
        query = parse_qs(parsed.query)

        try:
            store = load_store()
            if path == "/":
                self.send_json(200, self.api_index())
            elif path == "/api/health":
                self.send_json(200, {"status": "ok", "service": "smart-resource-allocation", "time": now_iso()})
            elif path in {"/api/data", "/api/dashboard"}:
                self.send_json(200, dashboard_payload(store))
            elif path == "/api/needs":
                needs = sorted(
                    apply_filters(store.get("needs", []), query),
                    key=lambda need: safe_int(need.get("score"), 0),
                    reverse=True,
                )
                self.send_json(200, {"needs": needs, "count": len(needs)})
            elif match := re.fullmatch(r"/api/needs/([^/]+)", path):
                need = find_by_id(store.get("needs", []), unquote(match.group(1)))
                if not need:
                    self.send_json(404, error_payload("Need not found"))
                    return
                self.send_json(200, {"need": need})
            elif path == "/api/volunteers":
                volunteers = apply_filters(store.get("volunteers", []), query)
                skill = first_query_value(query, "skill")
                if skill:
                    volunteers = [
                        volunteer
                        for volunteer in volunteers
                        if skill.lower() in [item.lower() for item in volunteer.get("skills", [])]
                    ]
                self.send_json(200, {"volunteers": volunteers, "count": len(volunteers)})
            elif match := re.fullmatch(r"/api/volunteers/([^/]+)", path):
                volunteer = find_by_id(store.get("volunteers", []), unquote(match.group(1)))
                if not volunteer:
                    self.send_json(404, error_payload("Volunteer not found"))
                    return
                self.send_json(200, {"volunteer": volunteer})
            elif path == "/api/tasks/open":
                needs = [
                    need
                    for need in store.get("needs", [])
                    if need.get("status") == "Unassigned"
                ]
                needs.sort(key=lambda need: safe_int(need.get("score"), 0), reverse=True)
                self.send_json(200, {"needs": needs, "count": len(needs)})
            elif match := re.fullmatch(r"/api/tasks/([^/]+)/matches", path):
                need = find_by_id(store.get("needs", []), unquote(match.group(1)))
                if not need:
                    self.send_json(404, error_payload("Need not found"))
                    return
                limit = safe_int(first_query_value(query, "limit"), 5)
                matches = build_matches(need, store)[: max(1, min(limit, 20))]
                self.send_json(200, {"need": need, "matches": matches, "count": len(matches)})
            elif path == "/api/assignments":
                self.send_json(200, {"assignments": store.get("assignments", []), "count": len(store.get("assignments", []))})
            elif path == "/api/notifications":
                notifications = store.get("notifications", [])
                unread_only = first_query_value(query, "unread").lower() == "true"
                if unread_only:
                    notifications = [item for item in notifications if not item.get("isRead")]
                self.send_json(200, {"notifications": notifications, "count": len(notifications)})
            elif path == "/api/analytics":
                self.send_json(200, analytics_payload(store))
            elif path == "/api/reports/impact":
                self.send_json(200, self.impact_report(store))
            else:
                self.send_json(404, error_payload("Route not found"))
        except Exception as exc:
            self.send_json(500, error_payload("Internal server error", str(exc)))

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/") or "/"

        try:
            body = self.read_json_body()
        except ValueError as exc:
            self.send_json(400, error_payload(str(exc)))
            return

        try:
            if path == "/api/needs":
                self.create_need(body)
            elif path == "/api/volunteers":
                self.create_volunteer(body)
            elif path == "/api/assignments":
                self.create_assignment(body)
            elif path == "/api/messages/broadcast":
                self.create_broadcast(body)
            else:
                self.send_json(404, error_payload("Route not found"))
        except Exception as exc:
            self.send_json(500, error_payload("Internal server error", str(exc)))

    def do_PATCH(self) -> None:
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/") or "/"

        try:
            body = self.read_json_body()
        except ValueError as exc:
            self.send_json(400, error_payload(str(exc)))
            return

        try:
            if match := re.fullmatch(r"/api/needs/([^/]+)", path):
                self.update_need(unquote(match.group(1)), body)
            elif match := re.fullmatch(r"/api/volunteers/([^/]+)", path):
                self.update_volunteer(unquote(match.group(1)), body)
            elif match := re.fullmatch(r"/api/assignments/([^/]+)", path):
                self.update_assignment(unquote(match.group(1)), body)
            elif match := re.fullmatch(r"/api/notifications/([^/]+)", path):
                self.update_notification(unquote(match.group(1)), body)
            else:
                self.send_json(404, error_payload("Route not found"))
        except Exception as exc:
            self.send_json(500, error_payload("Internal server error", str(exc)))

    def create_need(self, body: dict[str, Any]) -> None:
        category = clean_text(body.get("category") or body.get("needCategory"))
        severity = normalize_severity(body.get("severity") or body.get("urgencyLevel"))
        location = clean_text(body.get("location"))
        description = clean_text(body.get("description"), max_length=2000)

        missing = [
            field
            for field, value in {
                "category": category,
                "severity": severity,
                "location": location,
            }.items()
            if not value
        ]
        if missing:
            self.send_json(400, error_payload("Missing required fields", missing))
            return

        with WRITE_LOCK:
            store = load_store()
            duplicate = self.find_duplicate_need(store, category, location, severity)
            if duplicate:
                duplicate["peopleAffected"] = max(
                    safe_int(duplicate.get("peopleAffected")),
                    safe_int(body.get("peopleAffected"), safe_int(duplicate.get("peopleAffected"))),
                )
                if description and description not in duplicate.get("description", ""):
                    duplicate["description"] = f"{duplicate.get('description', '')}\n\nAdditional report: {description}".strip()
                duplicate["updatedAt"] = now_iso()
                duplicate["score"] = calculate_need_score(duplicate, store.get("needs", []))
                add_notification(
                    store,
                    "deduplication",
                    "Duplicate report merged",
                    f"{category} in {location} was merged with an existing open need.",
                    "info",
                    {"needId": duplicate.get("id")},
                )
                save_store(store)
                self.send_json(200, {"need": duplicate, "duplicateMerged": True})
                return

            need = {
                "id": next_id(store.get("needs", []), "N-", 1042),
                "category": category,
                "location": location,
                "severity": severity,
                "reportedBy": clean_text(body.get("reportedBy")) or "Field Coordinator",
                "date": "just now",
                "status": "Unassigned",
                "score": 0,
                "peopleAffected": safe_int(body.get("peopleAffected"), 0),
                "description": description,
                "requiredSkills": body.get("requiredSkills") or infer_required_skills(category, description),
                "createdAt": now_iso(),
                "updatedAt": now_iso(),
            }
            store.setdefault("needs", []).append(need)
            need["score"] = calculate_need_score(need, store.get("needs", []))
            add_notification(
                store,
                "priority",
                "New community need reported",
                f"{severity} need reported in {location}: {category}.",
                "urgent" if severity == "Urgent" else "info",
                {"needId": need.get("id")},
            )
            save_store(store)
            self.send_json(201, {"need": need})

    def update_need(self, need_id: str, body: dict[str, Any]) -> None:
        with WRITE_LOCK:
            store = load_store()
            need = find_by_id(store.get("needs", []), need_id)
            if not need:
                self.send_json(404, error_payload("Need not found"))
                return
            allowed_fields = {
                "category",
                "location",
                "severity",
                "reportedBy",
                "status",
                "peopleAffected",
                "description",
                "requiredSkills",
            }
            for key, value in body.items():
                if key in allowed_fields:
                    need[key] = value
            if "severity" in body:
                need["severity"] = normalize_severity(body.get("severity")) or need["severity"]
            need["updatedAt"] = now_iso()
            need["score"] = calculate_need_score(need, store.get("needs", []))
            save_store(store)
            self.send_json(200, {"need": need})

    def create_volunteer(self, body: dict[str, Any]) -> None:
        name = clean_text(body.get("name"))
        location = clean_text(body.get("location"))
        skills = ensure_string_list(body.get("skills"))

        missing = [
            field
            for field, value in {"name": name, "location": location, "skills": skills}.items()
            if not value
        ]
        if missing:
            self.send_json(400, error_payload("Missing required fields", missing))
            return

        with WRITE_LOCK:
            store = load_store()
            volunteer = {
                "id": next_id(store.get("volunteers", []), "V-", 1),
                "name": name,
                "skills": skills,
                "location": location,
                "availability": clean_text(body.get("availability")) or "Available Now",
                "reliabilityScore": max(0, min(100, safe_int(body.get("reliabilityScore"), 80))),
                "avatar": clean_text(body.get("avatar")) or "https://i.pravatar.cc/150?img=12",
                "languages": ensure_string_list(body.get("languages")),
                "certifications": ensure_string_list(body.get("certifications")),
                "activeAssignments": [],
                "createdAt": now_iso(),
                "updatedAt": now_iso(),
            }
            store.setdefault("volunteers", []).append(volunteer)
            add_notification(
                store,
                "volunteer",
                "New volunteer registered",
                f"{name} joined the volunteer network in {location}.",
                "success",
                {"volunteerId": volunteer.get("id")},
            )
            save_store(store)
            self.send_json(201, {"volunteer": volunteer})

    def update_volunteer(self, volunteer_id: str, body: dict[str, Any]) -> None:
        with WRITE_LOCK:
            store = load_store()
            volunteer = find_by_id(store.get("volunteers", []), volunteer_id)
            if not volunteer:
                self.send_json(404, error_payload("Volunteer not found"))
                return
            allowed_fields = {
                "name",
                "skills",
                "location",
                "availability",
                "reliabilityScore",
                "avatar",
                "languages",
                "certifications",
            }
            for key, value in body.items():
                if key in allowed_fields:
                    volunteer[key] = value
            volunteer["updatedAt"] = now_iso()
            save_store(store)
            self.send_json(200, {"volunteer": volunteer})

    def create_assignment(self, body: dict[str, Any]) -> None:
        need_id = clean_text(body.get("needId"))
        volunteer_id = clean_text(body.get("volunteerId"))
        assigned_by = clean_text(body.get("assignedBy")) or "Coordinator"
        if not need_id or not volunteer_id:
            self.send_json(400, error_payload("needId and volunteerId are required"))
            return

        with WRITE_LOCK:
            store = load_store()
            need = find_by_id(store.get("needs", []), need_id)
            volunteer = find_by_id(store.get("volunteers", []), volunteer_id)
            if not need:
                self.send_json(404, error_payload("Need not found"))
                return
            if not volunteer:
                self.send_json(404, error_payload("Volunteer not found"))
                return
            if need.get("status") != "Unassigned":
                self.send_json(409, error_payload("Need is not open for assignment", {"status": need.get("status")}))
                return
            if active_assignment_for(volunteer, store.get("assignments", [])):
                self.send_json(409, error_payload("Volunteer already has an active assignment"))
                return
            availability = str(volunteer.get("availability", "")).lower()
            if "unavailable" in availability:
                self.send_json(409, error_payload("Volunteer is unavailable"))
                return

            match = match_volunteer_to_need(volunteer, need, store.get("assignments", []))
            assignment = {
                "id": next_id(store.get("assignments", []), "A-", 5001),
                "needId": need_id,
                "volunteerId": volunteer_id,
                "status": "Assigned",
                "assignedBy": assigned_by,
                "matchScore": match["matchScore"],
                "assignedAt": now_iso(),
                "completedAt": None,
                "auditLog": [
                    {
                        "action": "ASSIGNED",
                        "actor": assigned_by,
                        "timestamp": now_iso(),
                        "note": clean_text(body.get("note")) or "Assigned through smart matching API.",
                    }
                ],
            }
            store.setdefault("assignments", []).append(assignment)
            need["status"] = "Assigned"
            need["assignedVolunteerId"] = volunteer_id
            need["updatedAt"] = now_iso()
            volunteer.setdefault("activeAssignments", []).append(assignment["id"])
            volunteer["availability"] = "Unavailable"
            volunteer["updatedAt"] = now_iso()
            add_notification(
                store,
                "assignment",
                "Volunteer deployed",
                f"{volunteer.get('name')} was assigned to {need.get('category')} in {need.get('location')}.",
                "success",
                {"assignmentId": assignment.get("id"), "needId": need_id, "volunteerId": volunteer_id},
            )
            save_store(store)
            self.send_json(201, {"assignment": assignment, "need": need, "volunteer": volunteer})

    def update_assignment(self, assignment_id: str, body: dict[str, Any]) -> None:
        status = clean_text(body.get("status"))
        if status not in {"Assigned", "In Progress", "Completed", "Cancelled"}:
            self.send_json(400, error_payload("Invalid status", "Use Assigned, In Progress, Completed, or Cancelled"))
            return

        with WRITE_LOCK:
            store = load_store()
            assignment = find_by_id(store.get("assignments", []), assignment_id)
            if not assignment:
                self.send_json(404, error_payload("Assignment not found"))
                return
            need = find_by_id(store.get("needs", []), str(assignment.get("needId")))
            volunteer = find_by_id(store.get("volunteers", []), str(assignment.get("volunteerId")))
            assignment["status"] = status
            assignment.setdefault("auditLog", []).append(
                {
                    "action": f"STATUS_{status.upper().replace(' ', '_')}",
                    "actor": clean_text(body.get("actor")) or "Coordinator",
                    "timestamp": now_iso(),
                    "note": clean_text(body.get("note")),
                }
            )
            if status == "Completed":
                assignment["completedAt"] = now_iso()
                if need:
                    need["status"] = "Completed"
                    need["updatedAt"] = now_iso()
                if volunteer:
                    remove_assignment_from_volunteer(volunteer, assignment_id)
                    volunteer["availability"] = "Available Now"
                    volunteer["updatedAt"] = now_iso()
                add_notification(
                    store,
                    "impact",
                    "Task completed",
                    f"{need.get('category') if need else 'A task'} was marked completed.",
                    "success",
                    {"assignmentId": assignment_id, "needId": assignment.get("needId")},
                )
            elif status == "Cancelled":
                assignment["completedAt"] = now_iso()
                if need:
                    need["status"] = "Unassigned"
                    need.pop("assignedVolunteerId", None)
                    need["updatedAt"] = now_iso()
                if volunteer:
                    remove_assignment_from_volunteer(volunteer, assignment_id)
                    volunteer["availability"] = "Available Now"
                    volunteer["updatedAt"] = now_iso()
                add_notification(
                    store,
                    "assignment",
                    "Assignment cancelled",
                    f"{need.get('category') if need else 'A task'} is open again for matching.",
                    "warning",
                    {"assignmentId": assignment_id, "needId": assignment.get("needId")},
                )
            save_store(store)
            self.send_json(200, {"assignment": assignment, "need": need, "volunteer": volunteer})

    def update_notification(self, notification_id: str, body: dict[str, Any]) -> None:
        with WRITE_LOCK:
            store = load_store()
            notification = find_by_id(store.get("notifications", []), notification_id)
            if not notification:
                self.send_json(404, error_payload("Notification not found"))
                return
            if "isRead" in body:
                notification["isRead"] = bool(body.get("isRead"))
            save_store(store)
            self.send_json(200, {"notification": notification})

    def create_broadcast(self, body: dict[str, Any]) -> None:
        message = clean_text(body.get("message"), max_length=500)
        audience = clean_text(body.get("audience")) or "all volunteers"
        if not message:
            self.send_json(400, error_payload("message is required"))
            return

        with WRITE_LOCK:
            store = load_store()
            notification = add_notification(
                store,
                "broadcast",
                "Broadcast queued",
                f"Message queued for {audience}: {message}",
                clean_text(body.get("severity")) or "info",
                {"audience": audience},
            )
            save_store(store)
            self.send_json(202, {"message": "Broadcast accepted for delivery", "notification": notification})

    def find_duplicate_need(
        self,
        store: dict[str, Any],
        category: str,
        location: str,
        severity: str,
    ) -> dict[str, Any] | None:
        for need in store.get("needs", []):
            if need.get("status") == "Completed":
                continue
            same_category = str(need.get("category", "")).lower() == category.lower()
            same_location = str(need.get("location", "")).lower() == location.lower()
            same_severity = str(need.get("severity", "")).lower() == severity.lower()
            if same_category and same_location and same_severity:
                return need
        return None

    def impact_report(self, store: dict[str, Any]) -> dict[str, Any]:
        needs = store.get("needs", [])
        assignments = store.get("assignments", [])
        completed = [need for need in needs if need.get("status") == "Completed"]
        return {
            "summary": {
                "totalNeeds": len(needs),
                "completedNeeds": len(completed),
                "openNeeds": sum(1 for need in needs if need.get("status") == "Unassigned"),
                "assignedNeeds": sum(1 for need in needs if need.get("status") == "Assigned"),
                "totalAssignments": len(assignments),
                "peopleReachedEstimate": sum(safe_int(need.get("peopleAffected"), 0) for need in completed),
            },
            "byCategory": summarize_by(needs, "category"),
            "byLocation": summarize_by(needs, "location"),
        }

    def read_json_body(self) -> dict[str, Any]:
        length = safe_int(self.headers.get("Content-Length"), 0)
        if length > 1_000_000:
            raise ValueError("Request body is too large")
        if length == 0:
            return {}
        raw = self.rfile.read(length)
        try:
            decoded = raw.decode("utf-8")
            data = json.loads(decoded)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError("Request body must be valid JSON") from exc
        if not isinstance(data, dict):
            raise ValueError("Request body must be a JSON object")
        return data

    def send_json(self, status: int, payload: dict[str, Any]) -> None:
        body = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def api_index(self) -> dict[str, Any]:
        return {
            "name": "Smart Resource Allocation API",
            "version": "1.0",
            "endpoints": {
                "health": "GET /api/health",
                "frontendData": "GET /api/data",
                "dashboard": "GET /api/dashboard",
                "listNeeds": "GET /api/needs",
                "createNeed": "POST /api/needs",
                "listVolunteers": "GET /api/volunteers",
                "createVolunteer": "POST /api/volunteers",
                "openTasks": "GET /api/tasks/open",
                "matches": "GET /api/tasks/{needId}/matches",
                "assign": "POST /api/assignments",
                "analytics": "GET /api/analytics",
                "notifications": "GET /api/notifications",
                "impactReport": "GET /api/reports/impact",
            },
        }


def clean_text(value: Any, max_length: int = 255) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    return text[:max_length]


def normalize_severity(value: Any) -> str:
    text = clean_text(value).lower()
    mapping = {
        "critical": "Urgent",
        "urgent": "Urgent",
        "high": "High",
        "normal": "Normal",
        "medium": "Normal",
        "low": "Low",
    }
    return mapping.get(text, "")


def ensure_string_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [item.strip() for item in value.split(",") if item.strip()]
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    return []


def remove_assignment_from_volunteer(volunteer: dict[str, Any], assignment_id: str) -> None:
    volunteer["activeAssignments"] = [
        item
        for item in volunteer.get("activeAssignments", [])
        if item != assignment_id
    ]


def summarize_by(needs: list[dict[str, Any]], field: str) -> list[dict[str, Any]]:
    summary: dict[str, dict[str, Any]] = {}
    for need in needs:
        key = str(need.get(field) or "Unknown")
        bucket = summary.setdefault(
            key,
            {"name": key, "total": 0, "open": 0, "assigned": 0, "completed": 0},
        )
        bucket["total"] += 1
        status = str(need.get("status", "")).lower()
        if status == "unassigned":
            bucket["open"] += 1
        elif status == "assigned":
            bucket["assigned"] += 1
        elif status == "completed":
            bucket["completed"] += 1
    return sorted(summary.values(), key=lambda item: item["total"], reverse=True)


def run() -> None:
    initialize_database()
    server = ThreadingHTTPServer((HOST, PORT), SmartAllocationHandler)
    print(f"Smart Resource Allocation API running at http://{HOST}:{PORT}")
    print(f"SQLite database: {DB_FILE}")
    print("Press Ctrl+C to stop.")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopping server...")
    finally:
        server.server_close()


if __name__ == "__main__":
    run()
