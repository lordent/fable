import logging
from dataclasses import dataclass
from typing import Any, Final, Literal, TypedDict


@dataclass(frozen=True)
class AnalyzerConfig:
    seq_scan_danger_threshold: int = 10000
    seq_scan_warning_threshold: int = 100
    stats_error_multiplier: float = 10.0
    sql_preview_len: int = 100


NodeType = Literal[
    "Seq Scan",
    "Index Scan",
    "Index Only Scan",
    "Bitmap Heap Scan",
    "Sort",
    "Hash Join",
    "Nested Loop",
    "Merge Join",
    "Aggregate",
    "Limit",
]

PlanNode = TypedDict(
    "PlanNode",
    {
        "Node Type": NodeType | str,
        "Relation Name": str,
        "Actual Rows": int,
        "Plan Rows": int,
        "Sort Method": str,
        "Sort Space Type": Literal["Memory", "Disk"],
        "Plans": list["PlanNode"],
    },
    total=False,
)

PlanData = TypedDict(
    "PlanData", {"Plan": PlanNode, "Execution Time": float, "Planning Time": float}
)

SEC_SCAN_DANGER: Final = "🛑 DANGER: SEQ SCAN на '%s' (%s строк). Срочно нужен индекс!"
SEC_SCAN_WARN: Final = "🟡 WARNING: SEQ SCAN на '%s' (%s строк)."
SORT_DISK_ERR: Final = (
    "🛑 DANGER: SORT ON DISK. Сортировка на диске убивает производительность!"
)
STATS_ERR: Final = "🟠 STATS ERROR: оценка (%s) vs реальность (%s). Сделайте ANALYZE."

logger = logging.getLogger("fable.analyzer")


def analyze_plan(sql: str, plan_data: PlanData, config: AnalyzerConfig):
    issues: list[tuple[int, str, tuple[Any, ...]]] = []
    root_node = plan_data.get("Plan", {})

    def walk(node: PlanNode) -> None:
        node_type = node.get("Node Type")
        actual_rows = node.get("Actual Rows", 0)
        rel = node.get("Relation Name", "unknown")

        if node_type == "Seq Scan":
            if actual_rows > config.seq_scan_danger_threshold:
                issues.append((logging.ERROR, SEC_SCAN_DANGER, (rel, actual_rows)))
            elif actual_rows > config.seq_scan_warning_threshold:
                issues.append((logging.WARNING, SEC_SCAN_WARN, (rel, actual_rows)))

        sort_method = node.get("Sort Method", "")
        if "external" in sort_method.lower():
            issues.append((logging.ERROR, SORT_DISK_ERR, ()))

        est_rows = node.get("Plan Rows", 1)
        if actual_rows > 0:
            ratio = max(actual_rows / est_rows, est_rows / actual_rows)
            if ratio > config.stats_error_multiplier:
                issues.append((logging.WARNING, STATS_ERR, (est_rows, actual_rows)))

        for sub_plan in node.get("Plans", []):
            walk(sub_plan)

    walk(root_node)

    if issues:
        logger.info("═" * 30)
        logger.info("🚀 SQL PERFORMANCE ADVISOR")
        logger.info("Query: %s...", sql[: config.sql_preview_len])
        logger.info("Total Time: %sms", plan_data.get("Execution Time", 0))

        for level, msg, args in issues:
            logger.log(level, msg, *args)
        logger.info("═" * 30)
