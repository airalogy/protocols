from datetime import datetime, timezone
import time
from typing import Any

from airalogy.assigner import AssignerResult, assigner


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@assigner(
    assigned_fields=["a_f3"],
    dependent_fields=["a_f1", "a_f2"],
    mode="auto",
)
def line_a_f3(dependent_fields: dict[str, Any]) -> AssignerResult:
    a_f1 = int(dependent_fields["a_f1"])
    a_f2 = int(dependent_fields["a_f2"])
    return AssignerResult(assigned_fields={"a_f3": a_f1 + a_f2})


@assigner(
    assigned_fields=["a_f4"],
    dependent_fields=["a_f3"],
    mode="auto",
)
def line_a_f4(dependent_fields: dict[str, Any]) -> AssignerResult:
    a_f3 = int(dependent_fields["a_f3"])
    return AssignerResult(assigned_fields={"a_f4": a_f3 * 3})


@assigner(
    assigned_fields=["a_f5"],
    dependent_fields=["a_f4"],
    mode="auto",
)
def line_a_f5(dependent_fields: dict[str, Any]) -> AssignerResult:
    a_f4 = int(dependent_fields["a_f4"])
    return AssignerResult(assigned_fields={"a_f5": a_f4 * a_f4})


@assigner(
    assigned_fields=["b_f5"],
    dependent_fields=["b_f1", "b_f2", "b_f3"],
    mode="manual",
)
def line_b_f5_manual(dependent_fields: dict[str, Any]) -> AssignerResult:
    b_f1 = int(dependent_fields["b_f1"])
    b_f2 = int(dependent_fields["b_f2"])
    b_f3 = int(dependent_fields["b_f3"])
    return AssignerResult(assigned_fields={"b_f5": b_f1 * b_f2 + b_f3})


@assigner(
    assigned_fields=["b_f6", "b_f7", "b_f8"],
    dependent_fields=["b_f5", "b_f4"],
    mode="auto",
)
def line_b_slow_auto(dependent_fields: dict[str, Any]) -> AssignerResult:
    b_f5 = int(dependent_fields["b_f5"])
    delay_seconds = int(dependent_fields.get("b_f4") or 0)

    if delay_seconds < 0:
        return AssignerResult(
            success=False,
            error_message="b_f4 (delay seconds) must be >= 0.",
        )
    if delay_seconds > 10:
        delay_seconds = 10

    started_at = _now_iso()
    time.sleep(delay_seconds)
    finished_at = _now_iso()

    return AssignerResult(
        assigned_fields={
            "b_f6": b_f5 * 2,
            "b_f7": started_at,
            "b_f8": finished_at,
        }
    )


@assigner(
    assigned_fields=["c_f3"],
    dependent_fields=["c_f1", "c_f2"],
    mode="auto_first",
)
def line_c_auto_first(dependent_fields: dict[str, Any]) -> AssignerResult:
    c_f1 = int(dependent_fields["c_f1"])
    c_f2 = int(dependent_fields["c_f2"])
    return AssignerResult(assigned_fields={"c_f3": c_f1 + c_f2})


@assigner(
    assigned_fields=["d_f3"],
    dependent_fields=["d_f1", "d_f2"],
    mode="auto",
)
def line_d_f3(dependent_fields: dict[str, Any]) -> AssignerResult:
    d_f1 = int(dependent_fields["d_f1"])
    d_f2 = int(dependent_fields["d_f2"])
    return AssignerResult(assigned_fields={"d_f3": d_f1 + d_f2})


@assigner(
    assigned_fields=["d_f4"],
    dependent_fields=["d_f3"],
    mode="auto",
)
def line_d_f4(dependent_fields: dict[str, Any]) -> AssignerResult:
    d_f3 = int(dependent_fields["d_f3"])
    return AssignerResult(assigned_fields={"d_f4": d_f3 * 3})


@assigner(
    assigned_fields=["d_f5"],
    dependent_fields=["d_f4"],
    mode="auto",
)
def line_d_f5(dependent_fields: dict[str, Any]) -> AssignerResult:
    d_f4 = int(dependent_fields["d_f4"])
    return AssignerResult(assigned_fields={"d_f5": d_f4 * d_f4})


@assigner(
    assigned_fields=["e_f3"],
    dependent_fields=["e_f1", "e_f2"],
    mode="auto_first",
)
def line_e_auto_first_missing_defaults(
    dependent_fields: dict[str, Any],
) -> AssignerResult:
    e_f1 = int(dependent_fields["e_f1"])
    e_f2 = int(dependent_fields["e_f2"])
    return AssignerResult(assigned_fields={"e_f3": e_f1 + e_f2})
