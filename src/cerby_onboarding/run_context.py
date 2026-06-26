"""Shared runtime context for interactive, automated, and service modes."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from cerby_onboarding.running_report import RunningReportWriter
    from cerby_onboarding.run_logging import RunLogger


@dataclass
class ActiveRun:
    logger: RunLogger
    running_report: Optional[RunningReportWriter] = None
    interactive: bool = True

    def log_info(self, message: str, **fields: object) -> None:
        self.logger.info(message, **fields)

    def log_warning(self, message: str, **fields: object) -> None:
        self.logger.warning(message, **fields)

    def log_error(self, message: str, **fields: object) -> None:
        self.logger.error(message, **fields)

    def log_debug(self, message: str, **fields: object) -> None:
        self.logger.debug(message, **fields)

    def record_event(self, message: str, *, level: str = "info", **fields: object) -> None:
        self.log_info(message, **fields)
        if self.running_report is not None:
            self.running_report.record_event(message, level=level, **fields)
