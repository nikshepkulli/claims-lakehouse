"""Orchestration: question -> prompt -> LLM -> guardrails -> sandbox execution -> masked answer.

Every call writes exactly one audit record, whatever the outcome. Statuses:
  answered   SQL passed the guardrails and ran
  rejected   input screen or SQL guardrails said no (reasons attached)
  refused    the model itself declined (REFUSE:)
  error      provider failure, timeout or budget exhausted
"""

from __future__ import annotations

import hashlib
import time
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime

from .audit import AuditLog
from .catalog import Catalog
from .config import GenAIConfig
from .executor import Executor, QueryTimeout
from .guardrails import GuardDecision, check_sql
from .llm import LLMClient, LLMError, LLMResponse, extract_sql
from .masking import mask_rows, scrub
from .prompt import build_system_prompt
from .screen import screen_question

ANSWERED, REJECTED, REFUSED, ERROR = "answered", "rejected", "refused", "error"


@dataclass
class Answer:
    request_id: str
    question: str
    status: str
    stage: str  # screen | budget | llm | model | guardrails | execution | done
    reasons: list[str] = field(default_factory=list)
    generated_sql: str | None = None
    sql: str | None = None
    columns: list[str] = field(default_factory=list)
    rows: list[tuple] = field(default_factory=list)
    truncated: bool = False
    latency_ms: float = 0.0
    decision: GuardDecision | None = None

    @property
    def ok(self) -> bool:
        return self.status == ANSWERED


class GovernedQA:
    def __init__(
        self,
        llm: LLMClient,
        executor: Executor,
        catalog: Catalog,
        cfg: GenAIConfig | None = None,
        audit: AuditLog | None = None,
        user: str = "cli",
    ):
        self.llm, self.executor, self.catalog = llm, executor, catalog
        self.cfg = cfg or GenAIConfig(role=catalog.role)
        self.audit = audit or AuditLog(None)
        self.user = user
        self.queries_used = 0
        self.tokens_used = 0
        self._system = build_system_prompt(catalog, executor.column_types, self.cfg.max_rows)
        self._prompt_hash = hashlib.sha256(self._system.encode()).hexdigest()

    def ask(self, question: str) -> Answer:
        t0 = time.perf_counter()
        ans = Answer(uuid.uuid4().hex, question, ERROR, "screen")
        usage_box = [LLMResponse("", 0, 0, getattr(self.llm, "model_id", "unknown"))]
        try:
            self._run(ans, usage_box)
        finally:
            ans.latency_ms = round((time.perf_counter() - t0) * 1000, 2)
            self._log(ans, usage_box[0])
        return ans

    def _run(self, ans: Answer, usage_box: list[LLMResponse]) -> None:
        q = ans.question
        if not q.strip() or len(q) > self.cfg.max_question_chars:
            ans.status, ans.stage = REJECTED, "screen"
            ans.reasons = [f"question must be 1-{self.cfg.max_question_chars} characters"]
            return
        if self.cfg.screen_questions and (why := screen_question(q)):
            ans.status, ans.stage, ans.reasons = REJECTED, "screen", [why]
            return
        if self.queries_used >= self.cfg.session_max_queries:
            ans.status, ans.stage = ERROR, "budget"
            ans.reasons = [f"session query cap {self.cfg.session_max_queries} reached"]
            return
        if self.tokens_used >= self.cfg.session_max_tokens:
            ans.status, ans.stage = ERROR, "budget"
            ans.reasons = [f"session token cap {self.cfg.session_max_tokens} reached"]
            return

        self.queries_used += 1
        try:
            resp = self.llm.generate(self._system, q)
        except LLMError as exc:
            ans.status, ans.stage, ans.reasons = ERROR, "llm", [str(exc)]
            return
        usage_box[0] = resp
        self.tokens_used += resp.input_tokens + resp.output_tokens

        sql, refusal = extract_sql(resp.text)
        ans.generated_sql = sql
        if refusal is not None:
            ans.status, ans.stage, ans.reasons = REFUSED, "model", [refusal]
            return

        decision = check_sql(sql, self.catalog, self.cfg)
        ans.decision = decision
        if not decision.allowed:
            ans.status, ans.stage, ans.reasons = REJECTED, "guardrails", decision.reasons
            return
        ans.sql = decision.sql
        try:
            res = self.executor.run(decision.sql, self.cfg.timeout_s, self.cfg.max_rows)
        except QueryTimeout as exc:
            ans.status, ans.stage, ans.reasons = ERROR, "execution", [str(exc)]
            return
        except Exception as exc:  # DuckDB binder/runtime errors on valid-looking SQL
            ans.status, ans.stage = ERROR, "execution"
            ans.reasons = [f"execution failed: {type(exc).__name__}: {str(exc)[:160]}"]
            return
        rows, _ = mask_rows(res.rows)
        ans.status, ans.stage = ANSWERED, "done"
        ans.columns, ans.rows, ans.truncated = res.columns, rows, res.truncated

    def _log(self, ans: Answer, usage: LLMResponse) -> None:
        d = ans.decision
        self.audit.write(
            {
                "ts": datetime.now(UTC).isoformat(timespec="milliseconds"),
                "request_id": ans.request_id,
                "user": self.user,
                "role": self.catalog.role,
                "question": scrub(ans.question),
                "model_id": usage.model_id,
                "prompt_sha256": self._prompt_hash,
                "prompt_chars": len(self._system),
                "tokens_in": usage.input_tokens,
                "tokens_out": usage.output_tokens,
                "generated_sql": scrub(ans.generated_sql) if ans.generated_sql else None,
                "executed_sql": ans.sql,
                "decision": {
                    "status": ans.status,
                    "stage": ans.stage,
                    "rules": d.rules if d else [],
                    "reasons": [scrub(r) for r in ans.reasons],
                    "limit_applied": d.limit_applied if d else None,
                    "cost": d.cost if d else None,
                },
                "rows": len(ans.rows),
                "truncated": ans.truncated,
                "latency_ms": ans.latency_ms,
            }
        )
