from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable
from urllib import request

from dotenv import load_dotenv


def _norm(s: str) -> str:
    return " ".join((s or "").strip().lower().split())


def _load_deepseek_api_key() -> str | None:
    try:
        env_path = Path(__file__).resolve().parents[2] / ".env"
        load_dotenv(dotenv_path=env_path)
    except Exception:
        pass
    return os.getenv("DEEPSEEK_API_KEY")


def _load_deepseek_base_url() -> str:
    try:
        env_path = Path(__file__).resolve().parents[2] / ".env"
        load_dotenv(dotenv_path=env_path)
    except Exception:
        pass
    base = (os.getenv("DEEPSEEK_BASE_URL") or "https://api.deepseek.com").strip()
    # Accept both https://api.deepseek.com and https://api.deepseek.com/v1
    return base.rstrip("/")


def _load_deepseek_model() -> str:
    try:
        env_path = Path(__file__).resolve().parents[2] / ".env"
        load_dotenv(dotenv_path=env_path)
    except Exception:
        pass
    return (os.getenv("DEEPSEEK_MODEL") or "deepseek-chat").strip()


def _extract_json_object(text: str) -> str | None:
    if not text:
        return None
    s = text.strip()
    if s.startswith("{") and s.endswith("}"):
        return s
    a = s.find("{")
    b = s.rfind("}")
    if a >= 0 and b > a:
        return s[a : b + 1]
    return None


def deepseek_map_leagues(
    league_names: list[str],
    excluded_competitions: list[str],
    *,
    base_url: str | None = None,
    model: str | None = None,
    timeout_s: float = 35.0,
    api_key: str | None = None,
    logger: logging.Logger | None = None,
) -> dict[str, str]:
    """
    Ask DeepSeek to map SportyBet league names -> excluded competition name (from the provided list) or "NONE".
    Returns a dict keyed by the *original* league strings.
    """
    log = logger or logging.getLogger(__name__)
    api_key = api_key or _load_deepseek_api_key()
    if not api_key:
        raise ValueError("DEEPSEEK_API_KEY is missing (set it in .env).")
    base_url = (base_url or _load_deepseek_base_url()).rstrip("/")
    model = (model or _load_deepseek_model()).strip()
    if not league_names:
        return {}

    payload = {
        "model": model,
        "stream": False,
        "messages": [
            {
                "role": "system",
                "content": (
                    "You are a strict JSON-only mapper. "
                    "Given SportyBet football league/competition names, map each to one of the excluded competitions "
                    "provided by the user, or NONE if it does not belong to any excluded competition."
                ),
            },
            {
                "role": "user",
                "content": (
                    "Excluded competitions list:\n"
                    + json.dumps(excluded_competitions, ensure_ascii=False)
                    + "\n\nSportyBet league names to map (return a JSON object where keys are EXACTLY these strings):\n"
                    + json.dumps(league_names, ensure_ascii=False)
                    + "\n\nRules:\n"
                    "- Output MUST be a single JSON object.\n"
                    "- For each league name, value must be EXACTLY one excluded competition string from the list, or \"NONE\".\n"
                    "- Do not include explanations.\n"
                ),
            },
        ],
    }

    # OpenAI-compatible endpoint is /chat/completions, optionally under /v1.
    endpoint = f"{base_url}/chat/completions"
    req = request.Request(
        url=endpoint,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}",
        },
        method="POST",
    )

    log.info(
        "[League mapping] DeepSeek request: model=%s base_url=%s leagues=%d excluded_list=%d",
        model,
        base_url,
        len(league_names),
        len(excluded_competitions),
    )
    with request.urlopen(req, timeout=float(timeout_s)) as resp:
        raw = resp.read().decode("utf-8", errors="replace")
    log.debug("[League mapping] DeepSeek raw response (first 800 chars): %r", raw[:800])
    data = json.loads(raw)
    content = (
        (((data.get("choices") or [{}])[0]).get("message") or {}).get("content") or ""
    )
    log.debug(
        "[League mapping] DeepSeek message content (first 800 chars): %r", content[:800]
    )
    js = _extract_json_object(content)
    if not js:
        log.debug("DeepSeek raw content: %r", content[:400])
        raise ValueError("DeepSeek response did not contain a JSON object.")
    out = json.loads(js)
    if not isinstance(out, dict):
        raise ValueError("DeepSeek JSON output is not an object/dict.")
    mapped: dict[str, str] = {}
    allowed = set(excluded_competitions) | {"NONE"}
    for k in league_names:
        v = out.get(k, "NONE")
        if not isinstance(v, str) or v not in allowed:
            v = "NONE"
        mapped[k] = v
    log.info("[League mapping] DeepSeek mapped %d leagues.", len(mapped))
    log.debug("[League mapping] DeepSeek mapping result: %s", mapped)
    return mapped


@dataclass
class LeagueFilter:
    cache_path: str
    exclude_list: list[str]
    deepseek_enabled: bool = False
    deepseek_base_url: str | None = None
    deepseek_model: str | None = None
    deepseek_timeout_s: float = 35.0
    logger: logging.Logger | None = None
    _cache: dict[str, str] | None = None  # league_norm -> matched_exclusion OR "NONE"

    def load(self) -> None:
        self._cache = {}
        try:
            if os.path.exists(self.cache_path):
                with open(self.cache_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    if isinstance(data, dict):
                        self._cache = {str(k): str(v) for k, v in data.items()}
        except Exception:
            # Corrupt cache: start fresh
            self._cache = {}

    def save(self) -> None:
        if self._cache is None:
            return
        os.makedirs(os.path.dirname(self.cache_path), exist_ok=True)
        tmp = self.cache_path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(self._cache, f, ensure_ascii=False, indent=2)
        os.replace(tmp, self.cache_path)

    def update_mappings(self, leagues: Iterable[str]) -> int:
        """
        For leagues not in cache, ask DeepSeek to map them and persist results.
        Returns number of newly cached leagues.
        """
        if self._cache is None:
            self.load()
        if self._cache is None:
            self._cache = {}

        uniq: list[str] = []
        for x in leagues:
            x = (x or "").strip()
            if not x:
                continue
            nx = _norm(x)
            if not nx:
                continue
            if nx in self._cache:
                continue
            uniq.append(x)

        if not uniq:
            return 0

        log = self.logger or logging.getLogger(__name__)
        log.info(
            "[League mapping] Found %d new league names to map (cache miss).",
            len(uniq),
        )
        if not self.deepseek_enabled:
            for x in uniq:
                self._cache[_norm(x)] = "NONE"
            self.save()
            log.debug("League mapping: DeepSeek disabled; cached %d leagues as NONE.", len(uniq))
            return len(uniq)

        try:
            mapped = deepseek_map_leagues(
                uniq,
                self.exclude_list,
                base_url=self.deepseek_base_url,
                model=self.deepseek_model,
                timeout_s=self.deepseek_timeout_s,
                logger=log,
            )
        except Exception as e:
            log.warning(
                "[League mapping] DeepSeek mapping failed (%s). Will treat unknown leagues as NONE for now.",
                type(e).__name__,
            )
            for x in uniq:
                self._cache[_norm(x)] = "NONE"
            self.save()
            return len(uniq)

        for k, v in mapped.items():
            self._cache[_norm(k)] = v
        self.save()
        log.info("[League mapping] Cached %d new league mappings via DeepSeek.", len(mapped))
        return len(mapped)

    def should_exclude(self, league_name: str) -> tuple[bool, str]:
        """(exclude?, matched_name_or_NONE). Uses persistent cache keyed by normalized league."""
        if self._cache is None:
            self.load()
        league_n = _norm(league_name)
        if not league_n:
            return False, "NONE"
        hit = self._cache.get(league_n) if self._cache is not None else None
        if hit is not None:
            return (hit != "NONE"), hit
        # Unknown league: don't guess — cache as NONE and let the next update_mappings() batch call map it.
        if self._cache is not None:
            self._cache[league_n] = "NONE"
            self.save()
        return False, "NONE"

