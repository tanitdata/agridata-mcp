"""Schema registry — two-layer design.

Static layer  (loaded once from schemas.json, never changes at runtime):
  - domain_resource_index  — curated domain knowledge
  - clusters               — shared schema annotations
  - arabic_field_decoding  — mojibake table

Live layer (seeded from static at startup, refreshed every 6 hours from the portal):
  - actual DataStore-active resource inventory
  - field names and record counts per resource
  - dataset → organization mapping (fetched from portal)
  - coverage_by_gouvernorat per domain
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import unicodedata
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from tanitdata.ckan_client import CKANClient

logger = logging.getLogger(__name__)


def _fold(s: str) -> str:
    """Accent- and case-insensitive ASCII fold for tolerant name matching.

    `Délégation` → `delegation`, `BLÉ DUR` → `ble dur`. Whitespace is
    preserved so multi-word column names still collide on the same key.
    """
    nfkd = unicodedata.normalize("NFKD", s).lower()
    return "".join(c for c in nfkd if not unicodedata.combining(c))

# ---------------------------------------------------------------------------
# Governorate normalisation tables
# ---------------------------------------------------------------------------

# lower-case key → canonical French name
GOVERNORATE_MAP: dict[str, str] = {
    "tunis": "Tunis",
    "ariana": "Ariana",
    "ben arous": "Ben Arous",
    "manouba": "Manouba",
    "nabeul": "Nabeul",
    "zaghouan": "Zaghouan",
    "bizerte": "Bizerte",
    "beja": "Béja",
    "béja": "Béja",
    "jendouba": "Jendouba",
    "kef": "Le Kef",
    "le kef": "Le Kef",
    "siliana": "Siliana",
    "sousse": "Sousse",
    "monastir": "Monastir",
    "mahdia": "Mahdia",
    "sfax": "Sfax",
    "kairouan": "Kairouan",
    "kasserine": "Kasserine",
    "sidi bouzid": "Sidi Bouzid",
    "gabes": "Gabès",
    "gabès": "Gabès",
    "medenine": "Médenine",
    "médenine": "Médenine",
    "tataouine": "Tataouine",
    "gafsa": "Gafsa",
    "tozeur": "Tozeur",
    "kebili": "Kébili",
    "kébili": "Kébili",
}

# CRDA organisation slug → canonical governorate name
CRDA_SLUG_MAP: dict[str, str] = {
    "crda-tunis": "Tunis",
    "crda-ariana": "Ariana",
    "crda-ben-arous": "Ben Arous",
    "crda-manouba": "Manouba",
    "crda-nabeul": "Nabeul",
    "crda-zaghouan": "Zaghouan",
    "crda-bizerte": "Bizerte",
    "crda-beja": "Béja",
    "crda-jendouba": "Jendouba",
    "crda-kef": "Le Kef",
    "crda-siliana": "Siliana",
    "crda-sousse": "Sousse",
    "crda-monastir": "Monastir",
    "crda-mahdia": "Mahdia",
    "crda-sfax": "Sfax",
    "crda-kairouan": "Kairouan",
    "crda-kasserine": "Kasserine",
    "crda-sidi-bouzid": "Sidi Bouzid",
    "crda-gabes": "Gabès",
    "crda-medenine": "Médenine",
    "crda-tataouine": "Tataouine",
    "crda-gafsa": "Gafsa",
    "crda-tozeur": "Tozeur",
    "crda-kebili": "Kébili",
}

# National-level organisations — datasets from these are country-wide
NATIONAL_ORGS: set[str] = {
    # Short slugs
    "dgpa", "dgeda", "dgre", "dgf", "dgab", "dgacta", "dgbo", "dgfiop",
    "onagri", "onagri-dept",
    "oep", "otd", "inp", "apia", "utap", "sonede",
    "office-des-cereales", "secadenord",
    # Long-form slugs (as seen on the portal)
    "direction-generale-de-la-production-agricole",
    "direction-generale-de-la-peche-et-l-aquaculture",
    "direction-generale-du-genie-rural",
    "agence-de-promotion-des-investissements-agricoles",
    "office-national-de-l-huile",
}

# Regex for "gouvernorat-de-{name}" or "gouvernorat-{name}" in dataset slugs
_SLUG_GOV_RE = [
    re.compile(r"gouvernorat-de-([\w-]+)"),
    re.compile(r"gouvernorat-([\w-]+)"),
]

# Regex for "Gouvernorat de {Name}" in display names
_NAME_GOV_RE = [
    re.compile(r"[Gg]ouvernorat\s+de\s+([\w\s\-\u00C0-\u024F]+)"),
    re.compile(r"[Gg]ouvernorat\s+([\w\s\-\u00C0-\u024F]+)"),
]

# Short governorate keys that are distinctive enough to allow in bare-name matching
_SAFE_SHORT_KEYS: set[str] = {"kef"}

# Locality names (delegations, towns, station sites) → canonical governorate.
# Used as a final fallback for resources from national orgs where the governorate
# isn't named directly but a well-known sub-location is.
LOCALITY_MAP: dict[str, str] = {
    # Climate station sites
    "bir ben kemla": "Mahdia",
    "bouhertma": "Jendouba",
    "ghezala": "Bizerte",
    "fretissa": "Bizerte",
    "mateur": "Bizerte",
    "sbikha": "Kairouan",
    "nmeyria": "Siliana",
    "oued sbaihia": "Zaghouan",
    "tibar": "Béja",
    "el ksour": "Le Kef",
    "foussena": "Kasserine",
    "bne khaled": "Nabeul",
    "bouchrik": "Nabeul",
    "rayhana": "Sidi Bouzid",
    # Delegations that appear frequently in resource names
    "korba": "Nabeul",
    "grombalia": "Nabeul",
    "menzel temime": "Nabeul",
    "bou argoub": "Nabeul",
    "tabarka": "Jendouba",
    "ain draham": "Jendouba",
    "bou salem": "Jendouba",
    "hammam bourguiba": "Jendouba",
    "testour": "Béja",
    "nefza": "Béja",
    "medjez el bab": "Béja",
    "dahmani": "Le Kef",
    "bargou": "Siliana",
    "maktar": "Siliana",
    "thala": "Kasserine",
    "sbeitla": "Kasserine",
    "el ayoun": "Kasserine",
    "enfida": "Sousse",
    "msaken": "Sousse",
    "ksar hellal": "Monastir",
    "moknine": "Monastir",
    "el jem": "Mahdia",
    "chebba": "Mahdia",
    "ksour essaf": "Mahdia",
    "skhira": "Sfax",
    "el hencha": "Sfax",
    "haffouz": "Kairouan",
    "sidi bou ali": "Sousse",
    "menzel bourguiba": "Bizerte",
    "ras jebel": "Bizerte",
    "nefta": "Tozeur",
    "douz": "Kébili",
    "souk lahad": "Kébili",
    "zarzis": "Médenine",
    "djerba": "Médenine",
    "ben gardane": "Médenine",
    "ghomrassen": "Tataouine",
    "el hamma": "Gabès",
    "mareth": "Gabès",
    "metlaoui": "Gafsa",
    "redeyef": "Gafsa",
    "oum el araies": "Gafsa",
}


def _gov_from_org_slug(org_slug: str) -> str | None:
    """Primary: extract governorate from organisation slug (e.g. crda-beja → Béja)."""
    if org_slug in CRDA_SLUG_MAP:
        return CRDA_SLUG_MAP[org_slug]
    if org_slug in NATIONAL_ORGS:
        return "national"
    return None


def _gov_from_dataset_slug(slug: str) -> str | None:
    """Secondary: extract governorate from dataset name slug."""
    for pat in _SLUG_GOV_RE:
        m = pat.search(slug)
        if m:
            raw = m.group(1).replace("-", " ").strip().lower()
            if raw in GOVERNORATE_MAP:
                return GOVERNORATE_MAP[raw]
            # prefix match
            for key, val in GOVERNORATE_MAP.items():
                if raw.startswith(key) or key.startswith(raw):
                    return val
    return None


def _gov_from_display_name(name: str) -> str | None:
    """Tertiary: extract governorate from resource/dataset display name."""
    for pat in _NAME_GOV_RE:
        m = pat.search(name)
        if m:
            raw = m.group(1).strip().rstrip(")-.,").strip().lower()
            if raw in GOVERNORATE_MAP:
                return GOVERNORATE_MAP[raw]
            for key, val in GOVERNORATE_MAP.items():
                if raw.startswith(key) or key.startswith(raw):
                    return val
    return None


def _gov_from_bare_name(*texts: str) -> str | None:
    """Match a bare governorate name as a word in any text.

    Skips short keys (< 4 chars) except those in _SAFE_SHORT_KEYS
    to reduce false positives while still matching "kef" → Le Kef.
    """
    for text in texts:
        if not text:
            continue
        lower = text.lower()
        # Try longer keys first to prefer "sidi bouzid" over "sidi" or "ben arous" over "ben"
        for key, val in sorted(GOVERNORATE_MAP.items(), key=lambda kv: -len(kv[0])):
            if len(key) < 4 and key not in _SAFE_SHORT_KEYS:
                continue
            if re.search(r"\b" + re.escape(key) + r"\b", lower):
                return val
    return None


def _gov_from_locality(*texts: str) -> str | None:
    """Match a known locality/delegation name to its parent governorate.

    Used as a final fallback for national-org resources where the
    governorate itself isn't named but a sub-location is.
    """
    for text in texts:
        if not text:
            continue
        lower = text.lower()
        # Try longer locality names first to avoid partial matches
        for loc, gov in sorted(LOCALITY_MAP.items(), key=lambda kv: -len(kv[0])):
            if re.search(r"\b" + re.escape(loc) + r"\b", lower):
                return gov
    return None


def extract_governorate(
    org_slug: str = "",
    dataset_slug: str = "",
    resource_name: str = "",
) -> str | None:
    """Extract governorate using a 5-tier strategy.

    1. Organization slug  (crda-beja → Béja)  — definitive for CRDAs
    2. Dataset name slug  (gouvernorat-de-sfax → Sfax)
    3. Resource display name  ("Gouvernorat de Kairouan" → Kairouan)
    4. Bare governorate name in any text  ("Station Mahdia..." → Mahdia)
    5. Locality/delegation name  ("GDA Bir Ben Kemla" → Mahdia)

    For national organisations (DGACTA, DGGREE, etc.), tier 1 is skipped
    and tiers 2–5 are tried.  "national" is only returned as a fallback
    when no specific governorate can be extracted from any tier.
    """
    is_national_org = False
    if org_slug:
        gov = _gov_from_org_slug(org_slug)
        if gov and gov != "national":
            return gov  # CRDA match — definitive
        if gov == "national":
            is_national_org = True
    if dataset_slug:
        gov = _gov_from_dataset_slug(dataset_slug)
        if gov:
            return gov
    if resource_name:
        gov = _gov_from_display_name(resource_name)
        if gov:
            return gov
    # Bare governorate name match across all available text
    slug_text = dataset_slug.replace("-", " ") if dataset_slug else ""
    gov = _gov_from_bare_name(resource_name, slug_text)
    if gov:
        return gov
    # Locality/delegation fallback
    gov = _gov_from_locality(resource_name, slug_text)
    if gov:
        return gov
    # No specific governorate found — fall back to "national" for national orgs
    # and unknown orgs, None only for known non-CRDA non-national orgs
    if is_national_org or not org_slug or org_slug not in CRDA_SLUG_MAP:
        return "national"
    return None


# ---------------------------------------------------------------------------
# Year extraction from dataset names
# ---------------------------------------------------------------------------

_YEAR_RE = re.compile(r"\b(19\d{2}|20\d{2})\b")


def _extract_years(text: str) -> list[int]:
    """Extract plausible years (1900–2099) from a dataset name or resource name."""
    return [int(m) for m in _YEAR_RE.findall(text)]


# ---------------------------------------------------------------------------
# Live resource record
# ---------------------------------------------------------------------------

class LiveResource:
    """One DataStore resource in the live inventory."""

    __slots__ = ("id", "name", "dataset", "fields", "records")

    def __init__(
        self,
        id: str,
        name: str = "",
        dataset: str = "",
        fields: list[str] | None = None,
        records: int = 0,
    ) -> None:
        self.id = id
        self.name = name
        self.dataset = dataset
        self.fields: list[str] = fields or []
        self.records = records

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "dataset": self.dataset,
            "fields": self.fields,
            "records": self.records,
        }


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

class SchemaRegistry:
    """Two-layer schema registry.

    Static layer  — loaded once from schemas.json, never mutated at runtime.
    Live layer    — seeded from the static layer, then refreshed every
                    `refresh_interval_hours` by querying the portal.
    """

    def __init__(
        self,
        schemas_path: str | Path | None = None,
        refresh_interval_hours: float = 6.0,
    ) -> None:
        if schemas_path is None:
            schemas_path = (
                Path(__file__).resolve().parent.parent.parent / "schemas.json"
            )
        self._path = Path(schemas_path)
        self._refresh_interval = timedelta(hours=refresh_interval_hours)

        # Static layer
        self._meta: dict[str, Any] = {}
        self._domain_index: dict[str, dict[str, Any]] = {}
        self._clusters: list[dict[str, Any]] = []
        self._arabic_decoding: dict[str, Any] = {}
        self._static_loaded = False

        # Value hints (per-resource categorical values, loaded from value_hints.json)
        self._value_hints: dict[str, dict[str, list[str]]] = {}

        # Live layer
        self._live: dict[str, LiveResource] = {}
        # dataset_slug → organisation slug (populated during live refresh)
        self._dataset_orgs: dict[str, str] = {}
        # dataset_slug → {title, org_slug, org_title} for source attribution
        self._dataset_meta: dict[str, dict[str, str]] = {}
        # domain → { canonical_governorate → [resource_id, ...] }
        self._coverage: dict[str, dict[str, list[str]]] = {}
        self._resource_domains: dict[str, list[str]] = {}
        self._resource_gov: dict[str, str] = {}
        # resource_id → (dataset_slug, resource_name) for O(1) source attribution
        self._resource_to_dataset: dict[str, tuple[str, str]] = {}
        self._last_refreshed: datetime | None = None
        self._refresh_lock = asyncio.Lock()

    # ------------------------------------------------------------------
    # Static layer
    # ------------------------------------------------------------------

    def load(self) -> None:
        """Load static layer from schemas.json. Call once at startup."""
        with open(self._path, encoding="utf-8") as f:
            data = json.load(f)

        self._meta = data.get("meta", {})
        self._domain_index = data.get("domain_resource_index", {})
        self._clusters = data.get("clusters", [])
        self._arabic_decoding = data.get("arabic_field_decoding", {})
        self._static_loaded = True

        # Load value hints (per-resource categorical values) if available
        hints_path = self._path.parent / "value_hints.json"
        if hints_path.exists():
            with open(hints_path, encoding="utf-8") as f:
                hints_data = json.load(f)
            self._value_hints = hints_data.get("resource_values", {})
            logger.info(
                "Schema registry: value hints loaded (%d resources)",
                len(self._value_hints),
            )

        # Seed the live layer so tools work before the first live refresh
        self._seed_from_static()
        logger.info(
            "Schema registry: static layer loaded (%d domains, %d clusters)",
            len(self._domain_index),
            len(self._clusters),
        )

    def _seed_from_static(self) -> None:
        """Populate the live layer from the static registry as a baseline.

        Sets _last_refreshed so the first tool call doesn't immediately block
        on a network request — the live refresh runs as a background task instead.
        """
        for domain_data in self._domain_index.values():
            for res in domain_data.get("resources", []):
                rid = res["id"]
                if rid not in self._live:
                    self._live[rid] = LiveResource(
                        id=rid,
                        name=res.get("name", ""),
                        dataset=res.get("dataset", ""),
                        fields=list(res.get("fields", [])),
                        records=res.get("records", 0),
                    )
                # Seed dataset metadata for source attribution
                ds_slug = res.get("dataset", "")
                if ds_slug and ds_slug not in self._dataset_meta:
                    self._dataset_meta[ds_slug] = {
                        "title": res.get("dataset_title", ""),
                        "org_slug": res.get("organization", ""),
                        "org_title": res.get("organization_title", ""),
                    }
                # O(1) lookup for get_source_attribution
                if rid not in self._resource_to_dataset:
                    self._resource_to_dataset[rid] = (ds_slug, res.get("name", ""))
        for cluster in self._clusters:
            for res in cluster.get("resources", []):
                rid = res["id"]
                if rid not in self._live:
                    self._live[rid] = LiveResource(
                        id=rid,
                        name=res.get("name", ""),
                        dataset=res.get("dataset", ""),
                        fields=list(cluster["fields"]),
                        records=res.get("records", 0),
                    )
        # Best-effort coverage from names/slugs only (no org data yet)
        self._compute_coverage()
        # Mark as "refreshed now" so maybe_refresh doesn't immediately block
        # the first tool call on a network request. The server's lifespan
        # starts a background task for the real live refresh.
        self._last_refreshed = datetime.now(tz=timezone.utc)

    # ------------------------------------------------------------------
    # Live layer
    # ------------------------------------------------------------------

    async def maybe_refresh(self, client: "CKANClient") -> None:
        """Refresh the live inventory if the refresh interval has elapsed.

        Designed to be called at the start of every tool invocation.
        Most calls just check a timestamp and return instantly (~0 µs).
        """
        now = datetime.now(tz=timezone.utc)
        if (
            self._last_refreshed is not None
            and now - self._last_refreshed < self._refresh_interval
        ):
            return

        async with self._refresh_lock:
            # Re-check after acquiring the lock (another coroutine may have refreshed)
            now = datetime.now(tz=timezone.utc)
            if (
                self._last_refreshed is not None
                and now - self._last_refreshed < self._refresh_interval
            ):
                return
            await self._refresh(client)

    async def _refresh(self, client: "CKANClient") -> None:
        """Fetch the live DataStore inventory and dataset→org mapping."""
        logger.info("Schema registry: starting live refresh…")
        try:
            # --- Step 1: fetch DataStore-active resource IDs ---
            result = await client.datastore_search(
                resource_id="_table_metadata",
                limit=5000,
            )
            if not result:
                logger.warning(
                    "Schema registry: _table_metadata returned no data — keeping existing inventory"
                )
                self._last_refreshed = datetime.now(tz=timezone.utc)
                return

            records = result.get("records", [])
            live_ids: set[str] = set()
            for rec in records:
                rid = rec.get("name") or rec.get("_id")
                if rid and rid != "_table_metadata" and not rec.get("alias_of"):
                    live_ids.add(str(rid))

            # Fetch schemas for resources not yet known
            new_ids = [
                rid
                for rid in live_ids
                if rid not in self._live or not self._live[rid].fields
            ]
            logger.info(
                "Schema registry: %d active resources, %d need schema fetch",
                len(live_ids),
                len(new_ids),
            )

            for rid in new_ids:
                try:
                    sr = await client.datastore_search(resource_id=rid, limit=0)
                    if sr:
                        fields = [
                            f["id"]
                            for f in sr.get("fields", [])
                            if f.get("id") not in ("_id", "_full_text")
                        ]
                        total = sr.get("total", 0)
                        if rid in self._live:
                            self._live[rid].fields = fields
                            self._live[rid].records = total
                        else:
                            self._live[rid] = LiveResource(
                                id=rid, fields=fields, records=total
                            )
                except Exception as exc:
                    logger.debug(
                        "Schema registry: schema fetch failed for %s: %s", rid, exc
                    )

            # --- Step 2: fetch dataset → organisation mapping ---
            await self._fetch_dataset_orgs(client)

            # --- Step 3: recompute coverage with org data ---
            self._compute_coverage()
            self._last_refreshed = datetime.now(tz=timezone.utc)
            logger.info(
                "Schema registry: live refresh complete (%d resources, %d dataset→org mappings)",
                len(self._live),
                len(self._dataset_orgs),
            )

        except Exception as exc:
            logger.error("Schema registry: live refresh failed: %s", exc)
            self._last_refreshed = datetime.now(tz=timezone.utc)

    async def _fetch_dataset_orgs(self, client: "CKANClient") -> None:
        """Fetch all datasets from the portal to build dataset_slug → org_slug mapping.

        Uses paginated package_search (100 per page, ~12 calls for 1102 datasets).
        """
        self._dataset_orgs = {}
        offset = 0
        page_size = 100
        while True:
            result = await client.package_search(
                query="", rows=page_size, start=offset
            )
            if not result:
                break
            datasets = result.get("results", [])
            if not datasets:
                break
            for ds in datasets:
                slug = ds.get("name", "")
                org = ds.get("organization")
                if slug and org and isinstance(org, dict):
                    org_slug = org.get("name", "")
                    self._dataset_orgs[slug] = org_slug
                    self._dataset_meta[slug] = {
                        "title": ds.get("title", ""),
                        "org_slug": org_slug,
                        "org_title": org.get("title", ""),
                    }
            offset += page_size
            total = result.get("count", 0)
            if offset >= total:
                break
        logger.info(
            "Schema registry: fetched %d dataset→org mappings", len(self._dataset_orgs)
        )

    def _compute_coverage(self) -> None:
        """Build coverage_by_gouvernorat for each domain.

        Also builds reverse lookups: resource → domains, resource → governorate.

        Uses a 4-tier extraction strategy:
          1. Organisation slug  (crda-beja → Béja, dgpa → national)
          2. Dataset name slug  (gouvernorat-de-sfax → Sfax)
          3. Resource display name  ("Gouvernorat de Kairouan" → Kairouan)
          4. Bare governorate name in any text
        """
        self._coverage = {}
        self._resource_domains = {}
        self._resource_gov = {}
        for domain, domain_data in self._domain_index.items():
            coverage: dict[str, list[str]] = {}
            for res in domain_data.get("resources", []):
                rid = res["id"]
                dataset_slug = res.get("dataset", "")
                resource_name = res.get("name", "")
                org_slug = self._dataset_orgs.get(dataset_slug, "")

                gov = extract_governorate(
                    org_slug=org_slug,
                    dataset_slug=dataset_slug,
                    resource_name=resource_name,
                )
                if gov:
                    coverage.setdefault(gov, []).append(rid)
                    if rid not in self._resource_gov:
                        self._resource_gov[rid] = gov
                self._resource_domains.setdefault(rid, []).append(domain)
            self._coverage[domain] = coverage

    # ------------------------------------------------------------------
    # Reverse lookups (built during _compute_coverage)
    # ------------------------------------------------------------------

    # ------------------------------------------------------------------
    # Query interface
    # ------------------------------------------------------------------

    def get_domain_resources(
        self,
        domain: str,
        gouvernorat: str | None = None,
    ) -> list[dict[str, Any]]:
        """Return resources for a domain, optionally filtered by governorate.

        Returns a list of dicts: {id, name, dataset, fields, records}.
        Enriches static registry entries with live data (updated field lists
        and record counts) when available.
        """
        domain_data = self._domain_index.get(domain, {})
        if not domain_data:
            return []

        all_resources = domain_data.get("resources", [])

        def _enrich(res: dict) -> dict[str, Any]:
            rid = res["id"]
            live = self._live.get(rid)
            return {
                "id": rid,
                "name": res.get("name") or (live.name if live else ""),
                "dataset": res.get("dataset") or (live.dataset if live else ""),
                "fields": (
                    live.fields
                    if live and live.fields
                    else res.get("fields", [])
                ),
                "records": live.records if live else res.get("records", 0),
            }

        if not gouvernorat:
            return [_enrich(res) for res in all_resources]

        # Normalise the requested governorate
        gov_lower = gouvernorat.lower().strip()
        canonical = GOVERNORATE_MAP.get(gov_lower)
        if not canonical:
            for key, val in GOVERNORATE_MAP.items():
                if gov_lower in key or key in gov_lower:
                    canonical = val
                    break
        if not canonical:
            # Also accept "national"
            canonical = gouvernorat

        covered_ids = set(self._coverage.get(domain, {}).get(canonical, []))
        return [_enrich(res) for res in all_resources if res["id"] in covered_ids]

    def get_coverage_summary(self, domain: str) -> dict[str, int]:
        """Return {governorate: resource_count} for a domain, sorted alphabetically."""
        return {
            gov: len(ids)
            for gov, ids in sorted(self._coverage.get(domain, {}).items())
        }

    def get_resource_context(self, resource_id: str) -> dict[str, Any]:
        """Return domain(s) and governorate for a resource, or empty dict if unknown.

        Returns: {"domains": ["crop_production", ...], "gouvernorat": "Béja"}
        """
        domains = self._resource_domains.get(resource_id, [])
        gov = self._resource_gov.get(resource_id)
        if not domains:
            return {}
        result: dict[str, Any] = {"domains": domains}
        if gov:
            result["gouvernorat"] = gov
        return result

    def get_data_availability(
        self,
        domain: str,
        gouvernorat: str | None = None,
    ) -> str:
        """Return a human-readable availability summary for a domain/governorate.

        Summarises:
          - Number of DataStore-active resources
          - Year range (extracted from dataset names)
          - Governorate coverage for the domain
        """
        resources = self.get_domain_resources(domain, gouvernorat=gouvernorat)

        if not resources:
            # Check if the domain exists at all
            if domain not in self._domain_index:
                return f"Unknown domain `{domain}`."
            if gouvernorat:
                # Domain exists but no data for this governorate
                total_domain = self._domain_index[domain].get("count", 0)
                return (
                    f"No DataStore resources for {gouvernorat} in `{domain}`. "
                    f"The domain has {total_domain} resources across other governorates. "
                    f"This governorate may have downloadable files (CSV/XLSX) "
                    f"that were not ingested into the DataStore."
                )
            return f"No DataStore resources for domain `{domain}`."

        # Extract year range from dataset names
        years: set[int] = set()
        for res in resources:
            for y in _extract_years(res.get("dataset", "")):
                years.add(y)
            for y in _extract_years(res.get("name", "")):
                years.add(y)

        total_records = sum(res.get("records", 0) for res in resources)

        parts: list[str] = []
        parts.append(f"{len(resources)} DataStore resource(s)")
        if total_records:
            parts.append(f"{total_records:,} total records")
        if years:
            parts.append(f"{min(years)}–{max(years)}")

        summary = ", ".join(parts)
        if gouvernorat:
            summary = f"{gouvernorat} — {summary}"
        else:
            # Add governorate coverage info
            coverage = self.get_coverage_summary(domain)
            gov_count = len([g for g in coverage if g != "national"])
            summary += f", {gov_count}/24 governorates"

        return summary

    def get_source_attribution(self, resource_id: str) -> dict[str, str] | None:
        """Return source metadata for a DataStore resource.

        Returns a dict with keys: resource_id, resource_name, dataset_name,
        dataset_title, organization, organization_title, portal_url.
        Returns None if the resource is completely unknown.
        """
        live = self._live.get(resource_id)
        dataset_slug = live.dataset if live else ""
        resource_name = live.name if live else ""

        # O(1) fallback from pre-built lookup
        if not dataset_slug:
            cached = self._resource_to_dataset.get(resource_id)
            if cached:
                dataset_slug, fallback_name = cached
                resource_name = resource_name or fallback_name

        if not dataset_slug and not resource_name:
            return None

        meta = self._dataset_meta.get(dataset_slug, {})

        return {
            "resource_id": resource_id,
            "resource_name": resource_name,
            "dataset_name": dataset_slug,
            "dataset_title": meta.get("title", ""),
            "organization": meta.get("org_slug", ""),
            "organization_title": meta.get("org_title", ""),
            "portal_url": (
                f"https://catalog.agridata.tn/dataset/{dataset_slug}"
                if dataset_slug
                else ""
            ),
        }

    # ------------------------------------------------------------------
    # Backwards-compatible interface (used by existing tools)
    # ------------------------------------------------------------------

    def _ensure_loaded(self) -> None:
        if not self._static_loaded:
            self.load()

    @property
    def meta(self) -> dict[str, Any]:
        self._ensure_loaded()
        return self._meta

    @property
    def domains(self) -> list[str]:
        self._ensure_loaded()
        return list(self._domain_index.keys())

    def find_resources_by_domain(self, domain: str) -> dict[str, Any]:
        self._ensure_loaded()
        return self._domain_index.get(domain, {})

    def get_cluster_resources(self, field_pattern: list[str] | set[str]) -> list[dict]:
        self._ensure_loaded()
        target = set(field_pattern)
        results: list[dict] = []
        for cluster in self._clusters:
            if set(cluster["fields"]) == target:
                results.extend(cluster["resources"])
        return results

    def get_resource_schema(self, resource_id: str) -> list[str] | None:
        self._ensure_loaded()
        # Live layer first (most up-to-date)
        live = self._live.get(resource_id)
        if live and live.fields:
            return list(live.fields)
        # Cluster fallback
        for cluster in self._clusters:
            for res in cluster["resources"]:
                if res["id"] == resource_id:
                    return list(cluster["fields"])
        # Domain index fallback
        for domain_data in self._domain_index.values():
            for res in domain_data.get("resources", []):
                if res["id"] == resource_id:
                    return res.get("fields", [])
        return None

    def get_column_hints(
        self,
        resource_id: str,
        column_names: list[str],
    ) -> dict[str, list[str]]:
        """Return known categorical values for columns in a resource.

        Used by query_datastore to append value hints to tool responses,
        so the LLM knows exact strings for follow-up WHERE clauses.

        Matching is exact first, then falls back to an accent-folded
        comparison. CKAN's DataStore strips diacritics from column names
        (`Délégation` → `Delegation`) but some sources (openpyxl parses
        of raw XLSX) preserve them. The fallback makes hint retrieval
        resilient to either spelling. Returned dict is keyed by the
        *requested* column name so tool output stays aligned with the
        actual headers shown to the LLM.
        """
        resource_hints = self._value_hints.get(resource_id, {})
        if not resource_hints:
            return {}

        exact = {
            col: resource_hints[col]
            for col in column_names
            if col in resource_hints
        }
        missing = [c for c in column_names if c not in exact]
        if missing:
            folded_map = {_fold(k): v for k, v in resource_hints.items()}
            for col in missing:
                folded_hits = folded_map.get(_fold(col))
                if folded_hits is not None:
                    exact[col] = folded_hits
        return exact

    def get_arabic_field_mapping(self) -> dict[str, Any]:
        self._ensure_loaded()
        return self._arabic_decoding
