"""Regenerate the domain_resource_index in schemas.json from audit_full.json.

Matches expanded domain keywords against resource_name and dataset_name fields
in the local audit data — no API calls needed. Extracts governorates using the
4-tier strategy from schema_registry. Completes in under 5 seconds.

Usage:
    python scripts/regenerate_domains.py
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

# Add src/ to path so we can import tanitdata
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from tanitdata.schema_registry import extract_governorate

# ---------------------------------------------------------------------------
# Expanded domain keyword vocabulary
# ---------------------------------------------------------------------------
# Keywords are matched case-insensitively against dataset_name (slug, with
# hyphens replaced by spaces) and resource_name. A resource matches a domain
# if ANY keyword appears as a substring.

DOMAIN_KEYWORDS: dict[str, list[str]] = {
    "climate_stations": [
        "station climatique",
        "station meteo",
        "capteur environnement",
        "capteur pluviometrie",
        "capteur humidite sol",
        "capteur mouillage foliaire",
        "fieldclimate",
        "dgacta station",
        "station kebili avfa",
    ],
    "rainfall": [
        "pluviometrie",
        "precipitation",
        "pluie enregistree",
        "donnees climatiques gouvernorat",
        "stations pluviometriques",
    ],
    "dams": [
        "barrage",
        "barrages collinaires",
        "lacs collinaires",
        "situation hydraulique",
        "retenue collinaire",
        "lacs et retenues collinaires",
    ],
    "crop_production": [
        "production des cereales",
        "production des olives",
        "arboriculture fruitiere",
        "cultures fourrageres",
        "estimation de la production",
        "production vegetale",
        "cultures annuelles",
        "cultures irriguees",
        "cultures pluviales",
        "exploitation des terres agricoles",
        "arbres fruitiers",
        "cultures maraicheres",
        "cultures biologiques",
        "stockage des cereales",
        "superficie des cultures",
        "grandes cultures",
        "cultures industrielles",
        "campagne agricole",
        "cultures hivernales",
        "cultures arriere saison",
        "production arboricole",
        "superficie agricole",
        "superficie ensemencee",
        "collecte des cereales",
        "terres cultivables",
        "production et superficie",
        "repartition de la production",
        "cultures de saison",
        # Land use and fruit trees
        "repartition des terres agricoles",
        "terres agricoles selon la vocation",
        "exploitation des terres",
        "superficie des terres agricoles",
        "superficies emblavees",
        "cereales irrigues",
        "ventes mensuelles des cereales",
        "production du ble",
        "production du triticale",
        "superficies pommiers",
        "production du pommier",
        "production du poirier",
        "superficies des grenadiers",
        "production des grenadiers",
        "superficie des agrumes",
        "production des agrumes",
        "superficies oasis",
        "arboriculture en sec",
        "arboriculture en irrigue",
        "superficies arboricoles",
        "cultures sous serre",
        "cultures primeurs",
        "cultures estivales",
        "cultures d hiver",
        "cultures d ete",
        "pommes de terre",
        "tomate de saison",
        "production totale des fruits",
        "nouvelles plantations",
        "agriculture biologique",
        "superficies certifiees bio",
        "operateur en agriculture biologique",
        "operateurs biologiques",
        "production de l amande biologique",
        "fourrages ensemences",
        "superficie et propduction des fourrages",
        "repartition des superficies des cereales",
        "foret oleicole",
        "situation fonciere des terres",
        "repartition des exploitations agricoles",
        "production des dattes",
        "pepinieres forestieres",
        "production agrumes",
        "superficies et des productions des cereales",
        "cultures arrieres saison",
        "culture de primeur sous serre",
        "superficies des differentes cultures en mode biologique",
        "surfaces et production de fourrage",
        "superficie des cereales recoltes",
        "production de l amande biologique",
    ],
    "olive_harvest": [
        "recolte olive",
        "recolte d'olive",
        "production olive",
        "huile d'olive",
        "huile d olive",
        "olivier",
        "nombre oliviers",
        "huileries",
        "trituration",
        "campagne oleicole",
        "nombre d huilerie",
        "foret oleicole",
        "exportations des huiles",
    ],
    "prices": [
        "prix des produits agricoles",
        "prix de vente",
        "prix semences",
        "prix fob",
        "prix cereales",
        "mercuriale",
        "cours des produits",
        # Import/input prices
        "prix d achat",
        "prix moyen d achat",
        "prix cf ",
        "prix des engrais",
        "prix des insecticides",
        "prix des fongicides",
        "prix des intrants",
        "prix de mecanisation",
        "prix des plants",
        "prix des aliments",
        "prix historiques",
        "prix moyen de vente des viandes",
        "prix moyen des brebis",
        "prix moyen des genisses",
        "prix moyen des vaches",
        "alimentation animale",
    ],
    "fisheries": [
        "peche",
        "aquaculture",
        "produits de la peche",
        "flottille",
        "ports de peche",
        "poisson",
        "thon",
        "crevette",
        "exportation peche",
        "gens de mer",
        "marins pecheurs",
        "produits halieutiques",
        "certificats de capture",
    ],
    "investments": [
        "investissement agricole",
        "investissements approuves",
        "investissements declares",
        "primes investissement",
        "subventions agricoles",
        "apia",
        "categorie a",
        "projets agricoles",
        "suivi des investissements",
        "suivi des subventions",
        "repartition des subventions",
        "investissements publiques",
    ],
    "livestock": [
        "cheptel",
        "elevage",
        "bovin",
        "ovin",
        "caprin",
        "aviculture",
        "apiculture",
        "volaille",
        "vaches laitieres",
        "insemination",
        "effectif du cheptel",
        "petit elevage",
        "cuniculture",
        # Milk/dairy
        "collecte du lait",
        "collecte de lait",
        "centres de collecte du lait",
        "centres de collecte de lait",
        "quantites de lait",
        "bilan laitier",
        "transformation de lait",
        "stock de lait",
        "centrales laitieres",
        "prix minimum garantie du litre lait",
        # Meat
        "production animale",
        "viande rouge",
        "viande de dinde",
        "production de miel",
        "production des oeufs",
        # Poultry
        "poulaillers",
        "secteur avicole",
        "activite cunicole",
        "activite apicole",
        "aliment compose",
        "abattoir",
        "centres frigorifiques",
        "production des produits d origine animale",
        "prix moyen des vaches suitees",
        "prix moyens des vaches suitees",
        "stockage frigorifique",
    ],
    "water_resources": [
        "nappe phreatique",
        "nappe profonde",
        "ressources en eau",
        "eau potable",
        "alimentation en eau",
        "forage",
        "puits",
        "perimetre irrigue",
        "perimetres publics irrigues",
        "reseau d'assainissement",
        "gda eau",
        "sonede",
        "eaux usees traitees",
        # Irrigation
        "perimetres irrigues",
        "superficies irrigues",
        "superficies irrigables",
        "superficies irriguees",
        "eau d irrigation",
        "consommation de l eau",
        "stress hydrique",
        "conservation des eaux",
        "ressources hydrauliques",
        "tarif de vente du m3 d eau",
        "consommation energetique de la secadenord",
        "associations d interets collectifs eau",
        "exploitation de la nappes",
        "gda des ppi",
        "nombre d abonnes",
        "analyses eau sol",
    ],
    "trade_exports": [
        "exportation des agrumes",
        "export des agrumes",
        "exportations des dattes",
        "exportations des amandes",
        "exportations des huiles",
        "exportations biologiques",
        "produits agricoles exportes",
        "produits agricoles importes",
        "importation des cereales",
        "importations du tourteau",
        "importations de mais",
        "consommation moyenne mensuelle du mais",
        "tourteau de soja",
        "balance commerciale alimentaire",
        "valeur ajoutee agricole",
    ],
    "documentation": [
        "fond documentaire",
        "base de documentation",
        "documents et livres",
        "bibliotheque",
        "legislation agricole",
        "textes reglementaires",
    ],
}


def _normalize(text: str) -> str:
    """Lowercase and strip accents for matching."""
    import unicodedata
    text = text.lower().replace("-", " ")
    # Strip combining accents
    nfkd = unicodedata.normalize("NFKD", text)
    return "".join(c for c in nfkd if not unicodedata.combining(c))


def matches_domain(dataset_name: str, resource_name: str, dataset_title: str, dataset_notes: str, keywords: list[str]) -> bool:
    """Check if a resource matches any keyword in a domain."""
    # Build search text from dataset slug (hyphens→spaces), resource name, title, notes
    search_text = _normalize(f"{dataset_name} {resource_name} {dataset_title} {dataset_notes}")
    for kw in keywords:
        if kw in search_text:
            return True
    return False


def main():
    root = Path(__file__).resolve().parent.parent
    audit_path = root / "audit_full.json"
    schemas_path = root / "schemas.json"

    print("Loading audit_full.json...", flush=True)
    with open(audit_path, encoding="utf-8") as f:
        audit = json.load(f)

    print("Loading schemas.json...", flush=True)
    with open(schemas_path, encoding="utf-8") as f:
        schemas = json.load(f)

    # Build lookup: dataset_name → {organization, organization_title, title, notes}
    dataset_info: dict[str, dict] = {}
    for ds in audit["datasets"]:
        dataset_info[ds["name"]] = {
            "organization": ds.get("organization", ""),
            "organization_title": ds.get("organization_title", ""),
            "title": ds.get("title", ""),
            "notes": ds.get("notes", ""),
        }

    # Build lookup: resource_id → schema from datastore_schemas
    ds_schemas = audit["datastore_schemas"]  # dict[rid, {fields, total_records, ...}]

    # Build resource lookup: resource_id → {dataset_name, name, datastore_active}
    resource_lookup: dict[str, dict] = {}
    for r in audit["resources"]:
        if r.get("datastore_active"):
            resource_lookup[r["id"]] = r

    print(f"  {len(resource_lookup)} DataStore-active resources, {len(ds_schemas)} schemas")

    # Save old counts
    old_index = schemas.get("domain_resource_index", {})
    old_counts = {d: dd.get("count", 0) for d, dd in old_index.items()}

    new_index: dict[str, dict] = {}
    domain_assignments: dict[str, set[str]] = {}
    domain_coverage: dict[str, dict[str, int]] = {}

    for domain, keywords in DOMAIN_KEYWORDS.items():
        matched: dict[str, dict] = {}

        for rid, schema in ds_schemas.items():
            dataset_name = schema.get("dataset_name", "")
            resource_name = schema.get("resource_name", "")
            ds_info = dataset_info.get(dataset_name, {})
            title = ds_info.get("title", "")
            notes = ds_info.get("notes", "")

            if matches_domain(dataset_name, resource_name, title, notes, keywords):
                fields = [
                    (f.get("name") or f.get("id", "")) if isinstance(f, dict) else f
                    for f in schema.get("fields", [])
                    if ((f.get("name") or f.get("id", "")) if isinstance(f, dict) else f)
                    not in ("_id", "_full_text", "")
                ]
                matched[rid] = {
                    "id": rid,
                    "name": resource_name,
                    "dataset": dataset_name,
                    "dataset_title": title,
                    "organization": ds_info.get("organization", ""),
                    "organization_title": (ds_info.get("organization_title") or "").strip(),
                    "records": schema.get("total_records", 0),
                    "fields": fields,
                }

        # Compute governorate coverage
        coverage: dict[str, int] = {}
        for rid, r in matched.items():
            ds_info = dataset_info.get(r["dataset"], {})
            org_slug = ds_info.get("organization", "")
            gov = extract_governorate(
                org_slug=org_slug,
                dataset_slug=r["dataset"],
                resource_name=r["name"],
            )
            if gov and gov != "national":
                coverage[gov] = coverage.get(gov, 0) + 1

        res_list = sorted(matched.values(), key=lambda r: -r["records"])
        total_records = sum(r["records"] for r in res_list)

        new_index[domain] = {
            "count": len(res_list),
            "total_records": total_records,
            "resources": res_list,
        }
        domain_assignments[domain] = set(matched.keys())
        domain_coverage[domain] = coverage

        print(f"  {domain:25s}  {len(matched):4d} resources  {len(coverage):2d}/24 governorates")

    # Cross-domain overlaps
    print(f"\nCross-domain overlaps:")
    domains = list(domain_assignments.keys())
    for i, d1 in enumerate(domains):
        for d2 in domains[i + 1:]:
            overlap = domain_assignments[d1] & domain_assignments[d2]
            if overlap:
                print(f"  {d1} & {d2}: {len(overlap)}")

    # Before/after comparison
    print(f"\n{'Domain':25s} {'Before':>8s} {'After':>8s} {'Delta':>8s} {'Govs':>6s}")
    print("-" * 60)
    for domain in DOMAIN_KEYWORDS:
        before = old_counts.get(domain, 0)
        after = new_index[domain]["count"]
        delta = after - before
        govs = len(domain_coverage.get(domain, {}))
        sign = "+" if delta > 0 else ""
        print(f"{domain:25s} {before:8d} {after:8d} {sign}{delta:>7d} {govs:5d}/24")

    # Coverage detail per domain
    print(f"\nGovernorate coverage per domain:")
    for domain in DOMAIN_KEYWORDS:
        cov = domain_coverage.get(domain, {})
        if cov:
            gov_names = sorted(cov.keys())
            print(f"  {domain}: {', '.join(gov_names)}")

    # Write updated schemas.json
    schemas["domain_resource_index"] = new_index
    with open(schemas_path, "w", encoding="utf-8") as f:
        json.dump(schemas, f, ensure_ascii=False, indent=2)
    print(f"\nWrote updated schemas.json")


if __name__ == "__main__":
    main()
