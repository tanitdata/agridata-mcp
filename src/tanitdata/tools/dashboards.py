"""Dashboard link tool: map topics to interactive dashboards on dashboards.agridata.tn."""

from __future__ import annotations

import unicodedata

_BASE = "https://dashboards.agridata.tn/fr/detail_dashboard"

_DASHBOARDS: list[dict[str, str | list[str]]] = [
    {
        "key": "agriculture_global",
        "title": "Indicateurs agricoles globaux",
        "url": f"{_BASE}/tableau-de-bord-des-indicateurs-agricoles-globaux/",
        "desc": "PIB agricole, valeur ajoutée, balance commerciale alimentaire, investissement global",
        "keywords": [
            "agriculture", "agricole", "agricultural", "secteur agricole",
            "pib", "gdp", "valeur ajoutee", "balance commerciale", "investissement global",
            "indicateurs globaux", "global", "economie agricole", "agricultural economy",
            "economie",
        ],
    },
    {
        "key": "climate",
        "title": "Tableau de bord changement climatique",
        "url": f"{_BASE}/tableau-de-bord-changement-climatique/",
        "desc": "Suivi des indicateurs climatiques : température, précipitations, évapotranspiration",
        "keywords": [
            "climat", "climate", "temperature", "precipitation", "pluviometrie",
            "evapotranspiration", "changement climatique", "climate change", "meteo",
            "meteorologie", "weather",
        ],
    },
    {
        "key": "cereals_annual",
        "title": "Indicateurs annuels des céréales",
        "url": f"{_BASE}/tableau-de-bord-des-indicateurs-annuels-des-cereales/",
        "desc": "Production, superficies, rendements des céréales par campagne agricole",
        "keywords": [
            "cereales", "cereals", "cereal", "ble", "wheat", "orge", "barley",
            "triticale", "avoine", "sorgho", "annuel", "annual", "campagne",
            "production cereales", "cereal production", "rendement",
        ],
    },
    {
        "key": "cereals_monthly",
        "title": "Indicateurs mensuels des céréales",
        "url": f"{_BASE}/tableau-de-bord-des-indicateurs-mensuels-des-cereales/",
        "desc": "Collecte mensuelle, stocks, prix des céréales",
        "keywords": [
            "cereales", "cereals", "cereal", "ble", "wheat", "orge", "barley",
            "mensuel", "monthly", "collecte", "stock",
        ],
    },
    {
        "key": "cereal_prices",
        "title": "Évolution des prix FOB du blé dur selon l'origine",
        "url": f"{_BASE}/evolution-des-prix-fob-du-ble-dur-selon-lorigine/",
        "desc": "Prix FOB du blé dur canadien et français, évolution quotidienne et mensuelle",
        "keywords": [
            "cereales", "cereals", "cereal", "ble", "ble dur", "durum",
            "prix", "price", "fob", "import",
            "canada", "france", "cours", "marche",
        ],
    },
    {
        "key": "olive_oil",
        "title": "Indicateurs de l'huile d'olive",
        "url": f"{_BASE}/tableau-de-bord-des-indicateurs-de-lhuile-dolive/",
        "desc": "Production, exportation, prix de l'huile d'olive",
        "keywords": [
            "olive", "huile", "oil", "oleiculture", "oleicole",
            "olive oil", "trituration",
        ],
    },
    {
        "key": "dates_production",
        "title": "Indicateurs de la production des dattes",
        "url": f"{_BASE}/tableau-de-bord-des-indicateurs-de-la-production-des-dattes/",
        "desc": "Production de dattes par gouvernorat, variétés, nombre de palmiers",
        "keywords": [
            "dattes", "dates", "deglet", "palmier", "palm",
            "production dattes", "oasis",
        ],
    },
    {
        "key": "dates_annual",
        "title": "Indicateurs annuels des dattes",
        "url": f"{_BASE}/tableau-de-bord-des-indicateurs-annuels-des-dattes/",
        "desc": "Exportations annuelles de dattes : quantités, valeur, part des exportations alimentaires",
        "keywords": [
            "dattes", "dates", "export", "annuel", "annual",
            "exportation dattes",
        ],
    },
    {
        "key": "dates_monthly",
        "title": "Indicateurs mensuels des dattes",
        "url": f"{_BASE}/tableau-de-bord-des-indicateurs-mensuels-des-dattes/",
        "desc": "Exportations mensuelles de dattes : quantités et valeur par mois",
        "keywords": [
            "dattes", "dates", "export", "mensuel", "monthly",
        ],
    },
    {
        "key": "fisheries",
        "title": "Indicateurs de la pêche et de l'aquaculture",
        "url": f"{_BASE}/tableau-de-bord-des-indicateurs-de-la-peche-et-de-laquaculture/",
        "desc": "Production halieutique, aquaculture, flotte de pêche",
        "keywords": [
            "peche", "fishing", "fisheries", "aquaculture", "poisson", "fish",
            "crevette", "shrimp", "thon", "tuna", "halieutique",
            "mer", "sea",
        ],
    },
    {
        "key": "vegetables",
        "title": "Indicateurs annuels des cultures maraîchères",
        "url": f"{_BASE}/tableau-de-bord-des-indicateurs-annuels-des-cultures-maraicheres/",
        "desc": "Production, superficies des cultures maraîchères par saison",
        "keywords": [
            "maraichere", "maraichage", "legume", "legumes", "vegetable", "vegetables",
            "cultures maraicheres", "tomate", "tomato",
            "pomme de terre", "potato", "piment", "pepper", "oignon", "onion",
        ],
    },
    {
        "key": "dams",
        "title": "Situation hydraulique (SECADENORD)",
        "url": f"{_BASE}/situation-hydraulique-de-la-societe-dexploitation-du-canal-et-des-adductions-des-eaux-du-nord/",
        "desc": "Taux de remplissage et apports des barrages du nord de la Tunisie",
        "keywords": [
            "barrage", "dam", "hydraulique", "remplissage", "eau",
            "water", "secadenord", "retenue",
        ],
    },
    {
        "key": "investments",
        "title": "Indicateurs de l'investissement dans le secteur de l'agriculture et de la pêche",
        "url": f"{_BASE}/tableau-de-bord-des-indicateurs-de-linvestissement-dans-le-secteur-de-lagriculture-et-de-la-peche/",
        "desc": "Investissements approuvés et déclarés, crédits fonciers (APIA)",
        "keywords": [
            "investissement", "investment", "apia", "credit foncier",
            "declare", "approuve", "financement", "agricole",
        ],
    },
    {
        "key": "citrus",
        "title": "Indicateurs de performance de la filière agrumicole",
        "url": f"{_BASE}/indicateurs-de-performance-de-la-filiere-agrumicole/",
        "desc": "Superficies, production, rendements des agrumes",
        "keywords": [
            "agrume", "citrus", "orange", "mandarine", "citron", "lemon",
            "clementine", "agrumicole",
        ],
    },
    {
        "key": "citrus_exports",
        "title": "Exportations des agrumes tunisiens : indicateurs clés en quantité et en valeur",
        "url": f"{_BASE}/exportations-des-agrumes-tunisiens-indicateurs-cles-en-quantite-et-en-valeur/",
        "desc": "Quantités et valeur des exportations d'agrumes par destination",
        "keywords": [
            "agrume", "citrus", "export", "orange", "mandarine",
            "exportation agrumes", "citrus exports",
        ],
    },
    {
        "key": "citrus_production",
        "title": "Analyse spatiale et temporelle de la production d'agrumes (en tonnes)",
        "url": f"{_BASE}/analyse-spatiale-et-temporelle-de-la-production-dagrumes-en-tonnes/",
        "desc": "Production d'agrumes par gouvernorat et par année, cartes et tendances",
        "keywords": [
            "agrume", "citrus", "production", "gouvernorat", "spatial",
            "carte", "map",
        ],
    },
    {
        "key": "forest_fires",
        "title": "Tableau de bord des incendies de forêt",
        "url": f"{_BASE}/tableau-de-bord-des-incendies-de-foret/",
        "desc": "Nombre d'incendies et superficies brûlées par gouvernorat",
        "keywords": [
            "incendie", "fire", "foret", "forest", "brulee", "burned",
            "feu", "wildfire",
        ],
    },
    {
        "key": "rainfall",
        "title": "Quantités journalières de pluie enregistrées",
        "url": f"{_BASE}/tableau-de-bord-des-quantites-journalieres-de-pluie-enregistrees/",
        "desc": "Pluviométrie quotidienne par station",
        "keywords": [
            "pluie", "rain", "rainfall", "pluviometrie", "precipitation",
            "quotidien", "daily", "journalier",
            "eau", "water",
        ],
    },
]


def _normalize(text: str) -> str:
    """Strip accents and lowercase."""
    nfkd = unicodedata.normalize("NFKD", text.lower())
    return "".join(c for c in nfkd if not unicodedata.combining(c))


def get_dashboard_link(topic: str) -> str:
    """Match a topic to relevant dashboards and return links."""
    query = _normalize(topic)

    scored: list[tuple[int, dict]] = []
    for dash in _DASHBOARDS:
        score = sum(1 for kw in dash["keywords"] if _normalize(kw) in query)
        if score > 0:
            scored.append((score, dash))

    scored.sort(key=lambda x: x[0], reverse=True)

    if not scored:
        # No match — return full list
        lines = [f"No dashboard matched **{topic}**. Available dashboards:\n"]
        for dash in _DASHBOARDS:
            lines.append(f"- [{dash['title']}]({dash['url']}) — {dash['desc']}")
        return "\n".join(lines)

    if len(scored) == 1 or scored[0][0] > scored[1][0]:
        # Single best match
        dash = scored[0][1]
        lines = [
            f"## {dash['title']}",
            f"{dash['desc']}",
            f"",
            f"**Link:** {dash['url']}",
        ]
        return "\n".join(lines)

    # Multiple matches — only show those with the top score
    top_score = scored[0][0]
    scored = [(s, d) for s, d in scored if s == top_score]
    lines = [f"**{len(scored)} dashboards** match **{topic}**:\n"]
    for _, dash in scored:
        lines.append(f"- [{dash['title']}]({dash['url']}) — {dash['desc']}")
    return "\n".join(lines)
