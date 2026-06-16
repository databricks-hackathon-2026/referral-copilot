import streamlit as st
import html
import json
import math
import re
import requests
import traceback
from databricks import sql
from databricks.sdk.core import Config, oauth_service_principal
import os
from urllib.parse import urlparse

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="SOS · Source of Support",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ── Design tokens ─────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Serif+Display&family=DM+Sans:wght@300;400;500;600&display=swap');

html, body, [class*="css"] {
    font-family: 'DM Sans', sans-serif;
    background-color: #F7F5F2;
    color: #1A1A1A;
}

/* Header */
.sos-header {
    padding: 2.5rem 0 1.5rem 0;
    border-bottom: 2px solid #1A1A1A;
    margin-bottom: 2rem;
}
.sos-wordmark {
    font-family: 'DM Serif Display', serif;
    font-size: 2.8rem;
    letter-spacing: -0.02em;
    color: #1A1A1A;
    line-height: 1;
}
.sos-tagline {
    font-size: 0.95rem;
    color: #666;
    font-weight: 300;
    margin-top: 0.3rem;
}

/* Search bar */
.search-label {
    font-size: 0.8rem;
    font-weight: 600;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    color: #666;
    margin-bottom: 0.4rem;
}

/* Result card */
.facility-card {
    background: #FFFFFF;
    border: 1px solid #E0DDD8;
    border-radius: 6px;
    padding: 1.4rem 1.6rem;
    margin-bottom: 1rem;
    position: relative;
}
.facility-card:hover {
    border-color: #1A1A1A;
}
.facility-name {
    font-family: 'DM Serif Display', serif;
    font-size: 1.25rem;
    color: #1A1A1A;
    margin-bottom: 0.2rem;
}
.facility-meta {
    font-size: 0.85rem;
    color: #666;
    margin-bottom: 0.8rem;
}
.distance-badge {
    display: inline-block;
    background: #1A1A1A;
    color: #F7F5F2;
    font-size: 0.78rem;
    font-weight: 600;
    padding: 0.2rem 0.6rem;
    border-radius: 3px;
    margin-right: 0.5rem;
}
.confidence-badge {
    display: inline-block;
    font-size: 0.78rem;
    font-weight: 500;
    padding: 0.2rem 0.6rem;
    border-radius: 3px;
    margin-right: 0.5rem;
}
.conf-confirmed    { background: #E8F5E9; color: #2E7D32; }
.conf-coordinate   { background: #FFF8E1; color: #F57F17; }
.conf-ambiguous    { background: #FFF3E0; color: #E65100; }
.conf-unresolved   { background: #FFEBEE; color: #C62828; }

.evidence-section {
    margin-top: 0.8rem;
    padding-top: 0.8rem;
    border-top: 1px solid #E0DDD8;
    font-size: 0.85rem;
}
.evidence-label {
    font-size: 0.75rem;
    font-weight: 600;
    letter-spacing: 0.06em;
    text-transform: uppercase;
    color: #999;
    margin-bottom: 0.3rem;
}
.evidence-tag {
    display: inline-block;
    background: #F0EDE8;
    color: #444;
    font-size: 0.78rem;
    padding: 0.15rem 0.5rem;
    border-radius: 3px;
    margin: 0.15rem 0.15rem 0.15rem 0;
}
.no-results {
    text-align: center;
    padding: 4rem 2rem;
    color: #999;
    font-size: 1rem;
}
.result-count {
    font-size: 0.85rem;
    color: #666;
    margin-bottom: 1.2rem;
    font-weight: 500;
}
</style>
""", unsafe_allow_html=True)

# ── City gazetteer ─────────────────────────────────────────────────────────────
CITIES = {
    "delhi": (28.6139, 77.2090), "new delhi": (28.6139, 77.2090),
    "mumbai": (19.0760, 72.8777), "bombay": (19.0760, 72.8777),
    "bangalore": (12.9716, 77.5946), "bengaluru": (12.9716, 77.5946),
    "hyderabad": (17.3850, 78.4867),
    "chennai": (13.0827, 80.2707), "madras": (13.0827, 80.2707),
    "kolkata": (22.5726, 88.3639), "calcutta": (22.5726, 88.3639),
    "pune": (18.5204, 73.8567),
    "ahmedabad": (23.0225, 72.5714),
    "jaipur": (26.9124, 75.7873),
    "patna": (25.5941, 85.1376),
    "lucknow": (26.8467, 80.9462),
    "bhopal": (23.2599, 77.4126),
    "indore": (22.7196, 75.8577),
    "chandigarh": (30.7333, 76.7794),
    "kochi": (9.9312, 76.2673), "cochin": (9.9312, 76.2673),
    "nagpur": (21.1458, 79.0882),
    "visakhapatnam": (17.6868, 83.2185), "vizag": (17.6868, 83.2185),
    "surat": (21.1702, 72.8311),
    "coimbatore": (11.0168, 76.9558),
    "agra": (27.1767, 78.0081),
    "varanasi": (25.3176, 82.9739),
    "amritsar": (31.6340, 74.8723),
    "guwahati": (26.1445, 91.7362),
    "bhubaneswar": (20.2961, 85.8245),
    "thiruvananthapuram": (8.5241, 76.9366), "trivandrum": (8.5241, 76.9366),
    "ranchi": (23.3441, 85.3096),
    "raipur": (21.2514, 81.6296),
}

# ── Care need keyword map ──────────────────────────────────────────────────────
CARE_NEEDS = {
    "dialysis":         ["dialysis", "nephrology", "renal", "kidney", "haemodialysis", "hemodialysis"],
    "emergency":        ["emergency", "trauma", "casualty", "critical care", "icu", "accident"],
    "maternity":        ["maternity", "obstetrics", "gynaecology", "gynecology", "delivery", "neonatal", "prenatal"],
    "cardiac":          ["cardiac", "cardiology", "heart", "cardiovascular", "angioplasty", "bypass"],
    "cancer":           ["cancer", "oncology", "chemotherapy", "radiation", "tumour", "tumor"],
    "orthopedic":       ["orthopedic", "orthopaedic", "fracture", "joint replacement", "spine", "bone"],
    "eye":              ["eye", "ophthalmology", "cataract", "retina", "glaucoma", "vision"],
    "dental":           ["dental", "dentistry", "teeth", "oral", "tooth"],
    "neurology":        ["neurology", "neuro", "brain", "stroke", "seizure", "epilepsy"],
    "pediatric":        ["pediatric", "paediatric", "children", "child", "nicu", "infant"],
}

STOPWORDS = {
    "a", "an", "and", "around", "at", "best", "care", "center", "centre", "clinic",
    "doctor", "doctors", "facility", "find", "for", "help", "hospital", "hospitals",
    "in", "me", "near", "nearby", "need", "of", "please", "support", "the", "to",
    "treatment", "with",
}

EVIDENCE_FIELD_LABELS = {
    "specialties": "Matched specialty",
    "standardized_services": "Matched service",
    "parsed_capability": "Matched capability",
    "description": "Mentioned in facility notes",
}

# ── Haversine ─────────────────────────────────────────────────────────────────
def haversine_km(lat1, lon1, lat2, lon2):
    R = 6371.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlam/2)**2
    return 2*R*math.asin(math.sqrt(a))

# ── Parse query ───────────────────────────────────────────────────────────────
def parse_query(query: str):
    q = query.lower().strip()
    city_found, coords = None, None
    for city, latlon in CITIES.items():
        if city in q:
            city_found, coords = city.title(), latlon
            break
    care_found, keywords = None, []
    for care, kws in CARE_NEEDS.items():
        if any(kw in q for kw in kws) or care in q:
            care_found, keywords = care, kws
            break
    return city_found, coords, care_found, keywords


def unique_terms(terms: list, limit: int = 16) -> list:
    cleaned = []
    seen = set()
    for term in terms:
        term = re.sub(r"\s+", " ", str(term).lower()).strip()
        term = re.sub(r"[^a-z0-9 /+-]", "", term)
        if len(term) < 3 or term in seen:
            continue
        seen.add(term)
        cleaned.append(term)
        if len(cleaned) >= limit:
            break
    return cleaned


def clamp_radius_km(value, default_radius_km: int) -> int:
    try:
        radius = int(value)
    except (TypeError, ValueError):
        radius = int(default_radius_km)
    return min(max(radius, 5), 500)


def fallback_keywords(query: str, city: str, mapped_keywords: list) -> list:
    city_words = set()
    if city:
        city_words.update(city.lower().split())
    for city_name in CITIES:
        if city_name in query.lower():
            city_words.update(city_name.split())

    query_terms = [
        token
        for token in re.findall(r"[a-z][a-z0-9+-]{2,}", query.lower())
        if token not in STOPWORDS and token not in city_words
    ]
    return unique_terms(list(mapped_keywords or []) + query_terms)


def get_search_plan_fallback(query: str, default_radius_km: int) -> dict:
    city, coords, care_need, mapped_keywords = parse_query(query)
    keywords = fallback_keywords(query, city, mapped_keywords)
    if not care_need and keywords:
        care_need = " ".join(keywords[:3])
    if not care_need:
        care_need = "matching"

    return {
        "city": city,
        "coords": coords,
        "care_need": care_need,
        "keywords": keywords,
        "radius_km": clamp_radius_km(default_radius_km, 50),
        "source": "keyword fallback",
    }


def extract_json_object(text: str) -> dict:
    text = text.strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?", "", text).strip()
        text = re.sub(r"```$", "", text).strip()

    start = text.find("{")
    end = text.rfind("}")
    if start == -1 or end == -1:
        raise ValueError("Planner response did not contain a JSON object.")
    return json.loads(text[start:end + 1])


@st.cache_resource
def get_workspace_client():
    from databricks.sdk import WorkspaceClient

    return WorkspaceClient()


def get_gateway_access_token():
    return os.getenv("GATEWAY-ACCESS_TOKEN") or os.getenv("GATEWAY_ACCESS_TOKEN")


def query_llm_gateway(prompt: str) -> str:
    token = get_gateway_access_token()
    if not token:
        raise RuntimeError("Missing GATEWAY-ACCESS_TOKEN.")

    model = os.getenv("DATABRICKS_LLM_MODEL", "gpt-oss-102b")
    url = f"https://{get_databricks_server_hostname()}/ai-gateway/mlflow/v1/chat/completions"
    headers = {"Authorization": f"Bearer {token}"}
    headers["Content-Type"] = "application/json"

    response = requests.post(
        url,
        headers=headers,
        json={
            "model": model,
            "messages": [
                {
                    "role": "system",
                    "content": "You are a careful healthcare search planner. Return valid JSON only.",
                },
                {"role": "user", "content": prompt},
            ],
            "temperature": 0.1,
            "max_tokens": 1024,
        },
        timeout=30,
    )
    response.raise_for_status()
    data = response.json()
    return data["choices"][0]["message"]["content"]


def get_search_plan_agentic(query: str, default_radius_km: int) -> dict | None:
    if not get_gateway_access_token():
        return None

    city_names = ", ".join(sorted({city.title() for city in CITIES.keys()}))
    prompt = f"""
Convert this healthcare facility search into strict JSON.

User query: {query}

Known cities: {city_names}

Return only this JSON shape:
{{
  "city": string or null,
  "care_need": string or null,
  "keywords": string[],
  "must_have": string[],
  "nice_to_have": string[],
  "radius_km": number or null
}}

Rules:
- Expand clinical synonyms and Indian/British/American spellings.
- Keep keywords short enough for SQL LIKE matching against facility evidence text.
- Do not invent facility names, contacts, locations, or capabilities.
- The gold table is the only source of truth for facility facts.
"""

    parsed = extract_json_object(query_llm_gateway(prompt))

    fallback = get_search_plan_fallback(query, default_radius_km)
    city = parsed.get("city") or fallback["city"]
    coords = CITIES.get(str(city).lower()) if city else fallback["coords"]
    if city and not coords:
        city, coords = fallback["city"], fallback["coords"]

    keywords = unique_terms(
        list(parsed.get("keywords") or [])
        + list(parsed.get("must_have") or [])
        + list(parsed.get("nice_to_have") or [])
        + fallback["keywords"]
    )

    return {
        "city": city,
        "coords": coords,
        "care_need": parsed.get("care_need") or fallback["care_need"],
        "keywords": keywords,
        "radius_km": clamp_radius_km(parsed.get("radius_km"), default_radius_km),
        "source": "Databricks LLM planner",
    }


def plan_search(query: str, default_radius_km: int) -> dict:
    try:
        agent_plan = get_search_plan_agentic(query, default_radius_km)
        if agent_plan:
            return agent_plan
    except Exception as e:
        traceback.print_exc()
        detail = type(e).__name__
        if isinstance(e, requests.HTTPError) and e.response is not None:
            detail = f"HTTP {e.response.status_code}"
            print(f"Planner HTTP response: {e.response.text[:1000]}")
        st.caption(f"Planner unavailable ({detail}); using keyword fallback.")

    return get_search_plan_fallback(query, default_radius_km)

# ── Databricks SQL connection ─────────────────────────────────────────────────
def get_databricks_server_hostname():
    raw_host = os.getenv("DATABRICKS_SERVER_HOSTNAME") or os.getenv("DATABRICKS_HOST")
    if not raw_host:
        raise RuntimeError("Missing DATABRICKS_HOST or DATABRICKS_SERVER_HOSTNAME.")

    parsed = urlparse(raw_host if "://" in raw_host else f"https://{raw_host}")
    return parsed.netloc or parsed.path


def get_databricks_http_path():
    http_path = os.getenv("DATABRICKS_HTTP_PATH")
    if http_path:
        return http_path

    warehouse_id = os.getenv("DATABRICKS_WAREHOUSE_ID")
    if warehouse_id:
        return f"/sql/1.0/warehouses/{warehouse_id}"

    raise RuntimeError(
        "Missing SQL warehouse configuration. In app.yaml, set "
        "DATABRICKS_WAREHOUSE_ID from a SQL warehouse resource key, or set "
        "DATABRICKS_HTTP_PATH to /sql/1.0/warehouses/<warehouse-id>."
    )


@st.cache_resource
def get_connection():
    server_hostname = get_databricks_server_hostname()
    http_path = get_databricks_http_path()
    client_id = os.getenv("DATABRICKS_CLIENT_ID")
    client_secret = os.getenv("DATABRICKS_CLIENT_SECRET")

    if client_id and client_secret:
        def credential_provider():
            config = Config(
                host=f"https://{server_hostname}",
                client_id=client_id,
                client_secret=client_secret,
            )
            return oauth_service_principal(config)

        return sql.connect(
            server_hostname=server_hostname,
            http_path=http_path,
            credentials_provider=credential_provider,
        )

    access_token = os.getenv("DATABRICKS_TOKEN")
    if not access_token:
        raise RuntimeError(
            "Missing Databricks credentials. Set DATABRICKS_CLIENT_ID and "
            "DATABRICKS_CLIENT_SECRET for app auth, or DATABRICKS_TOKEN for local development."
        )

    return sql.connect(
        server_hostname=server_hostname,
        http_path=http_path,
        access_token=access_token,
    )

# ── Query gold table ──────────────────────────────────────────────────────────
def escape_sql_like_term(term: str) -> str:
    return term.replace("'", "''")


def html_safe(value) -> str:
    return html.escape(str(value or ""), quote=True)


def search_facilities(keywords: list, limit: int = 50):
    """Pull candidates matching any keyword across evidence columns."""
    safe_keywords = unique_terms(keywords)
    if not safe_keywords:
        return []

    kw_conditions = " OR ".join([
        f"(LOWER(specialties) LIKE '%{escape_sql_like_term(kw)}%' OR "
        f"LOWER(standardized_services) LIKE '%{escape_sql_like_term(kw)}%' OR "
        f"LOWER(parsed_capability) LIKE '%{escape_sql_like_term(kw)}%' OR "
        f"LOWER(description) LIKE '%{escape_sql_like_term(kw)}%')"
        for kw in safe_keywords
    ])
    query = f"""
        SELECT
            unique_id, name, organization_type,
            facility_latitude, facility_longitude,
            address_city, address_stateOrRegion, address_zipOrPostcode,
            district, state, location_confidence,
            phone_numbers, officialPhone, email,
            numberDoctors, capacity,
            specialties, standardized_services,
            parsed_capability, description
        FROM workspace.default.sos_facility_index
        WHERE facility_latitude IS NOT NULL
          AND facility_longitude IS NOT NULL
          AND ({kw_conditions})
        LIMIT {limit}
    """
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute(query)
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]

# ── Confidence badge ──────────────────────────────────────────────────────────
def confidence_badge(level):
    labels = {
        "confirmed":         ("confirmed location", "conf-confirmed"),
        "coordinate_based":  ("approximate location", "conf-coordinate"),
        "ambiguous_pin":     ("location uncertain", "conf-ambiguous"),
        "unresolved":        ("location unknown", "conf-unresolved"),
    }
    label, css = labels.get(level, ("unknown", "conf-unresolved"))
    return f'<span class="confidence-badge {css}">📍 {label}</span>'

# ── Evidence snippets ─────────────────────────────────────────────────────────
def clean_evidence_text(value: str) -> str:
    value = re.sub(r"\s+", " ", str(value or "")).strip()
    value = value.strip("[]{}()")
    value = value.strip(",;:.\"'")
    return value


def extract_evidence(row: dict, keywords: list) -> list:
    snippets = []
    for field in ["specialties", "standardized_services", "parsed_capability", "description"]:
        val = row.get(field) or ""
        for kw in keywords:
            if kw.lower() in val.lower():
                # Find a short excerpt around the keyword
                idx = val.lower().find(kw.lower())
                start = max(0, idx - 36)
                end = min(len(val), idx + 84)
                excerpt = clean_evidence_text(val[start:end])
                if start > 0:
                    excerpt = f"...{excerpt}"
                if end < len(val):
                    excerpt = f"{excerpt}..."
                label = EVIDENCE_FIELD_LABELS.get(field, "Matched evidence")
                snippet = f"{label}: {excerpt}"
                if excerpt and snippet not in snippets:
                    snippets.append(snippet)
                if len(snippets) >= 4:
                    return snippets
    return snippets

# ── Header ────────────────────────────────────────────────────────────────────
st.markdown("""
<div class="sos-header">
    <div class="sos-wordmark">SOS</div>
    <div class="sos-tagline">Source of Support &nbsp;·&nbsp; Finding the right healthcare facility shouldn't be a guessing game.</div>
</div>
""", unsafe_allow_html=True)

# ── Search input ──────────────────────────────────────────────────────────────
col1, col2 = st.columns([4, 1])
with col1:
    st.markdown('<div class="search-label">What do you need, and where?</div>', unsafe_allow_html=True)
    query = st.text_input(
        label="query",
        placeholder='e.g. "dialysis near Jaipur" or "emergency surgery near Patna"',
        label_visibility="collapsed",
    )
with col2:
    st.markdown('<div class="search-label">&nbsp;</div>', unsafe_allow_html=True)
    radius_km = st.selectbox("Radius", [25, 50, 100, 200], index=1, label_visibility="collapsed")

# ── Search ────────────────────────────────────────────────────────────────────
if query:
    plan = plan_search(query, radius_km)
    city = plan["city"]
    coords = plan["coords"]
    care_need = plan["care_need"]
    keywords = plan["keywords"]
    radius_km = plan["radius_km"]

    if not city or not coords:
        st.warning("Could not find a city in your query. Try including a city name, e.g. 'dialysis near Jaipur'.")
    elif not keywords:
        st.warning("Could not identify searchable care terms. Try adding a specialty, service, condition, or procedure.")
    else:
        with st.spinner(f"Searching for {care_need} facilities near {city}…"):
            try:
                results = search_facilities(keywords, limit=100)
            except Exception as e:
                st.error(f"Database error: {e}")
                results = []

        # Filter and rank by distance
        anchor_lat, anchor_lon = coords
        ranked = []
        for r in results:
            try:
                dist = haversine_km(anchor_lat, anchor_lon, r["facility_latitude"], r["facility_longitude"])
                if dist <= radius_km:
                    r["_distance_km"] = dist
                    ranked.append(r)
            except Exception:
                continue

        ranked.sort(key=lambda x: x["_distance_km"])

        if not ranked:
            safe_city = html_safe(city)
            safe_care_need = html_safe(care_need)
            st.markdown(f"""
            <div class="no-results">
                No {safe_care_need} facilities found within {radius_km} km of {safe_city}.<br>
                Try expanding the radius or checking the spelling.
            </div>
            """, unsafe_allow_html=True)
        else:
            safe_city = html_safe(city)
            st.markdown(f'<div class="result-count">{len(ranked)} facilit{"y" if len(ranked)==1 else "ies"} found within {radius_km} km of {safe_city}</div>', unsafe_allow_html=True)

            for r in ranked[:10]:
                dist = r["_distance_km"]
                loc_parts = [p for p in [r.get("district") or r.get("address_city"), r.get("state") or r.get("address_stateOrRegion")] if p]
                loc_str = ", ".join(loc_parts) if loc_parts else "Location on file"
                conf = r.get("location_confidence", "unresolved")
                evidence = extract_evidence(r, keywords)
                doctors = r.get("numberDoctors")
                capacity = r.get("capacity")

                meta_parts = [r.get("organization_type") or "Healthcare Facility"]
                if doctors: meta_parts.append(f"{doctors} doctors")
                if capacity: meta_parts.append(f"capacity {capacity}")

                evidence_html = ""
                if evidence:
                    tags = "".join(f'<span class="evidence-tag">{html_safe(e[:140])}</span>' for e in evidence)
                    evidence_html = f'<div class="evidence-section"><div class="evidence-label">Evidence</div>{tags}</div>'

                contact_parts = []
                for field in ["officialPhone", "phone_numbers", "email"]:
                    val = r.get(field)
                    if val and val.strip():
                        contact_parts.append(val.strip()[:40])
                        break
                contact_html = f'<span style="font-size:0.82rem;color:#888;">{html_safe(contact_parts[0])}</span>' if contact_parts else ""

                st.markdown(f"""
                <div class="facility-card">
                    <div class="facility-name">{html_safe(r.get("name", "Unknown Facility"))}</div>
                    <div class="facility-meta">{html_safe(" · ".join(meta_parts))} · {html_safe(loc_str)}</div>
                    <span class="distance-badge">{dist:.1f} km</span>
                    {confidence_badge(conf)}
                    {contact_html}
                    {evidence_html}
                </div>
                """, unsafe_allow_html=True)

else:
    st.markdown("""
    <div class="no-results" style="padding: 3rem 2rem;">
        Enter a care need and a city above to find the nearest facilities.<br>
        <span style="font-size:0.85rem;margin-top:0.5rem;display:block;">
        Try: &nbsp;<em>dialysis near Jaipur</em> &nbsp;·&nbsp; <em>emergency surgery near Patna</em> &nbsp;·&nbsp; <em>maternity near Mumbai</em>
        </span>
    </div>
    """, unsafe_allow_html=True)
