"""
Meta Ads AI Monitor v2 - Rapport enrichi avec créas + tunnel TOF/MOF/BOF
"""

import os
import io
import json
import smtplib
import logging
import requests
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from dotenv import load_dotenv

try:
    from xhtml2pdf import pisa
    PDF_AVAILABLE = True
except ImportError:
    PDF_AVAILABLE = False
    logging.warning("xhtml2pdf non installé — PDF désactivé. Installe avec: pip install xhtml2pdf")

load_dotenv()

META_ACCESS_TOKEN  = os.getenv("META_ACCESS_TOKEN")
META_AD_ACCOUNT_ID = os.getenv("META_AD_ACCOUNT_ID")
ANTHROPIC_API_KEY  = os.getenv("ANTHROPIC_API_KEY")
EMAIL_SENDER       = os.getenv("EMAIL_SENDER")
EMAIL_PASSWORD     = os.getenv("EMAIL_PASSWORD")
EMAIL_RECIPIENT    = os.getenv("EMAIL_RECIPIENT")
SMTP_HOST          = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT          = int(os.getenv("SMTP_PORT", "587"))

META_API_VERSION = "v21.0"
META_BASE_URL    = f"https://graph.facebook.com/{META_API_VERSION}"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# CHAMPS API
# ─────────────────────────────────────────────

CAMPAIGN_FIELDS = ",".join([
    "campaign_name", "campaign_id", "objective",
    "spend", "impressions", "clicks", "ctr", "cpc", "cpm",
    "actions", "action_values", "cost_per_action_type",
    "reach", "frequency",
])

AD_FIELDS = ",".join([
    "ad_name", "ad_id", "adset_name", "campaign_name", "campaign_id",
    "spend", "impressions", "clicks", "ctr", "cpc", "cpm",
    "actions", "action_values", "reach", "frequency",
])

# ─────────────────────────────────────────────
# UTILITAIRES
# ─────────────────────────────────────────────

def get_date_range(days_ago_start: int, days_ago_end: int = 0) -> dict:
    today = datetime.now().date()
    return {
        "since": str(today - timedelta(days=days_ago_start)),
        "until": str(today - timedelta(days=days_ago_end)),
    }

def extract_action_value(actions: list, action_type: str) -> float:
    if not actions:
        return 0.0
    for a in actions:
        if a.get("action_type") == action_type:
            return float(a.get("value", 0))
    return 0.0

def detect_tunnel_level(name: str) -> str:
    name_upper = name.upper()
    if any(k in name_upper for k in ["TOF", "TOP OF FUNNEL", "AWARENESS", "NOTORIETE", "TRAFIC", "COLD"]):
        return "TOF"
    elif any(k in name_upper for k in ["MOF", "MIDDLE", "CONSIDERATION", "RETARGETING", "WARM", "ENGAGEMENT"]):
        return "MOF"
    elif any(k in name_upper for k in ["BOF", "BOTTOM", "CONVERSION", "ACHAT", "PURCHASE", "HOT", "CLOSING"]):
        return "BOF"
    return "NON CLASSÉ"

# ─────────────────────────────────────────────
# FETCH META API
# ─────────────────────────────────────────────

def fetch_insights(date_range: dict, level: str, fields: str) -> list:
    url = f"{META_BASE_URL}/{META_AD_ACCOUNT_ID}/insights"
    params = {
        "access_token": META_ACCESS_TOKEN,
        "fields": fields,
        "time_range": json.dumps(date_range, separators=(',', ':')),
        "level": level,
        "limit": 200,
    }
    results = []
    while url:
        resp = requests.get(url, params=params, timeout=30)
        if not resp.ok:
            log.error(f"META API ERROR: {resp.status_code} {resp.text}")
        resp.raise_for_status()
        data = resp.json()
        results.extend(data.get("data", []))
        paging = data.get("paging", {})
        url = paging.get("next")
        params = {}
    return results

def parse_campaign(raw: dict) -> dict:
    actions       = raw.get("actions", [])
    action_values = raw.get("action_values", [])
    spend         = float(raw.get("spend", 0))
    impressions   = int(raw.get("impressions", 0))
    clicks        = int(raw.get("clicks", 0))
    leads         = extract_action_value(actions, "lead")
    purchases     = extract_action_value(actions, "purchase")
    revenue       = extract_action_value(action_values, "purchase")
    name          = raw.get("campaign_name", "Inconnu")

    return {
        "campaign_id":   raw.get("campaign_id"),
        "campaign_name": name,
        "tunnel":        detect_tunnel_level(name),
        "objective":     raw.get("objective", ""),
        "spend":         spend,
        "impressions":   impressions,
        "clicks":        clicks,
        "ctr":           float(raw.get("ctr", 0)),
        "cpc":           float(raw.get("cpc", 0)) if raw.get("cpc") else None,
        "cpm":           float(raw.get("cpm", 0)) if raw.get("cpm") else None,
        "reach":         int(raw.get("reach", 0)),
        "frequency":     float(raw.get("frequency", 0)),
        "leads":         leads,
        "purchases":     purchases,
        "revenue":       revenue,
        "cpl":           round(spend / leads, 2) if leads > 0 else None,
        "cpa":           round(spend / purchases, 2) if purchases > 0 else None,
        "roas":          round(revenue / spend, 2) if spend > 0 else None,
    }

def parse_ad(raw: dict) -> dict:
    actions       = raw.get("actions", [])
    action_values = raw.get("action_values", [])
    spend         = float(raw.get("spend", 0))
    impressions   = int(raw.get("impressions", 0))
    clicks        = int(raw.get("clicks", 0))
    leads         = extract_action_value(actions, "lead")
    purchases     = extract_action_value(actions, "purchase")
    revenue       = extract_action_value(action_values, "purchase")
    frequency     = float(raw.get("frequency", 0))
    ctr           = float(raw.get("ctr", 0))

    # Détection fatigue
    fatigue_signals = []
    if frequency > 3.5:
        fatigue_signals.append(f"Fréquence élevée ({frequency:.1f})")
    if ctr < 0.5 and impressions > 1000:
        fatigue_signals.append(f"CTR faible ({ctr:.2f}%)")
    if frequency > 5:
        fatigue_signals.append("⚠️ Saturation audience")

    status = "🔴 FATIGUÉ" if len(fatigue_signals) >= 2 else ("🟡 À SURVEILLER" if fatigue_signals else "🟢 OK")

    return {
        "ad_id":         raw.get("ad_id"),
        "ad_name":       raw.get("ad_name", "Inconnu"),
        "adset_name":    raw.get("adset_name", ""),
        "campaign_name": raw.get("campaign_name", ""),
        "tunnel":        detect_tunnel_level(raw.get("campaign_name", "")),
        "spend":         spend,
        "impressions":   impressions,
        "clicks":        clicks,
        "ctr":           ctr,
        "cpc":           float(raw.get("cpc", 0)) if raw.get("cpc") else None,
        "cpm":           float(raw.get("cpm", 0)) if raw.get("cpm") else None,
        "frequency":     frequency,
        "leads":         leads,
        "purchases":     purchases,
        "revenue":       revenue,
        "cpl":           round(spend / leads, 2) if leads > 0 else None,
        "cpa":           round(spend / purchases, 2) if purchases > 0 else None,
        "roas":          round(revenue / spend, 2) if spend > 0 else None,
        "fatigue_signals": fatigue_signals,
        "status":        status,
    }

def collect_all_data() -> dict:
    """
    Collecte les données en prenant HIER comme jour principal.
    Raison : le rapport est envoyé à 9h00, aujourd'hui n'a que quelques minutes de données.
    - Jour principal  = hier (J-1) — journée complète
    - Comparaison     = avant-hier (J-2) — journée complète
    - Référence       = moyenne des 7 jours précédents (J-8 à J-2)
    """
    log.info("📡 Collecte des données Meta Ads...")
    main_day_range  = get_date_range(1, 1)   # Hier — journée complète
    prev_day_range  = get_date_range(2, 2)   # Avant-hier
    week_range      = get_date_range(8, 2)   # 7 jours glissants (J-8 à J-2)

    raw_campaigns_main  = fetch_insights(main_day_range, "campaign", CAMPAIGN_FIELDS)
    raw_campaigns_prev  = fetch_insights(prev_day_range, "campaign", CAMPAIGN_FIELDS)
    raw_campaigns_week  = fetch_insights(week_range, "campaign", CAMPAIGN_FIELDS)
    raw_ads_main        = fetch_insights(main_day_range, "ad", AD_FIELDS)

    campaigns_today     = [parse_campaign(c) for c in raw_campaigns_main]
    campaigns_yesterday = [parse_campaign(c) for c in raw_campaigns_prev]
    campaigns_week      = [parse_campaign(c) for c in raw_campaigns_week]
    ads_today           = [parse_ad(a) for a in raw_ads_main]

    log.info(f"✅ {len(campaigns_today)} campagnes | {len(ads_today)} créas récupérées")
    return {
        "campaigns_today":     campaigns_today,
        "campaigns_yesterday": campaigns_yesterday,
        "campaigns_week":      campaigns_week,
        "ads_today":           ads_today,
        "dates": {
            "today":     main_day_range,
            "yesterday": prev_day_range,
            "week":      week_range,
        }
    }

# ─────────────────────────────────────────────
# AGRÉGATION KPI
# ─────────────────────────────────────────────

def aggregate_kpis(campaigns: list) -> dict:
    if not campaigns:
        return {}
    spend       = sum(c["spend"] for c in campaigns)
    impressions = sum(c["impressions"] for c in campaigns)
    clicks      = sum(c["clicks"] for c in campaigns)
    leads       = sum(c["leads"] for c in campaigns)
    purchases   = sum(c["purchases"] for c in campaigns)
    revenue     = sum(c["revenue"] for c in campaigns)
    return {
        "spend":       round(spend, 2),
        "impressions": impressions,
        "clicks":      clicks,
        "ctr":         round(clicks / impressions * 100, 2) if impressions > 0 else 0,
        "cpc":         round(spend / clicks, 2) if clicks > 0 else None,
        "cpm":         round(spend / impressions * 1000, 2) if impressions > 0 else None,
        "leads":       leads,
        "purchases":   purchases,
        "revenue":     round(revenue, 2),
        "cpl":         round(spend / leads, 2) if leads > 0 else None,
        "cpa":         round(spend / purchases, 2) if purchases > 0 else None,
        "roas":        round(revenue / spend, 2) if spend > 0 else None,
    }

def aggregate_by_tunnel(campaigns: list) -> dict:
    tunnel_data = {"TOF": [], "MOF": [], "BOF": [], "NON CLASSÉ": []}
    for c in campaigns:
        tunnel_data[c["tunnel"]].append(c)
    return {t: aggregate_kpis(camps) for t, camps in tunnel_data.items() if camps}

def compute_averages(week_campaigns: list) -> dict:
    agg = aggregate_kpis(week_campaigns)
    avg = {}
    for k, v in agg.items():
        if v is not None and k in ["spend", "impressions", "clicks", "leads", "purchases", "revenue"]:
            avg[k] = round(v / 7, 2)
        else:
            avg[k] = v
    return avg

# ─────────────────────────────────────────────
# ANALYSE IA
# ─────────────────────────────────────────────

def analyze_with_ai(data: dict) -> str:
    today_kpis     = aggregate_kpis(data["campaigns_today"])
    yesterday_kpis = aggregate_kpis(data["campaigns_yesterday"])
    avg_kpis       = compute_averages(data["campaigns_week"])
    tunnel_kpis    = aggregate_by_tunnel(data["campaigns_today"])

    ads_sorted_ctr  = sorted(data["ads_today"], key=lambda x: x["ctr"], reverse=True)
    ads_fatigued    = [a for a in data["ads_today"] if a["status"] == "🔴 FATIGUÉ"]
    ads_winners     = [a for a in data["ads_today"] if a["status"] == "🟢 OK" and a["spend"] > 0]

    prompt = f"""Tu es un media buyer Meta Ads senior avec 10 ans d'expérience en gestion de campagnes de génération de leads.
Tu produis des rapports de santé publicitaire clairs, actionnables et professionnels.

╔══════════════════════════════════════════════════════╗
║  PÉRIMÈTRE D'ANALYSE — RÈGLES PROFESSIONNELLES      ║
╚══════════════════════════════════════════════════════╝

TON RÔLE : analyser exclusivement la qualité de la diffusion publicitaire Meta.
Tu n'as pas accès aux données post-clic (CRM, ventes, inscriptions, tunnel).
→ Tu évalues : est-ce que Meta diffuse bien ? Les annonces performent-elles ?
→ Tu ne juges PAS : est-ce que le business performe ? Y a-t-il des ventes ?
→ Tu utilises toujours le terme "santé publicitaire", jamais "santé du business".
→ Toute conclusion sur les ventes ou le tunnel est hors périmètre — le mentionner.

LOGIQUE D'ANALYSE PROFESSIONNELLE :
Un bon media buyer ne juge JAMAIS une campagne sur une seule journée.
Il compare toujours les données sur une fenêtre glissante de 3 à 7 jours.
Il cherche des TENDANCES, pas des chiffres isolés.
Il formule des hypothèses prudentes, pas des verdicts définitifs.
Il priorise les actions à fort impact et faible effort.

═══════════════════════════════════════
DONNÉES À ANALYSER
═══════════════════════════════════════
HIER (J-1, journée complète) : {json.dumps(today_kpis, indent=2, ensure_ascii=False)}
AVANT-HIER (J-2) : {json.dumps(yesterday_kpis, indent=2, ensure_ascii=False)}
MOYENNE GLISSANTE 7J : {json.dumps(avg_kpis, indent=2, ensure_ascii=False)}

RÉPARTITION PAR NIVEAU (TOF/MOF/BOF) : {json.dumps(tunnel_kpis, indent=2, ensure_ascii=False)}
DÉTAIL CAMPAGNES : {json.dumps(data["campaigns_today"], indent=2, ensure_ascii=False)}
TOP CRÉAS PAR CTR : {json.dumps(ads_sorted_ctr[:10], indent=2, ensure_ascii=False)}
CRÉAS EN FATIGUE : {json.dumps(ads_fatigued, indent=2, ensure_ascii=False)}
CRÉAS PERFORMANTES : {json.dumps(ads_winners[:5], indent=2, ensure_ascii=False)}

═══════════════════════════════════════
GRILLE D'ANALYSE — BENCHMARKS PROS
═══════════════════════════════════════
Utilise ces benchmarks sectoriels comme repères relatifs (pas comme vérités absolues) :

CTR (Link Click-Through Rate) :
  🟢 > 2%       → Excellente accroche créative
  🟡 1% - 2%    → Dans la norme, surveiller l'évolution
  🔴 < 1%       → Accroche faible ou audience mal ciblée — action requise

CPM (Coût pour mille impressions) :
  🟢 < 8€       → Enchères compétitives, audience bien définie
  🟡 8€ - 15€   → Normal selon saison et secteur
  🔴 > 15€      → Saturation d'audience ou forte concurrence sur la cible

CPC (Coût par clic) :
  🟢 < 0,50€    → Trafic très peu coûteux
  🟡 0,50€ - 1,50€ → Standard pour la lead gen
  🔴 > 1,50€    → Audience trop large ou créa peu engageante

CPL (Coût par Lead) :
  Évaluer en relatif : comparer à la moyenne 7J et à la tendance
  Une hausse de +20% sur 3 jours consécutifs = signal d'alerte
  Une baisse de -15% = signal positif à capitaliser

Fréquence :
  🟢 < 2         → Audience fraîche, pas de saturation
  🟡 2 - 3       → Normal, surveiller CTR en parallèle
  🔴 > 3         → Risque de saturation — envisager nouvelle créa ou audience élargie

Volume de leads :
  Alerter si baisse > 30% vs moyenne 7J sur 2 jours consécutifs
  Toujours croiser avec le spend : baisse leads + spend stable = problème créa/audience
  Baisse leads + baisse spend = normal (budget réduit ou période creuse)

═══════════════════════════════════════
STRUCTURE DU RAPPORT HTML (9 sections)
═══════════════════════════════════════

1. SCORE DE SANTÉ PUBLICITAIRE DU JOUR
   Badge : 🟢 EXCELLENTE / 🟡 CORRECTE / 🔴 DÉGRADÉE + score /10
   Justification en 2-3 phrases basée sur les tendances 7J, pas uniquement sur J-1
   Préciser les 2-3 indicateurs qui ont le plus influencé le score

2. RÉSUMÉ EXÉCUTIF (5 points clés)
   Chaque point = 1 observation factuelle + 1 interprétation prudente
   Utiliser les flèches ↑↓→ avec la variation en % vs moy. 7J
   Conclure par : "Les performances de conversion post-clic sont mesurées hors Meta."

3. ANALYSE PAR NIVEAU DE DIFFUSION (tableau)
   Colonnes : Niveau | Spend | CPL | CTR | CPM | Fréquence | Leads | Tendance
   Pour chaque niveau : évaluer si la diffusion est saine, en tension ou dégradée
   Identifier si un niveau cannibale un autre (ex: BOF sature l'audience du MOF)

4. CAMPAGNES EN BONNE SANTÉ PUBLICITAIRE (tableau)
   Colonnes : Campagne | Objectif | CPL vs moy. | CTR | Fréquence | Signal | Action
   Actions possibles : Maintenir / Augmenter budget prudemment / Dupliquer sur nouvelle audience
   Toujours justifier pourquoi cette campagne mérite d'être scalée

5. CAMPAGNES NÉCESSITANT UNE ATTENTION (tableau)
   Colonnes : Campagne | Indicateur en tension | Valeur actuelle | Benchmark | Hypothèse | Action
   Formuler une hypothèse sur la cause probable (fatigue créa ? audience saturée ? enchère trop basse ?)
   Proposer 1 action corrective concrète par campagne

6. CRÉAS GAGNANTES À EXPLOITER (tableau)
   Colonnes : Nom créa | CTR | CPC | CPL vs moy. | Fréquence | Leads | Recommandation
   Recommandations : Dupliquer en nouvel adset / Tester variantes / Augmenter budget adset
   Indiquer combien de temps encore cette créa peut performer (selon fréquence actuelle)

7. CRÉAS EN FIN DE VIE À RENOUVELER (tableau)
   Colonnes : Nom créa | Fréquence | CTR (évolution) | Signal de fatigue | Action immédiate
   Pour chaque créa fatiguée : proposer un brief créatif succinct (format + angle + accroche suggérée)

8. DIAGNOSTICS PUBLICITAIRES
   Pour chaque anomalie détectée, structure : Signal observé → Hypothèse → Action recommandée
   - 🎨 Fatigue créative : CTR en baisse sur 3J + fréquence > 3
   - 👥 Saturation d'audience : CPM en hausse + reach qui stagne + fréquence élevée
   - 📊 Trafic peu qualifié : CTR > 2% mais CPL élevé (clics sans conversion lead)
   - ⚙️ Anomalie de tracking : spend significatif mais leads = 0 (vérifier pixel et événement)
   - 💸 Sous-enchère : impressions en forte baisse malgré budget disponible (CPM anormalement bas)
   Note systématique : "Les données de conversion post-clic (ventes, inscriptions, etc.) sont suivies dans votre CRM ou outil de suivi externe."

9. PLAN D'ACTION PRIORITAIRE
   Format : [🔴 URGENT — aujourd'hui] / [🟡 IMPORTANT — cette semaine] / [🟢 OPTIMISATION — quand possible]
   Structure chaque action : Quoi faire → Où (campagne/adset/créa) → Impact attendu → Temps estimé
   Maximum 7 actions, classées par ordre d'impact potentiel.
   Ne recommander QUE des actions réalisables dans l'interface Meta Ads.

STYLE HTML :
- Tableaux avec en-têtes foncés (#1a1a2e texte blanc), lignes alternées légèrement colorées
- Badges de statut : vert (#22c55e), orange (#f59e0b), rouge (#ef4444), gris (#6b7280)
- Sections séparées par une bordure gauche colorée et un titre clair
- Police Arial, taille 13px, style inline uniquement
- Langue : FRANÇAIS professionnel et direct
- Retourne UNIQUEMENT le HTML sans balises html/head/body
"""

    headers = {
        "x-api-key": ANTHROPIC_API_KEY,
        "anthropic-version": "2023-06-01",
        "content-type": "application/json",
    }
    payload = {
        "model": "claude-opus-4-6",
        "max_tokens": 8000,
        "messages": [{"role": "user", "content": prompt}],
    }

    log.info("🤖 Analyse IA en cours...")
    resp = requests.post(
        "https://api.anthropic.com/v1/messages",
        headers=headers,
        json=payload,
        timeout=180,
    )
    if not resp.ok:
        print("ERREUR ANTHROPIC:", resp.status_code, resp.text)
    resp.raise_for_status()
    result = resp.json()
    return result["content"][0]["text"]

# ─────────────────────────────────────────────
# GÉNÉRATION EMAIL HTML
# ─────────────────────────────────────────────

def fmt(v, prefix="", suffix="", decimals=2):
    if v is None:
        return "N/A"
    if isinstance(v, float):
        return f"{prefix}{v:,.{decimals}f}{suffix}"
    return f"{prefix}{v:,}{suffix}"

def build_kpi_row(label, today_val, yesterday_val, avg_val, prefix="", suffix="", decimals=2):
    def arrow(today, ref):
        if today is None or ref is None:
            return "→", "#6b7280"
        if today > ref * 1.05:
            return "↑", "#22c55e"
        if today < ref * 0.95:
            return "↓", "#ef4444"
        return "→", "#f59e0b"

    arr_y, col_y = arrow(today_val, yesterday_val)
    arr_w, col_w = arrow(today_val, avg_val)

    return f"""
    <tr style="border-bottom:1px solid #f0f0f0;">
        <td style="padding:8px 14px; font-size:13px; color:#374151;">{label}</td>
        <td style="padding:8px 14px; text-align:right; font-weight:bold; font-size:13px;">{fmt(today_val, prefix, suffix, decimals)}</td>
        <td style="padding:8px 14px; text-align:center; font-size:16px; color:{col_y};">{arr_y}</td>
        <td style="padding:8px 14px; text-align:right; font-size:12px; color:#6b7280;">{fmt(yesterday_val, prefix, suffix, decimals)}</td>
        <td style="padding:8px 14px; text-align:center; font-size:16px; color:{col_w};">{arr_w}</td>
        <td style="padding:8px 14px; text-align:right; font-size:12px; color:#6b7280;">{fmt(avg_val, prefix, suffix, decimals)}</td>
    </tr>"""

def build_email_html(ai_report: str, data: dict, date_str: str) -> str:
    today_kpis     = aggregate_kpis(data["campaigns_today"])
    yesterday_kpis = aggregate_kpis(data["campaigns_yesterday"])
    avg_kpis       = compute_averages(data["campaigns_week"])

    header_row = """
    <tr style="background:#1a1a2e;">
        <th style="padding:10px 14px; text-align:left; color:white; font-size:12px;">KPI</th>
        <th style="padding:10px 14px; text-align:right; color:white; font-size:12px;">AUJOURD'HUI</th>
        <th style="padding:10px 14px; text-align:center; color:#a8b2d8; font-size:11px;">vs Hier</th>
        <th style="padding:10px 14px; text-align:right; color:#a8b2d8; font-size:12px;">HIER</th>
        <th style="padding:10px 14px; text-align:center; color:#a8b2d8; font-size:11px;">vs 7J</th>
        <th style="padding:10px 14px; text-align:right; color:#a8b2d8; font-size:12px;">MOY. 7J</th>
    </tr>"""

    kpi_rows = "".join([
        build_kpi_row("💰 Dépenses", today_kpis.get("spend"), yesterday_kpis.get("spend"), avg_kpis.get("spend"), "€", "", 2),
        build_kpi_row("👁 Impressions", today_kpis.get("impressions"), yesterday_kpis.get("impressions"), avg_kpis.get("impressions"), "", "", 0),
        build_kpi_row("🖱 Clics", today_kpis.get("clicks"), yesterday_kpis.get("clicks"), avg_kpis.get("clicks"), "", "", 0),
        build_kpi_row("📊 CTR", today_kpis.get("ctr"), yesterday_kpis.get("ctr"), avg_kpis.get("ctr"), "", "%", 2),
        build_kpi_row("💵 CPC", today_kpis.get("cpc"), yesterday_kpis.get("cpc"), avg_kpis.get("cpc"), "€", "", 2),
        build_kpi_row("📣 CPM", today_kpis.get("cpm"), yesterday_kpis.get("cpm"), avg_kpis.get("cpm"), "€", "", 2),
        build_kpi_row("🎯 Leads", today_kpis.get("leads"), yesterday_kpis.get("leads"), avg_kpis.get("leads"), "", "", 0),
        build_kpi_row("🛒 Achats", today_kpis.get("purchases"), yesterday_kpis.get("purchases"), avg_kpis.get("purchases"), "", "", 0),
        build_kpi_row("💎 Revenus", today_kpis.get("revenue"), yesterday_kpis.get("revenue"), avg_kpis.get("revenue"), "€", "", 2),
        build_kpi_row("🎯 CPL", today_kpis.get("cpl"), yesterday_kpis.get("cpl"), avg_kpis.get("cpl"), "€", "", 2),
        build_kpi_row("🎯 CPA", today_kpis.get("cpa"), yesterday_kpis.get("cpa"), avg_kpis.get("cpa"), "€", "", 2),
        build_kpi_row("🚀 ROAS", today_kpis.get("roas"), yesterday_kpis.get("roas"), avg_kpis.get("roas"), "", "x", 2),
    ])

    ads_count      = len(data["ads_today"])
    fatigued_count = len([a for a in data["ads_today"] if a["status"] == "🔴 FATIGUÉ"])
    ok_count       = len([a for a in data["ads_today"] if a["status"] == "🟢 OK"])

    html = f"""<!DOCTYPE html>
<html lang="fr">
<head><meta charset="UTF-8"><title>Rapport Meta Ads — {date_str}</title></head>
<body style="margin:0; padding:0; background:#f0f2f5; font-family:Arial,Helvetica,sans-serif;">
<table width="100%" cellpadding="0" cellspacing="0" style="background:#f0f2f5; padding:24px 0;">
<tr><td align="center">
<table width="680" cellpadding="0" cellspacing="0" style="max-width:680px; width:100%;">

  <!-- HEADER -->
  <tr>
    <td style="background:linear-gradient(135deg,#1a1a2e 0%,#16213e 50%,#0f3460 100%);
               padding:32px 40px; border-radius:12px 12px 0 0; text-align:center;">
      <p style="margin:0 0 6px; color:#e0aaff; font-size:12px; letter-spacing:3px; text-transform:uppercase;">Assistant IA Meta Ads</p>
      <h1 style="margin:0 0 8px; color:#ffffff; font-size:26px; font-weight:800;">📊 Rapport Quotidien</h1>
      <p style="margin:0 0 16px; color:#a8b2d8; font-size:14px;">{date_str}</p>
      <table cellpadding="0" cellspacing="0" style="margin:0 auto;">
        <tr>
          <td style="background:rgba(255,255,255,0.1); border-radius:8px; padding:10px 20px; text-align:center; margin:0 6px;">
            <p style="margin:0; color:#e0aaff; font-size:20px; font-weight:bold;">{len(data["campaigns_today"])}</p>
            <p style="margin:0; color:#a8b2d8; font-size:11px;">Campagnes</p>
          </td>
          <td style="width:12px;"></td>
          <td style="background:rgba(255,255,255,0.1); border-radius:8px; padding:10px 20px; text-align:center;">
            <p style="margin:0; color:#86efac; font-size:20px; font-weight:bold;">{ok_count}</p>
            <p style="margin:0; color:#a8b2d8; font-size:11px;">Créas OK</p>
          </td>
          <td style="width:12px;"></td>
          <td style="background:rgba(255,255,255,0.1); border-radius:8px; padding:10px 20px; text-align:center;">
            <p style="margin:0; color:#fca5a5; font-size:20px; font-weight:bold;">{fatigued_count}</p>
            <p style="margin:0; color:#a8b2d8; font-size:11px;">Créas fatiguées</p>
          </td>
          <td style="width:12px;"></td>
          <td style="background:rgba(255,255,255,0.1); border-radius:8px; padding:10px 20px; text-align:center;">
            <p style="margin:0; color:#fde68a; font-size:20px; font-weight:bold;">{ads_count}</p>
            <p style="margin:0; color:#a8b2d8; font-size:11px;">Total créas</p>
          </td>
        </tr>
      </table>
    </td>
  </tr>

  <!-- KPI TABLE -->
  <tr>
    <td style="background:#ffffff; padding:28px 40px; border-left:1px solid #e2e8f0; border-right:1px solid #e2e8f0;">
      <h2 style="margin:0 0 16px; color:#1a1a2e; font-size:15px; font-weight:700; text-transform:uppercase; letter-spacing:1px;">
        📈 KPI Comparatifs — Aujourd'hui vs Hier vs Moy. 7 jours
      </h2>
      <table style="width:100%; border-collapse:collapse; border-radius:8px; overflow:hidden;">
        <thead>{header_row}</thead>
        <tbody>{kpi_rows}</tbody>
      </table>
      <p style="margin:8px 0 0; font-size:11px; color:#9ca3af;">↑ Meilleur que référence &nbsp;|&nbsp; ↓ Moins bon &nbsp;|&nbsp; → Stable (±5%)</p>
    </td>
  </tr>

  <!-- RAPPORT IA -->
  <tr>
    <td style="background:#ffffff; padding:0 40px 32px; border-left:1px solid #e2e8f0; border-right:1px solid #e2e8f0;">
      <div style="border-top:2px solid #e2e8f0; padding-top:24px;">
        {ai_report}
      </div>
    </td>
  </tr>

  <!-- FOOTER -->
  <tr>
    <td style="background:#1a1a2e; padding:20px 40px; border-radius:0 0 12px 12px; text-align:center;">
      <p style="margin:0; color:#6b7280; font-size:11px;">
        Rapport généré automatiquement par votre Assistant IA Meta Ads v2
        &nbsp;·&nbsp; Powered by Anthropic Claude
      </p>
    </td>
  </tr>

</table>
</td></tr>
</table>
</body>
</html>"""
    return html


# ─────────────────────────────────────────────
# GÉNÉRATION PDF
# ─────────────────────────────────────────────

def generate_pdf(html_content: str) -> bytes | None:
    """Convertit le rapport HTML en PDF."""
    if not PDF_AVAILABLE:
        return None
    try:
        buffer = io.BytesIO()
        pisa.CreatePDF(html_content.encode("utf-8"), dest=buffer, encoding="utf-8")
        return buffer.getvalue()
    except Exception as e:
        log.warning(f"Erreur génération PDF : {e}")
        return None

# ─────────────────────────────────────────────
# ENVOI EMAIL
# ─────────────────────────────────────────────

def send_email(html_content: str, date_str: str, subject_prefix: str = "📊 Rapport Meta Ads", filename_prefix: str = "rapport_meta_ads"):
    msg = MIMEMultipart("mixed")
    msg["Subject"] = f"{subject_prefix} — {date_str}"
    msg["From"]    = EMAIL_SENDER
    msg["To"]      = EMAIL_RECIPIENT

    # Corps HTML
    msg.attach(MIMEText(html_content, "html", "utf-8"))

    # Pièce jointe PDF
    pdf_bytes = generate_pdf(html_content)
    if pdf_bytes:
        date_file = datetime.now().strftime("%Y-%m-%d")
        filename  = f"{filename_prefix}_{date_file}.pdf"
        part = MIMEBase("application", "octet-stream")
        part.set_payload(pdf_bytes)
        encoders.encode_base64(part)
        part.add_header("Content-Disposition", f"attachment; filename={filename}")
        msg.attach(part)
        log.info(f"📎 PDF joint : {filename}")
    else:
        log.info("⚠️ PDF non généré — email envoyé sans pièce jointe")

    log.info(f"📧 Envoi du rapport à {EMAIL_RECIPIENT}...")
    with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
        server.ehlo()
        server.starttls()
        server.login(EMAIL_SENDER, EMAIL_PASSWORD)
        server.sendmail(EMAIL_SENDER, EMAIL_RECIPIENT, msg.as_string())
    log.info("✅ Email envoyé avec succès !")

# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def main():
    log.info("=" * 55)
    log.info("🚀 Démarrage de l'Assistant IA Meta Ads v2")
    log.info("=" * 55)
    yesterday_str = (datetime.now() - timedelta(days=1)).strftime("%d/%m/%Y")
    date_str  = f"Journée du {yesterday_str} — rapport envoyé le {datetime.now().strftime('%d/%m/%Y à %H:%M')}"
    is_monday = datetime.now().weekday() == 0  # 0 = lundi

    # Rapport quotidien (tous les jours)
    data      = collect_all_data()
    ai_report = analyze_with_ai(data)
    html      = build_email_html(ai_report, data, date_str)
    send_email(html, date_str)

    # Rapport hebdomadaire (lundi uniquement)
    if is_monday:
        send_weekly_report()

    log.info("🎉 Mission accomplie ! Rapport(s) envoyé(s).")

if __name__ == "__main__":
    main()

# ─────────────────────────────────────────────
# RAPPORT HEBDOMADAIRE (lundi uniquement)
# ─────────────────────────────────────────────

def collect_weekly_data() -> dict:
    """Collecte les données des 7 derniers jours pour le rapport hebdo."""
    log.info("📡 Collecte des données hebdomadaires...")
    this_week  = get_date_range(7, 1)
    last_week  = get_date_range(14, 8)

    raw_this = fetch_insights(this_week, "campaign", CAMPAIGN_FIELDS)
    raw_last = fetch_insights(last_week, "campaign", CAMPAIGN_FIELDS)
    raw_ads  = fetch_insights(this_week, "ad", AD_FIELDS)

    return {
        "this_week":  [parse_campaign(c) for c in raw_this],
        "last_week":  [parse_campaign(c) for c in raw_last],
        "ads":        [parse_ad(a) for a in raw_ads],
        "dates":      {"this_week": this_week, "last_week": last_week},
    }


def analyze_weekly_with_ai(data: dict) -> str:
    """Analyse hebdomadaire stratégique avec Claude."""
    this_kpis  = aggregate_kpis(data["this_week"])
    last_kpis  = aggregate_kpis(data["last_week"])
    tunnel_this = aggregate_by_tunnel(data["this_week"])
    tunnel_last = aggregate_by_tunnel(data["last_week"])

    top_ads    = sorted(data["ads"], key=lambda x: x.get("roas") or 0, reverse=True)[:10]
    worst_ads  = sorted(data["ads"], key=lambda x: x.get("cpl") or 9999)[-5:]

    prompt = f"""Tu es un media buyer Meta Ads senior avec 10 ans d'expérience.
Tu produis un bilan stratégique hebdomadaire de santé publicitaire, clair et actionnable.

╔══════════════════════════════════════════════════════╗
║  PÉRIMÈTRE D'ANALYSE HEBDOMADAIRE                   ║
╚══════════════════════════════════════════════════════╝
Tu analyses la tendance publicitaire sur 7 jours — pas des chiffres ponctuels.
Tu n'as pas accès aux données post-clic (ventes, inscriptions, CRM, tunnel).
→ Tu évalues la trajectoire : est-ce que la semaine s'améliore ou se dégrade ?
→ Tu identifies les leviers à actionner pour la semaine suivante.
→ Tu restes dans ton périmètre : diffusion publicitaire Meta uniquement.

LOGIQUE HEBDOMADAIRE D'UN MEDIA BUYER PRO :
- Comparer S vs S-1 pour détecter les tendances structurelles
- Identifier les créas qui ont fait la semaine et celles qui arrivent en bout de course
- Équilibrer le budget TOF/MOF/BOF selon la maturité des audiences
- Préparer le pipeline créatif avant la semaine suivante
- Formuler un plan d'action concret, priorisé, réaliste

═══════════════════════════════════════
DONNÉES HEBDOMADAIRES
═══════════════════════════════════════
CETTE SEMAINE (S) : {json.dumps(this_kpis, indent=2, ensure_ascii=False)}
SEMAINE PRÉCÉDENTE (S-1) : {json.dumps(last_kpis, indent=2, ensure_ascii=False)}

NIVEAUX DE DIFFUSION CETTE SEMAINE : {json.dumps(tunnel_this, indent=2, ensure_ascii=False)}
NIVEAUX DE DIFFUSION S-1 : {json.dumps(tunnel_last, indent=2, ensure_ascii=False)}

MEILLEURES CRÉAS DE LA SEMAINE : {json.dumps(top_ads, indent=2, ensure_ascii=False)}
CRÉAS EN FIN DE COURSE : {json.dumps(worst_ads, indent=2, ensure_ascii=False)}

═══════════════════════════════════════
GRILLE D'INTERPRÉTATION HEBDOMADAIRE
═══════════════════════════════════════
Pour chaque KPI, évaluer la DIRECTION (amélioration / dégradation / stabilité) :

Évolution positive (semaine saine) :
  CPL en baisse ou stable | CTR en hausse ou stable | Fréquence maîtrisée < 3
  Volume leads stable ou en hausse | CPM stable ou en baisse

Signaux d'alerte hebdomadaires :
  CPL en hausse > +20% vs S-1 sur plusieurs campagnes = problème structurel
  CTR global en baisse sur toutes les campagnes = fatigue créative généralisée
  Fréquence > 3.5 sur plusieurs adsets = saturation des audiences
  Leads en baisse > 30% vs S-1 = volume insuffisant à investiguer
  CPM en forte hausse + reach en baisse = pression concurrentielle ou audience épuisée

Équilibre TOF/MOF/BOF sain :
  TOF : 40-50% du budget (alimentation du funnel)
  MOF : 20-30% du budget (nurturing et engagement)
  BOF : 20-30% du budget (conversion des leads chauds)
  Déséquilibre important = fuite dans le pipeline à corriger

═══════════════════════════════════════
STRUCTURE DU RAPPORT HEBDOMADAIRE (7 sections)
═══════════════════════════════════════

1. BILAN DE SANTÉ PUBLICITAIRE DE LA SEMAINE
   Score : 🟢 EXCELLENTE / 🟡 CORRECTE / 🔴 DÉGRADÉE
   Tableau comparatif S vs S-1 avec variation % : CPL | CTR | CPM | CPC | Fréquence | Leads | Spend
   3 faits marquants publicitaires de la semaine (ce qui a changé, en bien ou en mal)
   Verdict global en 3 phrases : ce qui a fonctionné, ce qui inquiète, ce qui est à surveiller

2. PERFORMANCE PAR NIVEAU DE DIFFUSION (S vs S-1)
   Tableau : Niveau | Spend S | Spend S-1 | Δ% Spend | CPL S | CPL S-1 | CTR S | Leads S | Tendance
   Pour chaque niveau : qualifier la diffusion (saine / en tension / dégradée)
   Analyser l'équilibre TOF/MOF/BOF — est-il cohérent avec un objectif de lead gen ?
   Recommandations de réallocation si déséquilibre détecté

3. TOP 5 CRÉAS DE LA SEMAINE (tableau)
   Colonnes : Créa | CTR moy. | CPC moy. | CPL | Fréquence fin de semaine | Leads | Durée de vie estimée
   Pour chaque créa : recommandation (prolonger / scaler / préparer la relève)
   Estimer combien de temps encore chaque créa peut performer (selon fréquence)

4. CRÉAS À RENOUVELER LA SEMAINE PROCHAINE (tableau)
   Colonnes : Créa | Fréquence moy. | CTR évolution | Signal de fatigue | Brief créatif suggéré
   Pour chaque créa fatiguée : proposer un brief concis (format + angle + accroche + audience)
   Prioriser les créas à remplacer en urgence vs celles qui peuvent attendre

5. ANALYSE STRATÉGIQUE DE LA DIFFUSION
   Lecture globale de la semaine d'un point de vue media buyer :
   - L'équilibre funnel est-il maintenu ? Où sont les tensions ?
   - Les audiences se renouvellent-elles suffisamment ?
   - Y a-t-il des signaux de fatigue structurelle (créas, audiences, offre) ?
   - Quelle est la tendance de fond : le compte s'améliore ou se dégrade sur ce mois ?
   Conclure par : "Les résultats de conversion post-clic (inscriptions, ventes) sont mesurés dans votre CRM ou outil de suivi externe et ne sont pas pris en compte dans cette analyse."

6. FEUILLE DE ROUTE PUBLICITAIRE — SEMAINE À VENIR
   3 priorités stratégiques concrètes pour la semaine (avec justification)
   Répartition budget recommandée par niveau (TOF/MOF/BOF) avec rationale
   Nouvelles créas à lancer : format suggéré + angle + audience cible + objectif

7. PLAN D'ACTION HEBDOMADAIRE (priorisé et actionnable)
   Format : [🔴 LUNDI MATIN] / [🟡 MILIEU DE SEMAINE] / [🟢 VENDREDI — BILAN]
   Pour chaque action : Quoi → Où (campagne/adset) → Pourquoi → Impact attendu
   Minimum 6 actions, maximum 10, toutes réalisables dans Meta Ads

STYLE HTML : tableaux structurés avec en-têtes colorés, badges de statut, style inline, Arial 13px, langue française professionnelle.
Retourne UNIQUEMENT le HTML sans balises html/head/body.
"""

    headers = {
        "x-api-key": ANTHROPIC_API_KEY,
        "anthropic-version": "2023-06-01",
        "content-type": "application/json",
    }
    payload = {
        "model": "claude-opus-4-6",
        "max_tokens": 8000,
        "messages": [{"role": "user", "content": prompt}],
    }

    log.info("🤖 Analyse hebdomadaire IA en cours...")
    resp = requests.post(
        "https://api.anthropic.com/v1/messages",
        headers=headers,
        json=payload,
        timeout=180,
    )
    if not resp.ok:
        print("ERREUR ANTHROPIC:", resp.status_code, resp.text)
    resp.raise_for_status()
    return resp.json()["content"][0]["text"]


def build_weekly_email_html(ai_report: str, data: dict, date_str: str) -> str:
    """Template email pour le rapport hebdomadaire."""
    this_kpis = aggregate_kpis(data["this_week"])
    last_kpis = aggregate_kpis(data["last_week"])

    def pct_change(today, ref):
        if not today or not ref or ref == 0:
            return "N/A", "#6b7280"
        pct = (today - ref) / ref * 100
        color = "#22c55e" if pct > 0 else "#ef4444"
        sign  = "+" if pct > 0 else ""
        return f"{sign}{pct:.1f}%", color

    rows = ""
    metrics = [
        ("💰 Dépenses", "spend", "€", 2),
        ("🎯 Leads", "leads", "", 0),
        ("🛒 Achats", "purchases", "", 0),
        ("💎 Revenus", "revenue", "€", 2),
        ("📊 CTR", "ctr", "%", 2),
        ("🎯 CPL", "cpl", "€", 2),
        ("🎯 CPA", "cpa", "€", 2),
        ("🚀 ROAS", "roas", "x", 2),
    ]
    for label, key, suffix, dec in metrics:
        tv = this_kpis.get(key)
        lv = last_kpis.get(key)
        pct, col = pct_change(tv, lv)
        rows += f"""<tr style="border-bottom:1px solid #f0f0f0;">
            <td style="padding:8px 14px; font-size:13px;">{label}</td>
            <td style="padding:8px 14px; text-align:right; font-weight:bold;">{fmt(tv, '', suffix, dec)}</td>
            <td style="padding:8px 14px; text-align:right;">{fmt(lv, '', suffix, dec)}</td>
            <td style="padding:8px 14px; text-align:center; color:{col}; font-weight:bold;">{pct}</td>
        </tr>"""

    return f"""<!DOCTYPE html>
<html lang="fr">
<head><meta charset="UTF-8"><title>Bilan Hebdomadaire Meta Ads — {date_str}</title></head>
<body style="margin:0; padding:0; background:#f0f2f5; font-family:Arial,Helvetica,sans-serif;">
<table width="100%" cellpadding="0" cellspacing="0" style="background:#f0f2f5; padding:24px 0;">
<tr><td align="center">
<table width="680" cellpadding="0" cellspacing="0" style="max-width:680px; width:100%;">

  <tr>
    <td style="background:linear-gradient(135deg,#0f3460 0%,#533483 50%,#e94560 100%);
               padding:32px 40px; border-radius:12px 12px 0 0; text-align:center;">
      <p style="margin:0 0 6px; color:#fde68a; font-size:12px; letter-spacing:3px; text-transform:uppercase;">
        Bilan Stratégique Hebdomadaire
      </p>
      <h1 style="margin:0 0 8px; color:#ffffff; font-size:26px; font-weight:800;">
        📅 Rapport Semaine
      </h1>
      <p style="margin:0; color:#e2e8f0; font-size:14px;">{date_str}</p>
    </td>
  </tr>

  <tr>
    <td style="background:#ffffff; padding:28px 40px; border-left:1px solid #e2e8f0; border-right:1px solid #e2e8f0;">
      <h2 style="margin:0 0 16px; color:#1a1a2e; font-size:15px; font-weight:700; text-transform:uppercase; letter-spacing:1px;">
        📊 Cette semaine vs Semaine précédente
      </h2>
      <table style="width:100%; border-collapse:collapse;">
        <thead>
          <tr style="background:#1a1a2e;">
            <th style="padding:10px 14px; text-align:left; color:white; font-size:12px;">KPI</th>
            <th style="padding:10px 14px; text-align:right; color:white; font-size:12px;">CETTE SEMAINE</th>
            <th style="padding:10px 14px; text-align:right; color:#a8b2d8; font-size:12px;">SEMAINE PRÉC.</th>
            <th style="padding:10px 14px; text-align:center; color:#a8b2d8; font-size:12px;">ÉVOLUTION</th>
          </tr>
        </thead>
        <tbody>{rows}</tbody>
      </table>
    </td>
  </tr>

  <tr>
    <td style="background:#ffffff; padding:0 40px 32px; border-left:1px solid #e2e8f0; border-right:1px solid #e2e8f0;">
      <div style="border-top:2px solid #e2e8f0; padding-top:24px;">
        {ai_report}
      </div>
    </td>
  </tr>

  <tr>
    <td style="background:#0f3460; padding:20px 40px; border-radius:0 0 12px 12px; text-align:center;">
      <p style="margin:0; color:#6b7280; font-size:11px;">
        Bilan hebdomadaire automatique — Assistant IA Meta Ads v2 · Powered by Anthropic Claude
      </p>
    </td>
  </tr>

</table>
</td></tr>
</table>
</body>
</html>"""


def send_weekly_report():
    """Génère et envoie le rapport hebdomadaire."""
    log.info("📅 C'est lundi — génération du rapport hebdomadaire...")
    date_str    = datetime.now().strftime("Semaine du %d/%m/%Y")
    data        = collect_weekly_data()
    ai_report   = analyze_weekly_with_ai(data)
    html        = build_weekly_email_html(ai_report, data, date_str)

    send_email(html, date_str,
               subject_prefix="📅 Bilan Hebdomadaire Meta Ads",
               filename_prefix="bilan_hebdomadaire_meta_ads")
    log.info("✅ Rapport hebdomadaire envoyé !")
