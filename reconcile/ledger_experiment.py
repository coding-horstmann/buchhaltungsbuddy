from __future__ import annotations

import pandas as pd


LEDGER_EXPERIMENT_COLUMNS = [
    "doc_id",
    "doc_ref",
    "doc_type",
    "date",
    "amount",
    "platform",
    "counterparty",
    "main_status",
    "ledger_status",
    "ledger_source",
    "ledger_date",
    "ledger_amount",
    "ledger_category",
    "bank_bridge",
    "confidence",
    "recommendation",
    "note",
]

EXTERNAL_ACCOUNT_HINTS = {
    "cursor",
    "google",
    "pinterest",
    "microsoft",
    "ideogram",
    "runware",
}


def build_ledger_experiment_report(evidence: pd.DataFrame) -> pd.DataFrame:
    """Separate explanatory report; it does not change any match status."""
    if evidence.empty:
        return pd.DataFrame(columns=LEDGER_EXPERIMENT_COLUMNS)

    rows: list[dict[str, object]] = []
    work = evidence.copy()
    work["date"] = pd.to_datetime(work["date"], errors="coerce")
    for _, row in work.sort_values(["evidence_level", "date", "doc_ref"]).iterrows():
        main_status = text(row.get("evidence_level"))
        if main_status == "vollständig belegt":
            continue
        rows.append(ledger_row(row))
    if not rows:
        return pd.DataFrame(columns=LEDGER_EXPERIMENT_COLUMNS)
    return pd.DataFrame(rows, columns=LEDGER_EXPERIMENT_COLUMNS)


def ledger_row(row: pd.Series) -> dict[str, object]:
    has_platform = bool(text(row.get("platform_detail_ids")))
    has_paypal = bool(text(row.get("paypal_detail_ids")))
    has_direct_bank = bool(text(row.get("fyrst_tx_ids")) or text(row.get("bridge_fyrst_tx_ids")) or text(row.get("platform_bridge_fyrst_tx_ids")))
    categories = text(row.get("platform_categories"))
    platform = text(row.get("platforms")) or text(row.get("platform"))
    counterparty = text(row.get("counterparty"))

    if has_platform:
        ledger_status = platform_ledger_status(categories)
        ledger_source = platform_label(platform) or "Plattform"
        ledger_date = first_part(row.get("platform_dates"))
        ledger_amount = first_part(row.get("platform_amounts"))
        ledger_category = categories
        recommendation = "Zusatznachweis prüfen; Hauptstatus nicht automatisch umstufen."
        note = "Beleg ist in Plattformdaten enthalten, aber die Bankkette ist im Hauptmatching nicht vollständig geschlossen."
    elif has_paypal:
        ledger_status = "PayPal-Ledger belegt"
        ledger_source = "PayPal"
        ledger_date = first_part(row.get("paypal_dates"))
        ledger_amount = first_part(row.get("paypal_amounts"))
        ledger_category = "paypal_detail"
        recommendation = "PayPal-Detail prüfen; Bankumbuchung ggf. nur gesammelt nachvollziehbar."
        note = "Beleg ist in PayPal enthalten, aber nicht vollständig bis FYRST geschlossen."
    elif likely_external_account(row):
        ledger_status = "wahrscheinlich anderes Konto"
        ledger_source = "kein Plattformledger"
        ledger_date = ""
        ledger_amount = ""
        ledger_category = ""
        recommendation = "Manuell als anderes Konto markieren, wenn der Zahlungsnachweis dort liegt."
        note = "Der Beleg wirkt nicht wie ein fehlendes Plattformmatching, sondern wie ein Zahlungsweg außerhalb des FYRST-Uploads."
    else:
        ledger_status = "kein Ledgernachweis"
        ledger_source = ""
        ledger_date = ""
        ledger_amount = ""
        ledger_category = ""
        recommendation = "Beleg oder Zahlungsweg manuell prüfen."
        note = "In den importierten Plattform-, PayPal- und Bankdaten wurde kein belastbarer Ledgerbezug gefunden."

    return {
        "doc_id": row.get("doc_id", ""),
        "doc_ref": row.get("doc_ref", ""),
        "doc_type": row.get("doc_type", ""),
        "date": row.get("date"),
        "amount": row.get("amount"),
        "platform": platform,
        "counterparty": counterparty,
        "main_status": row.get("evidence_level", ""),
        "ledger_status": ledger_status,
        "ledger_source": ledger_source,
        "ledger_date": ledger_date,
        "ledger_amount": ledger_amount,
        "ledger_category": ledger_category,
        "bank_bridge": "ja" if has_direct_bank else "nein",
        "confidence": row.get("confidence", ""),
        "recommendation": recommendation,
        "note": note,
    }


def platform_ledger_status(categories: str) -> str:
    category_text = categories.lower()
    if "fee_month" in category_text:
        return "Monats-/Gebührenledger belegt"
    if "ledger_" in category_text:
        return "Statement-Ledger belegt"
    if "wallet" in category_text:
        return "Wallet-Ledger belegt"
    if "charge" in category_text:
        return "Lieferantenledger belegt"
    return "Plattformdetail belegt"


def likely_external_account(row: pd.Series) -> bool:
    combined = " ".join(text(row.get(field)).lower() for field in ["platform", "counterparty", "description"])
    return any(hint in combined for hint in EXTERNAL_ACCOUNT_HINTS)


def first_part(value: object) -> str:
    parts = [part.strip() for part in text(value).split("|") if part.strip()]
    return parts[0] if parts else ""


def text(value: object) -> str:
    if value is None or pd.isna(value):
        return ""
    return str(value).strip()


def platform_label(value: object) -> str:
    raw = text(value)
    mapping = {
        "etsy": "Etsy",
        "ebay": "eBay",
        "paypal": "PayPal",
        "shopify": "Shopify",
        "printful": "Printful",
        "gelato": "Gelato",
        "ionos": "IONOS",
    }
    labels: list[str] = []
    for part in raw.split("|"):
        key = part.strip().lower()
        if not key:
            continue
        label = mapping.get(key, part.strip())
        if label not in labels:
            labels.append(label)
    return " + ".join(labels)
