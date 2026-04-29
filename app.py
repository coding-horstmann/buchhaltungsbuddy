from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pandas as pd

try:
    import streamlit as st
except ModuleNotFoundError as exc:  # pragma: no cover - startup helper
    print("Streamlit ist nicht installiert. Bitte ausfuehren: pip install -r requirements.txt")
    raise SystemExit(1) from exc

from reconcile.evidence import build_document_evidence, build_open_bank_after_evidence
from reconcile.annual_reports import build_etsy_accountable_comparison, build_etsy_annual_reconciliation
from reconcile.leftovers import build_hypothesis_candidate_report, build_leftover_candidate_report
from reconcile.ledger_experiment import build_ledger_experiment_report
from reconcile.llm import LlmConfig, dataframe_records, suggest_match
from reconcile.matching import (
    MatchSettings,
    candidate_docs_for_tx,
    reconcile,
    top_candidates_for_doc,
)
from reconcile.paypal import match_docs_to_paypal, match_paypal_transfers_to_bank
from reconcile.parsers import parse_accountable_file, parse_bank_file, parse_paypal_file
from reconcile.platform_packages import build_platform_package_matches
from reconcile.platforms import (
    assign_platform_payout_ids,
    deduplicate_platform_transactions,
    match_docs_to_platform,
    match_platform_payouts_to_bank,
    parse_platform_file,
)
from reconcile.plausibility import build_overall_plausibility_report
from reconcile.storage import save_run_to_sqlite
from reconcile.usage import build_bank_claim_usage


BASE_DIR = Path(__file__).resolve().parent
APP_STATE_VERSION = "tolerant-paypal-platform-v2"


def main() -> None:
    st.set_page_config(page_title="Buchhaltungs Buddy", layout="wide")
    inject_css()
    if st.session_state.get("app_state_version") != APP_STATE_VERSION:
        st.session_state.clear()
        st.session_state["app_state_version"] = APP_STATE_VERSION
        st.session_state["analysis_requested"] = False
    if "upload_nonce" not in st.session_state:
        st.session_state["upload_nonce"] = 0
    if "analysis_requested" not in st.session_state:
        st.session_state["analysis_requested"] = False
    if "external_payment_notes" not in st.session_state:
        st.session_state["external_payment_notes"] = {}
    if "manual_report_edits" not in st.session_state:
        st.session_state["manual_report_edits"] = {}
    if "manual_report_summary_note" not in st.session_state:
        st.session_state["manual_report_summary_note"] = ""

    st.title("Buchhaltungs Buddy")

    with st.sidebar:
        st.header("Import")
        if st.button("Hochgeladene Dateien entfernen", use_container_width=True):
            st.cache_data.clear()
            st.session_state["upload_nonce"] += 1
            st.session_state["analysis_requested"] = False
            st.rerun()

        nonce = st.session_state["upload_nonce"]
        accountable_upload = st.file_uploader("Accountable Excel/CSV", type=["xlsx", "xls", "csv"], key=f"accountable_{nonce}")
        bank_uploads = st.file_uploader("Bank-CSV Dateien", type=["csv"], accept_multiple_files=True, key=f"banks_{nonce}")
        paypal_upload = st.file_uploader("PayPal CSV optional", type=["csv"], key=f"paypal_{nonce}")
        st.caption("PayPal wird als Zwischenkonto ausgewertet und nicht als zusätzlicher Bankumsatz gezählt.")
        platform_uploads = st.file_uploader(
            "Plattform-CSV optional",
            type=["csv"],
            accept_multiple_files=True,
            key=f"platforms_{nonce}",
            help="Etsy SoldOrderItems, eBay Transaktionsbericht und Shopify Orders werden als Bestell-/Gebührendetails genutzt.",
        )
        st.caption("Etsy, eBay und Shopify werden als Plattformdetails genutzt. Sie zählen nicht als Bankkonto.")

        with st.expander("Matching-Einstellungen", expanded=False):
            direct_window = st.slider("1:1 Zeitfenster (Tage)", 0, 45, 10)
            batch_window = st.slider("Batch Zeitfenster (Tage)", 1, 60, 14)
            tolerance_cents = st.slider("Rundungstoleranz (Cent)", 0, 100, 15)
            max_fee_abs = st.number_input("Max. Gebührenlücke EUR", min_value=0.0, max_value=1000.0, value=35.0, step=5.0)
            max_fee_pct = st.slider("Max. Gebührenlücke %", 0, 60, 18) / 100
            max_candidates = st.slider("Batch-Kandidaten", 6, 24, 8)
            include_fee_documents = st.toggle("Gebühren-Belege in Netto-Batches nutzen", value=True)
            batch_outgoing = st.toggle("Ausgaben-Batches suchen", value=False)
            st.caption("KI ist hier nicht automatisch aktiv. Sie wird nur im KI-Fallback-Tab per Klick verwendet.")

        if st.button("Analyse starten / aktualisieren", type="primary", use_container_width=True):
            st.session_state["analysis_requested"] = True

    settings = MatchSettings(
        direct_window_days=direct_window,
        batch_window_days=batch_window,
        tolerance_cents=tolerance_cents,
        max_fee_abs=max_fee_abs,
        max_fee_pct=max_fee_pct,
        max_batch_candidates=max_candidates,
        include_fee_documents=include_fee_documents,
        batch_outgoing=batch_outgoing,
    )

    accountable_file = uploaded_to_tuple(accountable_upload)
    bank_files = [uploaded_to_tuple(file) for file in bank_uploads]
    paypal_file = uploaded_to_tuple(paypal_upload)
    platform_files = [uploaded_to_tuple(file) for file in platform_uploads]

    if accountable_file is None or not bank_files:
        show_required_data()
        st.info("Bitte Accountable-Datei und mindestens eine Bank-CSV laden.")
        return

    if not st.session_state["analysis_requested"]:
        requirement_tab, upload_tab = st.tabs(["BENÖTIGTE DATEN", "Aktueller Upload"])
        with requirement_tab:
            show_required_data()
        with upload_tab:
            st.success("Dateien sind ausgewählt. Starte die Analyse links in der Sidebar, wenn du bereit bist.")
            st.dataframe(
                pd.DataFrame(
                    [
                        {"Bereich": "Accountable", "Status": "bereit", "Dateien": accountable_file[0]},
                        {"Bereich": "Bank", "Status": "bereit", "Dateien": ", ".join(name for name, _ in bank_files)},
                        {"Bereich": "PayPal", "Status": "optional bereit" if paypal_file else "nicht geladen", "Dateien": paypal_file[0] if paypal_file else ""},
                        {"Bereich": "Plattformen", "Status": "optional bereit" if platform_files else "nicht geladen", "Dateien": ", ".join(name for name, _ in platform_files)},
                    ]
                ),
                hide_index=True,
                use_container_width=True,
            )
        st.info("Das normale Komplett-Matching nutzt keine KI. KI wird nur später im KI-Fallback-Tab per Klick verwendet.")
        return

    with st.spinner("Importiere Dateien und berechne deterministische Matches..."):
        docs, bank, paypal, platform_transactions, import_issues = parse_inputs(accountable_file, bank_files, paypal_file, platform_files)
        if docs.empty or bank.empty:
            st.error("Es wurden nicht genug Daten importiert.")
            show_import_issues(import_issues)
            return

        matches, links = reconcile(docs, bank, settings)
        paypal_doc_matches, paypal_doc_links = match_docs_to_paypal(docs, paypal, settings)
        paypal_bank_matches = match_paypal_transfers_to_bank(paypal, bank, settings)
        platform_doc_matches, platform_doc_links = match_docs_to_platform(docs, platform_transactions, settings)
        platform_bank_matches = match_platform_payouts_to_bank(platform_transactions, bank, settings)
        etsy_annual_report = build_etsy_annual_reconciliation(platform_transactions)
        platform_package_matches, platform_package_links, platform_package_report = build_platform_package_matches(
            docs,
            bank,
            matches,
            links,
            platform_transactions,
            platform_doc_matches,
            platform_doc_links,
            platform_bank_matches,
            settings,
        )
        if not platform_package_matches.empty:
            matches = pd.concat([matches, platform_package_matches], ignore_index=True)
            links = pd.concat([links, platform_package_links], ignore_index=True)
        document_evidence = build_document_evidence(
            docs,
            bank,
            paypal,
            matches,
            links,
            paypal_doc_matches,
            paypal_doc_links,
            paypal_bank_matches,
            platform_transactions,
            platform_doc_matches,
            platform_doc_links,
            platform_bank_matches,
        )
        open_doc_ids = set(document_evidence.loc[document_evidence["evidence_level"] == "offen", "doc_id"])
        open_docs = docs[docs["doc_id"].isin(open_doc_ids)].copy()
        open_bank = build_open_bank_after_evidence(bank, matches, paypal_bank_matches, platform_bank_matches)
        bank_claim_usage = build_bank_claim_usage(document_evidence, bank)
        overall_plausibility_report = build_overall_plausibility_report(docs, bank, paypal, document_evidence, bank_claim_usage)
        etsy_accountable_comparison = build_etsy_accountable_comparison(document_evidence, platform_transactions)
        leftover_candidate_report = build_leftover_candidate_report(open_docs, open_bank, settings)

    show_import_issues(import_issues)
    show_summary(
        docs,
        bank,
        matches,
        links,
        open_docs,
        open_bank,
        paypal,
        paypal_doc_links,
        paypal_bank_matches,
        document_evidence,
        platform_transactions,
        platform_doc_matches,
        platform_bank_matches,
        platform_package_report,
    )
    pdf_bundle, pdf_error = prepare_pdf_report_bundle(
        docs,
        bank,
        paypal,
        matches,
        links,
        settings,
        paypal_bank_matches,
        document_evidence,
        platform_transactions,
        platform_bank_matches,
        platform_package_report,
        etsy_annual_report,
        etsy_accountable_comparison,
        leftover_candidate_report,
    )
    with st.expander("PDF-Reports herunterladen", expanded=True):
        show_pdf_download_buttons(pdf_bundle, pdf_error, key_prefix="top")

    tabs = st.tabs(["BENÖTIGTE DATEN", "Belegketten", "FYRST-Matches", "Umsatz-Auslastung", "Plausibilität", "Plattformdaten", "Offene Belege", "Offene Bankumsätze", "Restprüfung", "PayPal-Brücke", "KI-Fallback", "Export"])
    with tabs[0]:
        show_required_data()
    with tabs[1]:
        show_document_evidence(document_evidence)
        show_manual_report_editor(document_evidence)
    with tabs[2]:
        show_matches(matches, links)
    with tabs[3]:
        show_bank_claim_usage(bank_claim_usage)
    with tabs[4]:
        show_overall_plausibility(overall_plausibility_report)
    with tabs[5]:
        show_platform_support(platform_transactions, platform_doc_matches, platform_doc_links, platform_bank_matches, platform_package_report, etsy_annual_report, etsy_accountable_comparison)
    with tabs[6]:
        show_open_docs(open_docs, bank)
    with tabs[7]:
        show_open_bank(open_bank)
    with tabs[8]:
        show_leftover_review(open_docs, open_bank)
    with tabs[9]:
        show_paypal_bridge(paypal, paypal_doc_matches, paypal_doc_links, paypal_bank_matches)
    with tabs[10]:
        show_llm_fallback(open_bank, open_docs, settings)
    with tabs[11]:
        show_export(
            docs,
            bank,
            matches,
            links,
            import_issues,
            settings,
            paypal,
            paypal_doc_matches,
            paypal_doc_links,
            paypal_bank_matches,
            document_evidence,
            platform_transactions,
            platform_doc_matches,
            platform_doc_links,
            platform_bank_matches,
            platform_package_report,
            etsy_annual_report,
            etsy_accountable_comparison,
            leftover_candidate_report,
            bank_claim_usage,
            overall_plausibility_report,
            pdf_bundle,
            pdf_error,
        )


def uploaded_to_tuple(upload) -> tuple[str, bytes] | None:
    if upload is None:
        return None
    return upload.name, upload.getvalue()


def show_required_data() -> None:
    st.markdown(
        """
        <div class="required-data-callout">
            <div class="required-data-title">Benötigte Daten für einen guten Abgleich</div>
            <div class="required-data-copy">
                Pflicht sind Accountable und FYRST. PayPal- und Plattformdateien machen die Sammelzahlungen nachvollziehbar und verbessern vor allem Etsy, eBay, Shopify, Printful und Gelato.
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    required = pd.DataFrame(
        [
            {
                "Priorität": "Pflicht",
                "Datei / Export": "Accountable Export",
                "Typischer Dateiname": "Daten.xlsx oder Accountable CSV",
                "Wofür": "Alle Einnahmen, Ausgaben, Belegnummern, Beträge und Gegenparteien.",
            },
            {
                "Priorität": "Pflicht",
                "Datei / Export": "FYRST Bank-CSV",
                "Typischer Dateiname": "fyrst.csv",
                "Wofür": "Hauptkonto: Zahlungseingänge, Lastschriften, Auszahlungen und Sammelumsätze.",
            },
            {
                "Priorität": "Sehr wichtig bei PayPal",
                "Datei / Export": "PayPal CSV",
                "Typischer Dateiname": "paypal.CSV",
                "Wofür": "PayPal als Zwischenkonto: Plattformzahlungen und PayPal-Lastschriften zu FYRST erklären.",
            },
        ]
    )
    st.subheader("Startdaten")
    st.dataframe(required, hide_index=True, use_container_width=True)

    platform_rows = [
        {
            "Plattform": "Etsy",
            "Benötigte Exporte": "Etsy Payments-Verkäufe; Etsy Payments-Überweisungen; monatliche etsy_statement-Dateien",
            "Wofür": "Bestellungen, Käufer, Gebühren, Auszahlungen, Monats-/Jahresabgleich je Shop.",
        },
        {
            "Plattform": "eBay",
            "Benötigte Exporte": "eBay Alle Bestellungen unter Berichte; eBay Abrechnungsübersicht Alle",
            "Wofür": "Käufer, Bestellungen, Gebühren, Anzeigenkosten und eBay-Abrechnungen.",
        },
        {
            "Plattform": "Shopify",
            "Benötigte Exporte": "Shopify einfache Bestellungen; Shopify Abrechnung Gebührentabelle",
            "Wofür": "Shopify-Bestellungen, Kunden, Gebühren und Zahlungs-/Abrechnungsdetails.",
        },
        {
            "Plattform": "Printful",
            "Benötigte Exporte": "Printful CSV",
            "Wofür": "Produktions- und Lieferantenkosten zu Accountable-Ausgaben und PayPal/FYRST-Zahlungen.",
        },
        {
            "Plattform": "Gelato",
            "Benötigte Exporte": "Gelato Kontoauszug CSV",
            "Wofür": "Gelato-Ausgaben, Wallet-/Zahlungsbewegungen und Abgleich zu PayPal/FYRST.",
        },
    ]
    st.subheader("Plattformdaten")
    st.dataframe(pd.DataFrame(platform_rows), hide_index=True, use_container_width=True)

    st.caption("Nicht jede Plattformdatei ist immer Pflicht. Für einen belastbaren Report sollten aber alle Plattformen hochgeladen werden, über die im Zeitraum Umsätze oder Gebühren liefen.")


def inject_css() -> None:
    st.markdown(
        """
        <style>
        :root {
            --app-ink: #1f2937;
            --app-muted: #6b7280;
            --app-line: #e5e7eb;
            --app-accent: #2563eb;
        }
        .block-container {
            padding-top: 2rem;
            padding-bottom: 3rem;
        }
        div[data-testid="stMetric"] {
            background: #ffffff;
            border: 1px solid var(--app-line);
            border-radius: 8px;
            padding: 0.85rem 0.95rem;
        }
        div[data-testid="stMetric"] * {
            opacity: 1 !important;
        }
        div[data-testid="stMetric"] [data-testid="stMetricLabel"],
        div[data-testid="stMetric"] [data-testid="stMetricLabel"] *,
        div[data-testid="stMetricLabel"] p,
        div[data-testid="stMetricLabel"] *,
        div[data-testid="stMetricDelta"] p,
        div[data-testid="stMetricDelta"] * {
            color: var(--app-muted) !important;
            -webkit-text-fill-color: var(--app-muted) !important;
            fill: var(--app-muted) !important;
        }
        div[data-testid="stMetricValue"],
        div[data-testid="stMetricValue"] * {
            color: var(--app-ink) !important;
            -webkit-text-fill-color: var(--app-ink) !important;
        }
        button[kind="primary"],
        button[data-testid="stBaseButton-primary"] {
            background: var(--app-accent) !important;
            border-color: var(--app-accent) !important;
            color: #ffffff !important;
        }
        button[kind="primary"]:hover,
        button[data-testid="stBaseButton-primary"]:hover {
            background: #1d4ed8 !important;
            border-color: #1d4ed8 !important;
            color: #ffffff !important;
        }
        .stTabs [data-baseweb="tab-list"] {
            gap: 0.25rem;
            border-bottom: 1px solid var(--app-line);
        }
        .stTabs [data-baseweb="tab"] {
            color: var(--app-ink) !important;
            background: transparent;
            border-radius: 6px 6px 0 0;
            padding: 0.55rem 0.85rem;
        }
        .stTabs [data-baseweb="tab"]:first-child {
            background: #eff6ff;
            border: 1px solid #bfdbfe;
            border-bottom: 0;
        }
        .stTabs [data-baseweb="tab"]:first-child p {
            color: #1d4ed8 !important;
            font-weight: 800;
        }
        .stTabs [data-baseweb="tab"] p {
            color: var(--app-ink) !important;
            font-weight: 600;
        }
        .stTabs [aria-selected="true"] {
            border-bottom: 3px solid var(--app-accent);
        }
        .stTabs [aria-selected="true"] p {
            color: var(--app-accent) !important;
        }
        div[data-testid="stDataFrame"] {
            border: 1px solid var(--app-line);
            border-radius: 8px;
        }
        section[data-testid="stSidebar"] {
            border-right: 1px solid var(--app-line);
        }
        .required-data-callout {
            background: #eff6ff;
            border: 1px solid #bfdbfe;
            border-left: 6px solid var(--app-accent);
            border-radius: 8px;
            padding: 1rem 1.1rem;
            margin: 0.4rem 0 1rem 0;
        }
        .required-data-title {
            color: #1e3a8a;
            font-size: 1.05rem;
            font-weight: 800;
            margin-bottom: 0.25rem;
        }
        .required-data-copy {
            color: var(--app-ink);
            font-size: 0.95rem;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


@st.cache_data(show_spinner=False)
def parse_inputs(
    accountable_file: tuple[str, bytes],
    bank_files: list[tuple[str, bytes]],
    paypal_file: tuple[str, bytes] | None,
    platform_files: list[tuple[str, bytes]],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, list[str]]:
    issues: list[str] = []
    accountable = parse_accountable_file(*accountable_file)
    issues.extend(accountable.issues)

    bank_frames = []
    for name, data in bank_files:
        parsed = parse_bank_file(name, data)
        issues.extend(parsed.issues)
        if not parsed.frame.empty:
            bank_frames.append(parsed.frame)

    bank = pd.concat(bank_frames, ignore_index=True) if bank_frames else pd.DataFrame()
    paypal = pd.DataFrame()
    if paypal_file is not None:
        parsed_paypal = parse_paypal_file(*paypal_file)
        issues.extend(parsed_paypal.issues)
        paypal = parsed_paypal.frame

    platform_frames = []
    for name, data in platform_files:
        parsed_platform = parse_platform_file(name, data)
        issues.extend(parsed_platform.issues)
        if not parsed_platform.frame.empty:
            platform_frames.append(parsed_platform.frame)
    platform_transactions = pd.concat(platform_frames, ignore_index=True) if platform_frames else pd.DataFrame()
    platform_transactions = assign_platform_payout_ids(platform_transactions)
    platform_transactions = deduplicate_platform_transactions(platform_transactions)

    return accountable.frame, bank, paypal, platform_transactions, issues


def show_import_issues(issues: list[str]) -> None:
    if not issues:
        return
    with st.expander("Import-Protokoll", expanded=False):
        for issue in issues:
            if "Fehler" in issue or "nicht erkannt" in issue or "keine Bank-CSV" in issue:
                st.warning(issue)
            else:
                st.write(issue)


def show_summary(
    docs: pd.DataFrame,
    bank: pd.DataFrame,
    matches: pd.DataFrame,
    links: pd.DataFrame,
    open_docs: pd.DataFrame,
    open_bank: pd.DataFrame,
    paypal: pd.DataFrame,
    paypal_doc_links: pd.DataFrame,
    paypal_bank_matches: pd.DataFrame,
    document_evidence: pd.DataFrame,
    platform_transactions: pd.DataFrame,
    platform_doc_matches: pd.DataFrame,
    platform_bank_matches: pd.DataFrame,
    platform_package_report: pd.DataFrame,
) -> None:
    matched_doc_count = links["doc_id"].nunique() if not links.empty else 0
    paypal_doc_count = paypal_doc_links["doc_id"].nunique() if not paypal_doc_links.empty else 0
    platform_doc_count = platform_doc_matches["match_id"].nunique() if not platform_doc_matches.empty else 0
    if document_evidence.empty:
        complete_count = matched_doc_count
        partial_count = 0
    else:
        status_counts = document_evidence["evidence_level"].value_counts()
        complete_count = int(status_counts.get("vollständig belegt", 0))
        partial_count = int(status_counts.get("teilweise belegt", 0))
    complete_rate = complete_count / len(docs) if len(docs) else 0

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Belege", f"{len(docs):,}".replace(",", "."))
    c2.metric("Vollständig belegt", f"{complete_count:,}".replace(",", "."), f"{complete_rate:.0%}")
    c3.metric("Teilweise belegt", f"{partial_count:,}".replace(",", "."))
    c4.metric("Offene Belege", f"{len(open_docs):,}".replace(",", "."))
    c5.metric("Offene FYRST-Umsätze", f"{len(open_bank):,}".replace(",", "."))

    summary_rows = []
    if not document_evidence.empty:
        for status, count in document_evidence["evidence_level"].value_counts().items():
            summary_rows.append({"Bereich": "Belegketten", "Typ": status, "Anzahl": count})
    if not matches.empty:
        for method, count in matches["method"].value_counts().items():
            summary_rows.append({"Bereich": "Technik FYRST", "Typ": method_label(method), "Anzahl": count})
    if not paypal.empty:
        summary_rows.append({"Bereich": "PayPal-Brücke", "Typ": "PayPal-Zeilen", "Anzahl": len(paypal)})
        summary_rows.append({"Bereich": "PayPal-Brücke", "Typ": "Belegtreffer", "Anzahl": paypal_doc_count})
        summary_rows.append({"Bereich": "PayPal-Brücke", "Typ": "Bank-Transfers", "Anzahl": len(paypal_bank_matches)})
    if not platform_transactions.empty:
        summary_rows.append({"Bereich": "Plattformdaten", "Typ": "Importierte Zeilen", "Anzahl": len(platform_transactions)})
        summary_rows.append({"Bereich": "Plattformdaten", "Typ": "Belegtreffer", "Anzahl": platform_doc_count})
        summary_rows.append({"Bereich": "Plattformdaten", "Typ": "Auszahlung zu FYRST", "Anzahl": len(platform_bank_matches)})
        if not platform_package_report.empty:
            summary_rows.append({"Bereich": "Plattformdaten", "Typ": "Abrechnungspakete", "Anzahl": len(platform_package_report)})
            summary_rows.append(
                {
                    "Bereich": "Plattformdaten",
                    "Typ": "Automatisch geschlossene Pakete",
                    "Anzahl": int((platform_package_report["package_status"] == "auto_matched").sum()),
                }
            )
    if summary_rows:
        st.dataframe(pd.DataFrame(summary_rows), hide_index=True, use_container_width=True)


def show_matches(matches: pd.DataFrame, links: pd.DataFrame) -> None:
    if matches.empty:
        st.info("Noch keine Verknüpfungen gefunden.")
        return

    view = matches.sort_values(["confidence", "bank_date"]).copy()
    view["Sicherheit"] = view["confidence"].apply(confidence_label)
    view["Methode"] = view["method"].apply(method_label)
    view = view.rename(
        columns={
            "bank_date": "Datum",
            "bank_amount": "Bankbetrag",
            "bank_counterparty": "Gegenpartei",
            "doc_count": "Belege",
            "doc_sum": "Belegsumme",
            "gap": "Differenz",
            "explanation": "Hinweis",
        }
    )
    view_cols = ["Datum", "Bankbetrag", "Gegenpartei", "Methode", "Sicherheit", "Belege", "Belegsumme", "Differenz", "Hinweis"]
    st.dataframe(format_frame(view[view_cols]), hide_index=True, use_container_width=True)

    max_expand = st.slider("Details anzeigen", 5, min(100, len(matches)), min(20, len(matches)))
    ordered = matches.sort_values(["confidence", "bank_date"], ascending=[True, True]).head(max_expand)
    for _, match in ordered.iterrows():
        label = (
            f"{as_date(match['bank_date'])} | {money(match['bank_amount'])} | "
            f"{match['bank_counterparty']} | {method_label(match['method'])} | {confidence_label(match['confidence'])}"
        )
        with st.expander(label):
            c1, c2 = st.columns([1, 1])
            with c1:
                st.write("Bankumsatz")
                st.json(
                    {
                        "tx_id": match["tx_id"],
                        "date": as_date(match["bank_date"]),
                        "amount": match["bank_amount"],
                        "counterparty": match["bank_counterparty"],
                        "description": match["bank_description"],
                        "gap": match["gap"],
                    }
                )
            with c2:
                st.write("Verknüpfte Belege")
                detail = links[links["match_id"] == match["match_id"]]
                st.dataframe(format_frame(detail), hide_index=True, use_container_width=True)
            st.caption(str(match["explanation"]))


def show_document_evidence(document_evidence: pd.DataFrame) -> None:
    if document_evidence.empty:
        st.info("Noch keine Belegketten berechnet.")
        return

    counts = document_evidence["evidence_level"].value_counts().rename_axis("Status").reset_index(name="Belege")
    st.dataframe(counts, hide_index=True, use_container_width=True)

    c1, c2, c3 = st.columns([1, 1, 2])
    level = c1.selectbox("Status", ["alle"] + sorted(document_evidence["evidence_level"].dropna().unique().tolist()))
    doc_type = c2.selectbox("Belegtyp", ["alle"] + sorted(document_evidence["doc_type"].dropna().unique().tolist()))
    search = c3.text_input("Suche in Belegketten", "")

    filtered = document_evidence.copy()
    if level != "alle":
        filtered = filtered[filtered["evidence_level"] == level]
    if doc_type != "alle":
        filtered = filtered[filtered["doc_type"] == doc_type]
    if search:
        needle = search.lower()
        haystack = (
            filtered["doc_ref"].astype(str)
            + " "
            + filtered["counterparty"].astype(str)
            + " "
            + filtered["description"].astype(str)
            + " "
            + filtered["evidence_path"].astype(str)
            + " "
            + filtered["evidence_note"].astype(str)
            + " "
            + filtered.get("platform_order_ids", pd.Series("", index=filtered.index)).astype(str)
            + " "
            + filtered.get("platform_counterparties", pd.Series("", index=filtered.index)).astype(str)
        ).str.lower()
        filtered = filtered[haystack.str.contains(needle, na=False)]

    view = filtered.rename(
        columns={
            "doc_ref": "Beleg",
            "doc_type": "Typ",
            "date": "Belegdatum",
            "date_source": "Datum-Basis",
            "amount": "Betrag",
            "platform": "Plattform",
            "counterparty": "Gegenpartei",
            "evidence_level": "Status",
            "evidence_path": "Nachweiskette",
            "evidence_note": "Hinweis",
            "fyrst_amounts": "FYRST-Betrag",
            "platform_order_ids": "Plattform-Order",
            "platform_amounts": "Plattform-Detail",
            "platform_bridge_fyrst_amounts": "Plattform→FYRST",
            "paypal_amounts": "PayPal-Detail",
            "bridge_fyrst_amounts": "PayPal→FYRST",
        }
    )
    cols = available_cols(view, [
        "Beleg",
        "Typ",
        "Belegdatum",
        "Datum-Basis",
        "Betrag",
        "Plattform",
        "Gegenpartei",
        "Status",
        "Nachweiskette",
        "FYRST-Betrag",
        "Plattform-Order",
        "Plattform-Detail",
        "Plattform→FYRST",
        "PayPal-Detail",
        "PayPal→FYRST",
        "Hinweis",
    ])
    st.dataframe(format_frame(view[cols]), hide_index=True, use_container_width=True)


def show_manual_report_editor(document_evidence: pd.DataFrame) -> None:
    if document_evidence.empty:
        return

    edits = st.session_state.setdefault("manual_report_edits", {})
    with st.expander("Beleg-Report manuell ergänzen", expanded=bool(edits)):
        st.caption("Diese Einträge ändern nur den Beleg-Report PDF und die Exportdatei. Die automatische Matching-Logik bleibt unverändert.")

        current_note = st.text_area(
            "Zusatznotiz im Kurzfazit",
            value=st.session_state.get("manual_report_summary_note", ""),
            height=80,
            key="manual_report_summary_note_input",
        )
        if current_note != st.session_state.get("manual_report_summary_note", ""):
            st.session_state["manual_report_summary_note"] = current_note

        search = st.text_input("Beleg suchen", "", key="manual_report_doc_search")
        candidates = document_evidence.copy()
        if search:
            needle = search.lower()
            haystack = (
                candidates["doc_ref"].astype(str)
                + " "
                + candidates["counterparty"].astype(str)
                + " "
                + candidates["description"].astype(str)
                + " "
                + candidates["evidence_level"].astype(str)
            ).str.lower()
            candidates = candidates[haystack.str.contains(needle, na=False)].copy()
        if candidates.empty:
            st.caption("Keine Belege fuer diese Suche.")
            return

        candidates = candidates.sort_values(["date", "doc_ref"])
        selected_doc_id = st.selectbox(
            "Beleg",
            candidates["doc_id"].astype(str).tolist(),
            key="manual_report_doc_id",
            format_func=lambda doc_id: doc_label(candidates, doc_id),
        )
        doc = candidates[candidates["doc_id"].astype(str) == str(selected_doc_id)].iloc[0]
        current = edits.get(str(selected_doc_id), {})

        auto_view = pd.DataFrame(
            [
                {
                    "Beleg": doc.get("doc_ref", ""),
                    "Status automatisch": doc.get("evidence_level", ""),
                    "Betrag": doc.get("amount", ""),
                    "Gegenpartei": doc.get("counterparty", ""),
                    "Nachweiskette": doc.get("evidence_path", ""),
                    "Hinweis": doc.get("evidence_note", ""),
                }
            ]
        )
        st.dataframe(format_frame(auto_view), hide_index=True, use_container_width=True)

        status_options = ["automatisch übernehmen", "vollständig belegt", "teilweise belegt", "offen"]
        current_status = str(current.get("Status") or "")
        status_index = status_options.index(current_status) if current_status in status_options else 0
        status = st.selectbox("Status im Beleg-Report", status_options, index=status_index, key=f"manual_status_{selected_doc_id}")
        fyrst_match = st.text_area(
            "FYRST-Umsatzmatch ersetzen",
            value=str(current.get("FYRST-Umsatzmatch") or ""),
            height=70,
            key=f"manual_fyrst_{selected_doc_id}",
        )
        detail = st.text_area(
            "Detailspur ersetzen",
            value=str(current.get("Detailspur") or ""),
            height=70,
            key=f"manual_detail_{selected_doc_id}",
        )
        note = st.text_area(
            "Hinweis ersetzen",
            value=str(current.get("Hinweis") or ""),
            height=70,
            key=f"manual_note_{selected_doc_id}",
        )

        c1, c2, c3 = st.columns([1, 1, 2])
        if c1.button("Manuelle Änderung speichern", key=f"save_manual_report_{selected_doc_id}"):
            payload: dict[str, object] = {
                "doc_id": str(selected_doc_id),
                "doc_ref": doc.get("doc_ref", ""),
                "date": doc.get("date", ""),
                "amount": doc.get("amount", ""),
                "counterparty": doc.get("counterparty", ""),
            }
            if status != "automatisch übernehmen":
                payload["Status"] = status
            if fyrst_match.strip():
                payload["FYRST-Umsatzmatch"] = fyrst_match.strip()
            if detail.strip():
                payload["Detailspur"] = detail.strip()
            if note.strip():
                payload["Hinweis"] = note.strip()
            if len(payload) > 5:
                edits[str(selected_doc_id)] = payload
                st.session_state["manual_report_edits"] = edits
                st.success("Manuelle Änderung gespeichert.")
            else:
                edits.pop(str(selected_doc_id), None)
                st.session_state["manual_report_edits"] = edits
                st.success("Manuelle Änderung entfernt.")
            st.rerun()
        if c2.button("Änderung für Beleg entfernen", key=f"delete_manual_report_{selected_doc_id}"):
            edits.pop(str(selected_doc_id), None)
            st.session_state["manual_report_edits"] = edits
            st.success("Manuelle Änderung entfernt.")
            st.rerun()
        if c3.button("Alle manuellen Belegreport-Änderungen entfernen", key="delete_all_manual_report_edits"):
            st.session_state["manual_report_edits"] = {}
            st.session_state["manual_report_summary_note"] = ""
            st.success("Alle manuellen Belegreport-Änderungen entfernt.")
            st.rerun()

        manual_frame = manual_report_edits_frame()
        if not manual_frame.empty:
            st.caption(f"Manuell geänderte Belege: {len(manual_frame)}")
            st.dataframe(format_frame(manual_frame), hide_index=True, use_container_width=True)


def show_bank_claim_usage(bank_claim_usage: pd.DataFrame) -> None:
    if bank_claim_usage.empty:
        st.info("Noch keine Umsatz-Auslastung berechnet.")
        return
    st.info(
        "Diese Kontrolle zeigt pro FYRST-Umsatz, welche Belegsumme rechnerisch dagegensteht. "
        "gross_platform_over_bank ist meist Etsy/eBay-Brutto gegen Netto-Auszahlung; over_claim ist die strengere Warnung."
    )
    counts = bank_claim_usage["usage_status"].value_counts().rename_axis("Status").reset_index(name="Anzahl")
    st.dataframe(counts, hide_index=True, use_container_width=True)
    status_filter = st.selectbox("Status", ["alle"] + sorted(bank_claim_usage["usage_status"].dropna().unique().tolist()))
    filtered = bank_claim_usage if status_filter == "alle" else bank_claim_usage[bank_claim_usage["usage_status"] == status_filter]
    cols = available_cols(
        filtered,
        [
            "bank_date",
            "bank_amount",
            "bank_counterparty",
            "claim_doc_count",
            "claim_doc_net_sum",
            "claim_doc_income_sum",
            "claim_doc_expense_sum",
            "delta_bank_minus_claim",
            "usage_status",
            "linked_doc_refs",
            "evidence_paths",
        ],
    )
    st.dataframe(format_frame(filtered[cols]), hide_index=True, use_container_width=True)


def show_overall_plausibility(report: pd.DataFrame) -> None:
    if report.empty:
        st.info("Noch kein Gesamt-Plausibilitätsbericht berechnet.")
        return
    st.info("Diese Sicht vergleicht Accountable-Belege mit FYRST-Zahlungen. Die Differenz muss nicht exakt 0 sein, wenn PayPal, Fremdwährungen, andere Konten, Steuern, Privatentnahmen oder Periodenverschiebungen enthalten sind.")
    st.dataframe(format_frame(report), hide_index=True, use_container_width=True)


def show_platform_support(
    platform_transactions: pd.DataFrame,
    platform_doc_matches: pd.DataFrame,
    platform_doc_links: pd.DataFrame,
    platform_bank_matches: pd.DataFrame,
    platform_package_report: pd.DataFrame,
    etsy_annual_report: pd.DataFrame,
    etsy_accountable_comparison: pd.DataFrame,
) -> None:
    if platform_transactions.empty:
        st.info("Keine Plattform-CSV geladen. Unterstützt werden aktuell Etsy SoldOrderItems, eBay Transaktionsbericht und Shopify Orders.")
        return

    st.info("Plattformdaten sind eine Stütze: Sie erklären Bestellungen, Kunden und Gebühren. Sie zählen nicht als Bankkonto.")
    if platform_doc_matches.empty:
        st.warning(
            "Es wurden Plattformdaten importiert, aber keine Accountable-Belege getroffen. "
            "Prüfe vor allem, ob die Zeiträume zusammenpassen. Plattformdateien aus 2026 können z.B. keine Accountable-Belege aus 2025 belastbar belegen."
        )

    order_count = int((platform_transactions["category"] == "order").sum())
    fee_count = int(platform_transactions["category"].isin(["fee", "charge"]).sum())
    payout_count = int((platform_transactions["category"] == "payout").sum())
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Plattformzeilen", f"{len(platform_transactions):,}".replace(",", "."))
    c2.metric("Bestellungen", f"{order_count:,}".replace(",", "."))
    c3.metric("Gebühren/Charges", f"{fee_count:,}".replace(",", "."))
    c4.metric("Auszahlungen", f"{payout_count:,}".replace(",", "."))
    c5.metric("Belegtreffer", f"{len(platform_doc_matches):,}".replace(",", "."))

    by_platform = (
        platform_transactions.groupby(["platform", "category"], dropna=False)
        .size()
        .reset_index(name="Anzahl")
        .sort_values(["platform", "category"])
    )
    st.dataframe(by_platform, hide_index=True, use_container_width=True)

    st.subheader("Accountable zu Plattform")
    if platform_doc_matches.empty:
        st.caption("Noch keine direkten Plattform-Belegtreffer gefunden.")
    else:
        view = platform_doc_matches.sort_values(["confidence", "bank_date"]).copy()
        view["Sicherheit"] = view["confidence"].apply(confidence_label)
        view = view.rename(
            columns={
                "bank_date": "Plattform-Datum",
                "bank_amount": "Plattform-Betrag",
                "bank_counterparty": "Kunde/Gegenpartei",
                "platform": "Plattform",
                "platform_category": "Kategorie",
                "order_id": "Order-ID",
                "payout_id": "Auszahlung-ID",
                "doc_count": "Belege",
                "doc_sum": "Belegsumme",
                "gap": "Differenz",
                "explanation": "Hinweis",
            }
        )
        cols = available_cols(
            view,
            [
                "Plattform-Datum",
                "Plattform-Betrag",
                "Plattform",
                "Kategorie",
                "Order-ID",
                "Auszahlung-ID",
                "Kunde/Gegenpartei",
                "Sicherheit",
                "Belege",
                "Belegsumme",
                "Differenz",
                "Hinweis",
            ],
        )
        st.dataframe(format_frame(view[cols]), hide_index=True, use_container_width=True)
        with st.expander("Verknüpfte Plattform-Belege"):
            st.dataframe(format_frame(platform_doc_links), hide_index=True, use_container_width=True)

    st.subheader("Plattform-Auszahlung zu FYRST")
    if platform_bank_matches.empty:
        st.caption("Keine Plattform-Auszahlung eindeutig zu FYRST gefunden. Bei Etsy/Shopify braucht es dafür meist separate Payout-/Statement-Exporte.")
    else:
        bridge = platform_bank_matches.sort_values(["confidence", "platform_date"]).copy()
        bridge["Sicherheit"] = bridge["confidence"].apply(confidence_label)
        bridge = bridge.rename(
            columns={
                "platform_date": "Plattform-Datum",
                "platform_amount": "Plattform-Betrag",
                "platform": "Plattform",
                "platform_description": "Plattform-Text",
                "payout_id": "Auszahlung-ID",
                "bank_date": "FYRST-Datum",
                "bank_amount": "FYRST-Betrag",
                "bank_counterparty": "FYRST-Gegenpartei",
                "amount_diff": "Differenz",
                "day_diff": "Tage",
            }
        )
        cols = available_cols(
            bridge,
            [
                "Plattform-Datum",
                "Plattform-Betrag",
                "Plattform",
                "Auszahlung-ID",
                "Plattform-Text",
                "FYRST-Datum",
                "FYRST-Betrag",
                "FYRST-Gegenpartei",
                "Sicherheit",
                "Differenz",
                "Tage",
            ],
        )
        st.dataframe(format_frame(bridge[cols]), hide_index=True, use_container_width=True)

    st.subheader("Abrechnungspakete")
    if platform_package_report.empty:
        st.caption("Noch keine Plattform-Abrechnungspakete berechnet. Pakete brauchen Plattform-Auszahlungen mit FYRST-Gegenbuchung.")
    else:
        package_counts = platform_package_report["package_status"].value_counts().rename_axis("Status").reset_index(name="Anzahl")
        st.dataframe(package_counts, hide_index=True, use_container_width=True)
        package_view = platform_package_report.rename(
            columns={
                "platform": "Plattform",
                "payout_date": "Auszahlung",
                "bank_date": "FYRST-Datum",
                "bank_amount": "FYRST-Betrag",
                "platform_detail_net": "Plattform-Netto",
                "linked_doc_sum": "Verknüpfte Belege",
                "candidate_fee_doc_sum": "Gebührenkandidaten",
                "package_doc_sum": "Paketsumme",
                "gap_to_bank": "Differenz",
                "package_status": "Status",
                "note": "Hinweis",
            }
        )
        cols = available_cols(
            package_view,
            [
                "Plattform",
                "Auszahlung",
                "FYRST-Datum",
                "FYRST-Betrag",
                "Plattform-Netto",
                "Verknüpfte Belege",
                "Gebührenkandidaten",
                "Paketsumme",
                "Differenz",
                "Status",
                "Hinweis",
            ],
        )
        st.dataframe(format_frame(package_view[cols]), hide_index=True, use_container_width=True)

    st.subheader("Etsy-Jahresabgleich")
    if etsy_annual_report.empty:
        st.caption("Keine Etsy-Monatsabrechnungen geladen.")
    else:
        annual = etsy_annual_report[etsy_annual_report["level"].isin(["year", "all_shops_year"])].copy()
        monthly = etsy_annual_report[etsy_annual_report["level"] == "month"].copy()
        st.dataframe(format_frame(annual), hide_index=True, use_container_width=True)
        with st.expander("Etsy-Monate"):
            st.dataframe(format_frame(monthly), hide_index=True, use_container_width=True)

    st.subheader("Etsy gegen Accountable")
    if etsy_accountable_comparison.empty:
        st.caption("Keine Etsy-Accountable-Gegenprobe berechnet.")
    else:
        comparison_annual = etsy_accountable_comparison[etsy_accountable_comparison["level"].isin(["year", "all"])].copy()
        comparison_monthly = etsy_accountable_comparison[etsy_accountable_comparison["level"] == "month"].copy()
        st.dataframe(format_frame(comparison_annual), hide_index=True, use_container_width=True)
        with st.expander("Etsy gegen Accountable nach Monaten"):
            st.dataframe(format_frame(comparison_monthly), hide_index=True, use_container_width=True)

    with st.expander("Normalisierte Plattformdaten"):
        cols = available_cols(
            platform_transactions,
            ["source", "platform", "category", "date", "payout_date", "amount", "gross_amount", "fee_amount", "currency", "counterparty", "order_id", "payout_id", "description"],
        )
        st.dataframe(format_frame(platform_transactions[cols].sort_values("date")), hide_index=True, use_container_width=True)


def show_open_docs(open_docs: pd.DataFrame, bank: pd.DataFrame) -> None:
    if open_docs.empty:
        st.success("Alle importierten Belege sind verknüpft.")
        return

    c1, c2 = st.columns([1, 2])
    doc_type = c1.selectbox("Typ", ["alle", "income", "expense"], index=0)
    search = c2.text_input("Suche", "")
    filtered = open_docs.copy()
    if doc_type != "alle":
        filtered = filtered[filtered["doc_type"] == doc_type]
    if search:
        needle = search.lower()
        filtered = filtered[filtered["text"].str.lower().str.contains(needle, na=False)]

    cols = available_cols(filtered, ["doc_ref", "doc_type", "date", "signed_amount", "platform", "counterparty", "description", "document_available"])
    st.dataframe(format_frame(filtered[cols].sort_values("date")), hide_index=True, use_container_width=True)

    if not filtered.empty:
        option = st.selectbox("Kandidaten für Beleg", filtered["doc_id"].tolist(), format_func=lambda doc_id: doc_label(filtered, doc_id))
        doc = filtered[filtered["doc_id"] == option].iloc[0]
        manual_external_account_editor(doc)
        candidates = top_candidates_for_doc(doc, bank, limit=8, window_days=30)
        if candidates.empty:
            st.caption("Keine naheliegenden Bankkandidaten im erweiterten Zeitfenster.")
        else:
            candidate_view = candidates.copy()
            candidate_view["Textnähe"] = candidate_view["text_score"].apply(confidence_label)
            st.dataframe(
                format_frame(candidate_view[["date", "amount", "counterparty", "description", "day_diff", "amount_diff", "Textnähe"]]),
                hide_index=True,
                use_container_width=True,
            )


def manual_external_account_editor(doc: pd.Series) -> None:
    notes = st.session_state.setdefault("external_payment_notes", {})
    doc_id = str(doc.get("doc_id"))
    current = notes.get(doc_id, {})
    with st.expander("Manuelle Zahlungsquelle", expanded=bool(current)):
        st.caption("Nutze das, wenn der Beleg über ein nicht hochgeladenes Konto lief, z.B. privates Konto, DKB, N26 oder ein anderes Bankkonto.")
        has_other = st.checkbox("Beleg lief ueber anderes Bankkonto", value=bool(current), key=f"other_bank_{doc_id}")
        account_name = st.text_input("Name des anderen Bankkontos", value=current.get("account_name", ""), key=f"other_bank_name_{doc_id}")
        note = st.text_area("Notiz", value=current.get("note", ""), key=f"other_bank_note_{doc_id}", height=80)
        if st.button("Markierung speichern", key=f"save_other_bank_{doc_id}"):
            if has_other:
                notes[doc_id] = {
                    "doc_id": doc_id,
                    "doc_ref": doc.get("doc_ref", ""),
                    "doc_type": doc.get("doc_type", ""),
                    "date": doc.get("date", ""),
                    "signed_amount": doc.get("signed_amount", ""),
                    "counterparty": doc.get("counterparty", ""),
                    "account_name": account_name.strip() or "anderes Bankkonto",
                    "note": note.strip(),
                }
                st.success("Manuelle Zahlungsquelle gespeichert.")
            else:
                notes.pop(doc_id, None)
                st.success("Manuelle Zahlungsquelle entfernt.")
            st.session_state["external_payment_notes"] = notes
            st.rerun()

    manual_frame = manual_external_account_frame()
    if not manual_frame.empty:
        st.caption(f"Manuell markierte Belege: {len(manual_frame)}")
        st.dataframe(format_frame(manual_frame), hide_index=True, use_container_width=True)


def show_open_bank(open_bank: pd.DataFrame) -> None:
    if open_bank.empty:
        st.success("Alle relevanten Bankumsätze sind verknüpft.")
        return
    source = st.selectbox("Quelle", ["alle"] + sorted(open_bank["source"].dropna().unique().tolist()))
    filtered = open_bank if source == "alle" else open_bank[open_bank["source"] == source]
    cols = available_cols(filtered, ["source", "date", "amount", "platform", "counterparty", "description", "currency"])
    st.dataframe(format_frame(filtered[cols].sort_values("date")), hide_index=True, use_container_width=True)


def show_leftover_review(open_docs: pd.DataFrame, open_bank: pd.DataFrame) -> None:
    st.info("Restbestand ohne neue automatische Zuordnung: alle offenen Belege und alle offenen Bankumsätze.")
    st.subheader("Offene Belege")
    if open_docs.empty:
        st.success("Keine offenen Belege.")
    else:
        doc_cols = available_cols(open_docs, ["doc_ref", "doc_type", "date", "date_source", "signed_amount", "platform", "counterparty", "description"])
        st.dataframe(format_frame(open_docs[doc_cols].sort_values(["date", "signed_amount"])), hide_index=True, use_container_width=True)

    st.subheader("Offene Bankumsätze")
    if open_bank.empty:
        st.success("Keine offenen Bankumsätze.")
    else:
        bank_cols = available_cols(open_bank, ["source", "date", "amount", "counterparty", "description", "currency"])
        st.dataframe(format_frame(open_bank[bank_cols].sort_values(["date", "amount"])), hide_index=True, use_container_width=True)


def show_paypal_bridge(
    paypal: pd.DataFrame,
    paypal_doc_matches: pd.DataFrame,
    paypal_doc_links: pd.DataFrame,
    paypal_bank_matches: pd.DataFrame,
) -> None:
    if paypal.empty:
        st.info("Keine PayPal-CSV geladen.")
        return

    st.info("PayPal wird hier als Brücke genutzt: Beleg zu PayPal-Detail zu Banktransfer. Diese Zeilen werden nicht als zusätzliche Bankumsätze gezählt.")

    detail_count = len(paypal[paypal["category"].isin(["commercial", "conversion", "fee"])])
    transfer_count = len(paypal[paypal["category"] == "bank_transfer"])
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("PayPal-Zeilen", f"{len(paypal):,}".replace(",", "."))
    c2.metric("Detailzeilen", f"{detail_count:,}".replace(",", "."))
    c3.metric("Banktransfers", f"{transfer_count:,}".replace(",", "."))
    c4.metric("Bank-Matches", f"{len(paypal_bank_matches):,}".replace(",", "."))

    categories = paypal["category"].value_counts().rename_axis("Kategorie").reset_index(name="Anzahl")
    st.dataframe(categories, hide_index=True, use_container_width=True)

    st.subheader("Beleg zu PayPal")
    if paypal_doc_matches.empty:
        st.caption("Keine direkten PayPal-Belegtreffer gefunden.")
    else:
        view = paypal_doc_matches.sort_values(["confidence", "bank_date"]).copy()
        view["Sicherheit"] = view["confidence"].apply(confidence_label)
        view = view.rename(
            columns={
                "bank_date": "PayPal-Datum",
                "bank_amount": "PayPal-Betrag",
                "bank_counterparty": "Gegenpartei",
                "doc_count": "Belege",
                "doc_sum": "Belegsumme",
                "gap": "Differenz",
                "explanation": "Hinweis",
            }
        )
        st.dataframe(
            format_frame(view[["PayPal-Datum", "PayPal-Betrag", "Gegenpartei", "Sicherheit", "Belege", "Belegsumme", "Differenz", "Hinweis"]]),
            hide_index=True,
            use_container_width=True,
        )

        with st.expander("Verknüpfte PayPal-Belege"):
            st.dataframe(format_frame(paypal_doc_links), hide_index=True, use_container_width=True)

    st.subheader("PayPal zu Bank")
    if paypal_bank_matches.empty:
        st.caption("Keine PayPal-Banktransfer-Gegenbuchungen gefunden.")
    else:
        bridge = paypal_bank_matches.sort_values(["confidence", "paypal_date"]).copy()
        bridge["Sicherheit"] = bridge["confidence"].apply(confidence_label)
        bridge = bridge.rename(
            columns={
                "paypal_date": "PayPal-Datum",
                "paypal_amount": "PayPal-Betrag",
                "paypal_description": "PayPal-Text",
                "bank_date": "Bankdatum",
                "bank_amount": "Bankbetrag",
                "bank_source": "Bank",
                "bank_counterparty": "Bank-Gegenpartei",
                "amount_diff": "Differenz",
                "day_diff": "Tage",
            }
        )
        st.dataframe(
            format_frame(
                bridge[
                    [
                        "PayPal-Datum",
                        "PayPal-Betrag",
                        "PayPal-Text",
                        "Bankdatum",
                        "Bankbetrag",
                        "Bank",
                        "Bank-Gegenpartei",
                        "Sicherheit",
                        "Differenz",
                        "Tage",
                    ]
                ]
            ),
            hide_index=True,
            use_container_width=True,
        )


def show_llm_fallback(open_bank: pd.DataFrame, open_docs: pd.DataFrame, settings: MatchSettings) -> None:
    st.info("KI ist nicht Teil des normalen Komplett-Matchings. Sie prüft nur den hier ausgewählten Restfall, damit Kosten, Datenschutzrisiko und Fehlentscheidungen klein bleiben.")
    st.caption("Sendet nur beim Klick Daten an OpenRouter. Standardmäßig werden Freitextfelder grob anonymisiert.")
    api_key = st.text_input("OpenRouter API Key", type="password")
    model = st.text_input("Modell", value="openrouter/auto")
    anonymize = st.toggle("Freitext anonymisieren", value=True)

    if open_bank.empty or open_docs.empty:
        st.info("Für den KI-Fallback braucht es offene Bankumsätze und offene Belege.")
        return

    tx_id = st.selectbox(
        "Offener Bankumsatz",
        open_bank["tx_id"].tolist(),
        format_func=lambda value: tx_label(open_bank, value),
    )
    tx = open_bank[open_bank["tx_id"] == tx_id].iloc[0]
    candidates = candidate_docs_for_tx(tx, open_docs, settings).head(15)
    candidate_view = candidates.copy()
    if candidate_view.empty:
        st.caption("Keine naheliegenden Belegkandidaten gefunden.")
    elif "candidate_score" in candidate_view.columns:
        candidate_view["Nähe"] = candidate_view["candidate_score"].apply(confidence_label)
        st.dataframe(
            format_frame(candidate_view[["doc_ref", "doc_type", "date", "signed_amount", "counterparty", "description", "Nähe"]]),
            hide_index=True,
            use_container_width=True,
        )

    if "llm_suggestions" not in st.session_state:
        st.session_state["llm_suggestions"] = []

    if st.button("KI-Vorschlag erzeugen", disabled=not api_key or candidates.empty):
        config = LlmConfig(api_key=api_key, model=model, anonymize=anonymize)
        with st.spinner("OpenRouter prüft den Fall..."):
            try:
                suggestion = suggest_match(
                    config,
                    tx.to_dict(),
                    dataframe_records(candidates, limit=15),
                )
                st.session_state["llm_suggestions"].append(suggestion)
            except Exception as exc:
                st.error(str(exc))

    if st.session_state["llm_suggestions"]:
        st.write("KI-Vorschläge")
        st.json(st.session_state["llm_suggestions"])


def prepare_pdf_report_bundle(
    docs: pd.DataFrame,
    bank: pd.DataFrame,
    paypal: pd.DataFrame,
    matches: pd.DataFrame,
    links: pd.DataFrame,
    settings: MatchSettings,
    paypal_bank_matches: pd.DataFrame,
    document_evidence: pd.DataFrame,
    platform_transactions: pd.DataFrame,
    platform_bank_matches: pd.DataFrame,
    platform_package_report: pd.DataFrame,
    etsy_annual_report: pd.DataFrame,
    etsy_accountable_comparison: pd.DataFrame,
    leftover_candidate_report: pd.DataFrame,
) -> tuple[tuple[bytes, bytes, bytes, bytes] | None, str | None]:
    try:
        with st.spinner("PDF-Reports werden vorbereitet..."):
            return (
                build_pdf_report_bytes(
                    docs,
                    bank,
                    paypal,
                    matches,
                    links,
                    settings,
                    paypal_bank_matches,
                    document_evidence,
                    platform_transactions,
                    platform_bank_matches,
                    platform_package_report,
                    etsy_annual_report,
                    etsy_accountable_comparison,
                    leftover_candidate_report,
                    st.session_state.get("manual_report_edits", {}),
                    st.session_state.get("manual_report_summary_note", ""),
                ),
                None,
            )
    except ModuleNotFoundError as exc:
        return None, f"PDF-Export braucht noch ein Paket: {exc.name}. Auf Railway wird es über requirements.txt installiert."
    except Exception as exc:  # pragma: no cover - visible UI fallback for hosted runs
        return None, f"PDF-Reports konnten nicht erzeugt werden: {exc}"


def show_pdf_download_buttons(
    pdf_bundle: tuple[bytes, bytes, bytes, bytes] | None,
    pdf_error: str | None,
    key_prefix: str,
) -> None:
    if pdf_error:
        st.warning(pdf_error)
        return
    if pdf_bundle is None:
        st.info("PDF-Reports sind für diesen Lauf noch nicht vorbereitet.")
        return

    beleg_pdf, kontroll_pdf, hypothesen_pdf, ledger_pdf = pdf_bundle
    st.caption("Die PDF-Reports beziehen sich auf den aktuell angezeigten Analyse-Lauf.")
    c1, c2, c3, c4 = st.columns(4)
    c1.download_button(
        "Beleg-Report PDF",
        beleg_pdf,
        "buchhaltungs_buddy_beleg_report.pdf",
        "application/pdf",
        key=f"{key_prefix}_beleg_pdf",
        on_click="ignore",
        use_container_width=True,
        type="primary",
    )
    c2.download_button(
        "Kontroll-Report PDF",
        kontroll_pdf,
        "buchhaltungs_buddy_kontroll_report.pdf",
        "application/pdf",
        key=f"{key_prefix}_kontroll_pdf",
        on_click="ignore",
        use_container_width=True,
    )
    c3.download_button(
        "Hypothesen-Report PDF",
        hypothesen_pdf,
        "buchhaltungs_buddy_hypothesen_report.pdf",
        "application/pdf",
        key=f"{key_prefix}_hypothesen_pdf",
        on_click="ignore",
        use_container_width=True,
    )
    c4.download_button(
        "Ledger-Experiment PDF",
        ledger_pdf,
        "buchhaltungs_buddy_ledger_experiment_report.pdf",
        "application/pdf",
        key=f"{key_prefix}_ledger_pdf",
        on_click="ignore",
        use_container_width=True,
    )


def show_export(
    docs: pd.DataFrame,
    bank: pd.DataFrame,
    matches: pd.DataFrame,
    links: pd.DataFrame,
    import_issues: list[str],
    settings: MatchSettings,
    paypal: pd.DataFrame,
    paypal_doc_matches: pd.DataFrame,
    paypal_doc_links: pd.DataFrame,
    paypal_bank_matches: pd.DataFrame,
    document_evidence: pd.DataFrame,
    platform_transactions: pd.DataFrame,
    platform_doc_matches: pd.DataFrame,
    platform_doc_links: pd.DataFrame,
    platform_bank_matches: pd.DataFrame,
    platform_package_report: pd.DataFrame,
    etsy_annual_report: pd.DataFrame,
    etsy_accountable_comparison: pd.DataFrame,
    leftover_candidate_report: pd.DataFrame,
    bank_claim_usage: pd.DataFrame,
    overall_plausibility_report: pd.DataFrame,
    pdf_bundle: tuple[bytes, bytes, bytes, bytes] | None = None,
    pdf_error: str | None = None,
) -> None:
    if pdf_bundle is None and pdf_error is None:
        pdf_bundle, pdf_error = prepare_pdf_report_bundle(
            docs,
            bank,
            paypal,
            matches,
            links,
            settings,
            paypal_bank_matches,
            document_evidence,
            platform_transactions,
            platform_bank_matches,
            platform_package_report,
            etsy_annual_report,
            etsy_accountable_comparison,
            leftover_candidate_report,
        )
    show_pdf_download_buttons(pdf_bundle, pdf_error, key_prefix="export")

    st.download_button("matches.csv", to_csv_bytes(matches), "matches.csv", "text/csv")
    st.download_button("match_links.csv", to_csv_bytes(links), "match_links.csv", "text/csv")
    st.download_button("accountable_docs_normalized.csv", to_csv_bytes(docs), "accountable_docs_normalized.csv", "text/csv")
    st.download_button("bank_transactions_normalized.csv", to_csv_bytes(bank), "bank_transactions_normalized.csv", "text/csv")
    st.download_button("document_evidence_chains.csv", to_csv_bytes(document_evidence), "document_evidence_chains.csv", "text/csv")
    open_doc_ids = set(document_evidence.loc[document_evidence["evidence_level"] == "offen", "doc_id"])
    export_open_docs = docs[docs["doc_id"].isin(open_doc_ids)].copy()
    export_open_bank = build_open_bank_after_evidence(bank, matches, paypal_bank_matches, platform_bank_matches)
    st.download_button("open_docs.csv", to_csv_bytes(export_open_docs), "open_docs.csv", "text/csv")
    st.download_button("open_bank.csv", to_csv_bytes(export_open_bank), "open_bank.csv", "text/csv")
    st.download_button("bank_claim_usage.csv", to_csv_bytes(bank_claim_usage), "bank_claim_usage.csv", "text/csv")
    st.download_button("overall_plausibility_report.csv", to_csv_bytes(overall_plausibility_report), "overall_plausibility_report.csv", "text/csv")
    if not paypal.empty:
        st.download_button("paypal_transactions_normalized.csv", to_csv_bytes(paypal), "paypal_transactions_normalized.csv", "text/csv")
        st.download_button("paypal_doc_matches.csv", to_csv_bytes(paypal_doc_matches), "paypal_doc_matches.csv", "text/csv")
        st.download_button("paypal_doc_links.csv", to_csv_bytes(paypal_doc_links), "paypal_doc_links.csv", "text/csv")
        st.download_button("paypal_bank_bridge.csv", to_csv_bytes(paypal_bank_matches), "paypal_bank_bridge.csv", "text/csv")
    if not platform_transactions.empty:
        st.download_button("platform_transactions_normalized.csv", to_csv_bytes(platform_transactions), "platform_transactions_normalized.csv", "text/csv")
        st.download_button("platform_doc_matches.csv", to_csv_bytes(platform_doc_matches), "platform_doc_matches.csv", "text/csv")
        st.download_button("platform_doc_links.csv", to_csv_bytes(platform_doc_links), "platform_doc_links.csv", "text/csv")
        st.download_button("platform_bank_bridge.csv", to_csv_bytes(platform_bank_matches), "platform_bank_bridge.csv", "text/csv")
        st.download_button("platform_package_report.csv", to_csv_bytes(platform_package_report), "platform_package_report.csv", "text/csv")
        st.download_button("etsy_annual_reconciliation.csv", to_csv_bytes(etsy_annual_report), "etsy_annual_reconciliation.csv", "text/csv")
        st.download_button("etsy_accountable_comparison.csv", to_csv_bytes(etsy_accountable_comparison), "etsy_accountable_comparison.csv", "text/csv")
    st.download_button("leftover_candidate_report.csv", to_csv_bytes(leftover_candidate_report), "leftover_candidate_report.csv", "text/csv")
    st.download_button("ledger_experiment_report.csv", to_csv_bytes(build_ledger_experiment_report(document_evidence)), "ledger_experiment_report.csv", "text/csv")
    manual_report_frame = manual_report_edits_frame()
    if not manual_report_frame.empty:
        st.download_button("manual_report_edits.csv", to_csv_bytes(manual_report_frame), "manual_report_edits.csv", "text/csv")
    manual_frame = manual_external_account_frame()
    if not manual_frame.empty:
        st.download_button("manual_external_accounts.csv", to_csv_bytes(manual_frame), "manual_external_accounts.csv", "text/csv")

    db_path = BASE_DIR / "data" / "reconciliation.sqlite"
    with st.expander("Lokalen SQLite-Speicher verwalten", expanded=False):
        st.caption("Der Import selbst bleibt nur in der App-Session. Dauerhaft ist nur diese SQLite-Datei, wenn du Runs gespeichert hast.")
        if db_path.exists():
            st.write(str(db_path))
            confirm_delete = st.text_input("Zum Löschen der SQLite-Datei RESET eingeben", value="")
            if st.button("SQLite-Speicher löschen", disabled=confirm_delete != "RESET"):
                db_path.unlink()
                st.cache_data.clear()
                st.success("SQLite-Speicher gelöscht.")
                st.rerun()
        else:
            st.caption("Keine gespeicherte SQLite-Datei vorhanden.")

    if st.button("Run in SQLite speichern"):
        run_id = save_run_to_sqlite(
            db_path,
            docs,
            bank,
            matches,
            links,
            import_issues,
            settings.__dict__,
            {
                "document_evidence_chains": document_evidence,
                "paypal_transactions": paypal,
                "paypal_doc_matches": paypal_doc_matches,
                "paypal_doc_links": paypal_doc_links,
                "paypal_bank_bridge": paypal_bank_matches,
                "platform_transactions": platform_transactions,
                "platform_doc_matches": platform_doc_matches,
                "platform_doc_links": platform_doc_links,
                "platform_bank_bridge": platform_bank_matches,
                "platform_package_report": platform_package_report,
                "etsy_annual_reconciliation": etsy_annual_report,
                "etsy_accountable_comparison": etsy_accountable_comparison,
                "bank_claim_usage": bank_claim_usage,
                "overall_plausibility_report": overall_plausibility_report,
                "leftover_candidate_report": leftover_candidate_report,
                "manual_report_edits": manual_report_frame,
                "manual_external_accounts": manual_frame,
            },
        )
        st.success(f"Gespeichert als Run {run_id}: {BASE_DIR / 'data' / 'reconciliation.sqlite'}")


def to_csv_bytes(frame: pd.DataFrame) -> bytes:
    prepared = frame.copy()
    for col in prepared.columns:
        if pd.api.types.is_datetime64_any_dtype(prepared[col]):
            prepared[col] = prepared[col].dt.strftime("%Y-%m-%d")
    return prepared.to_csv(index=False, sep=";").encode("utf-8-sig")


def build_pdf_report_bytes(
    docs: pd.DataFrame,
    bank: pd.DataFrame,
    paypal: pd.DataFrame,
    matches: pd.DataFrame,
    links: pd.DataFrame,
    settings: MatchSettings,
    paypal_bank_matches: pd.DataFrame,
    document_evidence: pd.DataFrame,
    platform_transactions: pd.DataFrame,
    platform_bank_matches: pd.DataFrame,
    platform_package_report: pd.DataFrame,
    etsy_annual_report: pd.DataFrame,
    etsy_accountable_comparison: pd.DataFrame,
    leftover_candidate_report: pd.DataFrame,
    manual_report_edits: dict[str, dict[str, object]] | None = None,
    manual_report_summary_note: str = "",
) -> tuple[bytes, bytes, bytes, bytes]:
    import scripts.build_explicit_match_report as report_pdf

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        report_pdf.REPORT_PATH = temp_path / "buchhaltungs_buddy_beleg_report.pdf"
        report_pdf.CONTROL_REPORT_PATH = temp_path / "buchhaltungs_buddy_kontroll_report.pdf"
        report_pdf.HYPOTHESIS_REPORT_PATH = temp_path / "buchhaltungs_buddy_hypothesen_report.pdf"
        report_pdf.LEDGER_EXPERIMENT_REPORT_PATH = temp_path / "buchhaltungs_buddy_ledger_experiment_report.pdf"
        open_doc_ids = set(document_evidence.loc[document_evidence["evidence_level"] == "offen", "doc_id"])
        open_docs = docs[docs["doc_id"].isin(open_doc_ids)].copy()
        open_bank = build_open_bank_after_evidence(bank, matches, paypal_bank_matches, platform_bank_matches)
        doc_report = report_pdf.build_doc_report(document_evidence, manual_report_edits)
        bank_report = report_pdf.build_bank_report(bank, document_evidence, matches, paypal_bank_matches, platform_bank_matches)
        settlement_detail_report = report_pdf.build_settlement_detail_report(bank_report, document_evidence)
        hypothesis_candidate_report = build_hypothesis_candidate_report(open_docs, open_bank, settlement_detail_report, settings)
        ledger_experiment_report = build_ledger_experiment_report(document_evidence)
        report_pdf.build_pdf(
            docs,
            bank,
            paypal,
            document_evidence,
            open_docs,
            open_bank,
            doc_report,
            bank_report,
            platform_transactions=platform_transactions,
            platform_package_report=platform_package_report,
            etsy_annual_report=etsy_annual_report,
            etsy_accountable_comparison=etsy_accountable_comparison,
            leftover_candidate_report=leftover_candidate_report,
            hypothesis_candidate_report=hypothesis_candidate_report,
            ledger_experiment_report=ledger_experiment_report,
            manual_edits=manual_report_edits,
            manual_summary_note=manual_report_summary_note,
        )
        return (
            report_pdf.REPORT_PATH.read_bytes(),
            report_pdf.CONTROL_REPORT_PATH.read_bytes(),
            report_pdf.HYPOTHESIS_REPORT_PATH.read_bytes(),
            report_pdf.LEDGER_EXPERIMENT_REPORT_PATH.read_bytes(),
        )


def manual_external_account_frame() -> pd.DataFrame:
    notes = st.session_state.get("external_payment_notes", {})
    if not notes:
        return pd.DataFrame(columns=["doc_id", "doc_ref", "doc_type", "date", "signed_amount", "counterparty", "account_name", "note"])
    return pd.DataFrame(list(notes.values()))


def manual_report_edits_frame() -> pd.DataFrame:
    edits = st.session_state.get("manual_report_edits", {})
    if not edits:
        return pd.DataFrame(columns=["doc_id", "doc_ref", "date", "amount", "counterparty", "Status", "FYRST-Umsatzmatch", "Detailspur", "Hinweis"])
    return pd.DataFrame(list(edits.values()))


def format_frame(frame: pd.DataFrame) -> pd.DataFrame:
    prepared = frame.copy()
    for col in prepared.columns:
        if pd.api.types.is_datetime64_any_dtype(prepared[col]):
            prepared[col] = prepared[col].dt.strftime("%Y-%m-%d")
    return prepared


def available_cols(frame: pd.DataFrame, columns: list[str]) -> list[str]:
    return [col for col in columns if col in frame.columns]


def confidence_label(value: object) -> str:
    try:
        confidence = float(value)
    except Exception:
        return "prüfen"
    pct = int(round(confidence * 100))
    if confidence >= 0.85:
        label = "sehr stark"
    elif confidence >= 0.70:
        label = "stark"
    elif confidence >= 0.58:
        label = "mittel"
    else:
        label = "prüfen"
    return f"{label} ({pct}%)"


def method_label(method: object) -> str:
    labels = {
        "direct": "Direkt",
        "batch_exact": "Sammelzahlung exakt",
        "batch_net_fee_gap": "Sammelzahlung netto",
        "paypal_detail": "PayPal-Detail",
        "paypal_batch": "PayPal-Sammelzahlung",
        "paypal_fx_batch": "PayPal-Fremdwährungspaket",
        "platform_detail": "Plattform-Detail",
        "platform_package_net": "Plattform-Abrechnungspaket",
    }
    return labels.get(str(method), str(method))


def money(value: object) -> str:
    try:
        number = float(value)
    except Exception:
        return str(value)
    return f"{number:,.2f} EUR".replace(",", "X").replace(".", ",").replace("X", ".")


def as_date(value: object) -> str:
    try:
        return pd.Timestamp(value).strftime("%Y-%m-%d")
    except Exception:
        return str(value)


def doc_label(frame: pd.DataFrame, doc_id: str) -> str:
    row = frame[frame["doc_id"] == doc_id].iloc[0]
    amount = row["signed_amount"] if "signed_amount" in row.index else row.get("amount", "")
    return f"{row['doc_ref']} | {as_date(row['date'])} | {money(amount)} | {row['counterparty']}"


def tx_label(frame: pd.DataFrame, tx_id: str) -> str:
    row = frame[frame["tx_id"] == tx_id].iloc[0]
    return f"{as_date(row['date'])} | {money(row['amount'])} | {row['counterparty']}"


if __name__ == "__main__":
    main()
