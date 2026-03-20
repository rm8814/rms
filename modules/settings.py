"""
modules/settings.py — Property management UI

Add, edit, deactivate properties and manage API keys.
DB is the source of truth. All properties managed via this UI.
"""

import streamlit as st
from datetime import date
from db import get_db, get_all_properties, get_connection

CONTRACT_TYPES = [
    "revshare_revenue",
    "revshare_gop",
    "revshare_revenue_gop",
    "lease",
    "advance_payment",
]

REVSHARE_TYPES = {"revshare_revenue", "revshare_gop", "revshare_revenue_gop"}


def render():
    st.subheader("Settings — Properties")

    props = get_all_properties()
    tab_list, tab_add, tab_channels = st.tabs(["Manage Properties", "Add New Property", "Channel Mapping"])

    # ── Tab 1: List + Edit + Deactivate ──────────────────────────────────────
    with tab_list:
        if not props:
            st.info("No properties yet. Use the **Add New Property** tab to add your first property.")
        else:
            for p in sorted(props, key=lambda x: str(x["id"])):
                is_active = bool(p["active"])
                status_label = "Active" if is_active else "Inactive"
                expander_label = f"{'🟢' if is_active else '⚫'}  [{p['id']}]  {p['name']}  ·  {p['city'] or ''}  ·  {p['contract_type']}  [{status_label}]"

                with st.expander(expander_label, expanded=False):
                    _render_edit_form(p)

    # ── Tab 2: Add New ────────────────────────────────────────────────────────
    with tab_add:
        _render_add_form()


    # ── Tab 3: Channel Mapping ────────────────────────────────────────────────
    with tab_channels:
        _render_channel_mapping()


def _render_channel_mapping():
    """Show all known agent_name values across all properties; let user set display name + type."""
    from db import get_connection
    import sqlite3
    from datetime import datetime

    conn = get_connection()
    props = conn.execute("SELECT id, name FROM properties WHERE active=1 ORDER BY name").fetchall()

    if not props:
        st.info("No active properties.")
        conn.close()
        return

    prop_options = {p["name"]: p["id"] for p in props}
    selected_name = st.selectbox("Property", options=list(prop_options.keys()))
    prop_id = prop_options[selected_name]

    mappings = conn.execute("""
        SELECT raw_agent_name, display_name, channel_type
        FROM channel_mappings
        WHERE property_id = ?
        ORDER BY channel_type, display_name
    """, (prop_id,)).fetchall()
    conn.close()

    if not mappings:
        st.info("No channel data yet for this property. Run an ingest first.")
        return

    # Also show the two built-in sources that aren't in agents array
    builtins = [
        {"raw_agent_name": "__front_desk__", "display_name": "Front Desk", "channel_type": "direct"},
        {"raw_agent_name": "__official_site__", "display_name": "Official Site", "channel_type": "direct"},
    ]

    st.caption("Set friendly names and whether each channel is **Direct** (own channels) or **Indirect** (OTAs/third-party).")
    st.caption("Built-in sources (Front Desk, Official Site) are always Direct and cannot be changed.")

    all_rows = list(mappings)

    with st.form("channel_mapping_form"):
        updated = {}
        for m in all_rows:
            raw = m["raw_agent_name"]
            col1, col2, col3 = st.columns([3, 2, 2])
            with col1:
                st.text(raw)
            with col2:
                disp = st.text_input(
                    "Display Name", value=m["display_name"],
                    key=f"disp_{prop_id}_{raw}", label_visibility="collapsed"
                )
            with col3:
                ctype = st.selectbox(
                    "Type", options=["booking_engine", "front_desk", "indirect"],
                    index=["booking_engine", "front_desk", "indirect"].index(m["channel_type"]) if m["channel_type"] in ["booking_engine", "front_desk", "indirect"] else 2,
                    key=f"type_{prop_id}_{raw}", label_visibility="collapsed"
                )
            updated[raw] = (disp.strip() or m["display_name"], ctype)

        if st.form_submit_button("💾 Save Channel Mappings", type="primary"):
            now = datetime.utcnow().isoformat()
            from db import get_db
            with get_db() as wconn:
                for raw, (disp, ctype) in updated.items():
                    wconn.execute("""
                        UPDATE channel_mappings
                        SET display_name=?, channel_type=?, updated_at=?
                        WHERE property_id=? AND raw_agent_name=?
                    """, (disp, ctype, now, prop_id, raw))
            st.success("Channel mappings saved.")
            st.rerun()


def _render_edit_form(p: dict):
    pid = p["id"]

    with st.form(key=f"edit_{pid}"):
        col1, col2 = st.columns(2)

        with col1:
            name = st.text_input("Property Name *", value=p["name"] or "")
            city = st.text_input("City", value=p["city"] or "")
            join_date = st.date_input(
                "Join Date *",
                value=date.fromisoformat(p["join_date"]) if p["join_date"] else date.today(),
            )
            room_count = st.number_input("Room Count *", min_value=1, value=int(p["room_count"] or 1), step=1)
            active = st.checkbox("Active", value=bool(p["active"]))

        with col2:
            contract_type = st.selectbox(
                "Contract Type *",
                options=CONTRACT_TYPES,
                index=CONTRACT_TYPES.index(p["contract_type"]) if p["contract_type"] in CONTRACT_TYPES else 0,
            )
            revshare_pct = st.number_input(
                "RevShare % of Revenue",
                min_value=0.0, max_value=100.0, step=0.5,
                value=float(p["revshare_pct"] or 0.0),
                disabled=contract_type not in {"revshare_revenue", "revshare_revenue_gop"},
                help="REHAT's cut as % of total revenue",
            )
            revshare_gop_pct = st.number_input(
                "RevShare % of GOP",
                min_value=0.0, max_value=100.0, step=0.5,
                value=float(p.get("revshare_gop_pct") or 0.0),
                disabled=contract_type not in {"revshare_gop", "revshare_revenue_gop"},
                help="REHAT's cut as % of GOP (revenue minus costs)",
            )
            lease_monthly = st.number_input(
                "Monthly Lease (IDR)",
                min_value=0, step=1_000_000,
                value=int(p["lease_monthly"] or 0),
                disabled=contract_type != "lease",
            )
            advance_total = st.number_input(
                "Advance Payment Total (IDR)",
                min_value=0, step=1_000_000,
                value=int(p["advance_total"] or 0),
                disabled=contract_type != "advance_payment",
            )
            contract_months = st.number_input(
                "Contract Duration (months)",
                min_value=1, max_value=360, step=1,
                value=int(p.get("contract_months") or 24),
                disabled=contract_type != "advance_payment",
                help="Advance payment amortized over this many months",
            )

        # API key — full width, masked
        api_key_current = p["api_key"] or ""
        api_key_display = ("*" * 20 + api_key_current[-6:]) if len(api_key_current) > 6 else api_key_current
        st.caption(f"Current API Key: `{api_key_display}`" if api_key_current else "⚠️ No API key set — ingestion will be skipped")
        new_api_key = st.text_input(
            "API Key (leave blank to keep existing)",
            value="",
            type="password",
            placeholder="Paste new key to update, or leave blank",
            key=f"apikey_{pid}",
        )

        col_save, col_delete = st.columns([3, 1])
        with col_save:
            submitted = st.form_submit_button("💾 Save Changes", type="primary", use_container_width=True)
        with col_delete:
            delete = st.form_submit_button("🗑 Remove", use_container_width=True)

    if submitted:
        errors = _validate(name, join_date, room_count, contract_type, revshare_pct, lease_monthly)
        if errors:
            for e in errors:
                st.error(e)
        else:
            final_api_key = new_api_key.strip() if new_api_key.strip() else api_key_current
            with get_db() as conn:
                conn.execute("""
                    UPDATE properties SET
                        name=?, city=?, join_date=?, room_count=?, active=?,
                        contract_type=?, revshare_pct=?, revshare_gop_pct=?,
                        lease_monthly=?, advance_total=?, contract_months=?, api_key=?
                    WHERE id=?
                """, (
                    name.strip(), city.strip(), str(join_date), room_count, int(active),
                    contract_type,
                    revshare_pct if contract_type in {"revshare_revenue", "revshare_revenue_gop"} else None,
                    revshare_gop_pct if contract_type in {"revshare_gop", "revshare_revenue_gop"} else None,
                    lease_monthly if contract_type == "lease" else None,
                    advance_total if contract_type == "advance_payment" else None,
                    contract_months if contract_type == "advance_payment" else None,
                    final_api_key or None,
                    pid,
                ))
            st.success(f"✓ {name} updated.")
            st.rerun()

    if delete:
        # Check if property has data — warn before removing
        conn = get_connection()
        row_count = conn.execute(
            "SELECT COUNT(*) as n FROM raw_services WHERE property_id=?", (pid,)
        ).fetchone()["n"]
        conn.close()
        if row_count > 0:
            st.warning(
                f"⚠️ **{p['name']}** has {row_count:,} service rows in the database. "
                f"Removing it here only deactivates the property — raw data is preserved. "
                f"To fully delete, deactivate and clear manually from the DB."
            )
            with get_db() as conn:
                conn.execute("UPDATE properties SET active=0 WHERE id=?", (pid,))
            st.info("Property deactivated (data preserved).")
        else:
            with get_db() as conn:
                conn.execute("DELETE FROM properties WHERE id=?", (pid,))
            st.success(f"✓ {p['name']} removed.")
        st.rerun()


def _render_add_form():
    st.markdown("**New Property**")

    with st.form("add_property"):
        col1, col2 = st.columns(2)

        with col1:
            pid     = st.text_input("Property ID *", placeholder="e.g. PROP004 — must be unique")
            name    = st.text_input("Property Name *", placeholder="e.g. Hotel Santika Cirebon")
            city    = st.text_input("City", placeholder="e.g. Cirebon")
            join_date   = st.date_input("Join Date *", value=date.today())
            room_count  = st.number_input("Room Count *", min_value=1, value=30, step=1)

        with col2:
            contract_type = st.selectbox("Contract Type *", options=CONTRACT_TYPES)
            revshare_pct  = st.number_input(
                "RevShare % of Revenue", min_value=0.0, max_value=100.0,
                value=0.0, step=0.5,
                disabled=contract_type not in {"revshare_revenue", "revshare_revenue_gop"},
                help="REHAT's cut as % of total revenue",
            )
            revshare_gop_pct = st.number_input(
                "RevShare % of GOP", min_value=0.0, max_value=100.0,
                value=0.0, step=0.5,
                disabled=contract_type not in {"revshare_gop", "revshare_revenue_gop"},
                help="REHAT's cut as % of GOP (revenue minus costs)",
            )
            lease_monthly = st.number_input(
                "Monthly Lease (IDR)", min_value=0, step=1_000_000,
                disabled=contract_type != "lease",
            )
            advance_total = st.number_input(
                "Advance Payment Total (IDR)", min_value=0, step=1_000_000,
                disabled=contract_type != "advance_payment",
            )
            contract_months = st.number_input(
                "Contract Duration (months)", min_value=1, max_value=360, step=1, value=24,
                disabled=contract_type != "advance_payment",
                help="Advance payment amortized over this many months",
            )

        api_key = st.text_input("API Key", type="password", placeholder="Paste Exely API key (can add later)")
        st.caption("API key stored in local DB. You can update it anytime from the property edit form.")

        submitted = st.form_submit_button("➕ Add Property", type="primary", use_container_width=True)

    if submitted:
        errors = _validate(name, join_date, room_count, contract_type, revshare_pct, lease_monthly)
        if not pid.strip():
            errors.append("Property ID is required.")

        # Check duplicate ID
        conn = get_connection()
        existing = conn.execute("SELECT id FROM properties WHERE id=?", (pid.strip(),)).fetchone()
        conn.close()
        if existing:
            errors.append(f"Property ID '{pid.strip()}' already exists.")

        if errors:
            for e in errors:
                st.error(e)
        else:
            with get_db() as conn:
                conn.execute("""
                    INSERT INTO properties
                        (id, name, city, contract_type, join_date, room_count,
                         revshare_pct, revshare_gop_pct, lease_monthly, advance_total,
                         contract_months, api_key, active)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,1)
                """, (
                    pid.strip(), name.strip(), city.strip(),
                    contract_type, str(join_date), room_count,
                    revshare_pct if contract_type in {"revshare_revenue", "revshare_revenue_gop"} else None,
                    revshare_gop_pct if contract_type in {"revshare_gop", "revshare_revenue_gop"} else None,
                    lease_monthly if contract_type == "lease" else None,
                    advance_total if contract_type == "advance_payment" else None,
                    contract_months if contract_type == "advance_payment" else None,
                    api_key.strip() or None,
                ))
            st.success(f"✓ {name} added successfully.")
            st.rerun()


def _validate(name, join_date, room_count, contract_type, revshare_pct, lease_monthly) -> list[str]:
    errors = []
    if not str(name).strip():
        errors.append("Property name is required.")
    if room_count < 1:
        errors.append("Room count must be at least 1.")
    if contract_type in REVSHARE_TYPES and (revshare_pct is None or revshare_pct <= 0):
        errors.append("RevShare % must be > 0 for revshare contract types.")
    if contract_type == "lease" and (lease_monthly is None or lease_monthly <= 0):
        errors.append("Monthly lease amount must be > 0 for lease contracts.")
    return errors
