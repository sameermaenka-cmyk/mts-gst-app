import streamlit as st
import os
import anthropic
from run import get_api_key, get_services, parse_tir_pdf, reconcile, build_excel

st.set_page_config(page_title="MTS TIR GST Reconciliation", page_icon="🏪", layout="wide")
st.markdown('<h2 style="color:#1a2d45">🏪 MTS TIR GST Reconciliation Tool</h2>', unsafe_allow_html=True)
st.markdown('<p style="color:#666">MTS Ventures Pty Ltd – IGA Campbell Town</p>', unsafe_allow_html=True)
st.markdown("---")

with st.sidebar:
    st.markdown("### ⚙️ System Status")
    try:
        api_key = get_api_key()
        svc1, svc2 = get_services()
        if svc1 and api_key:
            st.markdown("✅ **Gmail 1:** Connected")
            if svc2:
                st.markdown("✅ **Gmail 2:** Connected")
            else:
                st.markdown("⚠️ **Gmail 2:** Not configured")
            st.markdown("✅ **Claude API:** Connected")
        else:
            st.markdown("❌ Secrets missing")
    except Exception as e:
        api_key = None
        svc1 = svc2 = None
        st.markdown(f"❌ Error: {e}")
    st.markdown("---")
    st.markdown("🟢 **Green** = Verified\n\n🟠 **Orange** = Mismatch\n\n🔴 **Red** = Not found\n\n⬜ **Grey** = Paper invoice")
    st.caption("MTS Ventures Pty Ltd | v1.0 | Contact Sameer")

uploaded_pdf = st.file_uploader("📤 Upload TIR Statement PDF", type=["pdf"])

if uploaded_pdf:
    st.success(f"✅ Loaded: **{uploaded_pdf.name}**")

    if st.button("⚡ Run GST Reconciliation", type="primary", use_container_width=True):
        if not api_key or not svc1:
            st.error("❌ Cannot run — API key or Gmail tokens not configured. Check sidebar for details.")
        else:
            pdf_bytes = uploaded_pdf.getvalue()
            client = anthropic.Anthropic(api_key=api_key)

            # Step 1: Parse TIR PDF
            with st.status("📄 Parsing TIR statement...", expanded=True) as parse_status:
                st.write("Extracting invoice data from the uploaded PDF...")
                try:
                    tir_data = parse_tir_pdf(pdf_bytes, client)
                    st.write(f"Found **{len(tir_data)} invoices** across "
                             f"**{len(set(r[1] for r in tir_data))} suppliers**")
                    parse_status.update(label=f"📄 Parsed {len(tir_data)} invoices", state="complete")
                except Exception as e:
                    st.error(f"Failed to parse TIR PDF: {e}")
                    st.stop()

            # Step 2: Reconcile
            progress_bar = st.progress(0, text="Starting reconciliation...")
            status_container = st.empty()

            def update_progress(i, total, supplier, inv_no, status):
                pct = i / total
                icon = "🟢" if "VERIFIED" in status else ("🟠" if "MISMATCH" in status else ("⬜" if "PAPER" in status else "🔴"))
                progress_bar.progress(pct, text=f"[{i}/{total}] {supplier} — {inv_no}")
                status_container.caption(f"{icon} {supplier} {inv_no}: {status}")

            with st.spinner("⚡ Reconciling invoices against Gmail... this takes 20-25 minutes."):
                try:
                    results = reconcile(tir_data, svc1, svc2, api_key, progress_callback=update_progress)
                except Exception as e:
                    st.error(f"Reconciliation failed: {e}")
                    st.stop()

            progress_bar.progress(1.0, text="✅ Reconciliation complete!")
            status_container.empty()

            # Step 3: Build Excel & show results
            # Derive title date from last TIR entry
            last_date = tir_data[-1][0] if tir_data else ''
            excel_bytes, verified, total, total_gst = build_excel(
                results, f'Week Ended {last_date}')

            # Summary metrics
            st.markdown("---")
            st.markdown("### 📊 Results")
            c1, c2, c3 = st.columns(3)
            with c1:
                st.metric("Verified", f"{verified}/{total}")
            with c2:
                st.metric("Total GST", f"${total_gst:,.2f}")
            with c3:
                st.metric("TIR Total", f"${sum(r[3] for r in results):,.2f}")

            # Results table
            verified_rows = [(r[0], r[1], r[2], f"${r[3]:,.2f}",
                              f"${r[5]:,.2f}" if r[5] else "–",
                              f"${r[4]:,.2f}" if r[4] else "–",
                              r[6]) for r in results]
            st.dataframe(
                [dict(zip(['Date', 'Supplier', 'Invoice', 'TIR Amount', 'GST', 'Invoice Total', 'Status'], row))
                 for row in verified_rows],
                use_container_width=True,
                hide_index=True,
            )

            # Download button
            filename = f"MTS_GST_{last_date.replace('/', '')}.xlsx"
            st.download_button(
                label="📥 Download Excel Report",
                data=excel_bytes,
                file_name=filename,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                type="primary",
                use_container_width=True,
            )

else:
    st.info("👆 Upload a TIR PDF statement above to get started")
    c1, c2, c3 = st.columns(3)
    with c1:
        st.markdown("**Step 1.** Upload TIR PDF")
    with c2:
        st.markdown("**Step 2.** Click Run")
    with c3:
        st.markdown("**Step 3.** Download Excel")
