import streamlit as st
import os

st.set_page_config(page_title="MTS TIR GST Reconciliation", page_icon="🏪", layout="wide")
st.markdown('<h2 style="color:#1a2d45">🏪 MTS TIR GST Reconciliation Tool</h2>', unsafe_allow_html=True)
st.markdown('<p style="color:#666">MTS Ventures Pty Ltd – IGA Campbell Town</p>', unsafe_allow_html=True)
st.markdown("---")

with st.sidebar:
    st.markdown("### ⚙️ System Status")
    try:
        t1 = os.environ.get("TOKEN1") or st.secrets.get("TOKEN1")
        t2 = os.environ.get("TOKEN2") or st.secrets.get("TOKEN2")
        api = os.environ.get("ANTHROPIC_API_KEY") or st.secrets.get("ANTHROPIC_API_KEY")
        if t1 and t2 and api:
            st.markdown("✅ **Email 1:** iga.tas.campbelltown")
            st.markdown("✅ **Email 2:** office.iga.campbelltown")
        else:
            st.markdown("❌ Secrets missing")
    except Exception as e:
        st.markdown(f"❌ Error: {e}")
    st.markdown("---")
    st.markdown("🟢 **Green** = Verified\n\n🟠 **Orange** = Mismatch\n\n🔴 **Red** = Not found")
    st.caption("MTS Ventures Pty Ltd | v1.0 | Contact Sameer")

uploaded_pdf = st.file_uploader("📤 Upload TIR Statement PDF", type=["pdf"])
if uploaded_pdf:
    st.success(f"✅ Loaded: **{uploaded_pdf.name}**")
    if st.button("⚡ Run GST Reconciliation", type="primary", use_container_width=True):
        st.info("Processing... this takes 20-25 minutes. Do not close this tab.")
else:
    st.info("👆 Upload a TIR PDF statement above to get started")
    c1,c2,c3 = st.columns(3)
    with c1: st.markdown("**Step 1.** Upload TIR PDF")
    with c2: st.markdown("**Step 2.** Click Run")
    with c3: st.markdown("**Step 3.** Download Excel")
