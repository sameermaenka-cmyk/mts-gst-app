import streamlit as st
import pdfplumber
import pandas as pd
import os, io, subprocess, sys, json, tempfile
from datetime import datetime
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request

st.set_page_config(page_title="MTS TIR GST Reconciliation", page_icon="🏪", layout="wide")

st.markdown("""
<style>
.main-header { font-size: 28px; font-weight: bold; color: #1a2d45; margin-bottom: 5px; }
.sub-header { font-size: 14px; color: #666; margin-bottom: 20px; }
</style>
""", unsafe_allow_html=True)

st.markdown('<div class="main-header">🏪 MTS TIR GST Reconciliation Tool</div>', unsafe_allow_html=True)
st.markdown('<div class="sub-header">MTS Ventures Pty Ltd – IGA Campbell Town | Reads actual GST from supplier invoices, verified against TIR amounts</div>', unsafe_allow_html=True)
st.markdown("---")

# Setup tokens from Streamlit secrets
def setup_tokens():
    token_dir = tempfile.mkdtemp()
    t1_path = os.path.join(token_dir, 'token.json')
    t2_path = os.path.join(token_dir, 'token2.json')
    try:
        with open(t1_path, 'w') as f:
            json.dump(dict(st.secrets["gmail"]["token1"]), f)
        with open(t2_path, 'w') as f:
            json.dump(dict(st.secrets["gmail"]["token2"]), f)
        return t1_path, t2_path, True
    except Exception as e:
        return None, None, False

def check_token_valid(token_data):
    try:
        creds = Credentials.from_authorized_user_info(token_data)
        return True
    except:
        return False

with st.sidebar:
    st.markdown("### ⚙️ System Status")
    try:
        t1 = st.secrets["gmail"]["token1"]
        t2 = st.secrets["gmail"]["token2"]
        st.markdown("✅ **Email 1:** iga.tas.campbelltown")
        st.markdown("✅ **Email 2:** office.iga.campbelltown")
        emails_ok = True
    except:
        st.markdown("❌ **Email 1:** Not configured")
        st.markdown("❌ **Email 2:** Not configured")
        emails_ok = False
    st.markdown("---")
    st.markdown("### 📖 How to use")
    st.markdown("1. Upload TIR PDF statement\n2. Click **Run Reconciliation**\n3. Review results\n4. Download Excel")
    st.markdown("---")
    st.markdown("🟢 **Green** = Verified\n\n🟠 **Orange** = Amount mismatch\n\n🔴 **Red** = Invoice not found")
    st.markdown("---")
    st.caption("MTS Ventures Pty Ltd | TIR GST Reconciliation Tool v1.0\nFor questions, contact Sameer")

uploaded_pdf = st.file_uploader("📤 Upload TIR Statement PDF", type=["pdf"])

if uploaded_pdf:
    st.success(f"✅ Loaded: **{uploaded_pdf.name}**")

    with pdfplumber.open(io.BytesIO(uploaded_pdf.read())) as pdf:
        pdf_text = "\n".join(page.extract_text() or "" for page in pdf.pages)

    with st.expander("📄 Preview TIR Statement"):
        st.text(pdf_text[:1000])

    st.markdown("---")

    if st.button("⚡ Run GST Reconciliation", type="primary", use_container_width=True):
        if not emails_ok:
            st.error("Gmail not configured. Contact Sameer.")
        else:
            with st.spinner("Processing all invoices... This will take 20-25 minutes. Please wait."):
                # Write tokens to temp files
                t1_path, t2_path, ok = setup_tokens()
                if not ok:
                    st.error("Could not load Gmail credentials.")
                else:
                    # Write API key to temp env
                    api_key = st.secrets.get("anthropic", {}).get("api_key", "")
                    
                    # Save uploaded PDF temporarily
                    tmp_pdf = tempfile.NamedTemporaryFile(delete=False, suffix='.pdf')
                    tmp_pdf.write(uploaded_pdf.getvalue())
                    tmp_pdf.close()

                    # Run the reconciliation script
                    env = os.environ.copy()
                    env['TOKEN1_PATH'] = t1_path
                    env['TOKEN2_PATH'] = t2_path
                    env['ANTHROPIC_API_KEY'] = api_key
                    env['TIR_PDF_PATH'] = tmp_pdf.name

                    result = subprocess.run(
                        [sys.executable, 'run.py'],
                        capture_output=True, text=True, timeout=2400, env=env
                    )

                    excel_path = 'MTS_GST_output.xlsx'
                    if os.path.exists(excel_path):
                        df = pd.read_excel(excel_path, skiprows=3)
                        df.columns = ['Date','Supplier','Invoice','TIR Amount','Invoice Total','Actual GST','Status']
                        df = df.dropna(subset=['Supplier'])
                        df = df[~df['Supplier'].astype(str).str.contains('TOTAL', na=False)]

                        verified = df[df['Status'].astype(str).str.contains('VERIFIED', na=False)]
                        not_found = df[df['Status'].astype(str).str.contains('NOT FOUND|NOT IN', na=False)]
                        mismatch = df[df['Status'].astype(str).str.contains('MISMATCH', na=False)]
                        total_gst = pd.to_numeric(verified['Actual GST'], errors='coerce').sum()

                        st.markdown("### 📊 Results Summary")
                        c1, c2, c3, c4 = st.columns(4)
                        c1.metric("✅ Verified", len(verified))
                        c2.metric("❌ Not Found", len(not_found))
                        c3.metric("⚠️ Mismatch", len(mismatch))
                        c4.metric("💰 Total GST", f"${total_gst:.2f}")

                        st.markdown("---")
                        t1tab, t2tab, t3tab = st.tabs(["✅ Verified", "❌ Not Found", "⚠️ Mismatch"])
                        with t1tab:
                            st.dataframe(verified.reset_index(drop=True), use_container_width=True)
                        with t2tab:
                            st.dataframe(not_found.reset_index(drop=True), use_container_width=True)
                        with t3tab:
                            st.dataframe(mismatch.reset_index(drop=True), use_container_width=True)

                        with open(excel_path, "rb") as f:
                            st.download_button(
                                "⬇️ Download Excel Report",
                                f.read(),
                                file_name=f"MTS_GST_{datetime.now().strftime('%d%b%Y')}.xlsx",
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                type="primary",
                                use_container_width=True
                            )
                    else:
                        st.error("Something went wrong.")
                        st.code(result.stdout[-2000:])
else:
    st.info("👆 Upload a TIR PDF statement above to get started")
    st.markdown("---")
    st.markdown("### 🔍 What this tool does")
    c1, c2, c3 = st.columns(3)
    with c1:
        st.markdown("**Step 1. Reads TIR Statement**\nExtracts all supplier invoices and amounts from your TIR PDF")
    with c2:
        st.markdown("**Step 2. Finds Invoice PDFs**\nSearches both Gmail accounts for matching supplier invoices")
    with c3:
        st.markdown("**Step 3. Verifies & Reports**\nMatches totals, extracts actual GST, exports to Excel")
