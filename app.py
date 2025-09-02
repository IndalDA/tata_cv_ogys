import streamlit as st
import zipfile
import os
import pandas as pd
from datetime import datetime, timedelta
import tempfile
import shutil
import io
import warnings
import time
from streamlit_integration import StreamlitAuth
from auth_functions import AuthManager
from database_models import create_database_engine, get_session, User
from report import process_files
from log import ADMIN_EMAILS, show_user_log


st.set_page_config(page_title="Honda 2w", layout="wide") 
from database_models import log_event
ENGINE = create_database_engine()

def log_user_event(action, details=None, level="INFO"):
    log_event(
        ENGINE,
        user_id=st.session_state.get("user_id"),
        username=st.session_state.get("username"),
        email=st.session_state.get("email"),
        action=action,
        details=details,
        level=level
    )




st.title("🚗 Honda 2w Order Generator")
st.markdown("""
📊 Generate comprehensive reports from Honda 2w :
- OEM Reports
- Stock Reports
- Mrn Report
""")

state_vars = [
    "uploaded_file", "extracted_path", "validation_errors", "period_validation_errors",
    "missing_files", "validation_log", "continue_processing", "processing_complete",
    "report_results", "show_reports", "oem_mismatches","MRN_mismatches",
    "suppress_validation_display", "input_signature",
]

for var in state_vars:
    if var not in st.session_state:
        if var in ["validation_errors", "period_validation_errors", "missing_files"]:
            st.session_state[var] = []
        elif var in ["validation_log", "oem_mismatches", "MRN_mismatches","qty_mismatch_log"]:
            st.session_state[var] = pd.DataFrame()
        elif var in ["continue_processing", "processing_complete", "show_reports",
                    "suppress_validation_display"]:
            st.session_state[var] = False
        elif var == "report_results":
            st.session_state[var] = None
        else:
            st.session_state[var] = None

# periods
PERIOD_TYPES = { "Day": 1,"Week":7, "Month": 30, "Quarter": 180, "Year": 365}

    # ---------------- File Readers ---------------- #
    
def read_file(file_path):
    if not os.path.isfile(file_path):
        print(f"[ERROR] File not found: {file_path}")
        return None

    file_path_lower = file_path.lower()

    try:
        if file_path_lower.endswith('.xlsx'):
            return pd.read_excel(file_path, engine='openpyxl')

        elif file_path_lower.endswith('.xls'):
            try:
                return pd.read_excel(file_path, engine='xlrd')
            except:
                try:
                    return pd.read_excel(file_path, engine='openpyxl')
                except:
                    print(f"[WARN] Failed .xls read, trying as CSV: {file_path}")
                    return try_read_as_csv(file_path)

        elif file_path_lower.endswith('.xlsb'):
            try:
                return pd.read_excel(file_path, engine='pyxlsb')
            except Exception as e:
                print(f"[WARN] Failed .xlsb read: {e}, trying as CSV.")
                return try_read_as_csv(file_path)

        elif file_path_lower.endswith(('.csv', '.tsv', '.txt')):
            return try_read_as_csv(file_path)

        elif file_path_lower.endswith(('.html', '.htm')):
            return try_read_as_html(file_path)

        elif file_path_lower.endswith('.json'):
            return try_read_as_json(file_path)

        elif file_path_lower.endswith('.parquet'):
            return try_read_as_parquet(file_path)

        elif file_path_lower.endswith('.feather'):
            return try_read_as_feather(file_path)

        elif file_path_lower.endswith(('.pkl', '.pickle')):
            return try_read_as_pickle(file_path)

        else:
            print(f"[ERROR] Unsupported file type: {file_path}")
            return None

    except Exception as e:
        print(f"[ERROR] General read failure: {e}")
        return None


# ---------- SUPPORT FUNCTIONS ----------

def try_read_as_csv(file_path):
    try:
        return pd.read_csv(file_path, encoding='utf-8', sep=None, engine='python', on_bad_lines='skip')
    except UnicodeDecodeError:
        try:
            return pd.read_csv(file_path, encoding='windows-1252', sep=None, engine='python', on_bad_lines='skip')
        except Exception as e:
            print(f"[ERROR] CSV read failed with both encodings: {e}")
            return None


def try_read_as_html(file_path):
    try:
        tables = pd.read_html(file_path)
        if tables:
            return tables[0]  # return first table
        else:
            print(f"[WARN] No tables found in HTML: {file_path}")
            return None
    except Exception as e:
        print(f"[ERROR] HTML read failed: {e}")
        return None


def try_read_as_json(file_path):
    try:
        return pd.read_json(file_path, lines=True)
    except Exception as e:
        print(f"[ERROR] JSON read failed: {e}")
        return None


def try_read_as_parquet(file_path):
    try:
        return pd.read_parquet(file_path)
    except Exception as e:
        print(f"[ERROR] Parquet read failed: {e}")
        return None


def try_read_as_feather(file_path):
    try:
        return pd.read_feather(file_path)
    except Exception as e:
        print(f"[ERROR] Feather read failed: {e}")
        return None


def try_read_as_pickle(file_path):
    try:
        return pd.read_pickle(file_path)
    except Exception as e:
        print(f"[ERROR] Pickle read failed: {e}")
        return None
       

# ---------------- Validation Functions (periods) ---------------- #
def validate_periods(all_locations, start_date, end_date, period_days):
    validation_errors = []
    missing_periods_log = []

    periods = []
    current_date = start_date
    while current_date <= end_date:
        period_end = min(current_date + timedelta(days=period_days - 1), end_date)
        periods.append((current_date, period_end))
        current_date = period_end + timedelta(days=1)

    for brand, dealer, location, location_path in all_locations:
        oem_files = [f for f in os.listdir(location_path) if f.lower().startswith('bo')]
        mrn_files = [f for f in os.listdir(location_path) if f.lower().startswith('intransit')]

        oem_has_period = {p: False for p in periods}
        if oem_files:
            for oem_file in oem_files:
                try:
                    oem_df = read_file(os.path.join(location_path, oem_file))
                    if oem_df is None or oem_df.empty:
                        continue
                    oem_df['Order Date'] = pd.to_datetime(oem_df['Order Date'], errors='coerce')
                    for p in periods:
                        period_start, period_end = p
                        if any(period_start <= d.date() <= period_end for d in oem_df['Order Date'].dropna()):
                            oem_has_period[p] = True
                except Exception as e:
                    validation_errors.append(f"{location}: Error validating OEM periods - {str(e)}")
        
        mrn_has_period = {p: False for p in periods}
        if mrn_files: 
            for mrn_file in mrn_files:
                try:
                    mrn_df = read_file(os.path.join(location_path, mrn_file))
                    if mrn_df is None or mrn_df.empty:
                        continue
                    mrn_df['Purchase_Order_Date'] = pd.to_datetime(mrn_df['Purchase_Order_Date'], errors='coerce')
                    for p in periods:
                        period_start, period_end = p
                        if any(period_start <= d.date() <= period_end for d in mrn_df['Purchase_Order_Date'].dropna()):
                            mrn_has_period[p] = True
                except Exception as e:
                    validation_errors.append(f"{location}: Error validating Purchase Order Date periods - {str(e)}")

        for period_start, period_end in periods:
            missing_in = []
            if not oem_has_period[(period_start, period_end)]: missing_in.append("bo")
            if not mrn_has_period[(period_start, period_end)]: missing_in.append("intransit")

            if missing_in:
                missing_periods_log.append({
                    'Brand': brand, 'Dealer': dealer, 'Location': location,
                    'Period': f"{period_start} to {period_end}",
                    'Missing In': ", ".join(missing_in)
                })
                validation_errors.append(f"{location}: {' and '.join(missing_in)} missing for period {period_start} to {period_end}")

    # Ensure that missing_periods_log and validation_errors always return
    validation_log_df = pd.DataFrame(missing_periods_log) if missing_periods_log else pd.DataFrame(
        columns=['Brand', 'Dealer', 'Location', 'Period', 'Missing In']
    )

    # Return the results even if no errors are found
    return validation_errors if validation_errors else [], validation_log_df if not validation_log_df.empty else pd.DataFrame()

            
def validate_oem_mrn_po_codes(all_locations):
    """Safe/lenient for Hyundai; returns empty dataframes if structure not found."""
    try:
        df = pd.read_excel(
            r"https://docs.google.com/spreadsheets/d/e/2PACX-1vTeXEadE1Hf4G2T-o4XCvGYMyRKj6f2sVxsSDaPs_sJwmGbnCFoDzSJx9JHDaNzw5JKdk4l0Q0Yctmh/pub?output=xlsx"
        )
    except Exception:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    # Not used as a blocker here
    return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()


# ---------------- UI Functions ---------------- #
# def show_validation_issues():
#     if st.session_state.get("suppress_validation_display", False):
#         return

#     # file missing
#     st.warning("⚠ Validation Issues Found")
#     if st.session_state.missing_files:
#         st.write("#### Missing Files:")
#         for msg in st.session_state.missing_files:
#             st.write(f"- {msg}")
#     if st.session_state.period_validation_errors:
#         st.write("#### Missing Period Data:")
#         st.write(f"Found {len(st.session_state.period_validation_errors)} period validation issues")
#         for error in st.session_state.period_validation_errors[:2]:
#             st.write(f"- {error}")
#         if len(st.session_state.period_validation_errors) > 2:
#             st.write(f"- ... and {len(st.session_state.period_validation_errors)-2} more")

#     col3, col4 = st.columns(2)
#     with col3:
#         if not st.session_state.validation_log.empty:
#             st.download_button(
#                 "📥 Download Full Validation Log",
#                 data=st.session_state.validation_log.to_csv(index=False).encode('utf-8'),
#                 file_name="validation_issues_log.csv",
#                 mime="text/csv"
#             )
#     with col4:
#         pass

#     col1, col2 = st.columns(2)
#     with col1:
#         if st.button("✅ Continue Anyway", key="btn_continue_anyway"):
#             st.session_state.continue_processing = True
#             st.session_state.suppress_validation_display = True  
#             st.rerun()
#     with col2:
#         if st.button("❌ Stop Processing"):
#             st.session_state.continue_processing = False
#             st.session_state.show_reports = False
#             st.warning("Processing stopped by user")
#             time.sleep(1)
#             st.stop()

def show_validation_issues():
    if st.session_state.get("suppress_validation_display", False):
        return

    # Check if there are missing files or period validation issues
    if st.session_state.missing_files or st.session_state.period_validation_errors:
        # Show validation issues message
        st.warning("⚠ Validation Issues Found")

        # Display missing files if any
        if st.session_state.missing_files:
            st.write("#### Missing Files:")
            for msg in st.session_state.missing_files:
                st.write(f"- {msg}")
        
        # Display period validation errors if any
        if st.session_state.period_validation_errors:
            st.write("#### Missing Period Data:")
            st.write(f"Found {len(st.session_state.period_validation_errors)} period validation issues")
            for error in st.session_state.period_validation_errors[:2]:
                st.write(f"- {error}")
            if len(st.session_state.period_validation_errors) > 2:
                st.write(f"- ... and {len(st.session_state.period_validation_errors)-2} more")

        # Provide options to the user
        col1, col2 = st.columns(2)
        with col1:
            # Button to continue despite validation issues
            if st.button("✅ Continue Anyway", key="btn_continue_anyway"):
                st.session_state.continue_processing = True
                st.session_state.suppress_validation_display = True  # Hide validation issues after proceeding
                st.rerun()  # Re-run to continue processing with current state

        with col2:
            # Button to stop processing
            if st.button("❌ Stop Processing"):
                st.session_state.continue_processing = False
                st.session_state.show_reports = False
                st.warning("Processing stopped by user")
                time.sleep(1)
                st.stop()  # Stops the script execution

    else:
        # If there are no validation issues, process directly
        with st.spinner("Processing files..."):
            process_files([], all_locations, start_date, end_date, len(all_locations), progress_bar, status_text, select_categories)
            time.sleep(0.5)  # Small delay for smoother UI
        st.session_state.processing_complete = True
        st.session_state.show_reports = True



def show_reports():
    st.success("🎉 Reports generated successfully!")
    if st.session_state.report_results:
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for file_name, df in st.session_state.report_results.items():
                excel_buffer = io.BytesIO()
                with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
                    df.to_excel(writer, index=False)
                zipf.writestr(file_name, excel_buffer.getvalue())
        st.download_button(
            "📦 Download All Reports as ZIP",
            data=zip_buffer.getvalue(),
            file_name="Hyundai_Reports.zip",
            mime="application/zip"
        )

# ---------------- Sidebar ---------------- #

auth = StreamlitAuth()

# with st.sidebar:
#     auth.require_auth()



if st.session_state.get("user_id") or not  st.session_state.get("user_id") :
    User = st.session_state.get("user_id", "")
    with st.expander("User Information"):
        st.write(f"**User_ID:** {User}")


    with st.sidebar:
    #  auth.require_auth()
        st.header("⚙ Settings")
        uploaded_file = st.file_uploader("Upload Honda 2w ZIP file", type=['zip'])
        if uploaded_file is not None:
            st.session_state.uploaded_file = uploaded_file

        select_categories = st.multiselect(
            "Choose categories",
            options=['Spares', 'Accessories', 'All'],
            default=['Spares']
        )

        default_end = datetime.today()
        default_start = default_end - timedelta(days=61)
        start_date = st.date_input("Start Date", value=default_start)
        end_date = st.date_input("End Date", value=default_end)
        period_type = st.selectbox("Select period type", options=list(PERIOD_TYPES.keys()))
        st.session_state.period_type = period_type
        process_btn = st.button("🚀 Generate Reports", type="primary")

    # ---- Reset suppression flag when inputs change ----
    sig_file = st.session_state.uploaded_file.name if st.session_state.uploaded_file else "nofile"
    input_signature = f"{sig_file}|{start_date}|{end_date}|{st.session_state.period_type}|{tuple(sorted(select_categories))}"
    if st.session_state.get("input_signature") != input_signature:
        st.session_state.input_signature = input_signature
        st.session_state.suppress_validation_display = False
        st.session_state.continue_processing = False

    # ---------------- Main Processing ---------------- #
    if (process_btn or st.session_state.continue_processing) and st.session_state.uploaded_file is not None:
        if st.session_state.uploaded_file.size > 200 * 1024 * 1024:
            st.error("File size exceeds 200MB limit")
            st.stop()

        temp_dir = tempfile.mkdtemp()
        extract_path = os.path.join(temp_dir, "extracted_files")
        os.makedirs(extract_path, exist_ok=True)

        try:
            with zipfile.ZipFile(st.session_state.uploaded_file, 'r') as zip_ref:
                zip_ref.extractall(extract_path)
            st.session_state.extracted_path = extract_path
            st.success("✅ ZIP file extracted successfully")

            all_locations = []
            for brand in os.listdir(extract_path):
                brand_path = os.path.join(extract_path, brand)
                if not os.path.isdir(brand_path): continue
                for dealer in os.listdir(brand_path):
                    dealer_path = os.path.join(brand_path, dealer)
                    if not os.path.isdir(dealer_path): continue
                    for location in os.listdir(dealer_path):
                        location_path = os.path.join(dealer_path, location)
                        if os.path.isdir(location_path):
                            all_locations.append((brand, dealer, location, location_path))

            # file presence checks
            missing_files = []
            for brand, dealer, location, location_path in all_locations:
                required = {
                    'stock': False, 'intransit': False, 'Bo': False}
                
                for file in os.listdir(location_path):
                    f = file.lower()
                    if f.startswith('bo') or f.startswith('po'): required['Bo'] = True
                    if f.startswith('intransit') or f.startswith('intransit'): required['intransit'] = True
                    if f.startswith('stock') or f.startswith('stock'): required['stock'] = True
                    #if f.startswith('Stock'): required['Stock'] = True

                for k, v in required.items():
                    if not v:
                        missing_files.append(f"{brand}/{dealer}/{location} - Missing: {k}")

            period_days = PERIOD_TYPES.get(st.session_state.period_type, 1)
        
            period_validation_errors, validation_log = validate_periods(all_locations, start_date, end_date, period_days)
            
            # Ensure that if None is returned, it defaults to an empty list or DataFrame
            if period_validation_errors is None:
                period_validation_errors = []

            if validation_log is None:
                validation_log = pd.DataFrame()

            
            # save validation state
            st.session_state.missing_files = missing_files
            st.session_state.period_validation_errors = period_validation_errors
            st.session_state.validation_log = validation_log
            st.session_state.oem_mismatches = pd.DataFrame()
            st.session_state.mrn_files_mismatches = pd.DataFrame()
            st.session_state.stock_mismatches = pd.DataFrame()


            if st.session_state.continue_processing:
                progress_bar = st.progress(0)
                status_text = st.empty()
                with st.spinner("Processing files..."):
                    process_files([], all_locations, start_date, end_date, len(all_locations), progress_bar, status_text, select_categories)
                    time.sleep(0.5)
                st.session_state.processing_complete = True
                st.session_state.show_reports = True
                st.session_state.continue_processing = False
            else:
                st.session_state.show_reports = False

        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    # ---------------- Output ---------------- #
    if st.session_state.uploaded_file is not None:
        # Show blocking/non-blocking validations as appropriate
        if (
            st.session_state.missing_files
            or st.session_state.period_validation_errors):

            show_validation_issues()







