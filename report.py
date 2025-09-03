def process_files(validation_errors, all_locations, start_date, end_date, total_locations,
                  progress_bar, status_text, select_categories,IStatacv):

    import streamlit as st
    import os, io, zipfile
    import pandas as pd
    import re
    from collections import defaultdict
    from datetime import datetime
    import urllib.error

    Cur = datetime.now()
    Cur = Cur.date()


    # ---------- fetch master ----------
    try:
        Loc_master = pd.read_csv(
            r'https://docs.google.com/spreadsheets/d/e/2PACX-1vRH7652aG_wJDydjSITcOxRJAMhHAXwmy5w6GIWaeW4Y9S9KEonu_R4or-fATFv29EaJ3uOXP5OrnEA/pub?gid=254040781&single=true&output=csv'
        )
    except urllib.error.URLError:
        st.warning("⚠ Unable to fetch master data from Google Sheets. Please check your internet connection.")
        Loc_master = pd.DataFrame()

    # ---------- storages ----------
    file_bytes = {}   
    previews   = {}   

    def _store_xlsx(name: str, df: pd.DataFrame):
        previews[name] = df.copy()
        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="openpyxl") as w:
            df.to_excel(w, index=False)
        file_bytes[name] = buf.getvalue()

    
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


    # ---------- per location ----------
    for i, (brand, dealer, location, location_path) in enumerate(all_locations):
        progress_bar.progress((i + 1) / max(total_locations, 1))
        status_text.text(f"Generating reports for {location} ({i+1}/{total_locations})...")

        # reset per-location collectors
        #mrn_list, stock_list, po_list = [], [], []
        stock =[]
        bo =[]
        Intransit =[]
        CBO =[]

        for fname in os.listdir(location_path):
            file_path = os.path.join(location_path, fname)
            if not os.path.isfile(file_path):
                continue

            file = fname.lower().strip()
            if file.lower().startswith("stock"):
                df = read_file(file_path)
                df['Brand']=brand
                df['Dealer']=dealer
                df['Location']=location
                df['filename']=file
                df = df[['Brand','Dealer','Location','filename','Part #','Qty','Inventory Location','Status','Availability']]
                stock.append(df)
            elif file.lower().startswith("bo"):
                df = read_file(file_path)
                df['Brand']=brand
                df['Dealer']=dealer
                df['Location']=location
                df['filename']=file
                df = df[['Brand','Dealer','Location','filename','Division','Order Number','Order Date','Part No','Days Pending','Pending Qty.']]
                bo.append(df)
            elif file.lower().startswith("intransit"):
                df = read_file(file_path)
                df['Brand']=brand
                df['Dealer']=dealer
                df['Location']=location
                df['filename']=file
                df = df[['Brand','Dealer','Location','filename','Order #','Part #','Recd Qty','Division Name','Status','Invoice_Date','Purchase_Order_Date']]
                Intransit.append(df)

            elif file.lower().startswith("cbo"):
                df = read_file(file_path)
                df['Brand']=brand
                df['Dealer']=dealer
                df['Location']=location
                df['filename']=file
                df = df[['Brand','Dealer','Location','filename','Account code','Account Contact No.','Order Number',
                            'Order Date','Spares Order Type','Part No','Pending Qty','Division']]
                CBO.append(df)

        # ---------- create & store reports FOR THIS LOCATION ----------
        if stock:
            stk_Df = pd.concat(stock,ignore_index=True)
            stk_Df['Qty'] = pd.to_numeric(stk_Df['Qty'].astype(str).str.replace(',', '', regex=False),errors='coerce')
            stk= stk_Df[(stk_Df['Qty']>0)&(stk_Df['Status']=='Good')&(stk_Df['Availability']=='On Hand')]
            stk_f= Loc_master.merge(stk,left_on='Code',right_on='Inventory Location',how='inner')
            stk_fn = stk_f[['Brand_x','Dealer Name','Final Location','Part #','Qty']]
            stk_fn.rename(columns={'Brand_x':'Brand','Dealer Name':'Dealer','Final Location':'Location','Part #':'Partnumber'},inplace=True)
            stk_fn['Partnumber']=stk_fn['Partnumber'].astype(str)
           
            # **Generate report per dealer**
            stk_filename = f"stock_{brand}_{dealer}_{location}.xlsx"
            _store_xlsx(stk_filename, stk_fn)

        if bo:
            bo_Df = pd.concat(bo,ignore_index=True)
            bo_Df['Days Pending'] = pd.to_numeric(bo_Df['Days Pending'].astype(str).str.replace(',', '', regex=False),errors='coerce')
            bo_Loc = Loc_master.merge(bo_Df,left_on='Code',right_on='Division',how='inner')
            if IStatacv==True:
                Bo_ex = bo_Loc[bo_Loc['Days Pending']<=45][['Brand_x','Dealer Name','Final Location','Order Number','Order Date','Part No','Pending Qty.','Days Pending','filename']]
            else:
                Bo_ex = bo_Loc[bo_Loc['Days Pending']<=35][['Brand_x','Dealer Name','Final Location','Order Number','Order Date','Part No','Pending Qty.','Days Pending','filename']]

           # Bo_ex = bo_Loc[bo_Loc['Days Pending']<=45][['Brand_x','Dealer Name','Final Location','Order Number','Order Date','Part No','Pending Qty.','Days Pending','filename']]
            Bo_ex.rename(columns={'Brand_x':'Brand','Dealer Name':'Dealer','Final Location':'Location','Part No':'Partnumber',
                            'Order Number':'OrderNumber','Pending Qty.':'POQty','Order Date':'OrderDate','Days Pending':'DaysPending'},inplace=True)
            Bo_ex['OEMInvoiceNo']=''
            Bo_ex['OEMInvoiceDate']=''
            Bo_ex['OEMInvoiceQty']=''
            Bo_up = Bo_ex[['Brand','Dealer','Location','OrderNumber','OrderDate','Partnumber','POQty','OEMInvoiceNo','OEMInvoiceDate','OEMInvoiceQty','filename']]
            Bo_up['Partnumber']=Bo_up['Partnumber'].astype(str)
            d  = Bo_ex['OrderDate'].max()
            if IStatacv==True:          
              Bo_up.drop(Bo_up[((Bo_ex['OrderNumber'].str.contains('SAP-000'))|(Bo_ex['OrderNumber'].str.contains('SAP-200'))|(Bo_ex['OrderNumber'].str.contains('TOP')))&
                          (Bo_ex['OrderDate']<pd.to_datetime(d))].index,inplace=True)
            bo_con = Bo_up.copy()

        if Intransit:
            Intransit_Df = pd.concat(Intransit,ignore_index=True)
            Intransit_Df = Intransit_Df[Intransit_Df['Status']=='In Transit']
            Intransit_Df['Invoice_Date'] = pd.to_datetime(Intransit_Df['Invoice_Date'],errors='coerce')
            Intransit_Df['Purchase_Order_Date'] = pd.to_datetime(Intransit_Df['Purchase_Order_Date'],errors='coerce')
            Intransit_Df['Recd Qty']=pd.to_numeric(Intransit_Df['Recd Qty'].astype(str).str.replace(',', '', regex=False),errors='coerce')
            Int_Df = Loc_master.merge(Intransit_Df,left_on='Code',right_on='Division Name',how='inner')
            Int_Df['In_d'] = (pd.to_datetime(Cur).normalize() - Int_Df['Invoice_Date'].dt.normalize()).dt.days
            Int_ex = Int_Df[(Int_Df['In_d']<90)&(Int_Df['Recd Qty']>0)][['Brand_x','Dealer Name','Final Location','Order #','Purchase_Order_Date','Part #','Recd Qty',
                                                                         'In_d','filename']]
            Int_ex['OEMInvoiceNo']=''
            Int_ex['OEMInvoiceDate']=''
            Int_ex['OEMInvoiceQty']=''
            Int_ex.rename(columns={'Brand_x':'Brand','Dealer Name':'Dealer','Final Location':'Location','Part #':'Partnumber',
                            'Order #':'OrderNumber','Recd Qty':'POQty','Purchase_Order_Date':'OrderDate'},inplace=True)
            Int_up = Int_ex[['Brand','Dealer','Location','OrderNumber','OrderDate','Partnumber','POQty','OEMInvoiceNo','OEMInvoiceDate','OEMInvoiceQty','filename']]
            Int_up['Partnumber']=Int_up['Partnumber'].astype(str)
            Int_con = Int_up.copy()
            OEMinvoice=pd.concat([bo_con,Int_con],ignore_index=True)
            #OEMinvoice.to_excel(f'OEMinvoice_{brand}_{dealer}_{location}2.xlsx',index=False)
            po_filename = f"OEM_{brand}_{dealer}_{location}.xlsx"
            _store_xlsx(po_filename, OEMinvoice)
        if CBO:
          df=pd.concat(CBO,ignore_index=True) 
          if 'Order Reason' in df.columns:
            cbo = df[(~df['Order Reason'].str.contains('VOR Order CVBU').fillna(False)&(df['Order Reason']!='TOPS')&
            (~df['Order Reason'].str.contains('EXP - Express Order').fillna(False))&(df['Order Reason']!='Prolife Stock Order')
            &((df['Order Item Status'].str.lower()!='cancelled')&(df['Order Item Status'].str.lower()!='cancel')))]
          else:
            cbo =df.copy()
          cbo_df = Loc_master.merge(cbo,left_on='Code',right_on='Division',how='inner')
          st.write("Columns in cbo_df before selecting:", cbo_df.columns.tolist())
          cbo_f = cbo_df[['Brand_x','Dealer Name','Final Location','Account Name','Account City','Account code',
                          'Order Number','Order Date', 'Part No','Pending Qty']]
          cbo_f.rename(columns={'Brand_x':'Brand','Dealer Name':'Dealer','Final Location':'Location',
                      'Account Name':'PartyName','Account code':'PartyCode','Order Number':'OrderNumber',
                      'Order Date':'OrderDate','Part No':'Partnumber','Pending Qty':'Qty'
                      },inplace=True)
          cbo_filename = f"CBO_{brand}_{dealer}_{location}.xlsx"
          _store_xlsx(cbo_filename, cbo_f)
        Brand_name =brand            
    if validation_errors:
        st.warning("⚠ Validation issues found:")
        for error in validation_errors:
            st.write(f"- {error}")

    st.success("🎉 Reports generated successfully!")
    st.subheader("📥 Download Reports")

    report_types = {
        'OEM':   [k for k in file_bytes.keys() if k.startswith('OEM_')],
        'Stock': [k for k in file_bytes.keys() if k.lower().startswith('stock_')],
        'Cbo':   [k for k in file_bytes.keys() if k.startswith('CBO_')],
        'PO':    [k for k in file_bytes.keys() if k.startswith('Po_')],
    }

    # show previews + individual downloads
    for rtype, name_list in report_types.items():
        if name_list:
            with st.expander(f"📂 {rtype} Reports ({len(name_list)})", expanded=False):
                for fname in name_list:
                    df = previews.get(fname)
                    if df is not None and not df.empty:
                        st.markdown(f"### 📄 {fname}")
                        st.dataframe(df.head(5))

                        excel_buffer = io.BytesIO()
                        with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
                            df.to_excel(writer, index=False, sheet_name='Sheet1')
                        st.download_button(
                            label="⬇ Download Excel",
                            data=excel_buffer.getvalue(),
                            file_name=fname,
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            key=f"dl_{fname}"
                        )
                    else:
                        st.warning(f"⚠ No data for {fname}")

    # ---------- Create ZIP for all reports ----------
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zipf:
        # Add each file to the ZIP
        for file_name, file_data in file_bytes.items():
            zipf.writestr(file_name, file_data)
    foldername=f"{Brand_name},_Combined_Dealerwise_Reports.zip"
    # ---------- UI: Download ZIP ----------
    st.download_button(
        label="📦 Download Combined Dealer Reports ZIP",
        data=zip_buffer.getvalue(),
        file_name=foldername,
        mime="application/zip"
    )

#    st.success("🎉 Reports generated successfully!")

















