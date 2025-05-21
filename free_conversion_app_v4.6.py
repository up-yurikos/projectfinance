import streamlit as st
import pandas as pd
import zipfile, io, re
from datetime import date
from dateutil.relativedelta import relativedelta

# ──────────────────────────────────────────────
# ページ設定
# ──────────────────────────────────────────────
st.set_page_config(page_title="プロジェクト収益 v6.9",
                   layout="wide")

# ──────────────────────────────────────────────
# ユーティリティ
# ──────────────────────────────────────────────
def load_csv(uploaded_file):
    if uploaded_file is None:
        return None
    for enc in ("utf-8-sig", "cp932", "utf-8"):
        try:
            uploaded_file.seek(0)
            return pd.read_csv(io.StringIO(uploaded_file.read().decode(enc)))
        except Exception:
            continue
    return None


def detect_col(cols, patterns):
    for p in patterns:
        hits = [c for c in cols if re.sub(r"\s+", "", str(c)).casefold() == p]
        if hits:
            return hits[0]
    return None


def normalize_id(x):
    if pd.isna(x):
        return None
    s = str(x).strip()
    return None if s == "" or s.lower() in {"nan", "none"} else s


# ──────────────────────────────────────────────
# CSV アップロード UI（折りたたみ）
# ──────────────────────────────────────────────
with st.sidebar.expander("📂 CSV アップロード", expanded=True):
    uploaded_file = st.file_uploader("仕訳帳データ", type=["csv","zip"])
    master_file   = st.file_uploader("取引マスタ", type="csv")
    cost_file     = st.file_uploader("稼働コスト", type="csv")

if uploaded_file is None:
    st.sidebar.warning("仕訳帳データをアップロードしてください。")
    st.stop()

# ──────────────────────────────────────────────
# 仕訳帳読込
# ──────────────────────────────────────────────
df_src = load_csv(uploaded_file)
if df_src is None:
    st.error("仕訳帳CSV の読み込みに失敗しました。")
    st.stop()

df_src["取引日"] = pd.to_datetime(df_src.get("取引日"), errors="coerce")

# ──────────────────────────────────────────────
# 取引マスタ読込
# ──────────────────────────────────────────────
master_map = None
if master_file:
    df_master = load_csv(master_file)
    if df_master is not None:
        df_master.columns = df_master.columns.map(str.strip)

        id_col   = detect_col(df_master.columns,
                              ["取引id","レコードid","recordid","dealid"])
        name_col = detect_col(df_master.columns,
                              ["取引先","会社名","customer","client"])
        amt_col  = detect_col(df_master.columns,
                              ["金額","売上金額","見積金額","amount"])
        title_col   = detect_col(df_master.columns,
                                 ["取引名","案件名","dealname","title"])
        owner_col   = detect_col(df_master.columns,
                                 ["取引担当者","担当者","owner","sales"])
        ind_col     = detect_col(df_master.columns,
                                 ["industry","業界"])
        ind_det_col = detect_col(df_master.columns,
                                 ["industry詳細","industrydetail","業界詳細"])

        if id_col and amt_col:
            df_master[id_col]  = df_master[id_col].map(normalize_id)
            df_master[amt_col] = pd.to_numeric(df_master[amt_col],
                                               errors="coerce").fillna(0)

            keep = {id_col: "取引コード",
                    amt_col: "マスター金額"}
            if name_col:   keep[name_col]   = "マスター取引先"
            if title_col:  keep[title_col]  = "取引名"
            if owner_col:  keep[owner_col]  = "取引担当者"
            if ind_col:    keep[ind_col]    = "Industry"
            if ind_det_col:keep[ind_det_col]= "Industry詳細"

            master_map = (df_master[list(keep)]
                          .dropna(subset=[id_col])
                          .rename(columns=keep)
                          .drop_duplicates(subset=["取引コード"]))
        else:
            st.sidebar.warning("⚠️ マスタ: ID または 金額 列が不足")

# ──────────────────────────────────────────────
# 仕訳データ整形（売上・費用）
# ──────────────────────────────────────────────
df_sales = df_src[df_src.get("貸方勘定科目") == "売上高"].copy()
df_sales_out = pd.DataFrame({
    "取引コード": df_sales["貸方部門"].astype(str).str.strip(),
    "取引先"    : df_sales["貸方取引先名"],
    "勘定科目"  : "売上高",
    "金額"      : pd.to_numeric(df_sales["貸方金額"], errors="coerce").fillna(0),
    "日付"      : df_sales["取引日"]
})

targets = ["外注費", "交際費", "旅費交通費"]
df_exp = df_src[df_src.get("借方勘定科目").isin(targets)].copy()
df_exp_out = pd.DataFrame({
    "取引コード": df_exp["借方部門"].astype(str).str.strip(),
    "取引先"    : df_exp["借方取引先名"],
    "勘定科目"  : df_exp["借方勘定科目"],
    "金額"      : pd.to_numeric(df_exp["借方金額"], errors="coerce").fillna(0),
    "日付"      : df_exp["取引日"]
})

combo = pd.concat([df_sales_out, df_exp_out], ignore_index=True)

# ──────────────────────────────────────────────
# pivot → daily
# ──────────────────────────────────────────────
daily = (combo
         .pivot_table(index="取引コード",
                      columns="勘定科目",
                      values="金額",
                      aggfunc="sum",
                      fill_value=0)
         .reset_index())

meta = (combo.groupby("取引コード")
             .agg({"日付": ["min","max"], "取引先":"first"})
             .reset_index())
meta.columns = ["取引コード","日付（最小）","日付（最大）","取引先"]

daily = daily.merge(meta, on="取引コード", how="left")
if "売上高" not in daily.columns:
    daily["売上高"] = 0

# ──────────────────────────────────────────────
# 稼働コスト統合（人件費・稼働取引先）
# ──────────────────────────────────────────────
if cost_file:
    df_cost = load_csv(cost_file)
    if df_cost is not None:
        df_cost.columns = df_cost.columns.map(str.strip)
        id_c   = detect_col(df_cost.columns,
                            ["取引id","レコードid","recordid"])
        cost_c = detect_col(df_cost.columns,
                            ["稼働コスト","人件費","cost"])
        name_c = detect_col(df_cost.columns,
                            ["会社名","取引先","client","customer"])
        if id_c and cost_c:
            df_cost[id_c] = df_cost[id_c].map(normalize_id)
            df_cost = df_cost.dropna(subset=[id_c])
            df_cost["人件費"] = pd.to_numeric(df_cost[cost_c],
                                              errors="coerce").fillna(0)

            agg = {"人件費": "sum"}
            if name_c:
                agg[name_c] = "first"
            cost_info = (df_cost
                         .groupby(id_c, as_index=False)
                         .agg(agg)
                         .rename(columns={id_c:"取引コード"}))
            if name_c:
                cost_info = cost_info.rename(columns={name_c:"稼働取引先"})
            daily["取引コード"] = daily["取引コード"].map(normalize_id)
            daily = daily.merge(cost_info, on="取引コード", how="left")
        else:
            daily["人件費"] = 0
    else:
        daily["人件費"] = 0
else:
    daily["人件費"] = 0

# ──────────────────────────────────────────────
# マスタ補完（売上のみ、取引先は稼働CSV優先）
# ──────────────────────────────────────────────
if master_map is not None:
    daily = daily.merge(master_map, on="取引コード", how="left")
    need_fix = daily["売上高"] == 0
    daily.loc[need_fix, "売上高"] = daily.loc[need_fix, "マスター金額"]
    has_cost_name = daily["稼働取引先"].notna()
    daily.loc[need_fix & has_cost_name, "取引先"] = \
        daily.loc[need_fix & has_cost_name, "稼働取引先"]

# ──────────────────────────────────────────────
# 指標計算
# ──────────────────────────────────────────────
num_cols = daily.select_dtypes(include="number").columns
daily[num_cols] = daily[num_cols].fillna(0)
for c in ["売上高","外注費","交際費","旅費交通費","人件費"]:
    daily[c] = pd.to_numeric(daily[c], errors="coerce").fillna(0)

daily["月数"] = daily.apply(
    lambda r: max((r["日付（最大）"].year - r["日付（最小）"].year)*12 +
                  (r["日付（最大）"].month - r["日付（最小）"].month) + 1, 1),
    axis=1)
daily["月次売上"] = daily.apply(
    lambda r: r["売上高"]/r["月数"] if r["月数"]>0 else 0,
    axis=1)
daily["粗利"] = (daily["売上高"] - daily["外注費"]
                 - daily["交際費"] - daily["旅費交通費"] - daily["人件費"])
daily["粗利率"] = daily.apply(
    lambda r: (r["粗利"]/r["売上高"]*100) if r["売上高"]>0 else 0,
    axis=1)

daily.rename(columns={"取引コード":"レコードID"}, inplace=True)

# ──────────────────────────────────────────────
# フィルター UI
# ──────────────────────────────────────────────
st.sidebar.markdown("### 🔍 フィルター設定")

id_val = st.sidebar.text_input("取引ID（部分一致可）", "")

min_date = df_src["取引日"].min().date()
max_date = df_src["取引日"].max().date()
start_date, end_date = st.sidebar.date_input("期間範囲",
                                             [min_date, max_date])

# 追加フィルター（複数選択）
owner_sel, ind_sel, ind_det_sel = [], [], []
if "取引担当者" in daily.columns:
    options = sorted(daily["取引担当者"].dropna().unique())
    owner_sel = st.sidebar.multiselect("取引担当者", options)
if "Industry" in daily.columns:
    options = sorted(daily["Industry"].dropna().unique())
    ind_sel = st.sidebar.multiselect("Industry", options)
if "Industry詳細" in daily.columns:
    options = sorted(daily["Industry詳細"].dropna().unique())
    ind_det_sel = st.sidebar.multiselect("Industry詳細", options)

# ──────────────────────────────────────────────
# レコードフィルター
# ──────────────────────────────────────────────
mask  = daily["レコードID"].str.contains(id_val, na=False)
mask &= daily["日付（最大）"].dt.date >= start_date
mask &= daily["日付（最小）"].dt.date <= end_date

if owner_sel:
    mask &= daily["取引担当者"].isin(owner_sel)
if ind_sel:
    mask &= daily["Industry"].isin(ind_sel)
if ind_det_sel:
    mask &= daily["Industry詳細"].isin(ind_det_sel)

df_filtered = daily[mask].copy()

# ──────────────────────────────────────────────
# 月次テーブル & 案件数生成（期間内の月のみ）
# ──────────────────────────────────────────────
records_sales, records_profit = [], []
for _, r in df_filtered.iterrows():
    for i in range(int(r["月数"])):
        dt_month = r["日付（最小）"] + relativedelta(months=i)
        if not (start_date <= dt_month.date() <= end_date):
            continue
        ym_disp = dt_month.strftime("%y/%m")
        records_sales.append({
            "年月表示": ym_disp, "レコードID": r["レコードID"],
            "取引先": r["取引先"], "月次売上": round(r["月次売上"])
        })
        records_profit.append({
            "年月表示": ym_disp, "レコードID": r["レコードID"],
            "取引先": r["取引先"],
            "月次粗利": round(r["粗利"]/r["月数"]) if r["月数"]>0 else 0
        })

df_ms = pd.DataFrame(records_sales)
df_mp = pd.DataFrame(records_profit)

df_sales_p = (df_ms.pivot_table(index=["レコードID","取引先"],
                                columns="年月表示",
                                values="月次売上",
                                aggfunc="sum", fill_value=0)
              .reset_index())
df_profit_p = (df_mp.pivot_table(index=["レコードID","取引先"],
                                 columns="年月表示",
                                 values="月次粗利",
                                 aggfunc="sum", fill_value=0)
               .reset_index())

time_cols = sorted(df_ms["年月表示"].unique().tolist())

summary_sales = pd.DataFrame({
    "レコードID":["",""],
    "取引先":["①月次売上合計","②平均売上単価"],
    **{c:[df_sales_p[c].sum(), df_sales_p[c].mean()] for c in time_cols}
})
for c in time_cols:
    summary_sales[c] = summary_sales[c].map(lambda x: f"{int(x):,}")

summary_profit = pd.DataFrame({
    "レコードID":["",""],
    "取引先":["③月次粗利合計","④平均粗利単価"],
    **{c:[df_profit_p[c].sum(), df_profit_p[c].mean()] for c in time_cols}
})
for c in time_cols:
    summary_profit[c] = summary_profit[c].map(lambda x: f"{int(x):,}")

count_series = (df_ms[df_ms["月次売上"]>0]
                .groupby("年月表示")["レコードID"]
                .nunique()
                .reindex(time_cols, fill_value=0))

# ──────────────────────────────────────────────
# 粗利集計表
# ──────────────────────────────────────────────
base_cols = ["レコードID","日付（最小）","日付（最大）","取引先"]
extra_cols = [c for c in ["取引名","取引担当者","Industry","Industry詳細"]
              if c in df_filtered.columns]
metric_cols = ["売上高","人件費","外注費",
               "交際費","旅費交通費","粗利","粗利率"]
disp_cols = base_cols + extra_cols + metric_cols

disp = df_filtered[disp_cols].copy()
disp["日付（最小）"] = disp["日付（最小）"].dt.date
disp["日付（最大）"] = disp["日付（最大）"].dt.date
for c in ["売上高","人件費","外注費","交際費","旅費交通費","粗利"]:
    disp[c] = disp[c].map(lambda x: f"{int(x):,}")
disp["粗利率"] = disp["粗利率"].map(lambda x: f"{x:.1f}%")

# ──────────────────────────────────────────────
# 表示
# ──────────────────────────────────────────────
tab1, tab2 = st.tabs(["📊 Chart view", "📋 Table view"])

with tab1:
    st.subheader("月次合計売上・粗利")
    st.line_chart(pd.DataFrame({
        "月次売上合計":[int(summary_sales.loc[0,c].replace(',','')) for c in time_cols],
        "月次粗利合計":[int(summary_profit.loc[0,c].replace(',','')) for c in time_cols]
    }, index=time_cols))

    st.subheader("月次平均売上・粗利")
    st.line_chart(pd.DataFrame({
        "平均売上単価":[int(summary_sales.loc[1,c].replace(',','')) for c in time_cols],
        "平均粗利単価":[int(summary_profit.loc[1,c].replace(',','')) for c in time_cols]
    }, index=time_cols))

    st.subheader("案件数")
    st.bar_chart(pd.DataFrame({"案件数":count_series}, index=time_cols))

with tab2:
    st.subheader("📄 プロジェクト収益一覧")
    st.dataframe(disp, use_container_width=True)
    st.download_button("💾 粗利集計CSV",
        data=df_filtered[disp_cols].to_csv(index=False, encoding="utf-8-sig"),
        file_name="粗利集計.csv")

    st.subheader("📋 月次売上")
    ms_disp = df_sales_p.copy()
    for c in time_cols:
        ms_disp[c] = ms_disp[c].map(lambda x: f"{int(x):,}")
    st.dataframe(ms_disp[["レコードID","取引先"]+time_cols],
                 use_container_width=True)
    st.download_button("💾 月次売上一覧CSV",
        data=df_sales_p.to_csv(index=False, encoding="cp932"),
        file_name="月次売上一覧.csv")

    st.subheader("📋 月次粗利")
    mp_disp = df_profit_p.copy()
    for c in time_cols:
        mp_disp[c] = mp_disp[c].map(lambda x: f"{int(x):,}")
    st.dataframe(mp_disp[["レコードID","取引先"]+time_cols],
                 use_container_width=True)
    st.download_button("💾 月次粗利一覧CSV",
        data=df_profit_p.to_csv(index=False, encoding="cp932"),
        file_name="月次粗利一覧.csv")
