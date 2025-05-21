import streamlit as st
import pandas as pd
import zipfile, io, re, tempfile, gdown, calendar
from datetime import date, datetime
from dateutil.relativedelta import relativedelta

# ──────────────────────────────────────────────
# ページ設定
# ──────────────────────────────────────────────
st.set_page_config(page_title="プロジェクト収益 v7.1", layout="wide")

# ──────────────────────────────────────────────
# 共通ユーティリティ
# ──────────────────────────────────────────────
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


# ── CSV / ZIP ローダ ---------------------------------------------------------
def load_csv(uploaded):
    """① CSV ② ZIP 内 CSV を DataFrame で返す"""
    if uploaded is None:
        return None

    # ZIP
    if uploaded.name.lower().endswith(".zip"):
        try:
            with zipfile.ZipFile(uploaded) as zf:
                csv_files = [n for n in zf.namelist()
                             if n.lower().endswith(".csv") and not n.endswith("/")]
                if not csv_files:
                    st.error("ZIP に CSV が見つかりません。")
                    return None
                target = csv_files[0] if len(csv_files) == 1 else \
                         st.selectbox("ZIP 内 CSV を選択してください", csv_files)
                with zf.open(target) as fp:
                    for enc in ("utf-8-sig", "cp932", "utf-8"):
                        try:
                            txt = fp.read().decode(enc)
                            return pd.read_csv(io.StringIO(txt))
                        except Exception:
                            fp.seek(0)
        except zipfile.BadZipFile:
            st.error("ZIP ファイルが壊れています。")
            return None

    # 通常 CSV
    for enc in ("utf-8-sig", "cp932", "utf-8"):
        try:
            txt = uploaded.read().decode(enc)
            return pd.read_csv(io.StringIO(txt))
        except Exception:
            uploaded.seek(0)

    st.error("CSV の読み込みに失敗しました。文字コードを確認してください。")
    return None


# ── Google Drive 共有リンク ---------------------------------------------------
def read_gdrive_csv_gdown(url: str, **kw) -> pd.DataFrame:
    """共有リンク → gdown で DL → DataFrame"""
    with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp:
        gdown.download(url=url, output=tmp.name, quiet=False, fuzzy=True)
        return pd.read_csv(tmp.name, **kw)

# ──────────────────────────────────────────────
# サイドバー : データ入力（Expander）
# ──────────────────────────────────────────────
with st.sidebar.expander("📂 データアップロード / 選択", expanded=True):
    # 仕訳帳
    tab_local, tab_drive = st.tabs(["ローカル CSV / ZIP", "Google Drive 共有リンク"])

    with tab_local:
        uploaded_file = st.file_uploader("仕訳帳 (CSV / ZIP)", type=["csv", "zip"])

    with tab_drive:
        gdrive_url = st.text_input("共有リンクを貼って Enter",
                                   placeholder="https://drive.google.com/file/d/…/view?usp=sharing")
        if gdrive_url and not gdrive_url.startswith("http"):
            st.warning("リンク形式が正しくありません。")
            gdrive_url = ""

    st.markdown("---")

    # 取引マスタ / 稼働コスト
    master_file = st.file_uploader("取引マスタ (CSV)", type="csv")
    cost_file   = st.file_uploader("稼働コスト (CSV)", type="csv")

# ──────────────────────────────────────────────
# 仕訳帳読込
# ──────────────────────────────────────────────
df_src = None
if uploaded_file is not None:
    df_src = load_csv(uploaded_file)
elif gdrive_url:
    try:
        df_src = read_gdrive_csv_gdown(gdrive_url, encoding="cp932")
    except Exception as e:
        st.error(f"Google Drive 読み込み失敗: {e}")

if df_src is None:
    st.stop()

st.success(f"仕訳帳を読み込みました ({len(df_src):,} 行)")
df_src["取引日"] = pd.to_datetime(df_src["取引日"], errors="coerce")

# 以降: 取引マスタ / 稼働コスト / pivot / フィルター計算 は v7.0 と同じ
# -------------------------------------------------------------------
# 取引マスタ読込 -----------------------------------------------------
master_map = None
if master_file:
    df_master = load_csv(master_file)
    if df_master is not None:
        df_master.columns = df_master.columns.map(str.strip)
        id_col   = detect_col(df_master.columns, ["取引id","レコードid","recordid","dealid"])
        amt_col  = detect_col(df_master.columns, ["金額","売上金額","見積金額","amount"])
        if id_col and amt_col:
            df_master[id_col]  = df_master[id_col].map(normalize_id)
            df_master[amt_col] = pd.to_numeric(df_master[amt_col], errors="coerce").fillna(0)
            keep = {id_col:"取引コード", amt_col:"マスター金額"}
            # 追加列
            for src, dst in [(["取引先","会社名","customer","client"],"マスター取引先"),
                             (["取引名","案件名","dealname","title"],"取引名"),
                             (["取引担当者","担当者","owner","sales"],"取引担当者"),
                             (["industry","業界"],"Industry"),
                             (["industry詳細","industrydetail","業界詳細"],"Industry詳細")]:
                c = detect_col(df_master.columns, src)
                if c: keep[c] = dst
            master_map = (df_master[list(keep)]
                          .dropna(subset=[id_col])
                          .rename(columns=keep)
                          .drop_duplicates(subset=["取引コード"]))

# 稼働コスト読込 -----------------------------------------------------
df_cost_raw = None
if cost_file:
    df_cost_raw = load_csv(cost_file)        # 後で Utilization に使う
    if df_cost_raw is not None:
        df_cost_raw.columns = df_cost_raw.columns.map(str.strip)

# -------------- 既存ロジックで daily を作成 (売上・費用・人件費) --------------
# 売上・費用整形
df_sales = df_src[df_src.get("貸方勘定科目") == "売上高"].copy()
df_sales_out = pd.DataFrame({
    "取引コード": df_sales["貸方部門"].astype(str).str.strip(),
    "取引先": df_sales["貸方取引先名"],
    "勘定科目": "売上高",
    "金額": pd.to_numeric(df_sales["貸方金額"], errors="coerce").fillna(0),
    "日付": df_sales["取引日"]
})

targets = ["外注費","交際費","旅費交通費"]
df_exp = df_src[df_src.get("借方勘定科目").isin(targets)].copy()
df_exp_out = pd.DataFrame({
    "取引コード": df_exp["借方部門"].astype(str).str.strip(),
    "取引先": df_exp["借方取引先名"],
    "勘定科目": df_exp["借方勘定科目"],
    "金額": pd.to_numeric(df_exp["借方金額"], errors="coerce").fillna(0),
    "日付": df_exp["取引日"]
})
combo = pd.concat([df_sales_out, df_exp_out], ignore_index=True)

# pivot
daily = (combo.pivot_table(index="取引コード", columns="勘定科目",
                           values="金額", aggfunc="sum",
                           fill_value=0)
         .reset_index())
meta = (combo.groupby("取引コード")
             .agg({"日付":["min","max"],"取引先":"first"}).reset_index())
meta.columns = ["取引コード","日付（最小）","日付（最大）","取引先"]
daily = daily.merge(meta, on="取引コード", how="left")
if "売上高" not in daily.columns:
    daily["売上高"] = 0

# 人件費 (稼働コスト CSV) ------------------------------------------------------
if df_cost_raw is not None:
    id_c   = detect_col(df_cost_raw.columns, ["取引id","レコードid","recordid"])
    cost_c = detect_col(df_cost_raw.columns, ["稼働コスト","人件費","cost"])
    name_c = detect_col(df_cost_raw.columns, ["会社名","取引先","client","customer"])
    if id_c and cost_c:
        df_cost_raw[id_c] = df_cost_raw[id_c].map(normalize_id)
        df_cost = df_cost_raw.dropna(subset=[id_c]).copy()
        df_cost["人件費"] = pd.to_numeric(df_cost[cost_c], errors="coerce").fillna(0)
        agg = {"人件費":"sum"}
        if name_c: agg[name_c] = "稼働取引先"
        cost_info = (df_cost.groupby(id_c, as_index=False)
                     .agg(agg)
                     .rename(columns={id_c:"取引コード"}))
        daily["取引コード"] = daily["取引コード"].map(normalize_id)
        daily = daily.merge(cost_info, on="取引コード", how="left")
    else:
        daily["人件費"] = 0
else:
    daily["人件費"] = 0

# マスタ補完 -----------------------------------------------------------
if master_map is not None:
    daily = daily.merge(master_map, on="取引コード", how="left")
    need_fix = daily["売上高"] == 0
    daily.loc[need_fix, "売上高"] = daily.loc[need_fix, "マスター金額"]
    has_cost_name = daily["稼働取引先"].notna()
    daily.loc[need_fix & has_cost_name, "取引先"] = \
        daily.loc[need_fix & has_cost_name, "稼働取引先"]

# 指標計算 -------------------------------------------------------------
for c in ["売上高","外注費","交際費","旅費交通費","人件費"]:
    daily[c] = pd.to_numeric(daily[c], errors="coerce").fillna(0)
daily["月数"] = daily.apply(
    lambda r: max((r["日付（最大）"].year - r["日付（最小）"].year)*12 +
                  (r["日付（最大）"].month - r["日付（最小）"].month) + 1, 1),
    axis=1)
daily["月次売上"] = daily.apply(
    lambda r: r["売上高"]/r["月数"] if r["月数"] else 0, axis=1)
daily["粗利"] = (daily["売上高"] - daily["外注費"] - daily["交際費"]
                 - daily["旅費交通費"] - daily["人件費"])
daily["粗利率"] = daily.apply(
    lambda r: (r["粗利"]/r["売上高"]*100) if r["売上高"]>0 else 0, axis=1)
daily.rename(columns={"取引コード":"レコードID"}, inplace=True)

# ──────────────────────────────────────────────
# フィルター UI
# ──────────────────────────────────────────────
st.sidebar.markdown("### 🔍 フィルター設定")
id_val = st.sidebar.text_input("取引ID（部分一致可）", "")
min_date = df_src["取引日"].min().date()
max_date = df_src["取引日"].max().date()
start_date, end_date = st.sidebar.date_input("期間範囲", [min_date, max_date])

owner_sel = ind_sel = ind_det_sel = []
if "取引担当者" in daily.columns:
    owner_sel = st.sidebar.multiselect("取引担当者",
                                       sorted(daily["取引担当者"].dropna().unique()))
if "Industry" in daily.columns:
    ind_sel = st.sidebar.multiselect("Industry",
                                     sorted(daily["Industry"].dropna().unique()))
if "Industry詳細" in daily.columns:
    ind_det_sel = st.sidebar.multiselect("Industry詳細",
                                         sorted(daily["Industry詳細"].dropna().unique()))

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
# 月次テーブル & 案件数（期間内のみ）
# ──────────────────────────────────────────────
records_sales, records_profit = [], []
for _, r in df_filtered.iterrows():
    for i in range(int(r["月数"])):
        dt_m = r["日付（最小）"] + relativedelta(months=i)
        if not (start_date <= dt_m.date() <= end_date):
            continue
        ym = dt_m.strftime("%y/%m")
        records_sales.append({"年月表示":ym,"レコードID":r["レコードID"],
                              "取引先":r["取引先"],
                              "月次売上":round(r["月次売上"])})
        records_profit.append({"年月表示":ym,"レコードID":r["レコードID"],
                               "取引先":r["取引先"],
                               "月次粗利":round(r["粗利"]/r["月数"]) if r["月数"] else 0})
df_ms = pd.DataFrame(records_sales)
df_mp = pd.DataFrame(records_profit)

df_sales_p  = (df_ms.pivot_table(index=["レコードID","取引先"],
                                 columns="年月表示",
                                 values="月次売上", fill_value=0).reset_index())
df_profit_p = (df_mp.pivot_table(index=["レコードID","取引先"],
                                 columns="年月表示",
                                 values="月次粗利", fill_value=0).reset_index())
time_cols = sorted(df_ms["年月表示"].unique().tolist())

summary_sales = pd.DataFrame({
    "レコードID":["",""], "取引先":["①月次売上合計","②平均売上単価"],
    **{c:[df_sales_p[c].sum(), df_sales_p[c].mean()] for c in time_cols}
})
summary_profit = pd.DataFrame({
    "レコードID":["",""], "取引先":["③月次粗利合計","④平均粗利単価"],
    **{c:[df_profit_p[c].sum(), df_profit_p[c].mean()] for c in time_cols}
})
for c in time_cols:
    summary_sales[c]  = summary_sales[c].map(lambda x:f"{int(x):,}")
    summary_profit[c] = summary_profit[c].map(lambda x:f"{int(x):,}")

count_series = (df_ms[df_ms["月次売上"]>0]
                .groupby("年月表示")["レコードID"].nunique()
                .reindex(time_cols, fill_value=0))

# ──────────────────────────────────────────────
# Utilization 計算
# ──────────────────────────────────────────────
util_hours_pivot = pd.DataFrame()
util_pct_pivot   = pd.DataFrame()
std_hours_row    = {}

if df_cost_raw is not None:
    # 対象列
    cons_c  = detect_col(df_cost_raw.columns, ["コンサルタント","氏名","name"])
    hours_c = detect_col(df_cost_raw.columns, ["稼働時間","hours","workinghours","h"])
    date_c  = detect_col(df_cost_raw.columns, ["日付","稼働日","date"])
    if cons_c and hours_c and date_c:
        dc = df_cost_raw[[cons_c, hours_c, date_c]].copy()
        dc[date_c] = pd.to_datetime(dc[date_c], errors="coerce")
        dc["年月表示"] = dc[date_c].dt.strftime("%y/%m")
        # フィルター期間
        dc = dc[(dc[date_c].dt.date >= start_date) &
                (dc[date_c].dt.date <= end_date)]
        # 月次稼働時間
        util_hours  = (dc.pivot_table(index=cons_c, columns="年月表示",
                                      values=hours_c, aggfunc="sum",
                                      fill_value=0)
                       .reindex(columns=time_cols, fill_value=0))
        util_hours_pivot = util_hours.reset_index()
        # 標準稼働時間
        for col in time_cols:
            dt = datetime.strptime(col, "%y/%m")
            _, last_day = calendar.monthrange(dt.year, dt.month)
            wd = pd.date_range(dt.strftime("%Y-%m-01"),
                               dt.strftime(f"%Y-%m-{last_day}"),
                               freq="B").size
            std_hours_row[col] = wd * 8
        # チャージャビリティ %
        util_pct = util_hours.copy()
        for col in time_cols:
            util_pct[col] = (util_pct[col] / std_hours_row[col]) \
                            .replace([float("inf"), -float("inf")], 0)
        util_pct_pivot = util_pct.reset_index()
    else:
        st.sidebar.warning("稼働コストに コンサル名 / 稼働時間 / 日付 列が見つかりません。")

# ──────────────────────────────────────────────
# 画面表示
# ──────────────────────────────────────────────
tab1, tab2, tab3 = st.tabs(["📊 Chart view","📋 Table view","📈 Utilization"])

# ----- Chart view ------------------------------------------------------------
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

# ----- Table view ------------------------------------------------------------
with tab2:
    st.subheader("📄 プロジェクト収益一覧")
    show_cols = ["レコードID","取引先","売上高","粗利","粗利率"]
    st.dataframe(df_filtered[show_cols], use_container_width=True)
    st.download_button("💾 粗利集計CSV",
                       data=df_filtered.to_csv(index=False, encoding="utf-8-sig"),
                       file_name="粗利集計.csv")

    st.subheader("📋 月次売上 / 粗利")
    def _format(df):
        for c in time_cols:
            df[c] = df[c].map(lambda x:f"{int(x):,}")
        return df
    st.dataframe(_format(df_sales_p.copy()),
                 use_container_width=True)

# ----- Utilization view ------------------------------------------------------
with tab3:
    st.subheader("標準稼働時間 / 月")
    if std_hours_row:
        std_df = pd.DataFrame([std_hours_row], index=["標準稼働時間(h)"])
        st.table(std_df)
    else:
        st.info("稼働コストファイルに稼働時間が無いため利用率を計算できません。")

    if not util_hours_pivot.empty:
        st.subheader("コンサルタント別・月次稼働時間 (h)")
        st.dataframe(util_hours_pivot, use_container_width=True)

        st.subheader("コンサルタント別・チャージャビリティ (%)")
        pct_df = util_pct_pivot.copy()
        for c in time_cols:
            pct_df[c] = pct_df[c].map(lambda x:f"{x:.0%}")
        st.dataframe(pct_df, use_container_width=True)
