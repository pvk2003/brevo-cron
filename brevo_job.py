import os
import re
import html
import json
import time
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict, Tuple

import requests
import gspread
from google.oauth2.service_account import Credentials


# =========================
# CONFIG
# =========================
SPREADSHEET_ID = "1tgU5PRlCVY7H_Z5XpyeP-CCEuiWYKK0L4iRLIwl2fQM"  # chỉ ID
SERVICE_ACCOUNT_JSON = "./service_account.json"

CAMPAIGNS_SHEET = "campaigns"   # nếu bạn vẫn dùng Sheet1 thì đổi thành "Sheet1"
ACCOUNTS_SHEET  = "accounts"
LOGS_SHEET      = "send_logs"
SEND_NOW = False   # True = tạo xong gửi luôn, False = chỉ tạo (hoặc schedule nếu có scheduledAt)

# DRY_RUN=True: không gọi Brevo, chỉ render HTML + ghi preview file
# DRY_RUN=False: gọi API tạo campaign thật
DRY_RUN = False

# # Nếu campaign không có account_name -> dùng account này
# DEFAULT_ACCOUNT_NAME = "default"

BREVO_BASE = "https://api.brevo.com/v3"

# Nếu muốn schedule hằng ngày:
# SEND_EVERY_DAY_AT_VN = {"hour": 9, "minute": 0}
SEND_EVERY_DAY_AT_VN = None
ACTIVATE_SCHEDULE = False     # MẶC ĐỊNH AN TOÀN

# =========================
# HTML SHELL (cố định tối thiểu)
# =========================
# =========================
# HTML SHELL (giống mẫu ISSale)
# =========================
# =========================
# HTML SHELL (giống mẫu ISSale)
# =========================
HTML_SHELL = """<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Strict//EN"
  "http://www.w3.org/TR/xhtml1/DTD/xhtml1-strict.dtd">
<html xmlns="http://www.w3.org/1999/xhtml">
<head>
  <meta http-equiv="Content-Type" content="text/html; charset=utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>{{TITLE}}</title>

  <style type="text/css">
    body {
      margin: 0;
      padding: 0;
      width: 100% !important;
      background-color: #ffffff;
      font-family: arial, helvetica, sans-serif;
      color: #3b3f44;
      font-size: 16px;
      line-height: 1.5;
    }
    table { border-collapse: collapse; }
    a { color: #0092ff; text-decoration: underline; }
    p { margin: 0 0 12px 0; }
    ul { margin: 0 0 12px 20px; padding: 0; }
    li { margin-bottom: 6px; }
  </style>
</head>

<body>
  <table width="100%" cellpadding="0" cellspacing="0" border="0" bgcolor="#ffffff">
    <tr>
      <td align="center">
        <table width="600" cellpadding="0" cellspacing="0" border="0"
               style="max-width:600px; width:100%; background-color:#ffffff;">
          <tr>
            <td style="padding: 15px;">
              {{HEADER}}
              {{BODY}}
              {{FOOTER}}
            </td>
          </tr>
        </table>
      </td>
    </tr>
  </table>
</body>
</html>
"""

# def normalize_header_footer(raw: str) -> str:
#     """
#     Header/Footer lấy từ Google Sheet:
#     - Nếu có tag HTML (p/br/a/strong/em/ul/ol/li/hr) => dùng luôn
#     - Nếu là text thường => convert sang <p> + <br>, auto-link url/email
#     """
#     s = (raw or "").strip()
#     if not s:
#         return ""

#     looks_like_html = bool(re.search(r"</?(p|br|a|strong|em|ul|ol|li|hr)\b", s, flags=re.I))
#     if looks_like_html:
#         return s

#     # text thường -> html
#     t = s.replace("\r\n", "\n").replace("\r", "\n").strip()
#     t = html.escape(t)

#     # autolink email
#     t = re.sub(
#         r"([A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,})",
#         r'<a href="mailto:\1">\1</a>',
#         t,
#         flags=re.IGNORECASE
#     )

#     # autolink url
#     t = re.sub(
#         r"(https?://[^\s<]+)",
#         r'<a href="\1" target="_blank">\1</a>',
#         t,
#         flags=re.IGNORECASE
#     )

#     blocks = re.split(r"\n\s*\n", t)
#     out = []
#     for b in blocks:
#         b = b.strip()
#         if not b:
#             continue
#         out.append("<p>" + "<br>\n".join(b.split("\n")) + "</p>")
#     return "\n".join(out)

# def build_full_html(title: str, message: str, header_raw: str, footer_raw: str) -> str:
#     header_html = normalize_header_footer(header_raw)
#     footer_html = normalize_header_footer(footer_raw)

#     # BODY vẫn dùng renderer hiện tại của bạn (ul/ol/paragraph như screenshot)
#     body_html = message_to_html_like_screenshot(message)

#     # LƯU Ý: body_html đang tạo <p>, <ul>, <ol> => sẽ tự ăn style ở <style> phía trên
#     return (HTML_SHELL
#             .replace("{{TITLE}}", html.escape(title or "Email Campaign"))
#             .replace("{{HEADER}}", header_html)
#             .replace("{{BODY}}", body_html)
#             .replace("{{FOOTER}}", footer_html))


# =========================
# Helpers: Google Sheets
# =========================
def assert_service_account_file(path: str) -> None:
    if os.path.exists(path) and os.path.getsize(path) > 0:
        return
    raise FileNotFoundError(
        f"service_account.json không tồn tại hoặc rỗng: {path}\n"
        f"- Hãy đặt key JSON của Service Account tại đúng path này."
    )

def gs_client():
    assert_service_account_file(SERVICE_ACCOUNT_JSON)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_JSON, scopes=scopes)
    return gspread.authorize(creds)

def get_or_create_worksheet(sh, title: str, rows=2000, cols=20):
    try:
        return sh.worksheet(title)
    except gspread.exceptions.WorksheetNotFound:
        return sh.add_worksheet(title=title, rows=str(rows), cols=str(cols))

def ensure_headers(ws, headers):
    """
    Non-destructive:
    - If row1 empty: set exactly headers
    - If missing headers: append at end in one update
    """
    existing = ws.row_values(1)
    if not existing:
        ws.update("A1", [headers])
        return headers

    missing = [h for h in headers if h not in existing]
    if missing:
        merged = existing + missing
        ws.update("A1", [merged])
        return merged

    return existing

def header_map(ws):
    headers = ws.row_values(1)
    return {h: i+1 for i, h in enumerate(headers)}  # 1-based col index

def update_cells(ws, updates: List[Tuple[int, int, object]]):
    """
    Batch update single-cells in ONE API call.
    updates: list of (row, col, value)
    """
    if not updates:
        return
    data = []
    for r, c, v in updates:
        a1 = gspread.utils.rowcol_to_a1(r, c)
        data.append({"range": a1, "values": [[v]]})
    ws.batch_update(data)


# =========================
# Helpers: Brevo
# =========================
def brevo_headers(api_key: str) -> dict:
    return {"api-key": api_key, "Content-Type": "application/json", "Accept": "application/json"}

def iso_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")

def next_schedule_utc(vn_hour: int, vn_minute: int) -> datetime:
    now_utc = datetime.now(timezone.utc)
    vn_now = now_utc + timedelta(hours=7)
    vn_target = vn_now.replace(hour=vn_hour, minute=vn_minute, second=0, microsecond=0)
    if vn_now >= vn_target:
        vn_target += timedelta(days=1)
    return vn_target - timedelta(hours=7)

def scheduled_at_from_account(acc: dict) -> Optional[str]:
    """
    Lấy giờ gửi theo VN từ sheet accounts:
      - send_time_vn: "HH:MM"
      - send_offset_min: số phút lệch thêm (0/60/120...)
    Return: ISO UTC string cho Brevo scheduledAt
    """
    send_time = (acc.get("send_time_vn") or "").strip()
    if not send_time:
        return None  # không schedule nếu không set giờ

    try:
        hh, mm = send_time.split(":")
        dt_utc = next_schedule_utc(int(hh), int(mm))
    except Exception:
        raise RuntimeError(f"send_time_vn không đúng format HH:MM: '{send_time}'")

    offset_raw = (acc.get("send_offset_min") or "").strip()
    try:
        offset_min = int(offset_raw) if offset_raw else 0
    except:
        raise RuntimeError(f"send_offset_min phải là số (phút): '{offset_raw}'")

    dt_utc = dt_utc + timedelta(minutes=offset_min)
    return iso_utc(dt_utc)

def iso_z_to_dt(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00"))

def create_campaign(api_key: str, payload: dict) -> int:
    r = requests.post(f"{BREVO_BASE}/emailCampaigns",
                      headers=brevo_headers(api_key),
                      json=payload,
                      timeout=60)
    if r.status_code not in (200, 201):
        raise RuntimeError(f"{r.status_code}: {r.text}")
    return r.json()["id"]

def send_campaign_now(api_key: str, campaign_id: int) -> None:
    r = requests.post(
        f"{BREVO_BASE}/emailCampaigns/{campaign_id}/sendNow",
        headers=brevo_headers(api_key),
        timeout=60
    )
    if r.status_code not in (200, 201, 204):
        raise RuntimeError(f"{r.status_code}: {r.text}")

def brevo_check_account(api_key: str):
    r = requests.get(
        f"{BREVO_BASE}/account",
        headers={"api-key": api_key, "Accept": "application/json"},
        timeout=60
    )
    return r.status_code, r.text


# ---- cache /account ----
_ACCOUNT_CHECK_CACHE: Dict[str, bool] = {}
def brevo_check_account_cached(api_key: str) -> bool:
    """
    Return True if /account returns 200.
    Cache per api_key to avoid repeated calls.
    """
    key = (api_key or "").strip()
    if not key:
        return False
    if key in _ACCOUNT_CHECK_CACHE:
        return _ACCOUNT_CHECK_CACHE[key]

    st, _ = brevo_check_account(key)
    ok = (st == 200)
    _ACCOUNT_CHECK_CACHE[key] = ok
    return ok


def pick_working_account(accounts: dict, preferred: Optional[str] = None) -> List[str]:
    """
    Trả về danh sách account theo thứ tự ưu tiên:
    1) preferred (nếu có)
    2) các account còn lại
    """
    order = []
    if preferred and preferred in accounts:
        order.append(preferred)
    for k in accounts.keys():
        if k not in order:
            order.append(k)
    return order

def select_account_or_none(accounts: dict, preferred: Optional[str]):
    """
    Chọn account có api_key + /account ok (cached).
    Return: (account_name, acc_dict) hoặc (None, None)
    """
    for name in pick_working_account(accounts, preferred):
        acc = accounts.get(name)
        if not acc:
            continue
        api_key = (acc.get("api_key") or "").strip()
        if not api_key:
            continue
        if brevo_check_account_cached(api_key):
            return name, acc
    return None, None


# ---- paginate lists + cache per api_key ----
_LISTS_CACHE: Dict[str, List[dict]] = {}

def get_lists_paginated(api_key: str, limit: int = 50) -> List[dict]:
    """
    Fetch ALL lists via pagination. Cached per api_key.
    """
    key = (api_key or "").strip()
    if not key:
        return []
    if key in _LISTS_CACHE:
        return _LISTS_CACHE[key]

    all_lists: List[dict] = []
    offset = 0
    while True:
        r = requests.get(
            f"{BREVO_BASE}/contacts/lists?limit={limit}&offset={offset}",
            headers={"api-key": key, "Accept": "application/json"},
            timeout=60
        )
        r.raise_for_status()
        chunk = r.json().get("lists", []) or []
        all_lists.extend(chunk)

        if len(chunk) < limit:
            break
        offset += limit

    _LISTS_CACHE[key] = all_lists
    return all_lists

def resolve_list_ids(api_key: str, list_ids_str: Optional[str], list_name: Optional[str]) -> List[int]:
    # Prefer explicit list_ids
    if list_ids_str:
        parts = re.split(r"[|,;\s]+", str(list_ids_str).strip())
        ids = [int(p) for p in parts if p.strip().isdigit()]
        if ids:
            return ids

    if list_name:
        target = str(list_name).strip().lower()
        for lst in get_lists_paginated(api_key):
            if str(lst.get("name", "")).strip().lower() == target:
                return [int(lst["id"])]

    raise ValueError("Không resolve được listIds (cần list_ids hoặc list_name đúng).")


# =========================
# Message -> HTML (list + numbering)
# =========================
def message_to_html_like_screenshot(message: str) -> str:
    msg = (message or "").replace("\r\n", "\n").replace("\r", "\n").strip()
    msg = re.sub(r"\n\s*Best regards,\s*$", "", msg, flags=re.IGNORECASE).strip()

    # escape to keep safe
    msg = html.escape(msg)
    msg = apply_inline_markdown(msg)
    lines = [ln.rstrip() for ln in msg.split("\n")]

    def is_blank(s: str) -> bool:
        return s.strip() == ""

    def para(block_lines):
        text = "\n".join([ln.strip() for ln in block_lines]).strip()
        if not text:
            return ""
        return "<p>" + "<br>\n".join(text.split("\n")) + "</p>"

    out = []
    i = 0
    while i < len(lines):
        ln = lines[i].strip()

        if is_blank(ln):
            i += 1
            continue

        if re.match(r"^Program highlights:\s*$", ln, flags=re.IGNORECASE):
            out.append("<p><strong>Program Highlights</strong></p>")
            i += 1
            items = []
            while i < len(lines) and not is_blank(lines[i]):
                items.append(lines[i].strip())
                i += 1
            if items:
                out.append("<ul>\n" + "\n".join([f"<li>{it}</li>" for it in items]) + "\n</ul>")
            continue

        if re.match(r"^Here are .*:\s*$", ln, flags=re.IGNORECASE):
            out.append(f"<p><strong>{ln}</strong></p>")
            i += 1
            items = []
            while i < len(lines) and not is_blank(lines[i]):
                items.append(lines[i].strip())
                i += 1
            if items:
                ol = []
                for it in items:
                    if " – " in it:
                        name, desc = it.split(" – ", 1)
                        ol.append(f"<li><strong>{name.strip()}</strong> – {desc.strip()}</li>")
                    elif " - " in it:
                        name, desc = it.split(" - ", 1)
                        ol.append(f"<li><strong>{name.strip()}</strong> – {desc.strip()}</li>")
                    else:
                        ol.append(f"<li>{it}</li>")
                out.append("<ol>\n" + "\n".join(ol) + "\n</ol>")
            continue

        # paragraph block
        block = [lines[i]]
        i += 1
        while i < len(lines):
            nxt = lines[i].strip()
            if is_blank(nxt):
                break
            if re.match(r"^(Program highlights:|Here are .*:)\s*$", nxt, flags=re.IGNORECASE):
                break
            block.append(lines[i])
            i += 1

        p = para(block)
        p = re.sub(
            r"Contact:\s*([A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,})",
            r'Contact: <a href="mailto:\1">\1</a>',
            p,
            flags=re.IGNORECASE
        )
        out.append(p)

    return "\n".join(out)

def apply_inline_markdown(s: str) -> str:
    """
    Input: đã html.escape rồi
    Output: string có <strong>/<em>
    """
    # ***bold+italic***
    s = re.sub(r"\*\*\*([^\n*][\s\S]*?[^\n*])\*\*\*", r"<strong><em>\1</em></strong>", s)
    # **bold**
    s = re.sub(r"\*\*([^\n*][\s\S]*?[^\n*])\*\*", r"<strong>\1</strong>", s)
    # *italic* (không ăn nhầm **...**)
    s = re.sub(r"(?<!\*)\*([^\n*][\s\S]*?[^\n*])\*(?!\*)", r"<em>\1</em>", s)
    return s

def plain_text_to_html_paragraphs(text: str) -> str:
    t = (text or "").replace("\r\n", "\n").replace("\r", "\n").strip()
    if not t:
        return ""

    # 1) escape trước để an toàn
    t = html.escape(t)
    t = apply_inline_markdown(t)

    # 2) convert inline markdown an toàn:
    # ***bold+italic*** trước
    t = re.sub(r"\*\*\*([^\n*][\s\S]*?[^\n*])\*\*\*", r"<strong><em>\1</em></strong>", t)

    # **bold**
    t = re.sub(r"\*\*([^\n*][\s\S]*?[^\n*])\*\*", r"<strong>\1</strong>", t)

    # *italic* (không ăn nhầm **...**)
    t = re.sub(r"(?<!\*)\*([^\n*][\s\S]*?[^\n*])\*(?!\*)", r"<em>\1</em>", t)

    # 3) autolink email (sau markdown để không phá tag)
    t = re.sub(
        r"([A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,})",
        r'<a href="mailto:\1">\1</a>',
        t,
        flags=re.IGNORECASE
    )

    # 4) autolink url
    t = re.sub(
        r"(https?://[^\s<]+)",
        r'<a href="\1" target="_blank">\1</a>',
        t,
        flags=re.IGNORECASE
    )

    # 5) chia đoạn theo dòng trống
    blocks = re.split(r"\n\s*\n", t)
    out = []
    for b in blocks:
        b = b.strip()
        if not b:
            continue
        out.append("<p>" + "<br>\n".join(b.split("\n")) + "</p>")
    return "\n".join(out)


def normalize_header_footer(raw: str) -> str:
    """
    - Nếu input có HTML tag => dùng luôn
    - Nếu là text => convert sang <p>...
    - AUTO: nếu có 'Best regards' mà chưa có dashed line ngay sau => tự chèn '--------------------'
    """
    s = (raw or "").strip()
    if not s:
        return ""

    # auto insert dashed line after Best regards (nếu chưa có)
    # ví dụ: "Best regards," -> "Best regards,\n--------------------\n"
    s = re.sub(
        r"(?im)^(Best regards,?)\s*$",
        r"\1\n--------------------",
        s
    )

    looks_like_html = bool(re.search(r"</?(p|br|a|strong|em|ul|ol|li|hr)\b", s, flags=re.I))
    if looks_like_html:
        return s

    return plain_text_to_html_paragraphs(s)

def build_full_html(title: str, message: str, header_raw: str, footer_raw: str) -> str:
    header_html = normalize_header_footer(header_raw)
    footer_html = normalize_header_footer(footer_raw)
    body_html = message_to_html_like_screenshot(message)

    return (HTML_SHELL
            .replace("{{TITLE}}", html.escape(title or "TAHK Foundation"))
            .replace("{{HEADER}}", header_html)
            .replace("{{BODY}}", body_html)
            .replace("{{FOOTER}}", footer_html))
# =========================
# Accounts loader
# =========================
def load_accounts(accounts_ws):
    # ensure exists (non-destructive)
    ensure_headers(accounts_ws, [
        "account_name","api_key_ref","api_key","sender_name","sender_email","reply_to",
        "list_name","list_ids","notes","send_time_vn","send_offset_min"
    ])

    rows = accounts_ws.get_all_records()
    accounts = {}
    for r in rows:
        name = str(r.get("account_name","")).strip()
        if not name:
            continue

        api_key_ref = str(r.get("api_key_ref","")).strip()
        api_key = ""

        # if api_key_ref:
        #     api_key = os.getenv(api_key_ref, "").strip()
        #     if not api_key:
        #         # ép phải set ENV đúng, tránh chạy nhầm
        #         api_key = ""  # giữ trống để main báo lỗi "api_key trống"
        # else:
        #     api_key = str(r.get("api_key","")).strip()

        if api_key_ref:
            api_key = os.getenv(api_key_ref, "").strip()

        # fallback: nếu chưa có ENV thì lấy thẳng từ sheet
        if not api_key:
            api_key = str(r.get("api_key","")).strip()
            
        accounts[name] = {
            "api_key": api_key,
            "sender_name": str(r.get("sender_name","")).strip(),
            "sender_email": str(r.get("sender_email","")).strip(),
            "reply_to": str(r.get("reply_to","")).strip(),
            "list_name": str(r.get("list_name","")).strip(),
            "list_ids": str(r.get("list_ids","")).strip(),
            "send_time_vn": str(r.get("send_time_vn","")).strip(),
            "send_offset_min": str(r.get("send_offset_min","")).strip(),
        }
    return accounts


# =========================
# Sheet update helpers
# =========================
def update_cell(ws, row: int, col: int, value):
    ws.update_cell(row, col, value)

def append_log(logs_ws, row_dict):
    # ensure headers
    ensure_headers(logs_ws, ["ts","template_key","account_name","campaign_name","campaign_id","status","message"])
    logs_ws.append_row([
        row_dict.get("ts",""),
        row_dict.get("template_key",""),
        row_dict.get("account_name",""),
        row_dict.get("campaign_name",""),
        row_dict.get("campaign_id",""),
        row_dict.get("status",""),
        row_dict.get("message",""),
    ], value_input_option="RAW")

def slugify(text: str) -> str:
    """
    Convert campaign name to safe filename:
    - remove special chars
    - replace spaces with _
    """
    text = text.strip().lower()
    text = re.sub(r"[^\w\s-]", "", text)   # remove special chars
    text = re.sub(r"[\s]+", "_", text)     # spaces -> _
    return text

def ensure_dir(path: str):
    if not os.path.exists(path):
        os.makedirs(path, exist_ok=True)

VN_TZ = timezone(timedelta(hours=7))

def today_vn() -> str:
    return datetime.now(VN_TZ).strftime("%Y-%m-%d")

def pick_active_template(campaigns_ws) -> dict:
    ensure_headers(
        campaigns_ws,
        ["template_key","send_date_vn","rotation","done_date_vn",
        "Campaign name","Subject line","Preview text",
        "header_html","Message","footer_html",
        "status"]
    )
    rows = campaigns_ws.get_all_records()

    actives = [r for r in rows if str(r.get("status","")).strip().lower() == "active"]
    if not actives:
        raise RuntimeError("Không có template status=active trong campaigns")

    # Nếu có nhiều active: lấy dòng đầu tiên (hoặc bạn tự đổi logic theo ý)
    return actives[0]


def template_used_today_any_account(logs_ws, template_key: str) -> bool:
    day = today_vn()
    rows = logs_ws.get_all_records()
    for r in rows:
        ts = str(r.get("ts",""))
        if not ts.startswith(day):
            continue
        if str(r.get("template_key","")).strip() != (template_key or "").strip():
            continue
        st = str(r.get("status","")).strip().lower()
        if st in ("sent","scheduled","done"):
            return True
    return False

def pick_template_for_today_or_rotate(campaigns_ws, logs_ws) -> dict:
    ensure_headers(
        campaigns_ws,
        ["template_key","send_date_vn","rotation","done_date_vn",
        "Campaign name","Subject line","Preview text",
        "header_html","Message","footer_html",
        "status"]
    )

    rows = campaigns_ws.get_all_records()
    day = today_vn()

    # 1) ưu tiên template có send_date_vn = hôm nay
    todays = [r for r in rows if str(r.get("send_date_vn","")).strip() == day]
    if len(todays) == 1:
        return todays[0]
    if len(todays) > 1:
        keys = [str(r.get("template_key","")).strip() for r in todays]
        raise RuntimeError(f"Có {len(todays)} dòng send_date_vn={day}: {keys}. Chỉ được 1 dòng.")

    # 2) fallback: rotate qua các dòng status=active, rotation=1 (hoặc trống)
    candidates = []
    for r in rows:
        st = str(r.get("status","")).strip().lower()
        if st != "active":
            continue
        rot = str(r.get("rotation","")).strip()
        if rot not in ("", "1", "true", "yes"):
            continue
        tk = str(r.get("template_key","")).strip()
        if not tk:
            continue
        if not template_used_today_any_account(logs_ws, tk):
            candidates.append(r)

    if not candidates:
        # nếu hôm nay đã dùng hết, fallback về 1 dòng active đầu tiên
        actives = [r for r in rows if str(r.get("status","")).strip().lower() == "active"]
        if not actives:
            raise RuntimeError("Không có template status=active trong campaigns")
        return actives[0]

    # chọn dòng đầu tiên (bạn có thể thay thành random nếu thích)
    return candidates[0]


def pick_template_for_today_or_active(campaigns_ws) -> dict:
    """
    Cách 2:
    - Ưu tiên dòng có send_date_vn = hôm nay (YYYY-MM-DD)
    - Nếu không có, fallback về status=active (logic cũ)
    Rules:
    - Nếu có nhiều dòng cùng send_date_vn hôm nay -> báo lỗi (tránh gửi nhầm)
    """
    ensure_headers(
        campaigns_ws,
        ["template_key","send_date_vn","rotation","done_date_vn",
        "Campaign name","Subject line","Preview text",
        "header_html","Message","footer_html",
        "status"]
    )

    rows = campaigns_ws.get_all_records()
    day = today_vn()

    todays = [r for r in rows if str(r.get("send_date_vn","")).strip() == day]
    if len(todays) == 1:
        return todays[0]
    if len(todays) > 1:
        keys = [str(r.get("template_key","")).strip() for r in todays]
        raise RuntimeError(f"Có {len(todays)} dòng send_date_vn={day}: {keys}. Chỉ được 1 dòng.")

    # fallback: giữ nguyên cách cũ
    return pick_active_template(campaigns_ws)

def find_template_row_index(campaigns_ws, template_key: str) -> Optional[int]:
    """
    Return row index (1-based) của template_key trong campaigns sheet.
    Lưu ý: row 1 là header, data bắt đầu từ row 2.
    """
    all_values = campaigns_ws.get_all_values()
    if not all_values or len(all_values) < 2:
        return None

    headers = all_values[0]
    try:
        tk_col = headers.index("template_key")
    except ValueError:
        return None

    for i in range(1, len(all_values)):
        row = all_values[i]
        if tk_col < len(row) and str(row[tk_col]).strip() == template_key:
            return i + 1  # because i is 0-based in list, +1 => sheet row index
    return None

def mark_template_done(campaigns_ws, template_key: str, status_value: str = "done") -> None:
    headers = campaigns_ws.row_values(1)
    hm = {h: idx+1 for idx, h in enumerate(headers)}

    row_idx = find_template_row_index(campaigns_ws, template_key)
    if not row_idx:
        print(f"[WARN] Không tìm thấy template_key={template_key} để update status")
        return

    updates = []
    if "status" in hm:
        updates.append((row_idx, hm["status"], status_value))
    # optional: lưu ngày done cho dễ audit
    if "done_date_vn" in hm:
        updates.append((row_idx, hm["done_date_vn"], today_vn()))

    update_cells(campaigns_ws, updates)


def already_sent_today(logs_ws, template_key: str, account_name: str) -> bool:
    """
    Chống tạo/gửi trùng trong ngày:
    nếu hôm nay đã sent/scheduled/done cho (template_key + account_name) => skip
    """
    ensure_headers(logs_ws, ["ts","template_key","account_name","campaign_name","campaign_id","status","message"])
    day = today_vn()
    rows = logs_ws.get_all_records()
    for r in rows:
        ts = str(r.get("ts",""))
        if not ts.startswith(day):
            continue
        if str(r.get("template_key","")).strip() != (template_key or "").strip():
            continue
        if str(r.get("account_name","")).strip() != (account_name or "").strip():
            continue
        st = str(r.get("status","")).strip().lower()
        if st in ("sent","scheduled","done"):
            return True
    return False
# =========================
# Main
# =========================
def main():
    gc = gs_client()
    sh = gc.open_by_key(SPREADSHEET_ID)

    campaigns_ws = get_or_create_worksheet(sh, CAMPAIGNS_SHEET)
    accounts_ws  = get_or_create_worksheet(sh, ACCOUNTS_SHEET)
    logs_ws      = get_or_create_worksheet(sh, LOGS_SHEET)

    # campaigns headers: thêm template_key để dùng Cách A
    ensure_headers(
        campaigns_ws,
        ["template_key","send_date_vn","rotation","done_date_vn",
        "Campaign name","Subject line","Preview text",
        "header_html","Message","footer_html",
        "status"]
    )

    # logs headers (đã mở rộng template_key)
    ensure_headers(logs_ws, ["ts","template_key","account_name","campaign_name","campaign_id","status","message"])

    # load accounts (giữ nguyên hàm của bạn)
    accounts = load_accounts(accounts_ws)
    if not accounts:
        raise RuntimeError("Tab accounts không có account hợp lệ")

    # 1) pick template active từ campaigns
    tpl = pick_template_for_today_or_rotate(campaigns_ws, logs_ws)

    template_key = str(tpl.get("template_key","")).strip() or "active"

    base_name = str(tpl.get("Campaign name","")).strip()
    subject   = str(tpl.get("Subject line","")).strip()
    preview   = str(tpl.get("Preview text","")).strip()
    header_raw = str(tpl.get("header_html", "")).strip()
    message   = str(tpl.get("Message","")).strip()
    footer_raw = str(tpl.get("footer_html", "")).strip()

    # fallback nếu bạn muốn (không bắt buộc):
    if not header_raw:
        header_raw = 'Dear {{ contact.FIRSTNAME | default:"there" }},'

    if not base_name or not subject or not message:
        raise RuntimeError("Dòng campaigns status=active thiếu Campaign name / Subject line / Message")

    # schedule
    scheduled_at = None
    if SEND_EVERY_DAY_AT_VN:
        dt_utc = next_schedule_utc(SEND_EVERY_DAY_AT_VN["hour"], SEND_EVERY_DAY_AT_VN["minute"])
        scheduled_at = iso_utc(dt_utc)

    created = []

    # 2) loop tất cả accounts
    for account_name, acc in accounts.items():
        try:
            # chống trùng theo ngày
            if already_sent_today(logs_ws, template_key, account_name):
                print(f"[SKIP] {account_name}: đã chạy hôm nay {today_vn()} template={template_key}")
                continue

            api_key = (acc.get("api_key") or "").strip()
            if not api_key:
                raise RuntimeError("api_key trống (hãy dùng api_key_ref -> ENV cronjob.com)")

            # validate sender để tránh lỗi 'sender name is missing'
            sender_name  = (acc.get("sender_name") or "").strip()
            sender_email = (acc.get("sender_email") or "").strip()
            if not sender_name:
                raise RuntimeError("sender_name trống (sẽ gây lỗi sender name is missing)")
            if not sender_email:
                raise RuntimeError("sender_email trống")

            # check /account
            if not brevo_check_account_cached(api_key):
                raise RuntimeError("Brevo /account không OK (API key sai hoặc account bị hạn chế)")

            lists = get_lists_paginated(api_key)
            print(f"[DEBUG] {account_name} has {len(lists)} lists. Sample:",
                [l.get("name") for l in lists[:10]])

            # listIds theo account (mỗi acc list riêng)
            list_ids = resolve_list_ids(
                api_key=api_key,
                list_ids_str=(acc.get("list_ids") or "").strip(),
                list_name=(acc.get("list_name") or "").strip(),
            )

            scheduled_at_acc = scheduled_at_from_account(acc) or scheduled_at
            print(f"[DEBUG] {account_name}: scheduled_at_acc={scheduled_at_acc}")
            # unique campaign name per account per day
            campaign_name = base_name

            html_content = build_full_html(campaign_name, message, header_raw, footer_raw)


            payload = {
                "name": campaign_name,
                "subject": subject,
                "previewText": preview,
                "sender": {"name": sender_name, "email": sender_email},
                "replyTo": acc.get("reply_to") or sender_email,
                "recipients": {"listIds": list_ids},
                "htmlContent": html_content,
            }
            if scheduled_at_acc:
                payload["scheduledAt"] = scheduled_at_acc

            # create
            # if DRY_RUN:
            #     print(f"[DRY_RUN] {account_name}: would create '{campaign_name}' listIds={list_ids}")
            #     append_log(logs_ws, {
            #         "ts": datetime.now(VN_TZ).strftime("%Y-%m-%dT%H:%M:%S"),
            #         "template_key": template_key,
            #         "account_name": account_name,
            #         "campaign_name": campaign_name,
            #         "campaign_id": "",
            #         "status": "dry_run",
            #         "message": f"listIds={list_ids}"
            #     })
            #     continue
            if DRY_RUN:
                ensure_dir("previews")
                fname = f"previews/{slugify(campaign_name)}_{account_name}.html"
                with open(fname, "w", encoding="utf-8") as f:
                    f.write(html_content)

                print(f"[DRY_RUN] saved: {fname}")
                append_log(logs_ws, {
                    "ts": datetime.now(VN_TZ).strftime("%Y-%m-%dT%H:%M:%S"),
                    "template_key": template_key,
                    "account_name": account_name,
                    "campaign_name": campaign_name,
                    "campaign_id": "",
                    "status": "dry_run",
                    "message": f"saved={fname} | listIds={list_ids}"
                })
                continue

            cid = create_campaign(api_key, payload)

            print(f"[OK] {account_name}: created campaignId={cid} listIds={list_ids}")

            # send/schedule (FIX: không gửi sớm)
            if scheduled_at_acc:
                # Chỉ cần create campaign có scheduledAt là Brevo sẽ tự gửi đúng giờ
                # TUYỆT ĐỐI không gọi sendNow ở nhánh này
                st = "scheduled"
                msg = f"scheduledAt={scheduled_at_acc}"
            else:
                # Không có scheduledAt -> tùy SEND_NOW
                if SEND_NOW:
                    send_campaign_now(api_key, cid)
                    st = "sent"
                    msg = "sendNow"
                else:
                    st = "done"
                    msg = "created"


            append_log(logs_ws, {
                "ts": datetime.now(VN_TZ).strftime("%Y-%m-%dT%H:%M:%S"),
                "template_key": template_key,
                "account_name": account_name,
                "campaign_name": campaign_name,
                "campaign_id": str(cid),
                "status": st,
                "message": msg
            })

            created.append((account_name, campaign_name, cid, st))

        except Exception as e:
            err = str(e)[:500]
            print(f"[ERR] {account_name}: {err}")
            append_log(logs_ws, {
                "ts": datetime.now(VN_TZ).strftime("%Y-%m-%dT%H:%M:%S"),
                "template_key": template_key,
                "account_name": account_name,
                "campaign_name": base_name,
                "campaign_id": "",
                "status": "error",
                "message": err
            })

    if not DRY_RUN:
        success_count = sum(1 for _, _, _, st in created if st in ("sent", "scheduled", "done"))
        if success_count > 0:
            mark_template_done(campaigns_ws, template_key, status_value=f"done_{today_vn()}")
        else:
            print("[INFO] Không có account nào thành công -> không đổi status template")
    else:
        print("[DRY_RUN] Không đổi status campaigns")

    print("\nSUMMARY created:")
    for acc_name, cname, cid, st in created:
        print(f"- {acc_name}: {cname} -> campaignId={cid} ({st})")

if __name__ == "__main__":
    main()
