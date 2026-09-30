# -*- coding: utf-8 -*-
"""
海关查获 & 涉华出口风险 · RSSHub 通道（云端专用，纯标准库，零依赖）

链路：RSSHub 路由（自带 filter 粗筛）→ 本地三组关键词精筛 → 跨天去重 → 推送微信
产物：seen_rss.json（去重台账）+ rss-digest/YYYY-MM-DD.md（可读结果）+ Actions 日志

只读公开新闻源（全部为境外/外媒），不碰本地文件、不依赖本机环境。

筛选规则（两条线，命中任一即推，并在标题前标注来源线）：
  线1【查获】    标题命中 A 组「查获/执法词」
  线2【涉华出口】标题命中 C 组「敏感商品/管制议题词」，且标题或正文命中 B 组「涉华指向词」
                ← 例：「胡塞武装壮大背后中国商品　无人机材料点击滑鼠就能买到」
  宽松模式 LOOSE=1 额外允许：正文命中 A 组（召回更高、误报更多）

环境变量（GitHub Secrets / Variables）：
  RSSHUB_BASE / RSSHUB_FALLBACK   RSSHub 实例（留空用内置兜底链）
  SERVERCHAN_SENDKEY / PUSHPLUS_TOKEN / PUSHPLUS_TOPIC / WECOM_WEBHOOK   推送渠道
  FULLTEXT=1       推送带全文（默认 workflow 里为 1）
  LOOSE=1          放宽为「正文命中查获词」
  NOTIFY_WHEN_EMPTY=1  本轮无新增也推一条"报平安"（默认不推）
  PUSH_TEST=N      测试推送：每源取前 N 条、强制全文、忽略去重、不写台账/digest
  SELFTEST=1       只自检推送通道
  DRY_RUN=1        抓取但不推送（日志打印正文预览）
  TEXT_LIMIT / PUSH_LIMIT   单条正文上限 / 整条推送上限（字符）
"""

import html
import json
import os
import re
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
STATE = ROOT / "seen_rss.json"
DIGEST_DIR = ROOT / "rss-digest"          # 每日可读结果（每次运行追加一节，随仓库提交）
REPO_BLOB = "https://github.com/alex990077-rgb/RSSHub/blob/master"
HK = timezone(timedelta(hours=8))
UA = "Mozilla/5.0 (compatible; customs-rss/1.0; +https://github.com/alex990077-rgb/RSSHub)"
TIMEOUT = 20              # 单个实例单次请求超时（秒）
DEADLINE = 240            # 单轮抓取总预算（秒），超了就放弃剩余实例，避免拖垮 Actions
STATE_MAX = 8000          # 台账上限，超出丢最旧的
MAX_ITEMS_PUSH = 40       # 单次推送条数上限

# 实例兜底顺序：公共实例会限流/封 IP，某个不通自动换下一个
# （rsshub.app 在 GitHub Actions 上实测 403；自建容器用 http://localhost:1200）
DEFAULT_FALLBACK = (
    "https://rsshub.liumingye.cn,https://rsshub.ktachibana.party,http://localhost:1200,https://rsshub.app"
)

# ── 源清单：(名称, 地区, RSSHub 路由, 时效天数) ───────────────────────────────
# 全部为境外/外媒（按你的要求已移除中国大陆源：中國海關雜誌、海關總署）
# 时效天数：条目发布时间早于该天数就丢弃
FEEDS = [
    # —— 中文（港澳台、东南亚）——
    {"name": "香港01",         "region": "HK", "kind": "rsshub", "target": "/hk01/latest",                 "max_age": 3, "lang": "zh"},
    {"name": "星島日報",       "region": "HK", "kind": "rsshub", "target": "/stheadline/std/realtimenews", "max_age": 3, "lang": "zh"},
    {"name": "星洲網",         "region": "MY", "kind": "rsshub", "target": "/sinchew/latest",              "max_age": 3, "lang": "zh"},
    {"name": "星洲-天下事",     "region": "MY", "kind": "rsshub", "target": "/sinchew/category/国际/天下事", "max_age": 7, "lang": "zh"},
    {"name": "聯合早報",       "region": "SG", "kind": "rsshub", "target": "/zaobao/realtime/china",       "max_age": 3, "lang": "zh"},
    {"name": "中央社",         "region": "TW", "kind": "rsshub", "target": "/cna",                         "max_age": 3, "lang": "zh"},
    {"name": "8视界",          "region": "SG", "kind": "rsshub", "target": "/8world",                      "max_age": 5, "lang": "zh"},
    {"name": "日经中文网",     "region": "JP", "kind": "rsshub", "target": "/nikkei/cn",                   "max_age": 5, "lang": "zh"},
    # —— 英文（通讯社 / 大报 / 官方）——
    {"name": "韩联社-英文",    "region": "KR", "kind": "rsshub", "target": "/yna/en",                      "max_age": 3, "lang": "en"},
    # 路透社：RSSHub 路由对公共实例返回 403/503，改用 Google News 抓它的站内稿
    {"name": "路透社",         "region": "US", "kind": "gnews", "target": "https://news.google.com/rss/search?q=site:reuters.com+China+(customs+OR+smuggling+OR+%22export+control%22+OR+tariff)&hl=en-US&gl=US&ceid=US:en", "max_age": 3, "lang": "en"},
    {"name": "彭博社-政治",    "region": "US", "kind": "rsshub", "target": "/bloomberg/politics",          "max_age": 3, "lang": "en"},
    {"name": "彭博社-商业",    "region": "US", "kind": "rsshub", "target": "/bloomberg/business",          "max_age": 3, "lang": "en"},
    {"name": "华尔街日报-世界", "region": "US", "kind": "rss",   "target": "https://feeds.content.dowjones.io/public/rss/RSSWorldNews",     "max_age": 3, "lang": "en"},
    {"name": "华尔街日报-商业", "region": "US", "kind": "rss",   "target": "https://feeds.content.dowjones.io/public/rss/WSJcomUSBusiness", "max_age": 3, "lang": "en"},
    {"name": "USTR",           "region": "US", "kind": "rss",   "target": "https://ustr.gov/rss.xml",       "max_age": 14, "lang": "en"},
    {"name": "The Star",       "region": "MY", "kind": "gnews", "target": "https://news.google.com/rss/search?q=site:thestar.com.my+when:2d&hl=en-MY&gl=MY&ceid=MY:en", "max_age": 3, "lang": "en"},
    # Central Asia Times：站点在 Cloudflare 后，/feed/ 返回 404 —— 下面几条是候选路径，跑通后只留一条
    {"name": "CAT-1",          "region": "KZ", "kind": "rss",   "target": "https://www.centralasiatimes.com/rss/",            "max_age": 14, "lang": "en"},
    {"name": "CAT-2",          "region": "KZ", "kind": "rss",   "target": "https://www.centralasiatimes.com/feed/rss/",       "max_age": 14, "lang": "en"},
    {"name": "CAT-3",          "region": "KZ", "kind": "rss",   "target": "https://www.centralasiatimes.com/?feed=rss2",      "max_age": 14, "lang": "en"},
    {"name": "CAT-4",          "region": "KZ", "kind": "rss",   "target": "https://www.centralasiatimes.com/news/feed/",      "max_age": 14, "lang": "en"},
    {"name": "CAT-5",          "region": "KZ", "kind": "rss",   "target": "https://www.centralasiatimes.com/feed",            "max_age": 14, "lang": "en"},
    # —— 日文：朝日新闻官网 RSS 对境外有拦，改走 Google News 站内检索 ——
    {"name": "朝日新闻",       "region": "JP", "kind": "gnews", "target": "https://news.google.com/rss/search?q=site:asahi.com+%E4%B8%AD%E5%9B%BD+(%E7%A8%8E%E9%96%A2+OR+%E5%AF%86%E8%BC%B8+OR+%E8%BC%B8%E5%87%BA%E7%AE%A1%E7%90%86+OR+%E5%8D%8A%E5%B0%8E%E4%BD%93)&hl=ja&gl=JP&ceid=JP:ja", "max_age": 3, "lang": "ja"},
    # —— 越南文 ——
    {"name": "VietnamNet-时事", "region": "VN", "kind": "rss",  "target": "https://vietnamnet.vn/rss/thoi-su.rss",  "max_age": 3, "lang": "vi"},
    {"name": "VietnamNet-国际", "region": "VN", "kind": "rss",  "target": "https://vietnamnet.vn/rss/the-gioi.rss", "max_age": 3, "lang": "vi"},
]

# ── 关键词：按语言分组的 A/B/C 三组（规则见文件头）────────────────────────
# kind=rsshub 的源用 CORE_FILTER 粗筛；所有源都用 A/B/C 本地精筛（多语言取并集）
KW = {
    "zh": {
        "A": (
            r"海[關关]|查[獲获]|檢[獲获]|检[獲获]|緝[獲获]|缉[獲获]|截[獲获]|破[獲获]|偵破|侦破|"
            r"扣留|扣押|沒收|没收|收繳|收缴|查扣|走私|私[煙烟]|[緝缉]私|侵[權权]|假冒|盜版|盗版|"
            r"固[廢废]|洋垃圾|退[運运]|瞞報|瞒报|逃[稅税]|低報|低报|水[貨货]|販毒|贩毒|洗黑[錢钱]"
        ),
        "B": (
            r"中國|中国|中方|中企|國企|国企|中資|中资|中國製造|中国制造|Made in China|Chinese|"
            r"大陸|大陆|內地|内地|中港|香港|北京|上海|廣東|广东|義烏|义乌|人民幣|人民币"
        ),
        "C": (
            r"無人機|无人机|drone|兩用物項|两用物项|軍民兩用|军民两用|出口管制|出口禁令|管制清單|管制清单|"
            r"稀土|稀有金屬|稀有金属|鎵|镓|鍺|锗|石墨|碳纖維|碳纤维|晶片|芯片|半導體|半导体|光刻|"
            r"鋰電池|锂电池|光伏|太陽能|太阳能|軍工|军工|軍品|军品|軍事|军事|武器|彈藥|弹药|導彈|导弹|"
            r"槍械|枪械|炸藥|炸药|化學品|化学品|前體|前体|易制毒|芬太尼|核材料|鈾|铀|離心機|离心机|"
            r"衛星|卫星|雷達|雷达|夜視|夜视|防彈|防弹|頭盔|头盔|軍服|军服|制裁|規避|规避|洗產地|洗产地|"
            r"原產地|原产地|轉運|转运|轉口|转口|關稅|关税|反傾銷|反倾销|強迫勞動|强迫劳动|供應鏈|供应链|"
            r"出口退[稅税]|報關|报关|清關|清关|跨境電商|跨境电商"
        ),
        "D": r"胡塞|真主黨|真主党|哈瑪斯|哈马斯|伊朗|朝鮮|朝鲜|俄羅斯|俄罗斯|受制裁|繞道|绕道|第三國|第三国|黑市|掮客|中介|網店|网店|電商|电商|公開販售|公开贩售",
        "T": r"出口|进口|進口|貨物|货物|貨運|贸易|貿易|商品|转运|轉運|转口|轉口|走私|報關|报关|清關|清关|订单|訂單|採購|采购|供應鏈|供应链",
    },
    "en": {
        "A": (
            r"customs|seized|seizure|confiscat|smuggl|contraband|counterfeit|infringing|undeclared|"
            r"misdeclar|evasion|forced labo|laundering|trafficking|illicit trade"
        ),
        "B": r"China|Chinese|Beijing|Hong Kong|Made in China|Shenzhen|Guangzhou|Yiwu|Shanghai|Renminbi|yuan|mainland",
        "C": (
            r"export control|dual-use|dual use|drone|UAV|rare earth|gallium|germanium|graphite|semiconductor|"
            r"chip|advanced chip|lithium battery|solar panel|photovoltaic|weapon|ammunition|missile|firearm|"
            r"explosive|precursor|fentanyl|nuclear|uranium|centrifuge|satellite|radar|night vision|body armor|"
            r"helmet|military uniform|sanction|circumvent|transshipment|trans-shipment|origin fraud|tariff|"
            r"anti-dumping|antidumping|export tax rebate|customs broker|clearance|cross-border e-commerce|"
            r"military equipment|military drone|military technology|defense contractor|military export"
        ),
        "D": r"Houthi|Hezbollah|Hamas|Iran|North Korea|Russia|black market|broker|intermediary|online shop|e-commerce|third country",
        "T": r"export|import|cargo|shipment|trade|goods|supply|procure|order|consignment|container|port|exports",
    },
    "ja": {
        "A": r"税関|密輸|押収|没収|偽ブランド|侵害|申告漏れ|脱税|密輸出|密輸入|関税法違反",
        "B": r"中国|中国製|北京|香港|上海|中国企業|人民元",
        "C": (
            r"輸出管理|デュアルユース|軍民両用|ドローン|無人機|レアアース|希土類|ガリウム|ゲルマニウム|黒鉛|"
            r"半導体|電池|リチウム|太陽光|兵器|弾薬|ミサイル|銃器|火薬|化学|前駆体|核|ウラン|遠心分離機|"
            r"衛星|レーダー|制裁|迂回|転送|原産地|関税|反ダンピング|強制労働|サプライチェーン|通関|越境EC|軍事"
        ),
        "D": r"フーシ|ヒズボラ|ハマス|イラン|北朝鮮|ロシア|闇市場|ブローカー|仲介",
        "T": r"輸出|輸入|貨物|貿易|商品|積み替え|転送|通関|サプライチェーン|コンテナ",
    },
    "ko": {
        "A": r"세관|밀수|압수|위조|침해|탈세|허위신고|관세법 위반",
        "B": r"중국|중국산|베이징|홍콩|상하이|중국기업|위안화",
        "C": (
            r"수출통제|군민양용|드론|무인기|희토류|갈륨|게르마늄|흑연|반도체|배터리|리튬|태양광|무기|탄약|"
            r"미사일|총기|화약|화학|전구체|핵|우라늄|원심분리기|위성|레이더|제재|우회|전송|원산지|관세|"
            r"반덤핑|강제노동|공급망|통관|전자상거래|군사"
        ),
        "D": r"후티|헤즈볼라|하마스|이란|북한|러시아|암시장|브로커|중개",
        "T": r"수출|수입|화물|무역|상품|운송|환적|통관|컨테이너",
    },
    "vi": {
        "A": r"hải quan|tịch thu|buôn lậu|hàng giả|hàng nhái|xuất lậu|nhập lậu|gian lận thương mại|trốn thuế|vận chuyển trái phép",
        "B": r"Trung Quốc|Đài Loan|Hồng Kông|made in china|Thượng Hải|Quảng Đông",
        "C": (
            r"xuất khẩu|nhập khẩu|xuất xứ|gian lận xuất xứ|chuyển tải|trung chuyển|máy bay không người lái|"
            r"đất hiếm|chất bán dẫn|vi mạch|pin lithium|năng lượng mặt trời|vũ khí|đạn dược|tiền chất|"
            r"thuế quan|chống bán phá giá|chuỗi cung ứng|thương mại điện tử|quân sự"
        ),
        "D": r"Houthi|Hezbollah|Hamas|Iran|Triều Tiên|Nga|chợ đen|môi giới|trung gian",
        "T": r"xuất khẩu|nhập khẩu|hàng hóa|thương mại|vận chuyển|container|cảng",
    },
}
A_ENFORCE = re.compile("|".join("(?:%s)" % KW[k]["A"] for k in KW), re.I)
B_CHINA = re.compile("|".join("(?:%s)" % KW[k]["B"] for k in KW), re.I)
C_GOODS = re.compile("|".join("(?:%s)" % KW[k]["C"] for k in KW), re.I)
D_FLOW = re.compile("|".join("(?:%s)" % KW[k]["D"] for k in KW), re.I)
T_TRADE = re.compile("|".join("(?:%s)" % KW[k]["T"] for k in KW), re.I)

# RSSHub 端的粗筛（召回优先，只放高价值词，避免 URL 过长）：
# 真正决定"推不推"的是上面 A/B/C 三组的本地精筛。
CORE_FILTER = (
    r"海[關关]|查[獲获]|檢[獲获]|走私|[緝缉]私|扣留|沒收|没收|侵[權权]|固[廢废]|退[運运]|瞞報|瞒报|"
    r"中國|中国|中方|中企|大陸|大陆|內地|内地|中港|"
    r"無人機|无人机|兩用物項|两用物项|出口管制|稀土|石墨|鎵|镓|鍺|锗|晶片|芯片|半導體|半导体|"
    r"鋰電池|锂电池|光伏|武器|彈藥|弹药|導彈|导弹|制裁|洗產地|洗产地|原產地|原产地|轉運|转运|"
    r"轉口|转口|關稅|关税|反傾銷|反倾销|強迫勞動|强迫劳动|供應鏈|供应链|報關|报关|"
    r"胡塞|真主黨|真主党|哈瑪斯|哈马斯|伊朗|朝鮮|朝鲜|俄羅斯|俄罗斯|受制裁|繞道|绕道|第三國|第三国|"
    r"黑市|掮客|中介|網店|网店|電商|电商|"
    r"China|Chinese|customs|seized|smuggl|export control|dual-use|drone|rare earth|semiconductor|"
    r"sanction|transshipment|tariff|forced labo|supply chain|Houthi|Iran|Russia|"
    r"税関|密輸|輸出管理|ドローン|半導体|中国製|"
    r"세관|밀수|수출통제|드론|반도체|중국산"
)


def log(msg):
    print(msg, flush=True)


def http_get(url):
    """单次请求。多实例兜底已经在外面做了，这里不再重试，避免最坏情况拖到几分钟。"""
    req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "application/rss+xml, application/xml;q=0.9, */*;q=0.8"})
    with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
        return resp.read().decode("utf-8", "replace")


def clean_text(raw):
    """RSSHub 的 fulltext 正文是「转义后的 HTML」——先反转义，再去标签，再反转义一次。"""
    if not raw:
        return ""
    txt = html.unescape(raw)
    txt = re.sub(r"<(script|style)[^>]*>.*?</\1>", " ", txt, flags=re.S | re.I)
    txt = re.sub(r"<br\s*/?>|</p>|</div>", "\n", txt, flags=re.I)
    txt = re.sub(r"<[^>]+>", " ", txt)
    txt = html.unescape(txt)
    txt = re.sub(r"[ \t\u00a0]+", " ", txt)
    return re.sub(r"\n{3,}", "\n\n", txt).strip()


def parse_items(xml):
    """同时兼容 RSS(<item>) 与 Atom(<entry>)。返回 (标题, 链接, 正文/摘要, 发布时间) """
    out = []
    for block in re.findall(r"<item[\s>].*?</item>", xml, re.S) + re.findall(r"<entry[\s>].*?</entry>", xml, re.S):
        t = re.search(r"<title[^>]*>(?:<!\[CDATA\[)?(.*?)(?:\]\]>)?</title>", block, re.S)
        l = re.search(r"<link[^>]*>(?:<!\[CDATA\[)?(.*?)(?:\]\]>)?</link>", block, re.S) or re.search(
            r'<link[^>]*href="([^"]+)"', block
        )
        d = re.search(r"<(?:description|summary|content)[^>]*>(?:<!\[CDATA\[)?(.*?)(?:\]\]>)?</(?:description|summary|content)>", block, re.S)
        p = re.search(r"<(?:pubDate|published|updated)[^>]*>(?:<!\[CDATA\[)?(.*?)(?:\]\]>)?</(?:pubDate|published|updated)>", block, re.S)
        if not (t and l):
            continue
        title = clean_text(t.group(1))
        desc = clean_text(d.group(1)) if d else ""
        pub = None
        if p:
            raw = p.group(1).strip()
            try:
                pub = parsedate_to_datetime(raw)
            except Exception:               # noqa: BLE001 — Atom 的 ISO 时间
                try:
                    pub = datetime.fromisoformat(raw.replace("Z", "+00:00"))
                except Exception:           # noqa: BLE001
                    pub = None
            if pub is not None and pub.tzinfo is None:
                pub = pub.replace(tzinfo=timezone.utc)
        out.append((title, l.group(1).strip(), desc, pub))
    return out


def classify(title, desc, loose=False):
    """返回命中的线名；不命中返回 None。

    线1【查获】   = 标题命中 A 组（查获/执法词）
    线2【涉华出口】= 标题命中 C 组（敏感商品/管制词）且标题命中 B 组（涉华指向词）
    线3【涉华流向】= 标题同时命中 B + D（受制裁买家/规避手法/黑市渠道）+ T（贸易流向词）
                    —— 抓"中国货经第三国流向受制裁方"这类叙事，避免"China vs Iran 球赛"式误报
    LOOSE=1 放宽：B 可放宽到正文；另允许正文命中 A
    """
    if A_ENFORCE.search(title):
        return "查获"
    b_hit = B_CHINA.search(title) or (loose and B_CHINA.search(title + " " + desc))
    if C_GOODS.search(title) and b_hit:
        return "涉华出口"
    if D_FLOW.search(title) and T_TRADE.search(title) and b_hit:
        return "涉华流向"
    if loose and A_ENFORCE.search(title + " " + desc):
        return "查获·宽松"
    return None


def feed_url(base, path, fulltext=False):
    # 中文路径必须编码（"国际/天下事" 这类），safe="/" 保留层级分隔
    qpath = urllib.parse.quote(path, safe="/")
    # limit 在 RSSHub 里是先 filter → 再 limit → 最后才 fulltext，所以全文只解析留下来的条目；
    # 全文模式把 limit 压到 15，避免单源解析几十篇正文拖垮整轮。
    limit = 15 if fulltext else 50
    url = "%s%s?filter=%s&opencc=t2s&limit=%d" % (base, qpath, urllib.parse.quote(CORE_FILTER), limit)
    if fulltext:
        url += "&mode=fulltext"
    return url


def send_serverchan(key, title, content):
    """Server酱 Turbo：POST https://sctapi.ftqq.com/{sendkey}.send"""
    data = urllib.parse.urlencode({"title": title, "desp": content}).encode()
    req = urllib.request.Request("https://sctapi.ftqq.com/%s.send" % key, data=data, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=30) as r:
        return r.read().decode("utf-8", "replace")


def send_pushplus(token, title, content, topic=""):
    """PushPlus：POST https://www.pushplus.plus/send"""
    payload = {"token": token, "title": title, "content": content, "template": "txt"}
    if topic:
        payload["topic"] = topic
    data = json.dumps(payload).encode()
    req = urllib.request.Request(
        "https://www.pushplus.plus/send", data=data, headers={"Content-Type": "application/json", "User-Agent": UA}
    )
    with urllib.request.urlopen(req, timeout=30) as r:
        return r.read().decode("utf-8", "replace")


def send_wecom(hook, content):
    """企业微信群机器人"""
    data = json.dumps({"msgtype": "text", "text": {"content": content}}).encode()
    req = urllib.request.Request(hook, data=data, headers={"Content-Type": "application/json", "User-Agent": UA})
    with urllib.request.urlopen(req, timeout=30) as r:
        return r.read().decode("utf-8", "replace")


def build_body(hits, per_item, total_limit):
    """拼推送正文：每条 = 【地区·线】标题 + 链接 + 正文（FULLTEXT=1 时是全文）。超预算就截断。"""
    parts, used, truncated = [], 0, False
    for h in hits:
        piece = ["【%s·%s】%s%s" % (h["region"], h["line"], "★涉华 " if h["cn"] else "", h["title"]), h["link"]]
        if h["text"]:
            text = h["text"]
            if len(text) > per_item:
                text = text[:per_item] + "……（正文超长已截断，详见链接）"
                truncated = True
            piece.append(text)
        block = "\n".join(piece)
        if used + len(block) > total_limit:
            truncated = True
            break
        parts.append(block)
        used += len(block) + 12
    return "\n\n──────────\n\n".join(parts), used, truncated


def push_all(title, body):
    """把一条消息推到所有已配置的渠道，返回错误列表（空=全部成功或无需推送）。"""
    key = os.environ.get("SERVERCHAN_SENDKEY")
    token = os.environ.get("PUSHPLUS_TOKEN")
    hook = os.environ.get("WECOM_WEBHOOK")
    errors = []
    if key:
        try:
            log("Server酱：%s" % send_serverchan(key, title, body)[:120])
        except Exception as exc:            # noqa: BLE001
            errors.append("Server酱 %s" % exc)
    if token:
        try:
            log("PushPlus：%s" % send_pushplus(token, title, body, os.environ.get("PUSHPLUS_TOPIC", ""))[:120])
        except Exception as exc:            # noqa: BLE001
            errors.append("PushPlus %s" % exc)
    if hook:
        try:
            log("企业微信：%s" % send_wecom(hook, "%s\n%s" % (title, body))[:120])
        except Exception as exc:            # noqa: BLE001
            errors.append("企业微信 %s" % exc)
    if not (key or token or hook):
        log("⚠ 没有配置任何推送渠道（SERVERCHAN_SENDKEY / PUSHPLUS_TOKEN / WECOM_WEBHOOK），本轮只记录不推送。")
    if errors:
        log("⚠ 推送异常：%s" % "; ".join(errors))
    return errors


def main():
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8", errors="replace")
        except Exception:                   # noqa: BLE001
            pass

    dry = os.environ.get("DRY_RUN") == "1"
    loose = os.environ.get("LOOSE") == "1"
    fulltext = os.environ.get("FULLTEXT") == "1"
    notify_empty = os.environ.get("NOTIFY_WHEN_EMPTY") == "1"
    push_test = int(os.environ.get("PUSH_TEST") or 0)
    if push_test:
        fulltext = True                    # 测试推送一律带全文
    per_item = int(os.environ.get("TEXT_LIMIT") or 6000)      # 每条正文上限（字符）
    total_limit = int(os.environ.get("PUSH_LIMIT") or 28000)  # 整条推送上限（Server酱 32KB 以内）
    primary = (os.environ.get("RSSHUB_BASE") or "").strip().rstrip("/")
    fallback = [b.strip().rstrip("/") for b in (os.environ.get("RSSHUB_FALLBACK") or DEFAULT_FALLBACK).split(",") if b.strip()]
    bases = []
    for b in ([primary] if primary else []) + fallback:
        if b not in bases:
            bases.append(b)
    log("RSSHub 实例（按序尝试）：%s" % " → ".join(bases))
    log("规则：线1 标题命中A组查获词 ｜ 线2 标题命中C组敏感商品/管制词 且标题命中B组涉华词%s" % (" ｜ 宽松模式已开" if loose else ""))
    log("正文模式：%s ｜ 源 %d 个（%s）" % ("全文（mode=fulltext）" if fulltext else "摘要", len(FEEDS),
        "".join(sorted({f["lang"] for f in FEEDS}))))
    for lang in KW:
        log("\n[%s] A组·查获/执法词：%s" % (lang, KW[lang]["A"]))
        log("[%s] B组·涉华指向词：%s" % (lang, KW[lang]["B"]))
        log("[%s] C组·敏感商品/管制词：%s" % (lang, KW[lang]["C"]))
        log("[%s] D组·流向/手法/渠道：%s" % (lang, KW[lang]["D"]))
        log("[%s] T组·贸易流向词：%s" % (lang, KW[lang]["T"]))
    log("\nRSSHub 端粗筛：%s" % CORE_FILTER)
    if push_test:
        log("\nPUSH_TEST=%d：测试推送，每源取前 %d 条，强制全文，不写台账/digest" % (push_test, push_test))
    log("北京时间：%s" % datetime.now(HK).strftime("%Y-%m-%d %H:%M"))

    if os.environ.get("SELFTEST") == "1":
        log("SELFTEST=1：只自检推送通道，不抓取。")
        return 1 if push_all("海关RSS自检（%s）" % datetime.now(HK).strftime("%m-%d %H:%M"),
                             "收到这条说明推送通道已配好；接下来每天 07:30 / 15:10 / 22:10（北京）自动跑。") else 0

    seen = {}
    if STATE.exists():
        try:
            seen = json.loads(STATE.read_text(encoding="utf-8"))
        except Exception as exc:            # noqa: BLE001
            log("台账解析失败，按空台账处理：%s" % exc)

    today = datetime.now(HK).strftime("%Y-%m-%d")
    now = datetime.now(timezone.utc)
    hits, stats = [], []
    t0 = time.monotonic()
    for feed in FEEDS:
        name, region = feed["name"], feed["region"]
        kind, target, max_age = feed["kind"], feed["target"], feed["max_age"]
        # rsshub 源走多实例兜底；rss/gnews 直连只有一个 URL
        items, used, err = None, "", None
        for base in (list(bases) if kind == "rsshub" else [""]):
            if time.monotonic() - t0 > DEADLINE:
                err = err or RuntimeError("超过本轮 %ds 时间预算，跳过剩余源" % DEADLINE)
                break
            try:
                url = feed_url(base, target, fulltext) if kind == "rsshub" else target
                items = parse_items(http_get(url))
                used = (base or urllib.parse.urlparse(target).netloc).replace("https://", "").replace("http://", "")[:22]
                if kind == "rsshub" and base != bases[0]:   # 把刚成功的实例提到最前
                    bases.remove(base)
                    bases.insert(0, base)
                break
            except Exception as exc:        # noqa: BLE001 — 换下一个实例/放弃
                err, items = exc, None
        if items is None:
            stats.append((name, "FAIL", 0, 0, 0, "-"))
            log("[%s] 抓取失败：%s" % (name, str(err)[:120]))
            continue
        got = len(items)
        n_new = n_old = 0
        if push_test:
            # 测试：不看台账、不看时效；优先取命中规则的条目，没有命中就取前 N 条兜底
            matched = []
            for it in items:
                line = classify(it[0], it[2], loose)
                if line:
                    matched.append((it, line))
            picked = matched[:push_test] or [(it, classify(it[0], it[2], True) or "测试兜底") for it in items[:push_test]]
            if not matched:
                log("[%s] 测试兜底：该源当前没有命中规则的条目，取前 %d 条" % (name, len(picked)))
            for (title, link, desc, _pub), line in picked:
                hits.append({"name": name, "region": region, "title": title, "link": link,
                             "cn": bool(B_CHINA.search(title + " " + desc)), "text": desc, "line": line})
            stats.append((name, "OK", got, len(picked), 0, used.replace("https://", "").replace("http://", "")[:20]))
            continue
        for title, link, desc, pub in items:
            # 时效：发布时间过老的丢弃（抓不到时间的不丢，交给人判断）
            if pub is not None and (now - pub).days > max_age:
                n_old += 1
                continue
            if link in seen:
                continue
            line = classify(title, desc, loose)
            if not line:
                continue
            seen[link] = today
            n_new += 1
            hits.append({"name": name, "region": region, "title": title, "link": link,
                         "cn": bool(B_CHINA.search(title + " " + desc)), "text": desc, "line": line})
        stats.append((name, "OK", got, n_new, n_old, used.replace("https://", "").replace("http://", "")[:20]))

    log("\n源状态：")
    for name, status, got, n_new, n_old, used in stats:
        log("  %-18s %-5s 条目=%-4d 命中=%-3d 过期丢弃=%-3d 源=%s" % (name, status, got, n_new, n_old, used))

    log("\n本轮%s %d 条：" % ("测试取" if push_test else "命中", len(hits)))
    for h in hits:
        log("  【%s·%s】%s%s\n      %s" % (h["region"], h["line"], "★涉华 " if h["cn"] else "", h["title"], h["link"]))

    # ── 测试推送不写台账、不写 digest（避免把条目"吃掉"） ──
    if push_test:
        now_hk = datetime.now(HK)
        title = "海关查获情报·测试推送 %d 条（%s）" % (len(hits), now_hk.strftime("%m-%d %H:%M"))
        body, used_chars, truncated = build_body(hits, per_item, total_limit)
        log("\n推送正文：%d 字符%s" % (used_chars, "（已截断）" if truncated else ""))
        if dry or not any(os.environ.get(k) for k in ("SERVERCHAN_SENDKEY", "PUSHPLUS_TOKEN", "WECOM_WEBHOOK")):
            log("----- 正文预览（前 1500 字符）-----")
            log(body[:1500])
            log("----- 预览结束 -----")
        if dry:
            log("DRY_RUN=1，跳过推送。")
            return 0
        push_all(title, body)
        return 0

    # 台账瘦身：超过上限时按插入顺序丢弃最早的
    if len(seen) > STATE_MAX:
        seen = dict(list(seen.items())[-STATE_MAX:])
    if dry:
        log("DRY_RUN=1：不写去重台账、不推送（不会把条目当成已推送）。")
    else:
        STATE.write_text(json.dumps(seen, ensure_ascii=False, indent=1), encoding="utf-8")

    # ── 落盘可读结果：rss-digest/YYYY-MM-DD.md（每次运行追加一节，随仓库提交）──
    now_hk = datetime.now(HK)
    DIGEST_DIR.mkdir(exist_ok=True)
    day_file = DIGEST_DIR / ("%s.md" % now_hk.strftime("%Y-%m-%d"))
    sec = []
    if not day_file.exists():
        sec.append("# 海关查获 & 涉华出口风险 · %s\n" % now_hk.strftime("%Y-%m-%d"))
        sec.append("> 由 `.github/workflows/customs-rss.yml` 自动生成；每天 07:30 / 15:10 / 22:10（北京）各追加一节，只记新增，跨天不重复。")
        sec.append("> 线1【查获】= 标题命中查获/执法词；线2【涉华出口】= 标题命中敏感商品/管制词 且 涉华。\n")
    flags = []
    if dry:
        flags.append("dry-run 演练，未推送")
    if loose:
        flags.append("宽松模式")
    sec.append("\n## %s ｜ 新增 %d 条%s\n" % (now_hk.strftime("%H:%M"), len(hits), ("（%s）" % "，".join(flags)) if flags else ""))
    sec.append("\n| 源 | 状态 | 条目 | 命中 | 过期丢弃 | 实例 |")
    sec.append("| --- | --- | --- | --- | --- | --- |")
    for name, status, got, n_new, n_old, used in stats:
        sec.append("| %s | %s | %d | %d | %d | %s |" % (name, status, got, n_new, n_old, used))
    if hits:
        sec.append("\n")
        for h in hits:
            sec.append("- **【%s·%s】%s** %s  \n  <%s>" % (h["region"], h["line"], "★涉华 " if h["cn"] else "", h["title"], h["link"]))
    else:
        sec.append("\n本节无新增。\n")
    with day_file.open("a", encoding="utf-8") as fh:
        fh.write("\n".join(sec) + "\n")
    log("结果已写入：%s/rss-digest/%s" % (REPO_BLOB, day_file.name))

    if not hits:
        if notify_empty and not dry:
            # 心跳报平安：本轮无新增也推一条，便于确认三个时段都在跑
            ok = sum(1 for s in stats if s[1] == "OK")
            body = "本轮无新增。\n\n源状态（%d/%d 正常）：\n" % (ok, len(stats))
            body += "\n".join("- %s %s 条目=%d" % (s[0], s[1], s[2]) for s in stats)
            push_all("海关查获情报 0 条（%s）" % datetime.now(HK).strftime("%m-%d %H:%M"), body)
            log("无新增，已发心跳推送（NOTIFY_WHEN_EMPTY=1）。")
        else:
            log("无新增，未推送。")
        return 0

    if dry:
        log("DRY_RUN=1，跳过推送。")
        return 0

    push = hits[:MAX_ITEMS_PUSH]
    title = "海关查获情报 %d 条（%s）" % (len(hits), datetime.now(HK).strftime("%m-%d %H:%M"))
    body, used_chars, truncated = build_body(push, per_item, total_limit)
    log("推送正文：%d 字符%s" % (used_chars, "（超长已截断）" if truncated else ""))
    push_all(title, body)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
