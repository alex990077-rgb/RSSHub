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
ARCHIVE_DIR = ROOT / "archive"            # 回补存档（一次性，随仓库提交）
REPO_BLOB = "https://github.com/alex990077-rgb/RSSHub/blob/master"
HK = timezone(timedelta(hours=8))
UA = "Mozilla/5.0 (compatible; customs-rss/1.0; +https://github.com/alex990077-rgb/RSSHub)"
TIMEOUT = 20              # 单个实例单次请求超时（秒）
DEADLINE = 240            # 单轮抓取总预算（秒），超了就放弃剩余实例，避免拖垮 Actions
STATE_MAX = 8000          # 台账上限，超出丢最旧的
MAX_ITEMS_PUSH = 40       # 单次推送条数上限
# 回补时用 Google News 做「站内 + 日期范围」历史检索（feed 回不了整月）；
# 这两套词只用于历史检索，不影响日常推送的精确度。
GN_TERMS_ZH = "海关 OR 查获 OR 走私 OR 关税 OR 出口管制 OR 中国 OR 无人机 OR 稀土 OR 芯片"
GN_TERMS_EN = 'customs OR seized OR smuggling OR tariff OR "export control" OR China OR drone OR "rare earth"'

# 实例兜底顺序：公共实例会限流/封 IP，某个不通自动换下一个。
# 已移除 rsshub.app —— 它在 GitHub Actions 上对所有请求固定返回 403，只会掩盖真实原因（429/超时）。
DEFAULT_FALLBACK = (
    "https://rsshub.liumingye.cn,https://rsshub.ktachibana.party,http://localhost:1200"
)
# 三个推送时段（北京时间）；配合 workflow 里的主+备双 cron 使用
SLOTS = ("07:30", "15:10", "22:10")
BACKOFF_SECONDS = 20      # 全部实例都失败（多为 429 限流）时的退避秒数，然后整轮重试一次

# ── 源清单：(名称, 地区, RSSHub 路由, 时效天数) ───────────────────────────────
# 全部为境外/外媒（按你的要求已移除中国大陆源：中國海關雜誌、海關總署）
# 时效天数：条目发布时间早于该天数就丢弃
FEEDS = [
    # —— 中文（港澳台、东南亚）——
    {"name": "香港01",         "region": "HK", "kind": "rsshub", "target": "/hk01/latest",                 "max_age": 3, "lang": "zh", "domain": "hk01.com"},
    {"name": "星島日報",       "region": "HK", "kind": "rsshub", "target": "/stheadline/std/realtimenews", "max_age": 3, "lang": "zh", "domain": "stheadline.com"},
    {"name": "星洲網",         "region": "MY", "kind": "rsshub", "target": "/sinchew/latest",              "max_age": 3, "lang": "zh", "domain": "sinchew.com.my"},
    {"name": "星洲-天下事",     "region": "MY", "kind": "rsshub", "target": "/sinchew/category/国际/天下事", "max_age": 7, "lang": "zh", "domain": "sinchew.com.my"},
    {"name": "聯合早報",       "region": "SG", "kind": "rsshub", "target": "/zaobao/realtime/china",       "max_age": 3, "lang": "zh", "domain": "zaobao.com"},
    {"name": "中央社",         "region": "TW", "kind": "rsshub", "target": "/cna",                         "max_age": 3, "lang": "zh", "domain": "cna.com.tw"},
    {"name": "8视界",          "region": "SG", "kind": "rsshub", "target": "/8world",                      "max_age": 5, "lang": "zh", "domain": "8world.com"},
    {"name": "日经中文网",     "region": "JP", "kind": "rsshub", "target": "/nikkei/cn",                   "max_age": 5, "lang": "zh", "domain": "nikkei.com"},
    # —— 英文（通讯社 / 大报 / 官方）——
    {"name": "韩联社-英文",    "region": "KR", "kind": "rsshub", "target": "/yna/en",                      "max_age": 3, "lang": "en", "domain": "yna.co.kr"},
    {"name": "彭博社-政治",    "region": "US", "kind": "rsshub", "target": "/bloomberg/politics",          "max_age": 3, "lang": "en", "domain": "bloomberg.com"},
    {"name": "彭博社-商业",    "region": "US", "kind": "rsshub", "target": "/bloomberg/business",          "max_age": 3, "lang": "en", "domain": "bloomberg.com"},
    {"name": "USTR",           "region": "US", "kind": "rss",   "target": "https://ustr.gov/rss.xml",       "max_age": 14, "lang": "en", "domain": "ustr.gov"},
    # —— 菲律宾（RSSHub 无覆盖；GMA 官方 RSS + Google News 站内检索；摘要通道 min_text=0）——
    {"name": "GMA News",       "region": "PH", "kind": "rss",   "target": "https://data.gmanetwork.com/gno/rss/news/feed.xml", "max_age": 3, "lang": "en", "domain": "gmanetwork.com", "min_text": 0, "require_cn": True, "fetch_full": True},
    {"name": "BOC 菲海关",      "region": "PH", "kind": "gnews", "target": "https://news.google.com/rss/search?q=site:customs.gov.ph+when:7d&hl=en-PH&gl=PH&ceid=PH:en", "max_age": 7, "lang": "en", "domain": "customs.gov.ph", "min_text": 0, "require_cn": True},
    # —— 只有摘要/导语的源（按用户要求加回：接受摘要，标注"摘要"）——
    {"name": "路透社",          "region": "US", "kind": "gnews", "target": "https://news.google.com/rss/search?q=site:reuters.com+(China+customs+OR+smuggling+OR+%22export+control%22+OR+tariff)&hl=en-US&gl=US&ceid=US:en", "max_age": 3, "lang": "en", "domain": "reuters.com", "min_text": 0},
    {"name": "朝日新闻",        "region": "JP", "kind": "rsshub", "target": "/asahi/national", "max_age": 3, "lang": "ja", "domain": "asahi.com"},
    {"name": "The Star",       "region": "MY", "kind": "gnews", "target": "https://news.google.com/rss/search?q=site:thestar.com.my+when:2d&hl=en-MY&gl=MY&ceid=MY:en", "max_age": 3, "lang": "en", "domain": "thestar.com.my", "min_text": 0},
    {"name": "Central Asia Times", "region": "KZ", "kind": "gnews", "target": "https://news.google.com/rss/search?q=site:centralasiatimes.com+when:7d&hl=en-US&gl=US&ceid=US:en", "max_age": 14, "lang": "en", "domain": "centralasiatimes.com", "min_text": 0},
    {"name": "华尔街日报-世界",  "region": "US", "kind": "rss",   "target": "https://feeds.content.dowjones.io/public/rss/RSSWorldNews",     "max_age": 3, "lang": "en", "domain": "wsj.com", "min_text": 0, "fetch_full": True},
    {"name": "华尔街日报-商业",  "region": "US", "kind": "rss",   "target": "https://feeds.content.dowjones.io/public/rss/WSJcomUSBusiness", "max_age": 3, "lang": "en", "domain": "wsj.com", "min_text": 0, "fetch_full": True},
    {"name": "VietnamNet-时事", "region": "VN", "kind": "rss",   "target": "https://vietnamnet.vn/rss/thoi-su.rss",  "max_age": 3, "lang": "vi", "domain": "vietnamnet.vn", "min_text": 0, "fetch_full": True},
    {"name": "VietnamNet-国际", "region": "VN", "kind": "rss",   "target": "https://vietnamnet.vn/rss/the-gioi.rss", "max_age": 3, "lang": "vi", "domain": "vietnamnet.vn", "min_text": 0, "fetch_full": True},
]

# 已按"没有全文就去掉"移除的源（保留记录，便于日后回加）：
#   路透社 / 朝日新闻 / The Star / Central Asia Times —— 只能走 Google News 站内检索，仅标题+摘要
#   华尔街日报（feeds.content.dowjones.io）—— 正文 100~140 字，是导语不是全文
#   VietnamNet（vietnamnet.vn/rss）—— 正文 114~216 字，是摘要不是全文

# ── 关键词：按语言分组的 A/B/C 三组（规则见文件头）────────────────────────
# kind=rsshub 的源用 CORE_FILTER 粗筛；所有源都用 A/B/C 本地精筛（多语言取并集）
KW = {
    "zh": {
        "A": (
            r"查[獲获]|檢[獲获]|检[獲获]|緝[獲获]|缉[獲获]|截[獲获]|破[獲获]|偵破|侦破|"
            r"扣留|扣押|沒收|没收|收繳|收缴|查扣|走私|私[煙烟]|[緝缉]私|侵[權权]|假冒|盜版|盗版|"
            r"固[廢废]|洋垃圾|退[運运]|瞞報|瞒报|逃[稅税]|低報|低报|水[貨货]|販毒|贩毒|洗黑[錢钱]|"
            r"查[處处]|查[辦办]|截查|被查|查[緝缉]"
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
            r"出口退[稅税]|報關|报关|清關|清关|跨境電商|跨境电商|"
            r"煙花|烟花|爆竹|煙火爆竹|烟花爆竹|虛假申報|虚假申报|申報不實|申报不实|偽報|伪报"
        ),
        "D": r"胡塞|真主黨|真主党|哈瑪斯|哈马斯|伊朗|朝鮮|朝鲜|俄羅斯|俄罗斯|受制裁|繞道|绕道|第三國|第三国|黑市|掮客|中介|網店|网店|電商|电商|公開販售|公开贩售",
        "T": r"出口|进口|進口|貨物|货物|貨運|贸易|貿易|商品|转运|轉運|转口|轉口|走私|報關|报关|清關|清关|订单|訂單|採購|采购|供應鏈|供应链",
        # E：跨境/贸易语境词 —— 线1 必须同时命中 A + E，否则就只是"本地警察查获"的社会新闻
        "E": (
            r"海[關关]|關[稅税]|关税|[緝缉]私|走私|私[煙烟]|報關|报关|清[關关]|进出口|進出口|出口|进口|進口|"
            r"貨[物櫃]|货物|货柜|集裝箱|集装箱|口岸|邊境|边境|跨境|機場|机场|港口|碼頭|码头|郵包|邮包|"
            r"快遞|快递|保稅|保税|轉運|转运|轉口|转口|外貿|外贸|貿易|贸易|海巡|航警|移民署|關務|关务|"
            r"檢疫|检疫|查緝|查缉|貨輪|货轮|漁船|渔船|船[舶隻只]|自由貿易|自由贸易|口岸|緝毒|缉毒"
        ),
        # N：噪音词 —— 命中即丢弃（地方治安/社会新闻，与进出口无关）
        "N": (
            r"車手|车手|詐[騙欺團]|诈骗|诈团|賭|赌|竊|窃|搶|抢|鬥毆|斗殴|毆打|殴打|家暴|火警|酒駕|酒驾|"
            r"毒駕|毒驾|性侵|偷拍|棄養|弃养|幫派|帮派|槍擊|枪击|命案|凶殺|兇殺|凶杀|車禍|车祸|輕生|轻生|"
            r"自殺|自杀|竊盜|窃盗|吸毒|販毒集團|贩毒集团|虐[待貓狗]|糾紛|纠纷|討債|讨债|圍毆|围殴|消委會|消委会|商品說明|商品说明|不良營商|不良营商"
        ),
    },
    "en": {
        "A": (
            r"seiz|confiscat|smuggl|contraband|counterfeit|infringing|undeclared|crackdown|"
            r"misdeclar|evasion|forced labo|laundering|trafficking|illicit trade"
        ),
        "B": r"China|Chinese|Beijing|Hong Kong|Made in China|Shenzhen|Guangzhou|Yiwu|Shanghai|Renminbi|yuan|mainland",
        "C": (
            r"export control|dual-use|dual use|drone|UAV|rare earth|gallium|germanium|graphite|semiconductor|"
            r"chip|advanced chip|lithium battery|solar panel|photovoltaic|weapon|ammunition|missile|firearm|"
            r"explosive|precursor|fentanyl|nuclear|uranium|centrifuge|satellite|radar|night vision|body armor|"
            r"helmet|military uniform|sanction|circumvent|transshipment|trans-shipment|origin fraud|tariff|"
            r"anti-dumping|antidumping|export tax rebate|customs broker|clearance|cross-border e-commerce|"
            r"military equipment|military drone|military technology|defense contractor|military export|"
            r"firecracker|fireworks|pyrotechnic"
        ),
        "D": r"Houthi|Hezbollah|Hamas|Iran|North Korea|Russia|black market|broker|intermediary|online shop|e-commerce|third country",
        "T": r"export|import|cargo|shipment|trade|goods|supply|procure|order|consignment|container|port|exports",
        "E": (
            r"customs|border|port|airport|harbou?r|cargo|container|shipment|freight|vessel|warehouse|"
            r"export|import|trade|declaration|tariff|bonded|quarantine|smuggl|contraband|consignment|"
            r"bureau of customs|BOC|MICP|Philippine"
        ),
        "N": r"money mule|shoplifting|domestic violence|armed robbery|hit-and-run|car crash|carjacking|stalker|drunk driving",
    },
    "ja": {
        "A": r"密輸|押収|没収|偽ブランド|侵害|申告漏れ|脱税|密輸出|密輸入|関税法違反",
        "B": r"中国|中国製|北京|香港|上海|中国企業|人民元",
        "C": (
            r"輸出管理|デュアルユース|軍民両用|ドローン|無人機|レアアース|希土類|ガリウム|ゲルマニウム|黒鉛|"
            r"半導体|電池|リチウム|太陽光|兵器|弾薬|ミサイル|銃器|火薬|化学|前駆体|核|ウラン|遠心分離機|"
            r"衛星|レーダー|制裁|迂回|転送|原産地|関税|反ダンピング|強制労働|サプライチェーン|通関|越境EC|軍事"
        ),
        "D": r"フーシ|ヒズボラ|ハマス|イラン|北朝鮮|ロシア|闇市場|ブローカー|仲介",
        "T": r"輸出|輸入|貨物|貿易|商品|積み替え|転送|通関|サプライチェーン|コンテナ",
        "E": r"税関|通関|密輸|輸出|輸入|貨物|コンテナ|港湾|空港|保税|貿易|検疫|税関職員",
        "N": r"詐欺|ストーカー|痴漢|万引き|飲酒運転|ひき逃げ|強盗|殺人",
    },
    "ko": {
        "A": r"밀수|압수|위조|침해|탈세|허위신고|관세법 위반",
        "B": r"중국|중국산|베이징|홍콩|상하이|중국기업|위안화",
        "C": (
            r"수출통제|군민양용|드론|무인기|희토류|갈륨|게르마늄|흑연|반도체|배터리|리튬|태양광|무기|탄약|"
            r"미사일|총기|화약|화학|전구체|핵|우라늄|원심분리기|위성|레이더|제재|우회|전송|원산지|관세|"
            r"반덤핑|강제노동|공급망|통관|전자상거래|군사"
        ),
        "D": r"후티|헤즈볼라|하마스|이란|북한|러시아|암시장|브로커|중개",
        "T": r"수출|수입|화물|무역|상품|운송|환적|통관|컨테이너",
        "E": r"세관|통관|밀수|수출|수입|화물|컨테이너|항만|공항|보세|무역|검역",
        "N": r"보이스피싱|성범죄|음주운전|절도|강도|살인|마약사범",
    },
    "vi": {
        "A": r"tịch thu|buôn lậu|hàng giả|hàng nhái|xuất lậu|nhập lậu|gian lận thương mại|trốn thuế|vận chuyển trái phép",
        "B": r"Trung Quốc|Đài Loan|Hồng Kông|made in china|Thượng Hải|Quảng Đông",
        "C": (
            r"xuất khẩu|nhập khẩu|xuất xứ|gian lận xuất xứ|chuyển tải|trung chuyển|máy bay không người lái|"
            r"đất hiếm|chất bán dẫn|vi mạch|pin lithium|năng lượng mặt trời|vũ khí|đạn dược|tiền chất|"
            r"thuế quan|chống bán phá giá|chuỗi cung ứng|thương mại điện tử|quân sự"
        ),
        "D": r"Houthi|Hezbollah|Hamas|Iran|Triều Tiên|Nga|chợ đen|môi giới|trung gian",
        "T": r"xuất khẩu|nhập khẩu|hàng hóa|thương mại|vận chuyển|container|cảng",
        "E": r"hải quan|xuất khẩu|nhập khẩu|nhập lậu|xuất lậu|hàng hóa|container|cảng|sân bay|thương mại|cửa khẩu|kiểm dịch",
        "N": r"lừa đảo|cướp|tai nạn|trộm|giết người|hiếp dâm",
    },
}
A_ENFORCE = re.compile("|".join("(?:%s)" % KW[k]["A"] for k in KW), re.I)
B_CHINA = re.compile("|".join("(?:%s)" % KW[k]["B"] for k in KW), re.I)
C_GOODS = re.compile("|".join("(?:%s)" % KW[k]["C"] for k in KW), re.I)
D_FLOW = re.compile("|".join("(?:%s)" % KW[k]["D"] for k in KW), re.I)
T_TRADE = re.compile("|".join("(?:%s)" % KW[k]["T"] for k in KW), re.I)
E_CROSS = re.compile("|".join("(?:%s)" % KW[k]["E"] for k in KW), re.I)
N_NOISE = re.compile("|".join("(?:%s)" % KW[k]["N"] for k in KW), re.I)

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


def mark_slot(slots_done, slot_key, path):
    """记录某个时段已完成（供备用 cron 秒退），并只保留最近 60 条。"""
    if not slot_key:
        return
    slots_done[slot_key] = datetime.now(HK).strftime("%Y-%m-%d %H:%M")
    if len(slots_done) > 60:
        slots_done = dict(sorted(slots_done.items())[-60:])
    path.write_text(json.dumps(slots_done, ensure_ascii=False, indent=1), encoding="utf-8")


def http_get(url):
    """单次请求。多实例兜底已经在外面做了，这里不再重试，避免最坏情况拖到几分钟。"""
    req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "application/rss+xml, application/xml;q=0.9, */*;q=0.8"})
    with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
        return resp.read().decode("utf-8", "replace")


# ── 外文 → 中文翻译（云端免费接口，无需 API key）──────────────────────────
# 翻译前把易误译的机构缩写展开（否则 Google 会把菲律宾 BOC 译成「中国银行」）
TRANS_FIX = [
    # 金额简写先展开（否则 "P6.7-M" 会被机翻成"6.7米"）
    (re.compile(r"\bP(\d+(?:\.\d+)?)\s*-\s*M\b", re.I), r"\1 million pesos"),
    (re.compile(r"\bP(\d+(?:\.\d+)?)\s*-\s*B\b", re.I), r"\1 billion pesos"),
    # 机构缩写（加 re.I：BoC/BOC 都可能出现；BOC 也被 Google 误译成「中国银行/加拿大央行」）
    (re.compile(r"\bBOC\b", re.I), "Bureau of Customs"),
    (re.compile(r"\bMICP\b"), "Manila International Container Port"),
    (re.compile(r"\bPDEA\b"), "Philippine Drug Enforcement Agency"),
    (re.compile(r"\bNAIA\b"), "Ninoy Aquino International Airport"),
    (re.compile(r"\bNBI\b"), "National Bureau of Investigation"),
    (re.compile(r"\bBI\b(?=\s+(?:Lookout|Immigration))"), "Bureau of Immigration"),
]


def normalize_for_translation(text):
    for rx, rep_s in TRANS_FIX:
        text = rx.sub(rep_s, text)
    return text


def needs_translation(text):
    """判断是否需要翻成中文：含假名/韩文一定要翻；汉字占比低（英/越等拉丁文）也翻。"""
    if not text:
        return False
    if re.search(r"[\u3040-\u30ff\uac00-\ud7af]", text):     # 日文假名 / 韩文
        return True
    cjk = len(re.findall(r"[\u4e00-\u9fff]", text))
    return cjk / max(len(text), 1) < 0.25


def _translate_once(chunk):
    """Google 翻译公开端点（client=gtx，免费无 key）。失败返回空串，由调用方兜底。"""
    url = ("https://translate.googleapis.com/translate_a/single?client=gtx&sl=auto&tl=zh-CN&dt=t&q="
           + urllib.parse.quote(chunk))
    data = json.loads(http_get(url))
    return "".join(seg[0] for seg in data[0] if seg and seg[0])


def translate_text(text, limit=1600):
    """按段落切块翻译，块大小 <= limit，避免 URL 过长与单次失败全丢。"""
    if not text or not needs_translation(text):
        return text
    chunks, buf = [], ""
    for para in re.split(r"(?<=[。！？.!?])\s+|\n+", text):
        if not para:
            continue
        if len(buf) + len(para) + 1 > limit and buf:
            chunks.append(buf)
            buf = para
        else:
            buf = (buf + " " + para).strip()
    if buf:
        chunks.append(buf)
    out = []
    for c in chunks:
        try:
            out.append(_translate_once(c))
        except Exception:                   # noqa: BLE001 — 翻译失败就保留原文该段
            out.append(c)
    return "".join(out)


def extract_article(url):
    """命中后按链接补抓正文（只对 kind=rss 且标了 fetch_full 的源）。失败返回空串。"""
    try:
        req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "text/html,application/xhtml+xml"})
        with urllib.request.urlopen(req, timeout=25) as resp:
            raw = resp.read(900000).decode("utf-8", "replace")
    except Exception:                       # noqa: BLE001
        return ""
    txt = re.sub(r"(?is)<(script|style|noscript|svg|header|footer|nav|form)[^>]*>.*?</\1>", " ", raw)
    m = re.search(r"(?is)<body[^>]*>(.*?)</body>", txt)
    if m:
        txt = m.group(1)
    txt = re.sub(r"(?is)<br\s*/?>|</p>|</div>|</h[1-6]>", "\n", txt)
    txt = re.sub(r"(?s)<[^>]+>", " ", txt)
    txt = html.unescape(txt)
    txt = re.sub(r"[ \t\u00a0]+", " ", txt)
    txt = re.sub(r"\n{3,}", "\n\n", txt)
    return txt.strip()


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

    线1【查获】   = 标题命中 A（查获/执法词）**且**命中 E（跨境/贸易语境词）
                    —— 只命中 A 的是"本地警察查获"社会新闻（如"嘉市警查获3车手"），丢弃
    线2【涉华出口】= 标题命中 C（敏感商品/管制词）且标题命中 B（涉华指向词）
    线3【涉华流向】= 标题同时命中 B + D（受制裁买家/规避手法/黑市渠道）+ T（贸易流向词）
    另外：标题命中 N（噪音词：诈骗/赌博/车祸/家暴…）直接丢弃
    LOOSE=1 放宽：B 可放宽到正文；线1 允许 A 在正文（E 仍在标题）
    """
    if N_NOISE.search(title):
        return None
    if A_ENFORCE.search(title) and E_CROSS.search(title):
        return "查获"
    b_hit = B_CHINA.search(title) or (loose and B_CHINA.search(title + " " + desc))
    if C_GOODS.search(title) and b_hit:
        return "涉华出口"
    if D_FLOW.search(title) and T_TRADE.search(title) and b_hit:
        return "涉华流向"
    if loose and A_ENFORCE.search(title + " " + desc) and E_CROSS.search(title):
        return "查获·宽松"
    return None


def feed_url(base, path, fulltext=False, limit=None):
    # 中文路径必须编码（"国际/天下事" 这类），safe="/" 保留层级分隔
    qpath = urllib.parse.quote(path, safe="/")
    # limit 在 RSSHub 里是先 filter → 再 limit → 最后才 fulltext，所以全文只解析留下来的条目；
    # 全文模式把 limit 压到 15，避免单源解析几十篇正文拖垮整轮；回补时可放宽。
    if limit is None:
        limit = 30 if fulltext else 50
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
    """拼推送正文：每条 = 【地区·线】中文标题（原文）+ 链接 + 正文（中文译文或原文）。超预算就截断。"""
    parts, used, truncated = [], 0, False
    for h in hits:
        zh = h.get("zh_title")
        head = "%s%s" % ("★涉华 " if h["cn"] else "", zh or h["title"])
        if zh:
            head += "（原文：%s）" % h["title"]
        tag = "·摘要" if len(h.get("text") or "") < 300 else ""
        piece = ["【%s·%s%s】%s" % (h["region"], h["line"], tag, head), h["link"]]
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
    translate = os.environ.get("TRANSLATE") != "0"            # 默认开：外文标题/正文翻成中文
    translate_body = os.environ.get("TRANSLATE_BODY") != "0"  # 默认开：正文也翻
    min_text = int(os.environ.get("MIN_TEXT") or 300)        # 正文短于此长度视为"无全文"，丢弃
    backfill = int(os.environ.get("BACKFILL_DAYS") or 0)     # >0 = 回补模式：建存档、不推送、条目全部记账
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
    log("正文模式：%s ｜ 源 %d 个（%s）｜ 翻译：%s%s ｜ 无全文阈值：%d 字"
        % ("全文（mode=fulltext）" if fulltext else "摘要", len(FEEDS), "".join(sorted({f["lang"] for f in FEEDS})),
           "开" if translate else "关", "（含正文）" if (translate and translate_body) else "", min_text))
    for lang in KW:
        log("\n[%s] A组·查获/执法词：%s" % (lang, KW[lang]["A"]))
        log("[%s] B组·涉华指向词：%s" % (lang, KW[lang]["B"]))
        log("[%s] C组·敏感商品/管制词：%s" % (lang, KW[lang]["C"]))
        log("[%s] D组·流向/手法/渠道：%s" % (lang, KW[lang]["D"]))
        log("[%s] T组·贸易流向词：%s" % (lang, KW[lang]["T"]))
        log("[%s] E组·跨境/贸易语境词（线1 必备）：%s" % (lang, KW[lang]["E"]))
        log("[%s] N组·噪音词（命中即丢）：%s" % (lang, KW[lang]["N"]))
    log("\nRSSHub 端粗筛：%s" % CORE_FILTER)
    if push_test:
        log("\nPUSH_TEST=%d：测试推送，每源取前 %d 条，强制全文，不写台账/digest" % (push_test, push_test))
    log("北京时间：%s" % datetime.now(HK).strftime("%Y-%m-%d %H:%M"))

    if os.environ.get("SELFTEST") == "1":
        log("SELFTEST=1：只自检推送通道，不抓取。")
        return 1 if push_all("海关RSS自检（%s）" % datetime.now(HK).strftime("%m-%d %H:%M"),
                             "收到这条说明推送通道已配好；接下来每天 07:30 / 15:10 / 22:10（北京）自动跑。") else 0

    # ── 时段闸门 ──
    # 作用：cron-job.org 的外部 dispatch（workflow_dispatch）跑完后会记录"该时段已完成"；
    #       万一 GitHub 内置 schedule 也触发了（兜底路径），它发现时段已完成就秒退，不重复抓取/推送。
    # 手工触发永远照常运行（只记账、不秒退），方便随时测。
    event_name = (os.environ.get("EVENT_NAME") or "").strip()
    slots_path = ROOT / "slots.json"
    slots_done = {}
    if slots_path.exists():
        try:
            slots_done = json.loads(slots_path.read_text(encoding="utf-8"))
        except Exception:                   # noqa: BLE001
            slots_done = {}
    now_hk = datetime.now(HK)
    day_key = now_hk.strftime("%Y-%m-%d")
    cur_hm = now_hk.strftime("%H:%M")
    # 只关心"最近一个已过时段"：它已服务过就说明当前这轮是重复触发
    passed = [s for s in SLOTS if cur_hm >= s]
    due_slot = passed[-1] if passed else None
    if event_name == "schedule" and not (backfill or push_test or dry):
        if due_slot is None or ("%s_%s" % (day_key, due_slot)) in slots_done:
            log("定时触发：最近时段 %s 已服务过（今日 %s），秒退（未消耗抓取预算）。"
                % (due_slot or "无", "/".join(SLOTS)))
            return 0
        log("定时触发：本次负责 %s 时段（%s）" % (due_slot, day_key))
    slot_key = ("%s_%s" % (day_key, due_slot)) if due_slot else None

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
    budget = 1200 if backfill else DEADLINE          # 回补给足 20 分钟预算
    for feed in FEEDS:
        name, region = feed["name"], feed["region"]
        kind, target, max_age = feed["kind"], feed["target"], feed["max_age"]
        if backfill:
            max_age = backfill                       # 回补：按时效窗口放宽（如 30 天 = 整个 9 月）
        # rsshub 源走多实例兜底；rss/gnews 直连只有一个 URL
        items, used, err = None, "", None
        base_errors = []
        base_list = list(bases) if kind == "rsshub" else [""]
        # 公共实例会 429 限流，限流通常几十秒就恢复 → 全失败时退避后整轮重试一次
        for attempt in range(2):
            for base in base_list:
                if time.monotonic() - t0 > budget:
                    err = err or RuntimeError("超过本轮 %ds 时间预算，跳过剩余源" % budget)
                    break
                try:
                    url = feed_url(base, target, fulltext, limit=40 if backfill else None) if kind == "rsshub" else target
                    items = parse_items(http_get(url))
                    used = (base or urllib.parse.urlparse(target).netloc).replace("https://", "").replace("http://", "")[:22]
                    if kind == "rsshub" and base != bases[0]:   # 把刚成功的实例提到最前
                        bases.remove(base)
                        bases.insert(0, base)
                    break
                except Exception as exc:        # noqa: BLE001 — 换下一个实例/放弃
                    err, items = exc, None
                    tag = (base or target).replace("https://", "").replace("http://", "")[:22]
                    msg = "HTTP %s" % exc.code if getattr(exc, "code", None) else str(exc)[:28]
                    base_errors.append("%s=%s" % (tag, msg))
            if items is not None:
                break
            if attempt == 0 and kind == "rsshub" and time.monotonic() - t0 + BACKOFF_SECONDS < budget:
                log("[%s] 全部实例失败（%s），退避 %ds 后重试一次" % (name, " | ".join(base_errors[-len(base_list):]), BACKOFF_SECONDS))
                time.sleep(BACKOFF_SECONDS)
        if items is None:
            stats.append((name, "FAIL", 0, 0, 0, 0, "-"))
            log("[%s] 抓取失败：%s" % (name, " | ".join(base_errors[-len(base_list):])[:200]))
            continue
        got = len(items)
        n_new = n_old = n_short = n_nocn = 0
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
            stats.append((name, "OK", got, len(picked), 0, 0, used))
            continue
        for title, link, desc, pub in items:
            # 时效：发布时间过老的丢弃（抓不到时间的不丢，交给人判断）
            if pub is not None and (now - pub).days > max_age:
                n_old += 1
                continue
            if link in seen and not backfill:       # 回补时忽略台账，把窗口内全部收进存档
                continue
            line = classify(title, desc, loose)
            if not line:
                continue
            # 该源要求涉华：标题或摘要里必须出现中国指向词（否则是本地海关新闻）
            if feed.get("require_cn") and not B_CHINA.search(title + " " + desc):
                n_nocn += 1
                continue
            # 无全文（只有导语/摘要）→ 丢弃、不记账，符合"没有全文就去掉"
            if len(desc) < feed.get("min_text", min_text):
                n_short += 1
                continue
            seen[link] = today
            n_new += 1
            hits.append({"name": name, "region": region, "title": title, "link": link,
                         "cn": bool(B_CHINA.search(title + " " + desc)), "text": desc, "line": line,
                         "pub": pub.astimezone(HK).strftime("%Y-%m-%d") if pub else ""})
        stats.append((name, "OK", got, n_new, n_old, n_short + n_nocn, used))

    log("\n源状态：")
    for name, status, got, n_new, n_old, n_short, used in stats:
        log("  %-16s %-5s 条目=%-5d 命中=%-3d 过期=%-4d 丢弃=%-4d 源=%s"
            % (name, status, got, n_new, n_old, n_short, used[:18]))

    # ── 命中后补抓正文：只对"有真实文章链接"的源（feed 只给摘要时才有意义）──
    if not backfill and hits:
        feed_map = {f["name"]: f for f in FEEDS}
        n_full = 0
        for h in hits[:MAX_ITEMS_PUSH]:
            if not feed_map.get(h["name"], {}).get("fetch_full") or len(h["text"]) >= 600:
                continue
            body = extract_article(h["link"])
            if len(body) > max(400, len(h["text"]) + 200):
                h["text"] = body
                h["full_ok"] = True
                n_full += 1
        log("补抓正文成功 %d 条（仅限有真实文章链接的源）" % n_full)

    # ── 外文翻译（回补时只翻标题，避免上千次请求）──
    translated_t = translated_b = 0
    if translate and hits:
        for h in hits[:MAX_ITEMS_PUSH]:
            if needs_translation(h["title"]):
                zh = translate_text(normalize_for_translation(h["title"]))
                if zh and zh.replace(" ", "") != h["title"].replace(" ", ""):
                    h["zh_title"] = zh
                    translated_t += 1
            if translate_body and not backfill and h["text"] and needs_translation(h["text"]):
                zh_body = translate_text(normalize_for_translation(h["text"])[:per_item])
                if zh_body:
                    h["orig_text"] = h["text"]
                    h["text"] = zh_body
                    translated_b += 1
        if backfill and len(hits) > MAX_ITEMS_PUSH:
            log("回补模式：标题只翻前 %d 条（共 %d 条），其余保留原文" % (MAX_ITEMS_PUSH, len(hits)))
        log("翻译：标题 %d 条、正文 %d 条（Google 翻译公开端点，免费无 key）" % (translated_t, translated_b))

    # ── 回补模式：写存档 + 全部记账，绝不推送 ──
    if backfill:
        # ① 历史线索：Google News 站内检索 + 日期范围（feed 只暴露最近几条，回不了整月）
        now_hk = datetime.now(HK)
        after = (now_hk - timedelta(days=backfill)).strftime("%Y-%m-%d")
        before = now_hk.strftime("%Y-%m-%d")
        gn_rows, done_dom = [], set()
        for feed in FEEDS:
            dom = feed.get("domain")
            if not dom or dom in done_dom:
                continue
            done_dom.add(dom)
            zh_src = feed["lang"] == "zh"
            q = "site:%s (%s) after:%s before:%s" % (dom, GN_TERMS_ZH if zh_src else GN_TERMS_EN, after, before)
            url = "https://news.google.com/rss/search?q=%s&hl=%s&gl=%s&ceid=%s" % (
                urllib.parse.quote(q), "zh-CN" if zh_src else "en-US",
                "CN" if zh_src else "US", "CN:zh-Hans" if zh_src else "US:en")
            try:
                gn = parse_items(http_get(url))
            except Exception as exc:        # noqa: BLE001
                log("[GN:%s] 历史检索失败：%s" % (dom, str(exc)[:80]))
                continue
            kept = 0
            for title, link, desc, pub in gn:
                if kept >= 60:
                    break
                if pub is not None and (now - pub).days > backfill:
                    continue
                if link in seen or not classify(title, desc, False):
                    continue
                seen[link] = today
                gn_rows.append({"region": feed["region"], "source": feed["name"], "title": title, "link": link,
                                "pub": pub.astimezone(HK).strftime("%Y-%m-%d") if pub else "", "snippet": desc[:200]})
                kept += 1
            log("[GN:%s] 历史线索 %d 条" % (dom, kept))

        if len(seen) > STATE_MAX:
            seen = dict(list(seen.items())[-STATE_MAX:])
        STATE.write_text(json.dumps(seen, ensure_ascii=False, indent=1), encoding="utf-8")
        ARCHIVE_DIR.mkdir(exist_ok=True)
        # 存档文件名默认用当前月；跨月回补（如 11 月 1 日回补 10 月）可用 ARCHIVE_MONTH 指定
        month = os.environ.get("ARCHIVE_MONTH") or now_hk.strftime("%Y-%m")
        by_day = {}
        for h in hits:
            by_day.setdefault(h.get("pub") or "未知日期", []).append(h)
        lines = ["# 海关查获 & 涉华出口风险 · %s 存档\n" % month,
                 "> 回补窗口：最近 %d 天（截至 %s，北京时间）｜源 %d 个" % (backfill, now_hk.strftime("%Y-%m-%d %H:%M"), len(FEEDS)),
                 "> 本存档条目已**全部写入去重台账** `seen_rss.json`，之后任何时段都不会再推送；存档仅供回溯。\n",
                 "## 源覆盖（feed 现存量）\n", "| 源 | 状态 | 条目 | 命中 | 过期 | 丢弃 |", "| --- | --- | --- | --- | --- | --- |"]
        for name, status, got, n_new, n_old, n_short, _used in stats:
            lines.append("| %s | %s | %d | %d | %d | %d |" % (name, status, got, n_new, n_old, n_short))
        lines.append("\n## A. 全文条目（feed 现存量，%d 条）\n" % len(hits))
        if not hits:
            lines.append("（无：各源 feed 只暴露最近几条，9 月更早的条目见 B 段）\n")
        for day in sorted(by_day, reverse=True):
            items_day = sorted(by_day[day], key=lambda x: x["region"])
            lines.append("\n### %s（%d 条）\n" % (day, len(items_day)))
            for h in items_day:
                zh = h.get("zh_title")
                label = ("%s（%s）" % (zh, h["title"])) if zh else h["title"]
                lines.append("- **【%s·%s】%s** %s  \n  <%s>  \n  正文 %d 字"
                             % (h["region"], h["line"], "★涉华 " if h["cn"] else "", label, h["link"], len(h["text"])))
        lines.append("\n## B. 历史线索（Google News 站内检索 %s ~ %s，仅标题+摘要，共 %d 条）\n"
                     % (after, before, len(gn_rows)))
        gn_by_day = {}
        for r in gn_rows:
            gn_by_day.setdefault(r["pub"] or "未知日期", []).append(r)
        for day in sorted(gn_by_day, reverse=True):
            lines.append("\n### %s（%d 条）\n" % (day, len(gn_by_day[day])))
            for r in gn_by_day[day]:
                lines.append("- 【%s】%s  \n  <%s>" % (r["source"], r["title"], r["link"]))
        md = ARCHIVE_DIR / ("%s.md" % month)
        md.write_text("\n".join(lines) + "\n", encoding="utf-8")
        js = ARCHIVE_DIR / ("%s.json" % month)
        js.write_text(json.dumps({
            "window_days": backfill, "generated_at": now_hk.strftime("%Y-%m-%d %H:%M"),
            "feeds": [f["name"] for f in FEEDS],
            "stats": [{"name": s[0], "status": s[1], "got": s[2], "hit": s[3], "old": s[4], "no_fulltext": s[5]} for s in stats],
            "items": [{"pub": h.get("pub", ""), "region": h["region"], "source": h["name"], "line": h["line"],
                       "title": h["title"], "zh_title": h.get("zh_title", ""), "link": h["link"],
                       "cn": h["cn"], "fulltext": True, "text": h["text"]} for h in hits],
            "leads": [dict(r, fulltext=False) for r in gn_rows],
        }, ensure_ascii=False, indent=1), encoding="utf-8")
        log("\n回补完成：全文条目 %d 条 + 历史线索 %d 条 → %s/tree/master/archive" % (len(hits), len(gn_rows), REPO_BLOB.replace("/blob/master", "")))
        log("台账已记 %d 条：这些历史条目今后不会推送。" % len(seen))
        return 0

    log("\n本轮%s %d 条：" % ("测试取" if push_test else "命中", len(hits)))
    for h in hits:
        log("  【%s·%s】%s%s（正文 %d 字）\n      %s"
            % (h["region"], h["line"], "★涉华 " if h["cn"] else "", h.get("zh_title", h["title"]), len(h["text"]), h["link"]))

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
    # dry-run 演练不写 digest，避免演练记录污染仓库与下游周/月总结
    now_hk = datetime.now(HK)
    if dry:
        log("DRY_RUN=1：不写 digest（演练不留痕）。")
    else:
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
    sec.append("\n| 源 | 状态 | 条目 | 命中 | 过期 | 丢弃 | 源站 |")
    sec.append("| --- | --- | --- | --- | --- | --- | --- |")
    for name, status, got, n_new, n_old, n_short, used in stats:
        sec.append("| %s | %s | %d | %d | %d | %d | %s |" % (name, status, got, n_new, n_old, n_short, used))
    if hits:
        sec.append("\n")
        for h in hits:
            zh = h.get("zh_title")
            label = ("%s（%s）" % (zh, h["title"])) if zh else h["title"]
            sec.append("- **【%s·%s】%s** %s  \n  <%s>" % (h["region"], h["line"], "★涉华 " if h["cn"] else "", label, h["link"]))
    else:
        sec.append("\n本节无新增。\n")
    if not dry:
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
        if not dry:
            mark_slot(slots_done, slot_key, slots_path)
        return 0

    push = hits[:MAX_ITEMS_PUSH]
    title = "海关查获情报 %d 条（%s）" % (len(hits), datetime.now(HK).strftime("%m-%d %H:%M"))
    body, used_chars, truncated = build_body(push, per_item, total_limit)

    if dry:
        log("推送正文：%d 字符%s" % (used_chars, "（超长已截断）" if truncated else ""))
        log("----- dry-run 正文预览（前 1500 字符）-----")
        log(body[:1500])
        log("----- 预览结束 -----")
        log("DRY_RUN=1，跳过推送。")
        return 0

    log("推送正文：%d 字符%s" % (used_chars, "（超长已截断）" if truncated else ""))
    push_all(title, body)
    mark_slot(slots_done, slot_key, slots_path)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
