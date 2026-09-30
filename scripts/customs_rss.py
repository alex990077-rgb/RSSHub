# -*- coding: utf-8 -*-
"""
海关查获情报 · RSSHub 通道（云端专用，纯标准库，零依赖）

链路：RSSHub 路由（自带 filter 关键词过滤）→ 二次关键词过滤 → 跨天去重 → 推送微信
产物：seen_rss.json（去重台账，由本脚本维护）+ Actions 运行日志（即每日清单）

只读公开新闻源，不碰任何本地文件、不依赖本机环境。
可配置环境变量（GitHub Secrets / Variables）：
  RSSHUB_BASE         RSSHub 地址，默认 https://rsshub.app
  SERVERCHAN_SENDKEY  Server酱 Turbo SendKey（https://sct.ftqq.com）
  PUSHPLUS_TOKEN      PushPlus token
  PUSHPLUS_TOPIC      PushPlus 一对多群组编码（可选）
  WECOM_WEBHOOK       企业微信群机器人 Webhook（可选）
  DRY_RUN=1           只打印不推送
  LOOSE=1             放宽过滤：标题或摘要命中执法词即算（默认只看标题，精度优先）
"""

import json
import os
import re
import sys
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
STATE = ROOT / "seen_rss.json"
HK = timezone(timedelta(hours=8))
UA = "Mozilla/5.0 (compatible; customs-rss/1.0; +https://github.com/alex990077-rgb/RSSHub)"
TIMEOUT = 40
STATE_MAX = 8000          # 台账上限，超出丢最旧的
MAX_ITEMS_PUSH = 40       # 单次推送条数上限

# ── 源清单：(名称, 地区, RSSHub 路由, filter 正则, 时效天数) ─────────────────
# ⚠ RSSHub 的 filter 在 opencc(繁转简) 之前执行 → 繁体源必须用繁体关键词，
#   这里统一用「两体兼容」写法 海[關关] 这种；输出端 opencc=t2s 转简体。
# 时效天数：条目发布时间早于该天数就丢弃（刊物类是月刊，给 45 天）。
FEEDS = [
    ("香港01",      "HK", "/hk01/latest",                 r"海[關关]|查[獲获]|[緝缉]私|走私|检[獲获]|中國|中国", 3),
    ("星島日報",    "HK", "/stheadline/std/realtimenews", r"海[關关]|查[獲获]|[緝缉]私|走私|检[獲获]|中國|中国", 3),
    ("星洲網",      "MY", "/sinchew/latest",              r"海关|查获|走私|私烟|中国|关税局", 3),
    ("聯合早報",    "SG", "/zaobao/realtime/china",       r"海关|查获|走私|中国", 3),
    ("中國海關雜誌", "CN", "/gmcmonline/chinacustoms",     r"查获|走私|侵权|固废|处罚|退运", 90),
    # 需 Chromium + 国内 IP（RSSHub 官方注明）：自建实例才通，公共实例会 503
    ("海關總署",    "CN", "/gov/customs/list/latest",     r"查获|拍卖|法规|公告", 7),
]

# ── 二次过滤：必须命中「执法词」；同时标出是否涉华（便于人判优先级）───────
ENFORCE = re.compile(
    r"海[關关]|查[獲获]|缉私|緝私|走私|私[煙烟]|截[獲获]|检[獲获]|檢[獲获]|扣留|"
    r"侵[權权]|固[廢废]|退[運运]|瞒报|瞞報|逃[稅税]|出口管制|两用物项|兩用物項|没收|沒收|水货|水貨"
)
CN_HINT = re.compile(r"中国|中國|内地|內地|大陆|大陸|China|中港|北京|人民币|人民幣")


def log(msg):
    print(msg, flush=True)


def http_get(url):
    req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "application/rss+xml, application/xml;q=0.9, */*;q=0.8"})
    last = None
    for attempt in range(3):
        try:
            with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
                return resp.read().decode("utf-8", "replace")
        except Exception as exc:            # noqa: BLE001 — 单源失败不拖垮整轮
            last = exc
    raise last


def parse_items(xml):
    """同时兼容 RSS(<item>) 与 Atom(<entry>)。返回 (标题, 链接, 摘要, 发布时间) """
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
        title = re.sub(r"<[^>]+>", " ", t.group(1)).strip()
        desc = re.sub(r"<[^>]+>", " ", d.group(1)).strip() if d else ""
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


def feed_url(base, path, filt):
    return "%s%s?filter=%s&opencc=t2s&limit=50" % (base, path, urllib.parse.quote(filt))


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
    base = (os.environ.get("RSSHUB_BASE") or "").strip().rstrip("/") or "https://rsshub.app"
    log("RSSHub 实例：%s" % base)
    log("过滤模式：%s" % ("宽松（标题或摘要命中）" if loose else "严格（标题必须命中执法词）"))
    log("北京时间：%s" % datetime.now(HK).strftime("%Y-%m-%d %H:%M"))

    if os.environ.get("SELFTEST") == "1":
        log("SELFTEST=1：只自检推送通道，不抓取。")
        return 1 if push_all("海关RSS自检（%s）" % datetime.now(HK).strftime("%m-%d %H:%M"),
                             "收到这条说明推送通道已配好；接下来每天 07:10 / 15:10 自动跑。") else 0

    seen = {}
    if STATE.exists():
        try:
            seen = json.loads(STATE.read_text(encoding="utf-8"))
        except Exception as exc:            # noqa: BLE001
            log("台账解析失败，按空台账处理：%s" % exc)

    today = datetime.now(HK).strftime("%Y-%m-%d")
    now = datetime.now(timezone.utc)
    hits, stats = [], []
    for name, region, path, filt, max_age in FEEDS:
        url = feed_url(base, path, filt)
        try:
            items = parse_items(http_get(url))
            got = len(items)
            n_new = n_old = 0
            for title, link, desc, pub in items:
                # 时效：发布时间过老的丢弃（抓不到时间的不丢，交给人判断）
                if pub is not None and (now - pub).days > max_age:
                    n_old += 1
                    continue
                if link in seen:
                    continue
                # 精确度靠标题：执法词必须出现在标题里（摘要只用来判"是否涉华"）
                # LOOSE=1 放宽为「标题或摘要命中」——召回更高、误报更多
                if not (ENFORCE.search(title) or (loose and ENFORCE.search(title + " " + desc))):
                    continue
                blob = title + " " + desc
                seen[link] = today
                n_new += 1
                hits.append((name, region, title, link, bool(CN_HINT.search(blob))))
            stats.append((name, "OK", got, n_new, n_old))
        except Exception as exc:            # noqa: BLE001
            stats.append((name, "FAIL", 0, 0, 0))
            log("[%s] 抓取失败：%s" % (name, str(exc)[:160]))

    # 台账瘦身：超过上限时按插入顺序丢弃最早的
    if len(seen) > STATE_MAX:
        seen = dict(list(seen.items())[-STATE_MAX:])
    STATE.write_text(json.dumps(seen, ensure_ascii=False, indent=1), encoding="utf-8")

    log("\n源状态：")
    for name, status, got, n_new, n_old in stats:
        log("  %-14s %-5s 条目=%-4d 新增=%-3d 过期丢弃=%d" % (name, status, got, n_new, n_old))

    log("\n本轮新增 %d 条：" % len(hits))
    for name, region, title, link, cn in hits:
        log("  [%s]%s %s\n      %s" % (region, "★涉华" if cn else "", title, link))

    if not hits:
        log("无新增，未推送。")
        return 0

    if dry:
        log("DRY_RUN=1，跳过推送。")
        return 0

    push = hits[:MAX_ITEMS_PUSH]
    title = "海关查获情报 %d 条（%s）" % (len(hits), datetime.now(HK).strftime("%m-%d %H:%M"))
    body = "\n".join("- [%s]%s %s\n  %s" % (region, "★" if cn else "", t, l) for _n, region, t, l, cn in push)
    if len(hits) > len(push):
        body += "\n…（共 %d 条，日志里看全部）" % len(hits)

    push_all(title, body)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
