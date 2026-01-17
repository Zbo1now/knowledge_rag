import time
import urllib.parse
import os

try:
    import pandas as pd  # type: ignore[import-not-found]
except ImportError as e:
    raise SystemExit("缺少依赖 pandas；请先执行: pip install -r requirements.txt") from e

try:
    import requests  # type: ignore[import-not-found]
except ImportError as e:
    raise SystemExit("缺少依赖 requests；请先执行: pip install -r requirements.txt") from e

try:
    from bs4 import BeautifulSoup  # type: ignore[import-not-found]
except ImportError as e:
    raise SystemExit("缺少依赖 beautifulsoup4；请先执行: pip install -r requirements.txt") from e

# ================= 配置区域 =================
BASE_DOMAIN = "https://zh-cn.cncmachiningptj.com"
START_PAGE = 9
# 设为 None 表示一直爬到 404（末页）；也可以填数字，比如 20
END_PAGE: int | None = None
SLEEP_SECONDS = 1.5

# 网络与重试
CONNECT_TIMEOUT_SECONDS = 10
READ_TIMEOUT_SECONDS = 25
MAX_RETRIES_PER_PAGE = 3
BACKOFF_BASE_SECONDS = 2.0
MAX_CONSECUTIVE_FAILURES = 5

# 仅筛选：标题包含“压铸”或“铸造”
KEYWORDS = [
    "压铸",
    "铸造",
]

# 输出 Excel（会保留所有抓到的文章，并标记是否命中关键词）
OUTPUT_FILE = "foundry_articles.xlsx"
# ===========================================


def normalize_url(href: str) -> str:
    if not href:
        return ""
    return urllib.parse.urljoin(BASE_DOMAIN, href)


headers = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}


def fetch_html(session: "requests.Session", url: str) -> str | None:
    """抓取单页 HTML；遇到超时/短暂网络问题会重试。"""
    last_err: Exception | None = None
    for attempt in range(1, MAX_RETRIES_PER_PAGE + 1):
        try:
            resp = session.get(
                url,
                headers=headers,
                timeout=(CONNECT_TIMEOUT_SECONDS, READ_TIMEOUT_SECONDS),
            )

            # 404 由调用方判断是否结束
            if resp.status_code == 404:
                return "__HTTP_404__"

            if resp.status_code != 200:
                raise requests.HTTPError(f"HTTP {resp.status_code}")

            resp.encoding = "utf-8"
            return resp.text
        except (requests.Timeout, requests.ConnectionError, requests.HTTPError) as e:
            last_err = e
            if attempt < MAX_RETRIES_PER_PAGE:
                sleep_s = BACKOFF_BASE_SECONDS ** (attempt - 1)
                print(f"请求异常（第 {attempt}/{MAX_RETRIES_PER_PAGE} 次）：{e}；{sleep_s:.1f}s 后重试")
                time.sleep(sleep_s)
            else:
                break
        except Exception as e:
            # 其它未知异常不盲目重试，直接抛给调用方处理
            raise e

    print(f"本页重试仍失败：{last_err}")
    return None

page_index = START_PAGE
rows: list[dict] = []
seen_urls: set[str] = set()
consecutive_failures = 0

session = requests.Session()

while True:
    if END_PAGE is not None and page_index > END_PAGE:
        print(f"已爬取到设定结束页 Blog-{END_PAGE}，停止爬取。")
        break
    url = f"{BASE_DOMAIN}/Blog-{page_index}"
    print(f"正在抓取: {url}")

    try:
        html = fetch_html(session, url)
        if html == "__HTTP_404__":
            print("到达最后一页，停止爬取。")
            break
        if html is None:
            consecutive_failures += 1
            print(f"连续失败次数: {consecutive_failures}/{MAX_CONSECUTIVE_FAILURES}")
            if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                print("连续失败次数过多，停止爬取。")
                break
            page_index += 1
            time.sleep(SLEEP_SECONDS)
            continue

        consecutive_failures = 0
        soup = BeautifulSoup(html, "html.parser")

        # 根据测试脚本验证过的结构：div.media-body 内有 h4.media-heading a，简介在 p.des
        article_items = soup.select("div.media-body")
        if not article_items:
            print("本页未找到文章块（div.media-body），停止爬取。")
            break

        kws_lower = [k.lower() for k in KEYWORDS]
        page_rows = 0

        for item in article_items:
            title_tag = item.select_one("h4.media-heading a")
            desc_tag = item.select_one("p.des")

            if not title_tag:
                continue

            title = title_tag.get_text(strip=True)
            link = normalize_url(title_tag.get("href", ""))
            desc = desc_tag.get_text(strip=True) if desc_tag else ""

            if not link or link in seen_urls:
                continue

            title_lower = title.lower()
            is_relevant = any(k in title_lower for k in kws_lower)
            if not is_relevant:
                continue

            rows.append(
                {
                    "标题": title,
                    "链接": link,
                    "简介": desc,
                    "页码": page_index,
                }
            )
            seen_urls.add(link)
            page_rows += 1

        print(f"本页提取 {page_rows} 条（累计 {len(rows)} 条）")

        page_index += 1
        time.sleep(SLEEP_SECONDS)

    except Exception as e:
        print(f"出错: {e}")
        break


if not rows:
    print("\n⚠️ 未抓取到任何数据。")
else:
    df = pd.DataFrame(rows)

    # 合并历史数据，避免覆盖
    if os.path.exists(OUTPUT_FILE):
        try:
            old_df = pd.read_excel(OUTPUT_FILE)
            df = pd.concat([old_df, df], ignore_index=True)
            if "链接" in df.columns:
                df = df.drop_duplicates(subset=["链接"], keep="first")
            print(f"检测到已有 {OUTPUT_FILE}，已合并并按链接去重。")
        except Exception as e:
            ts = time.strftime("%Y%m%d_%H%M%S")
            fallback = f"foundry_articles_{ts}.xlsx"
            print(f"读取旧 Excel 失败（{e}），将写入新文件: {fallback}")
            OUTPUT_FILE = fallback

    df.to_excel(OUTPUT_FILE, index=False)

    print(f"\n✅ 共找到 {len(df)} 条标题包含‘压铸/铸造’的文章")
    print(f"💾 已保存: {OUTPUT_FILE}\n")

    for _, r in df.head(50).iterrows():
        print(f"📌 {r['标题']}\n   🔗 {r['链接']}\n")