import os
import time
import hashlib
import argparse
import json

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

from tqdm import tqdm
import re

# ================= 配置区域 =================
# 1. Excel 文件路径
EXCEL_PATH = "foundry_articles.xlsx" 

# 2. 保存位置
SAVE_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "data", "raw", "crawled_articles")

# 3. 🔥 根据截图修改：正文选择器
CONTENT_SELECTOR = "div.met-editor" 

# 网络与重试
CONNECT_TIMEOUT_SECONDS = 10
READ_TIMEOUT_SECONDS = 25
MAX_RETRIES_PER_ARTICLE = 3
BACKOFF_BASE_SECONDS = 2.0

# 文件名策略：标题 + url hash，避免同名覆盖
USE_URL_HASH_SUFFIX = True
# ===========================================

def clean_filename(title):
    name = re.sub(r'[\\/*?:"<>|]', "", str(title)).strip()
    return name if name else "untitled"


def extract_article_title(soup: "BeautifulSoup") -> str:
    # 尽量从正文页提取“真实标题”，失败再回退到 <title>
    candidates = [
        soup.select_one("h1"),
        soup.select_one("h1 a"),
        soup.select_one("h2"),
    ]
    for node in candidates:
        if node and node.get_text(strip=True):
            return node.get_text(strip=True)

    og = soup.select_one('meta[property="og:title"]')
    if og and og.get("content"):
        return str(og.get("content")).strip()

    if soup.title and soup.title.get_text(strip=True):
        title = soup.title.get_text(strip=True)
        # 常见的站点后缀清理（尽量保守）
        for sep in [" | ", " - "]:
            if sep in title:
                title = title.split(sep)[0].strip()
                break
        return title

    return ""


def table_to_markdown(table_tag: "BeautifulSoup") -> str:
    """把 HTML <table> 近似转换成 Markdown 表格，尽量保留结构。"""
    rows = []
    for tr in table_tag.select("tr"):
        cells = tr.find_all(["th", "td"])
        row = [c.get_text(" ", strip=True) for c in cells]
        if row and any(x for x in row):
            rows.append(row)

    if not rows:
        return ""

    # 补齐列数
    max_cols = max(len(r) for r in rows)
    rows = [r + [""] * (max_cols - len(r)) for r in rows]

    header = rows[0]
    sep = ["---"] * max_cols
    body = rows[1:]

    def fmt(r):
        return "| " + " | ".join((x or "").replace("\n", " ") for x in r) + " |"

    out = [fmt(header), fmt(sep)]
    out.extend(fmt(r) for r in body)
    return "\n".join(out)

def fetch_content(session: "requests.Session", url: str) -> tuple[str, str] | None:
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36"
    }

    last_err: Exception | None = None
    for attempt in range(1, MAX_RETRIES_PER_ARTICLE + 1):
        try:
            resp = session.get(
                url,
                headers=headers,
                timeout=(CONNECT_TIMEOUT_SECONDS, READ_TIMEOUT_SECONDS),
            )
            if resp.status_code != 200:
                return None

            resp.encoding = "utf-8"
            soup = BeautifulSoup(resp.text, "html.parser")

            real_title = extract_article_title(soup)
        
            # 🔥 核心逻辑：提取 met-editor 下的所有内容
            content_div = soup.select_one(CONTENT_SELECTOR)
        
            if content_div:
                # 先将正文区域内所有 table 转换为 Markdown（避免被 get_text 打平）
                content_clone = BeautifulSoup(str(content_div), "html.parser")
                root_tag = content_clone.find(True)
                if root_tag is not None:
                    for t in list(root_tag.select("table")):
                        md = table_to_markdown(t)
                        if md:
                            t.replace_with(content_clone.new_string("\n" + md + "\n"))
                        else:
                            t.decompose()
                    content_div = root_tag

                # 💡 优化：不直接 get_text，而是手动遍历，保留标题的层级感
                lines = []
                for child in content_div.children:
                    if getattr(child, "name", None) is None:
                        text = str(child).strip()
                        if text:
                            lines.append(text)
                    elif child.name in ['h1', 'h2', 'h3']:
                        # 给小标题加个标记，清洗时一看就知道这是重点
                        lines.append(f"\n### {child.get_text(strip=True)}\n")
                    elif child.name == 'p':
                        text = child.get_text(strip=True)
                        if text: # 跳过空段落
                            lines.append(text)
                    elif child.name == 'table':
                        md = table_to_markdown(child)
                        if md:
                            lines.append("\n" + md + "\n")
                    else:
                        # 其它标签兜底抽取（table 已被替换为 Markdown 文本）
                        text = child.get_text(separator="\n", strip=True)
                        if text:
                            lines.append(text)
            
                # 如果上面那种精细提取没拿到东西（防止网页结构微调），就兜底用 get_text
                if not lines:
                    return real_title, content_div.get_text(separator="\n", strip=True)
                
                return real_title, "\n".join(lines)
            else:
                return real_title, ""

        except (requests.Timeout, requests.ConnectionError) as e:
            last_err = e
            if attempt < MAX_RETRIES_PER_ARTICLE:
                time.sleep(BACKOFF_BASE_SECONDS ** (attempt - 1))
                continue
            break
        except Exception as e:
            print(f"抓取失败 {url}: {e}")
            return None

    if last_err is not None:
        print(f"抓取失败 {url}: {last_err}")
    return None

def main():
    parser = argparse.ArgumentParser(description="下载文章正文到本地 txt（支持单链接测试与批量模式）")
    parser.add_argument("--url", type=str, default="", help="单链接测试：传入文章详情页 URL")
    parser.add_argument("--out", type=str, default="", help="单链接测试：可选输出文件名（默认使用 标题+hash）")
    parser.add_argument("--no-save", action="store_true", help="单链接测试：只打印预览，不写入文件")
    parser.add_argument("--preview", type=int, default=400, help="单链接测试：正文预览字符数")
    parser.add_argument("--json", action="store_true", help="单链接测试：以 JSON 输出（title/url/content）")
    parser.add_argument("--overwrite", action="store_true", help="批量模式：覆盖已存在的 txt（用于重新下载以更新表格/内容）")
    args = parser.parse_args()

    if not os.path.exists(SAVE_DIR):
        os.makedirs(SAVE_DIR)

    session = requests.Session()

    # ---- 单链接测试模式 ----
    if args.url and str(args.url).startswith("http"):
        link = str(args.url).strip()
        result = fetch_content(session, link)
        if result is None:
            print("❌ 单链接抓取失败。")
            return

        real_title, content = result
        final_title = real_title.strip() if real_title and real_title.strip() else "untitled"

        if args.json:
            payload = {
                "title": final_title,
                "url": link,
                "content": content or "",
            }
            print(json.dumps(payload, ensure_ascii=False))
            return

        print(f"✅ 标题: {final_title}")
        print(f"✅ URL: {link}")
        print("\n--- 正文预览 ---")
        preview_n = max(0, int(args.preview))
        print((content or "")[:preview_n])
        print("\n--- 预览结束 ---\n")

        if args.no_save:
            return

        if args.out:
            file_name = args.out
            if not file_name.lower().endswith(".txt"):
                file_name += ".txt"
        else:
            safe_title = clean_filename(final_title)
            url_hash = hashlib.md5(link.encode("utf-8")).hexdigest()[:8]
            file_name = f"{safe_title}_{url_hash}.txt"

        save_path = os.path.join(SAVE_DIR, file_name)
        with open(save_path, "w", encoding="utf-8") as f:
            f.write(f"{final_title}\n{'='*20}\n\n")
            f.write(f"URL: {link}\n\n")
            f.write(content or "")
        print(f"💾 已保存: {save_path}")
        return
        
    print(f"📂 正在读取 {EXCEL_PATH}...")
    try:
        df = pd.read_excel(EXCEL_PATH)
    except:
        df = pd.read_excel(os.path.join(os.getcwd(), EXCEL_PATH))

    print(f"发现 {len(df)} 篇文章，开始批量下载...")
    
    success_count = 0
    
    # 遍历下载
    for index, row in tqdm(df.iterrows(), total=len(df), desc="下载进度"):
        title = row['标题']
        link = row['链接']
        
        if not str(link).startswith("http"): continue
            
        safe_title = clean_filename(title)
        if USE_URL_HASH_SUFFIX:
            url_hash = hashlib.md5(str(link).encode("utf-8")).hexdigest()[:8]
            file_name = f"{safe_title}_{url_hash}.txt"
        else:
            file_name = f"{safe_title}.txt"

        save_path = os.path.join(SAVE_DIR, file_name)
        
        # 断点续传
        if os.path.exists(save_path) and not args.overwrite:
            continue
            
        result = fetch_content(session, link)
        if result is None:
            continue

        real_title, content = result
        final_title = real_title.strip() if real_title and real_title.strip() else str(title).strip()
        
        if content and len(content) > 20:
            with open(save_path, "w", encoding="utf-8") as f:
                f.write(f"{final_title}\n{'='*20}\n\n") # 使用正文页真实标题
                f.write(f"URL: {link}\n\n")
                f.write(content)
            success_count += 1
        
        time.sleep(0.3) # 稍微快一点，0.3秒一篇
        
    print(f"\n✅ 全部完成！")
    print(f"共保存 {success_count} 篇文档到: {SAVE_DIR}")

if __name__ == "__main__":
    main()