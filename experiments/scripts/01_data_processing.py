"""数据清洗流水线脚本"""

import os
import re
import json
import hashlib
import uuid
import pdfplumber
import pandas as pd
from tqdm import tqdm

try:
    from docx import Document  # type: ignore[import-not-found]
except Exception:
    Document = None

# =================配置区域=================
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
RAW_DATA_DIR = os.path.join(BASE_DIR, "data", "raw")
PROCESSED_DATA_DIR = os.path.join(BASE_DIR, "data", "processed")
OUTPUT_FILE = os.path.join(PROCESSED_DATA_DIR, "knowledge.jsonl")

# 切片配置
CHUNK_SIZE = 500       # 目标块大小
MIN_CHUNK_SIZE = 50    # 丢弃太短的块
OVERLAP_SENTENCES = 2  # 重叠句子数量 (语义重叠)

# TODO 物理切割太过草率，后续进行修改
# 去噪配置：页面上下边缘裁切比例 (去除页眉页脚)
TOP_CROP_RATIO = 0.05    # 去除顶部 5%
BOTTOM_CROP_RATIO = 0.08 # 去除底部 8%
# =========================================

def generate_doc_id(file_name):
    """根据文件名生成唯一文档ID (MD5)"""
    return hashlib.md5(file_name.encode('utf-8')).hexdigest()

def clean_text_basic(text):
    """基础清洗：去多余空格，但保留句意"""
    if not text: return ""
    # 1. 替换连续空格
    text = re.sub(r'\s+', ' ', text)
    # 2. 去除控制字符
    text = "".join([c for c in text if c.isprintable()])
    return text.strip()

def split_sentences(text):
    """
    语义分割：按句号、感叹号、问号、换行符切分句子
    保留标点符号在句子末尾
    """
    # 正则逻辑：遇到 。！？ 或者 \n 就切分，并且保留分隔符
    pattern = r'([。！？\n])' 
    parts = re.split(pattern, text)
    sentences = []
    
    # 将分割的内容重新组合：句子 + 标点
    current_sent = ""
    for part in parts:
        current_sent += part
        if re.match(pattern, part): # 如果是标点，结束当前句
            sentences.append(current_sent.strip())
            current_sent = ""
            
    if current_sent: # 处理最后剩余部分
        sentences.append(current_sent.strip())
        
    return [s for s in sentences if s]

def semantic_chunking(text, chunk_size=CHUNK_SIZE):
    """
    语义切片：基于句子聚合，而不是字符硬切
    """
    sentences = split_sentences(text)
    chunks = []
    
    current_chunk = []
    current_len = 0
    
    for i, sent in enumerate(sentences):
        sent_len = len(sent)
        
        # 如果加上这句话超出了限制，先保存当前块
        if current_len + sent_len > chunk_size and current_len > 0:
            # 1. 保存当前块
            full_chunk_text = "".join(current_chunk)
            chunks.append(full_chunk_text)
            
            # 2. 开启新块，并回退重叠 (Overlap)
            # 取最后 OVERLAP_SENTENCES 句话作为新块的开头
            overlap_data = current_chunk[-OVERLAP_SENTENCES:] if len(current_chunk) >= OVERLAP_SENTENCES else current_chunk
            current_chunk = list(overlap_data)
            current_len = sum(len(s) for s in overlap_data)
        
        # 加入当前句
        current_chunk.append(sent)
        current_len += sent_len
        
    # 处理最后一块
    if current_chunk:
        chunks.append("".join(current_chunk))
        
    return chunks

def extract_title(text):
    """
    简单启发式规则提取标题 (用于增强上下文)
    例如： "1.1 安全须知" 或 "第一章 总则"
    """
    # 取文本的前50个字检查
    head = text[:50]
    match = re.search(r'(^第[一二三四五六七八九十]+章|^[\d\.]+\s)', head)
    if match:
        return match.group().strip()
    return ""

def process_pdf(file_path, file_name, doc_id):
    results = []
    current_section_title = "未知章节" # 记录上下文
    
    try:
        with pdfplumber.open(file_path) as pdf:
            for i, page in enumerate(tqdm(pdf.pages, desc=f"解析 {file_name}", leave=False)):
                page_num = i + 1
                
                # --- 1. 去噪：物理裁切 (Crop) ---
                # 获取页面尺寸
                width = page.width
                height = page.height
                
                # 定义保留区域 (去除页眉页脚)
                bbox = (
                    0,                      # x0
                    height * TOP_CROP_RATIO, # top
                    width,                  # x1
                    height * (1 - BOTTOM_CROP_RATIO) # bottom
                )
                
                cropped_page = page.crop(bbox)
                text = cropped_page.extract_text()
                
                if not text: continue
                
                # --- 2. 标题识别 ---
                # 如果这一页开头像是标题，更新当前章节
                possible_title = extract_title(text)
                if possible_title:
                    current_section_title = possible_title
                
                # --- 3. 清洗 ---
                cleaned_text = clean_text_basic(text)
                
                # --- 4. 语义切片 ---
                chunks = semantic_chunking(cleaned_text)
                
                for idx, chunk_text in enumerate(chunks):
                    if len(chunk_text) < MIN_CHUNK_SIZE:
                        continue
                        
                    # --- 5. 构建丰富元数据 ---
                    # 生成唯一 chunk_id
                    chunk_id = f"{doc_id}_{page_num}_{idx}"
                    
                    # 提取锚点 (Anchor)：取第一句话，用于前端高亮
                    # 如果找不到标点，就取前30个字
                    first_sent_match = re.match(r'[^。！？]*[。！？]', chunk_text)
                    anchor = first_sent_match.group() if first_sent_match else chunk_text[:30]
                    
                    record = {
                        "doc_id": doc_id,
                        "chunk_id": chunk_id,
                        "title": file_name,
                        "source": file_name,
                        "file_type": "pdf",
                        "page_num": page_num,
                        "section_title": current_section_title, # 🔥 增加上下文
                        "content": chunk_text, # 🔥 清洗后的纯文本 (用于搜索)
                        "anchor_text": anchor, # 🔥 用于前端高亮定位
                        "chunk_len": len(chunk_text)
                    }
                    results.append(record)
                    
    except Exception as e:
        print(f"❌ 解析 PDF 出错 {file_name}: {e}")
        
    return results

def process_excel(file_path, file_name, doc_id):
    # Excel 处理逻辑保持不变，增加 ID 生成即可
    results = []
    try:
        if file_name.endswith('.csv'):
            df = pd.read_csv(file_path)
        else:
            df = pd.read_excel(file_path)
        df = df.fillna('')
        
        for index, row in df.iterrows():
            row_num = index + 1
            # 将所有列合并，加上列名作为上下文
            parts = []
            for col_name, val in row.items():
                if str(val).strip():
                    parts.append(f"{col_name}:{val}")
            
            row_content = " ".join(parts)
            cleaned_content = clean_text_basic(row_content)
            
            if not cleaned_content: continue
            
            chunk_id = f"{doc_id}_row_{row_num}"
            
            results.append({
                "doc_id": doc_id,
                "chunk_id": chunk_id,
                "title": file_name,
                "source": file_name,
                "file_type": "table",
                "page_num": row_num,
                "section_title": "故障日志表",
                "content": cleaned_content,
                "anchor_text": cleaned_content[:30],
                "chunk_len": len(cleaned_content)
            })
    except Exception as e:
        print(f"❌ 解析表格出错 {file_name}: {e}")
    return results


def process_txt(file_path, file_name, doc_id):
    results = []
    try:
        with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
            raw_text = f.read()

        if not raw_text:
            return results

        # 解析下载脚本头部：标题行 + ===== 分隔 + URL: ...
        raw_lines = [ln.rstrip() for ln in raw_text.splitlines()]
        lines = [ln.strip() for ln in raw_lines if ln.strip()]

        title = "未知标题"
        source_url = ""

        if lines:
            # 兼容：第一行就是标题（02_download_details.py 会写真实标题）
            title = lines[0]
            if title.lower().startswith("title:"):
                title = title.split(":", 1)[1].strip() or title

            # 前几行里找 URL
            for ln in lines[:12]:
                if ln.lower().startswith("url:"):
                    source_url = ln.split(":", 1)[1].strip()
                    break

        # 计算正文起始位置：跳过 title、====、URL 行
        body_lines: list[str] = []
        for idx, ln in enumerate(lines):
            if idx == 0:
                continue
            if set(ln) == {"="}:
                continue
            if ln.lower().startswith("url:"):
                continue
            body_lines.append(ln)

        body_text = "\n".join(body_lines) if body_lines else raw_text
        cleaned_text = clean_text_basic(body_text)
        if not cleaned_text:
            return results

        chunks = semantic_chunking(cleaned_text)
        for idx, chunk_text in enumerate(chunks):
            if len(chunk_text) < MIN_CHUNK_SIZE:
                continue

            chunk_id = f"{doc_id}_txt_{idx}"
            first_sent_match = re.match(r'[^。！？]*[。！？]', chunk_text)
            anchor = first_sent_match.group() if first_sent_match else chunk_text[:30]

            results.append(
                {
                    "doc_id": doc_id,
                    "chunk_id": chunk_id,
                    "title": title,
                    "source_url": source_url,
                    "source": file_name,
                    "file_type": "txt",
                    "page_num": idx + 1,
                    "section_title": title[:50] if title else "未知标题",
                    "content": chunk_text,
                    "anchor_text": anchor,
                    "chunk_len": len(chunk_text),
                }
            )

    except Exception as e:
        print(f"❌ 解析 TXT 出错 {file_name}: {e}")

    return results


def process_docx(file_path, file_name, doc_id):
    results = []
    if Document is None:
        print(f"⚠️ 跳过 DOCX（缺少依赖 python-docx）：{file_name}")
        return results

    try:
        doc = Document(file_path)
        text = "\n".join([p.text for p in doc.paragraphs if p.text and p.text.strip()])
        cleaned_text = clean_text_basic(text)
        if not cleaned_text:
            return results

        chunks = semantic_chunking(cleaned_text)
        for idx, chunk_text in enumerate(chunks):
            if len(chunk_text) < MIN_CHUNK_SIZE:
                continue

            chunk_id = f"{doc_id}_docx_{idx}"
            first_sent_match = re.match(r'[^。！？]*[。！？]', chunk_text)
            anchor = first_sent_match.group() if first_sent_match else chunk_text[:30]

            results.append(
                {
                    "doc_id": doc_id,
                    "chunk_id": chunk_id,
                    "title": file_name,
                    "source": file_name,
                    "file_type": "docx",
                    "page_num": idx + 1,
                    "section_title": "Word文档",
                    "content": chunk_text,
                    "anchor_text": anchor,
                    "chunk_len": len(chunk_text),
                }
            )
    except Exception as e:
        print(f"❌ 解析 DOCX 出错 {file_name}: {e}")

    return results

def main():
    if not os.path.exists(PROCESSED_DATA_DIR):
        os.makedirs(PROCESSED_DATA_DIR)
        
    all_data = []
    
    print(f"🚀 开始处理，去噪策略：Top {TOP_CROP_RATIO*100}%, Bottom {BOTTOM_CROP_RATIO*100}%")
    
    for root, _, files in os.walk(RAW_DATA_DIR):
        for file_name in files:
            if file_name.startswith('.'): 
                continue

            file_path = os.path.join(root, file_name)
            if os.path.isdir(file_path):
                continue

            # 用相对路径做 source，避免同名文件冲突
            rel_path = os.path.relpath(file_path, RAW_DATA_DIR).replace("\\", "/")
            doc_id = generate_doc_id(rel_path)
            file_ext = file_name.lower().split('.')[-1]

            if file_ext == 'pdf':
                all_data.extend(process_pdf(file_path, rel_path, doc_id))
            elif file_ext in ['xlsx', 'xls', 'csv']:
                all_data.extend(process_excel(file_path, rel_path, doc_id))
            elif file_ext == 'txt':
                all_data.extend(process_txt(file_path, rel_path, doc_id))
            elif file_ext == 'docx':
                all_data.extend(process_docx(file_path, rel_path, doc_id))
            
    print(f"💾 正在保存 {len(all_data)} 条增强数据...")
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        for record in all_data:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
            
    print(f"✅ 处理完成！结果已保存至: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()