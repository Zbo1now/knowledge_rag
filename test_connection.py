from elasticsearch import Elasticsearch
from qdrant_client import QdrantClient
from loguru import logger
import sys

# 1. 连接 Elasticsearch
try:
    es = Elasticsearch("http://localhost:9200")
    es_info = es.info()
    logger.success(f"✅ Elasticsearch 连接成功! 版本: {es_info['version']['number']}")
except Exception as e:
    logger.error(f"❌ ES 连接失败: {e}")

# 2. 连接 Qdrant
try:
    # 注意：Qdrant 客户端连接的是 6333 端口
    qdrant = QdrantClient(url="http://localhost:6333")
    collections = qdrant.get_collections()
    logger.success(f"✅ Qdrant 连接成功! 当前集合列表: {collections}")
except Exception as e:
    logger.error(f"❌ Qdrant 连接失败: {e}")

# 3. 测试 PyTorch/Sentence-Transformers (验证 AI 库)
try:
    import torch
    from sentence_transformers import SentenceTransformer
    logger.info("正在测试 AI 环境 (首次运行可能需要下载小模型)...")
    # 加载一个极小的模型测试一下环境
    model = SentenceTransformer('all-MiniLM-L6-v2') 
    vec = model.encode("Hello World")
    logger.success(f"✅ 模型加载成功! 向量维度: {len(vec)}")
except Exception as e:
    logger.error(f"❌ AI 环境异常: {e}")