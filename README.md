# knowledge_rag

## 项目结构

```
knowledge_rag/
├── docker-compose.yml       # 基础设施（ES / Qdrant / Kibana）
├── requirements.txt         # 依赖清单
├── README.md                # 项目说明书
│
├── app/                     # [后端部分] 负责 Web 服务和 API
│   ├── main.py              # 启动入口
│   ├── api/                 # 接口定义 (如 /search, /upload)
│   ├── core/                # 全局配置 (数据库地址等)
│   └── services/            # 业务逻辑 (ES连接, Qdrant连接)
│
├── data/                    # [数据仓库] 存放原始材料与处理产物
│   ├── raw/                 # 原始文档 (PDF, Word, Excel日志)
│   ├── processed/           # 清洗后、切片好的 JSON/CSV
│   └── training/            # 微调用问答对 (train.csv, val.csv)
│
├── experiments/             # [AI 实验室] 离线实验与脚本（不直接跑在后端）
│   ├── notebooks/           # Jupyter Notebook 草稿本
│   │   ├── 01_pdf_parsing.ipynb
│   │   ├── 02_embedding_test.ipynb
│   │   └── 03_es_search_test.ipynb
│   └── scripts/             # 可在本地/Colab 运行的完整脚本
│       ├── data_processing.py
│       ├── fine_tune_reranker.py
│       └── offline_indexing.py
│
└── models/                  # [模型仓库] 存放下载/微调后的模型文件
	├── embedding/           # Embedding 模型（例如 bge-m3）
	└── reranker/            # 精排 Cross-Encoder（微调后权重）
```


## 技术选型

- Python 版本：3.10
- Web 框架：FastAPI
- 数据库驱动：
	- Elasticsearch（elasticsearch v8.x）
	- Qdrant（qdrant-client）
- AI/NLP 核心库：
	- Sentence-Transformers
	- PyTorch
- 数据处理：
	- Pandas（Excel 日志处理）
	- Pdfplumber（PDF 手册处理，支持表格）

---

依赖安装：

```bash
pip install -r requirements.txt
```

如未提供 requirements.txt，可参考如下内容：

```
fastapi
elasticsearch>=8.0.0
qdrant-client
sentence-transformers
torch
pandas
pdfplumber
```
