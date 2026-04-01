<p align="center">
  <img src="https://api.iconify.design/material-symbols/account-tree-outline-rounded.svg?color=%230e7c86" alt="logo" style="width: 80px; vertical-align: middle; ">
</p>
<h1 align="center">GraphDB Refractory Knowledge Graph</h1>
<p align="center">
  <img src="https://img.shields.io/github/languages/code-size/xiaolizi0v0/GraphDB" alt="code size"/>
  <img src="https://img.shields.io/badge/GraphDB-10.x-brightgreen" alt="GraphDB"/>
  <img src="https://img.shields.io/github/languages/count/xiaolizi0v0/GraphDB" alt="languages"/>
  <img src="https://img.shields.io/badge/Python-3.10%2B-blue" alt="Python"/>
  <img src="https://img.shields.io/github/last-commit/xiaolizi0v0/GraphDB" alt="last commit"/><br>
  <img src="https://img.shields.io/badge/Created-26.04.01-blue" alt="Created Time"/>
  <img src="https://img.shields.io/badge/Author-xiaolizi0v0-orange" alt="Author"/>
</p>
<hr>

## 项目简介

本项目用于构建耐火材料知识图谱，并基于 GraphDB + SPARQL 输出工况到推荐结果的决策链，包含：

- Wikipedia/Wikidata 数据抽取到 RDF 三元组
- 本体与知识库规则（ontology + kb）
- 本地 SPARQL CONSTRUCT 推理验证
- GraphDB 命名图导入脚本
- Windows 窗口程序（输入工况并直接查询 GraphDB）

## 主要文件

- `wikipedia_to_triples.py`：Wikipedia 标题 -> Wikidata 实体 -> TTL
- `sanitize_ttl_times.py`：清洗非法 `xsd:dateTime`（如 `2020-00-00`）
- `run_decision_chain.py`：本地执行决策链并输出 `recommendation.ttl`
- `recommend_construct.sparql`：推荐规则查询模板（CONSTRUCT）
- `import_to_graphdb.ps1`：一键导入 TTL 到 GraphDB 命名图
- `refractory_selector_gui.py`：窗口程序（工况输入 + 推荐展示）

## 环境依赖

```powershell
pip install -r requirements.txt
```

`requirements.txt` 包含：

- requests
- rdflib
- PySide6
- PyQt5
- PyQt-Fluent-Widgets

## 数据构建流程

1. 扩充词汇（可选）

```powershell
python expand_refractory_vocab.py --lang zh --seeds-file input_pages.txt --out expanded_pages.txt
```

2. 生成基础三元组

```powershell
python wikipedia_to_triples.py --lang zh --titles-file expanded_pages.txt --out out.ttl
```

3. 清洗时间字面量（建议）

```powershell
python sanitize_ttl_times.py --in out.ttl --out out.sanitized.ttl
```

4. 本地验证推荐决策链

```powershell
python run_decision_chain.py --out-ttl recommendation.ttl --workcondition example_workcondition.json
```

## 导入到 GraphDB

建议按以下命名图导入：

- `refractory_ontology.ttl` -> `http://example.com/graph/ontology`
- `refractory_kb.ttl` -> `http://example.com/graph/kb`
- `out.sanitized.ttl`（或 `out.ttl`） -> `http://example.com/graph/wikidata`
- `recommendation.ttl` -> `http://example.com/graph/recommendation`

PowerShell 导入命令：

```powershell
./import_to_graphdb.ps1 -GraphDbBaseUrl "http://localhost:7200" -RepositoryId "refractory" -ClearTargetGraphs
```

## 窗口程序使用

```powershell
python refractory_selector_gui.py
```

默认连接参数：

- URL: `http://localhost:7200`
- Repository: `RefMDB`

在界面中输入炉型、温度、气氛、渣系、热震与磨损等参数后，点击“查询GraphDB并生成推荐”即可得到：

- 推荐材料组合（工作层/隔热层/背衬）
- 失效机理与材质体系
- 牌号/配方范围、施工工艺与风险约束

## SPARQL 模板

可参考 `BACKEND_SPARQL_QUERIES.md`，内含：

- 命名图计数
- 条件对应推荐摘要
- 层级细节
- 风险与工艺信息

## 说明

- 若 `out.ttl` 含非法时间格式，请优先使用 `out.sanitized.ttl`。
- 若仅验证规则链路，可在本地运行时使用 `--skip-base` 跳过基础图谱加载。