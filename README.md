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
- Windows 窗口程序中的集中设置页（GraphDB + LLM 配置统一入口）
- Windows 窗口程序中的问题诊断页（输入症状 -> 图谱问题 -> 解决方案）
- Windows 窗口程序中的自然语言问答推荐页（输入问题 -> GraphDB 推荐）

## 主要文件

- `crawl_refractory_ttl.py`：一键扩词 -> 导出 TTL -> 清洗时间字面量
- `expand_refractory_domain_knowledge.py`：从本体/KB 自动抽取领域词并扩充 Wikipedia 标题列表
- `wikipedia_to_triples.py`：Wikipedia 标题 -> Wikidata 实体 -> TTL
- `sanitize_ttl_times.py`：清洗非法 `xsd:dateTime`（如 `2020-00-00`）
- `run_decision_chain.py`：本地执行决策链并输出 `recommendation.ttl`
- `recommend_construct.sparql`：推荐规则查询模板（CONSTRUCT）
- `import_to_graphdb.ps1`：一键导入 TTL 到 GraphDB 命名图
- `refractory_selector_gui.py`：窗口程序（集中设置 + 工况输入 + 问题诊断 + 推荐展示）

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

如果使用窗口程序，可以通过环境变量预填集中设置页的默认值：

- `GRAPHDB_BASE_URL`
- `GRAPHDB_REPOSITORY_ID`
- `MODEL_API_KEY`
- `MODEL_BASE_URL`
- `MODEL_NAME`

## 数据构建流程

0. 先扩充领域知识（可选，单独运行）

```powershell
python expand_refractory_domain_knowledge.py --seeds-file input_pages.txt --out expanded_domain_pages.txt --report-out expanded_domain_knowledge.json
```

1. 一键抓取并导出 TTL（推荐）

```powershell
python crawl_refractory_ttl.py --lang zh --seeds-file input_pages.txt --expanded-out expanded_pages.txt --ttl-out out.ttl --sanitized-out out.sanitized.ttl
```

2. 扩充词汇（可选，单独运行）

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

如果你想先从“症状”倒推问题，再看方案，可以切换到“问题诊断”页：

- 输入异常现象，例如“渣线掉块严重，裂纹很多，烘炉后继续开裂”
- 系统会结合当前工况和图谱中的失效机理，给出问题判断、根因和方案
- 方案包含推荐材质体系、牌号/配方范围、施工工艺、风险点和约束条件
知识图谱在耐火材料里，最适合做的不是“直接替代专家”，而是把“工况、问题、原因、方案、证据”串起来，形成可追溯的决策链。

可以按这个思路做：

先把输入工况标准化
把炉型、温度、气氛、渣系、热震、磨损、金属液接触这些条件统一成结构化字段。

再从图谱里识别“问题”
图谱不只存材料，还存失效机理和约束规则。比如：

温度波动大、频繁启停 -> 热震剥落风险
高碱度渣、渣线部位 -> 渣侵蚀和渗透风险
氧化气氛、含碳材料 -> 氧化烧损风险
磨损强、冲刷大 -> 机械冲蚀风险
然后给出“方案”
图谱把问题映射到材料体系、配方、工艺和风险控制上，例如：
热震剥落 -> 选抗热震更好的体系，如刚玉-尖晶石或镁质-尖晶石，并优化升温曲线和结构设计
渣侵蚀 -> 选抗渣侵蚀材料，如镁质或镁碳体系，控制孔隙率和渣碱度
氧化烧损 -> 降低碳损伤风险，增加抗氧化措施，必要时换成更适合氧化气氛的体系
施工缺陷 -> 通过烘烤制度、砌筑方式、伸缩缝设计来降低开裂概率
最后输出“问题 + 方案 + 证据”
一个完整的图谱答案最好长这样：
问题：热震剥落、渣侵蚀、氧化烧损
原因：高温波动、渣碱度高、气氛氧化
方案：推荐材质体系、配方范围、施工工艺、替代方案
风险：哪些条件下会失效
证据：规则、案例、标准、文献来源

如果你更习惯自然语言提问，可以切换到“问答推荐”页：

- 直接输入问题，例如“电弧炉渣线在氧化气氛下怎么选？”
- 程序先调用大模型解析工况语义，再复用 GraphDB 规则链输出推荐
- 页面采用聊天式布局，左侧提供快捷操作，右侧显示对话和推荐结果；模型配置统一在“设置”页维护
- 系统会先判断问题是否属于耐火材料领域，不相关的输入会直接拒答
- 如果 GraphDB 暂时没有对应结果，页面会明确提示“知识图谱中暂未查到对应方案”

新增“数据抓取”页可直接一键生成三份文件：

- `expanded_pages.txt`：从 Wikipedia 扩展后的标题列表
- `out.ttl`：Wikidata 抽取后的原始 TTL
- `out.sanitized.ttl`：清洗后的可导入 TTL

页面内可调整种子文件、输出路径、扩词深度、每个关键词上限、最大标题数、抓取睡眠与 User-Agent。
一键抓取流程会先用本体/KB 里的领域词扩充标题列表，再继续生成 TTL。

## 集中设置

在“设置”页中统一配置：

- GraphDB Base URL
- Repository ID
- LLM API Key
- LLM Base URL
- LLM Model Name

推荐页、诊断页和问答页都会直接读取这里的值。

## SPARQL 模板

可参考 `BACKEND_SPARQL_QUERIES.md`，内含：

- 命名图计数
- 条件对应推荐摘要
- 层级细节
- 风险与工艺信息

## 说明

- 若 `out.ttl` 含非法时间格式，请优先使用 `out.sanitized.ttl`。
- 若仅验证规则链路，可在本地运行时使用 `--skip-base` 跳过基础图谱加载。