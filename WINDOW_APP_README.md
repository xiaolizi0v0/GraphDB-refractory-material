# Windows窗口程序使用说明（GraphDB查询版）

## 1. 先更新GraphDB里的本体/KB

因为你刚扩充了本体和KB，请至少重新导入这两个文件到同名命名图：

- [refractory_ontology.ttl](refractory_ontology.ttl) -> `http://example.com/graph/ontology`
- [refractory_kb.ttl](refractory_kb.ttl) -> `http://example.com/graph/kb`

## 1.5. 先生成或更新 TTL 数据

如果你想先补充图谱数据，可以直接在 GUI 里切到“数据抓取”页，或者在命令行运行：

```powershell
python crawl_refractory_ttl.py --lang zh --seeds-file input_pages.txt --expanded-out expanded_pages.txt --ttl-out out.ttl --sanitized-out out.sanitized.ttl
```

默认会依次生成：

- `expanded_pages.txt`
- `out.ttl`
- `out.sanitized.ttl`

如果你想先扩充领域词库，再生成更丰富的标题列表，可以先运行：

```powershell
python expand_refractory_domain_knowledge.py --seeds-file input_pages.txt --out expanded_domain_pages.txt --report-out expanded_domain_knowledge.json
```

然后再把生成的标题列表继续送入抓取流程。

## 2. 启动窗口程序

在项目目录执行：

```powershell
python refractory_selector_gui.py
```

## 3. 在界面中填写工况

必填输入包含：

- 炉型（高炉/电弧炉/玻璃窑/水泥窑等）
- 温度曲线文本 + 最高温度
- 气氛（氧化/还原/CO富）
- 渣系成分（CaO/SiO2/Al2O3/MgO）
- 热震频率、冲刷磨损、是否接触金属液

程序会自动计算渣碱度 $CaO/SiO2$ 并参与KG规则查询。

## 4. 点击“查询GraphDB并生成推荐”

程序会通过 SPARQL 直接查询 GraphDB（仓库默认 `RefMDB`），并显示：

- 推荐材料组合（工作层/隔热层/背衬）
- 工况 -> 失效机理 -> 材质体系 -> 牌号/配方范围 -> 施工工艺 -> 风险点
- 关键指标阈值
- 替代方案
- 风险与约束解释

## 4.5. 问题诊断页

如果你先看到的是“异常现象”而不是完整工况，可以切换到“问题诊断”页：

- 输入症状文本，例如“渣线掉块严重，裂纹很多，烘炉后继续开裂”
- 程序会结合当前工况和图谱里的失效机理，识别最可能的问题
- 同时给出对应的材质体系、配方范围、施工工艺、风险点和约束条件
- 页面会先做耐火材料语义判别，不相关的输入会直接拒答
- 如果图谱暂时没有对应结果，页面会明确提示“知识图谱中暂未查到对应方案”

## 5. 连接参数

所有连接与模型配置都集中在“设置”页：

- GraphDB URL
- Repository ID
- LLM API Key
- LLM Base URL
- LLM Model Name

修改后会立即生效，并可在设置页分别测试 GraphDB 和 LLM 连接。

## 6. 数据抓取页

新增加的“数据抓取”页用于一键执行：

1. 从 Wikipedia 扩展耐火材料相关标题
2. 将标题映射到 Wikidata 实体并导出 TTL
3. 清洗非法 `xsd:dateTime` 字面量，生成可导入文件

常用输出默认值：

- 种子文件：`input_pages.txt`
- 扩词输出：`expanded_pages.txt`
- TTL 输出：`out.ttl`
- 清洗后 TTL：`out.sanitized.ttl`
