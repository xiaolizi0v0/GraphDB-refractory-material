# Windows窗口程序使用说明（GraphDB查询版）

## 1. 先更新GraphDB里的本体/KB

因为你刚扩充了本体和KB，请至少重新导入这两个文件到同名命名图：

- [refractory_ontology.ttl](refractory_ontology.ttl) -> `http://example.com/graph/ontology`
- [refractory_kb.ttl](refractory_kb.ttl) -> `http://example.com/graph/kb`

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

## 5. 连接参数

默认参数：

- URL: `http://localhost:7200`
- Repository: `RefMDB`

可在界面顶部修改，并用“测试连接”按钮验证。
