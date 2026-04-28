from __future__ import annotations

import sys
import os
import json
import importlib
import re
from dataclasses import dataclass
from functools import partial
from pathlib import Path
from typing import Dict, List, Optional

import requests
from qfluentwidgets import (
  FluentIcon as FIF,
  FluentWindow,
  PrimaryPushButton,
  PushButton,
  Theme,
  setTheme,
)
from requests.exceptions import HTTPError


def _detect_qt_binding() -> str:
  for cls in FluentWindow.__mro__:
    module = getattr(cls, "__module__", "")
    if module.endswith(".QtWidgets"):
      return module.split(".", 1)[0]
  return "PyQt5"


_QT_BINDING = _detect_qt_binding()
QtCore = importlib.import_module(f"{_QT_BINDING}.QtCore")
QtGui = importlib.import_module(f"{_QT_BINDING}.QtGui")
QtWidgets = importlib.import_module(f"{_QT_BINDING}.QtWidgets")

Qt = QtCore.Qt
Signal = getattr(QtCore, "Signal", getattr(QtCore, "pyqtSignal", None))
QAction = getattr(QtGui, "QAction", QtWidgets.QAction)
QAbstractItemView = QtWidgets.QAbstractItemView
QApplication = QtWidgets.QApplication
QCheckBox = QtWidgets.QCheckBox
QComboBox = QtWidgets.QComboBox
QDialog = QtWidgets.QDialog
QFileDialog = QtWidgets.QFileDialog
QFormLayout = QtWidgets.QFormLayout
QGridLayout = QtWidgets.QGridLayout
QHBoxLayout = QtWidgets.QHBoxLayout
QHeaderView = QtWidgets.QHeaderView
QFrame = QtWidgets.QFrame
QLabel = QtWidgets.QLabel
QLineEdit = QtWidgets.QLineEdit
QListWidget = QtWidgets.QListWidget
QMessageBox = QtWidgets.QMessageBox
QPlainTextEdit = QtWidgets.QPlainTextEdit
QScrollArea = QtWidgets.QScrollArea
QProcess = QtCore.QProcess
QSplitter = QtWidgets.QSplitter
QTableWidget = QtWidgets.QTableWidget
QTableWidgetItem = QtWidgets.QTableWidgetItem
QVBoxLayout = QtWidgets.QVBoxLayout
QWidget = QtWidgets.QWidget


EX_NS = "http://example.com/refractory#"


FURNACE_OPTIONS = {
    "高炉": "BlastFurnace",
    "电弧炉": "EAF",
    "玻璃窑": "GlassKiln",
    "水泥窑": "CementKiln",
    "转炉": "BOF",
    "钢包精炼炉": "LF",
    "加热炉": "ReheatingFurnace",
}

ATMOSPHERE_OPTIONS = {
    "氧化": "Oxidizing",
    "还原": "Reducing",
    "CO富": "CORich",
}

ZONE_OPTIONS = {
    "渣线": "SlagLine",
    "冲击区": "ImpactZone",
    "热面": "HotFace",
    "炉顶": "Roof",
    "出钢口/出铁口": "TapHole",
}

SLAG_TYPE_OPTIONS = {
    "碱性渣": "BasicSlag",
    "酸性渣": "AcidicSlag",
    "中性渣": "NeutralSlag",
}

OPERATION_MODE_OPTIONS = {
    "间歇式": "BatchMode",
    "连续式": "ContinuousMode",
}

ABRASION_OPTIONS = ["Low", "Medium", "High"]

ROLE_LABEL_MAP = {
    "WorkingLining": "工作层",
    "InsulatingLining": "隔热层",
    "BackupLining": "背衬",
}

MS_OPTIONS = {
  "镁碳(MgO-C)": "MS_MgOC",
  "刚玉-尖晶石(Al2O3-Spinel)": "MS_Al2O3Spinel",
  "镁质-尖晶石(MgO-Spinel)": "MS_MgOSpinel",
  "高铝体系(High-Alumina)": "MS_HighAlumina",
}

DIAGNOSIS_RULES = [
  {
    "fm": "ThermalShockSpalling",
    "label": "热震剥落",
    "keywords": ["热震", "裂纹", "开裂", "掉块", "剥落", "启停", "骤冷", "温差"],
    "cause": "温度循环大、升温/停炉过快、材料热膨胀失配。",
    "solution": "优先刚玉-尖晶石或高铝体系，配合分段升温、保温和伸缩缝设计。",
  },
  {
    "fm": "SlagCorrosion",
    "label": "渣蚀/化学侵蚀",
    "keywords": ["渣蚀", "侵蚀", "冲刷", "渣线", "化学侵蚀", "渣皮", "熔蚀"],
    "cause": "高温渣反应、润湿和渗透导致结构快速损失。",
    "solution": "优先镁碳或镁质-尖晶石，降低孔隙率并控制渣系匹配。",
  },
  {
    "fm": "AlkaliAttack",
    "label": "碱侵蚀/碱挥发结圈",
    "keywords": ["碱", "结圈", "挂料", "白霜", "碱侵蚀", "碱挥发"],
    "cause": "碱金属蒸汽或碱渣与耐火材料反应，形成低熔物和结圈。",
    "solution": "优先镁质-尖晶石或高铝体系，并控制碱源输入和停料波动。",
  },
  {
    "fm": "AbrasionErosion",
    "label": "冲刷/磨损",
    "keywords": ["磨损", "冲刷", "冲蚀", "飞料", "坑槽", "冲击"],
    "cause": "高速料流、熔体冲击或固体颗粒磨蚀导致表层快速减薄。",
    "solution": "优先镁碳或镁质-尖晶石，并优化流场、角度和局部补强。",
  },
  {
    "fm": "OxidationBurnout",
    "label": "氧化烧损(含碳材料)",
    "keywords": ["氧化", "掉碳", "烧损", "发红", "脱碳", "氧化气氛"],
    "cause": "氧化气氛下碳相被消耗，导致强度下降和结构松散。",
    "solution": "优先镁质-尖晶石或刚玉-尖晶石，降低碳暴露并强化控氧措施。",
  },
  {
    "fm": "MetalPenetration",
    "label": "金属/渣渗透",
    "keywords": ["金属液", "渗透", "渗漏", "侵入", "铁水", "钢水"],
    "cause": "金属液或低粘度介质沿孔隙/裂纹侵入，造成内部损伤。",
    "solution": "优先镁碳或镁质-尖晶石，降低开口气孔并强化密实度。",
  },
  {
    "fm": "HydrationCracking",
    "label": "水化开裂",
    "keywords": ["水化", "粉化", "受潮", "爆粉", "裂开", "起鼓"],
    "cause": "储存、施工或烘炉阶段受潮，导致含镁材料发生水化膨胀。",
    "solution": "优先镁质-尖晶石或高铝体系，并加强防潮、预烘与密封存储。",
  },
]

WD_ENTITY_PREFIX = "http://www.wikidata.org/entity/"
DEFAULT_MODEL_BASE_URL = "https://api." + "deep" + "seek.com/v1"
DEFAULT_MODEL_NAME = "deep" + "seek-chat"
CHAT_HISTORY_LIMIT = 8
DOMAIN_INTENT_CONFIDENCE_THRESHOLD = 0.65
DEFAULT_CRAWL_USER_AGENT = "GraphDB-Refractory-CrawlPipeline/0.1 (contact: you@example.com)"


def sparql_num(v: float) -> str:
    return f'"{float(v):.6f}"^^xsd:double'


def parse_sparql_binding(row: Dict[str, Dict[str, str]], key: str, default: str = "") -> str:
    cell = row.get(key)
    if not cell:
        return default
    return cell.get("value", default)


def iri_tail(iri: str) -> str:
    if "#" in iri:
        return iri.rsplit("#", 1)[-1]
    if "/" in iri:
        return iri.rsplit("/", 1)[-1]
    return iri

def pretty_num_str(text: str) -> str:
    try:
        v = float(text)
        if abs(v - round(v)) < 1e-9:
            return str(int(round(v)))
        return f"{v:.3f}".rstrip("0").rstrip(".")
    except Exception:
        return text


def _json_extract_object(text: str) -> Dict[str, object]:
  raw = text.strip()
  try:
    parsed = json.loads(raw)
    if isinstance(parsed, dict):
      return parsed
  except Exception:
    pass

  start = raw.find("{")
  end = raw.rfind("}")
  if start >= 0 and end > start:
    parsed = json.loads(raw[start : end + 1])
    if isinstance(parsed, dict):
      return parsed

  raise ValueError("模型返回内容不是有效的 JSON 对象")


class ChatComposerEdit(QPlainTextEdit):
  sendRequested = Signal()

  def keyPressEvent(self, event) -> None:
    if event.key() in (Qt.Key_Return, Qt.Key_Enter):
      if event.modifiers() & Qt.ShiftModifier:
        super().keyPressEvent(event)
      else:
        self.sendRequested.emit()
      return
    super().keyPressEvent(event)


class ChatBubbleWidget(QFrame):
  def __init__(self, role: str, title: str, content: str, parent: Optional[QWidget] = None) -> None:
    super().__init__(parent)
    self.role = role
    self.setObjectName("assistantBubble" if role == "assistant" else "userBubble")
    self.setAttribute(Qt.WA_StyledBackground, True)
    self.setMaximumWidth(780)
    self.setFrameShape(QFrame.NoFrame)
    if role == "assistant":
      self.setStyleSheet(
        """
        QFrame {
          background-color: #ccebff;
         
          border-radius: 16px;
        }
        QLabel#chatBubbleTitle {
          color: #dbeafe;
          font-size: 12px;
          font-weight: 600;
        }
        QLabel#chatBubbleBody {
          color: #000000;
          font-size: 14px;
          background: transparent;
        }
        """
      )
    else:
      self.setStyleSheet(
        """
        QFrame {
          background-color: #ccebff;
          
          border-radius: 16px;
        }
        QLabel#chatBubbleTitle {
          color: #dbeafe;
          font-size: 12px;
          font-weight: 600;
        }
        QLabel#chatBubbleBody {
          color: #000000;
          font-size: 14px;
          background: transparent;
        }
        """
      )

    layout = QVBoxLayout(self)
    layout.setContentsMargins(20, 16, 20, 16)
    layout.setSpacing(0)

    self.title_label = QLabel(title, self)
    self.title_label.setObjectName("chatBubbleTitle")
    self.title_label.hide()
    self.body_label = QLabel(self)
    self.body_label.setObjectName("chatBubbleBody")
    self.body_label.setWordWrap(True)
    self.body_label.setTextInteractionFlags(Qt.TextSelectableByMouse)

    layout.addWidget(self.title_label)
    layout.addWidget(self.body_label)
    self.set_content(content)

  def set_content(self, content: str) -> None:
    self.body_label.setText(content or "")


class ModelClient:
  def __init__(self, api_key: str, base_url: str = DEFAULT_MODEL_BASE_URL, model: str = DEFAULT_MODEL_NAME, timeout: int = 90) -> None:
    self.api_key = api_key.strip()
    self.base_url = base_url.rstrip("/")
    self.model = model.strip() or DEFAULT_MODEL_NAME
    self.timeout = timeout

  @property
  def chat_endpoint(self) -> str:
    return f"{self.base_url}/chat/completions"

  def chat(self, messages: List[Dict[str, str]], temperature: float = 0.2, max_tokens: int = 1200) -> Dict[str, object]:
    if not self.api_key:
      raise ValueError("请先填写模型 API Key。")

    headers = {
      "Authorization": f"Bearer {self.api_key}",
      "Content-Type": "application/json",
    }
    payload = {
      "model": self.model,
      "messages": messages,
      "temperature": temperature,
      "max_tokens": max_tokens,
    }
    resp = requests.post(self.chat_endpoint, json=payload, headers=headers, timeout=self.timeout)
    try:
      resp.raise_for_status()
    except HTTPError as e:
      detail = resp.text[:2000] if resp.text else ""
      raise RuntimeError(f"模型接口调用失败(HTTP {resp.status_code}): {detail}") from e

    data = resp.json()
    choices = data.get("choices") or []
    if not choices:
      raise RuntimeError("模型接口未返回可用回复")

    message = choices[0].get("message") or {}
    content = message.get("content") or ""
    return {"content": content, "raw": data}


def sparql_text(v: str) -> str:
  escaped = v.replace("\\", "\\\\").replace('"', '\\"')
  return f'"{escaped}"'


def normalize_wd_entity_iri(text: str) -> str:
  val = text.strip()
  if not val:
    return ""
  if val.startswith(WD_ENTITY_PREFIX):
    return val
  if re.fullmatch(r"Q\\d+", val, flags=re.IGNORECASE):
    return f"{WD_ENTITY_PREFIX}{val.upper()}"
  return ""


class GraphDBClient:
    def __init__(self, base_url: str, repository_id: str, timeout: int = 30) -> None:
        self.base_url = base_url.rstrip("/")
        self.repository_id = repository_id
        self.timeout = timeout

    @property
    def query_endpoint(self) -> str:
        return f"{self.base_url}/repositories/{self.repository_id}"

    def select(self, sparql: str) -> List[Dict[str, Dict[str, str]]]:
        headers = {
            "Accept": "application/sparql-results+json",
            "Content-Type": "application/sparql-query; charset=utf-8",
        }
        resp = requests.post(
            self.query_endpoint,
            data=sparql.encode("utf-8"),
            headers=headers,
            timeout=self.timeout,
        )
        try:
            resp.raise_for_status()
        except HTTPError as e:
            detail = resp.text[:2000] if resp.text else ""
            raise RuntimeError(f"GraphDB查询失败(HTTP {resp.status_code}): {detail}") from e
        payload = resp.json()
        return payload.get("results", {}).get("bindings", [])


@dataclass
class WorkConditionInput:
    furnace_type: str
    tmax: float
    atmosphere: str
    slag_basicity: float
    thermal_shock: float
    abrasion_level: str
    metal_contact: bool
    zone: str
    slag_type: str
    operation_mode: str
    target_campaign_heats: float
    max_shell_temp: float
    temperature_curve: str
    cao: float
    sio2: float
    al2o3: float
    mgo: float


def build_main_query(wc: WorkConditionInput) -> str:
    metal = "true" if wc.metal_contact else "false"
    return f"""
PREFIX ex: <http://example.com/refractory#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX schema: <http://schema.org/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
SELECT ?ms ?msLabel ?workingSpec ?workingLabel ?altSpec ?altLabel ?confidence ?thresholdText ?desc
WHERE {{
  BIND(ex:{wc.furnace_type} AS ?furnaceType)
  BIND(ex:{wc.zone} AS ?zone)
  BIND(ex:{wc.slag_type} AS ?slagType)
  BIND(ex:{wc.operation_mode} AS ?operationMode)
  BIND(ex:{wc.atmosphere} AS ?atm)

  BIND({sparql_num(wc.tmax)} AS ?tmax)
  BIND({sparql_num(wc.slag_basicity)} AS ?basicity)
  BIND({sparql_num(wc.thermal_shock)} AS ?ts)
  BIND({sparql_num(wc.target_campaign_heats)} AS ?campaign)
  BIND({sparql_num(wc.max_shell_temp)} AS ?shellTemp)
  BIND({metal} AS ?metalContact)
  BIND("{wc.abrasion_level}" AS ?abrasion)

  BIND(ex:NoChoice AS ?no)

  BIND(
    IF(
      LCASE(STR(?metalContact)) = "true" && ?atm != ex:Oxidizing && ?tmax >= "1400"^^xsd:double && (?zone = ex:SlagLine || ?zone = ex:ImpactZone),
      ex:MS_MgOC,
      IF(
        LCASE(STR(?metalContact)) = "true" && ?atm = ex:Oxidizing,
        ex:MS_MgOSpinel,
        IF(
          LCASE(STR(?metalContact)) = "false" && ?slagType = ex:AcidicSlag,
          ex:MS_HighAlumina,
          ex:MS_Al2O3Spinel
        )
      )
    )
    AS ?ms
  )

  BIND(
    IF(
      ?ms = ex:MS_MgOC,
      ex:Spec_MgOC_Working,
      IF(
        ?ms = ex:MS_MgOSpinel,
        ex:Spec_MgOSpinel_Working,
        IF(
          ?ms = ex:MS_Al2O3Spinel,
          ex:Spec_Al2O3Spinel_Working,
          IF(?ms = ex:MS_HighAlumina, ex:Spec_HighAlumina_Working, ?no)
        )
      )
    )
    AS ?workingSpec
  )

  FILTER(?workingSpec != ?no)

  BIND(
    IF(
      ?workingSpec = ex:Spec_MgOC_Working,
      ex:Spec_MgOSpinel_Working,
      IF(
        ?workingSpec = ex:Spec_MgOSpinel_Working,
        ex:Spec_Al2O3Spinel_Working,
        IF(?workingSpec = ex:Spec_Al2O3Spinel_Working, ex:Spec_MgOSpinel_Working, ex:Spec_Al2O3Spinel_Working)
      )
    ) AS ?altSpec
  )

  GRAPH <http://example.com/graph/kb> {{
    ?workingSpec schema:isPartOf ?ms ;
                 ex:layerRole ex:WorkingLining .

    OPTIONAL {{ ?ms ex:minTmax ?minT }}
    OPTIONAL {{ ?ms ex:maxTmax ?maxT }}

    ?ms ex:allowedAtmosphere ?atm .
    OPTIONAL {{ ?ms ex:minSlagBasicity ?minB }}
    OPTIONAL {{ ?ms ex:maxSlagBasicity ?maxB }}

    OPTIONAL {{ ?ms rdfs:label ?msLabelRaw . FILTER(LANG(?msLabelRaw) = "zh" || LANG(?msLabelRaw) = "") }}
    OPTIONAL {{ ?workingSpec rdfs:label ?workingLabelRaw . FILTER(LANG(?workingLabelRaw) = "zh" || LANG(?workingLabelRaw) = "") }}
    OPTIONAL {{ ?altSpec rdfs:label ?altLabelRaw . FILTER(LANG(?altLabelRaw) = "zh" || LANG(?altLabelRaw) = "") }}
    OPTIONAL {{ ?furnaceType rdfs:label ?furnaceLabelRaw . FILTER(LANG(?furnaceLabelRaw) = "zh" || LANG(?furnaceLabelRaw) = "") }}
  }}

  GRAPH <http://example.com/graph/ontology> {{
    OPTIONAL {{ ?zone rdfs:label ?zoneLabelRaw . FILTER(LANG(?zoneLabelRaw) = "zh" || LANG(?zoneLabelRaw) = "") }}
    OPTIONAL {{ ?slagType rdfs:label ?slagLabelRaw . FILTER(LANG(?slagLabelRaw) = "zh" || LANG(?slagLabelRaw) = "") }}
    OPTIONAL {{ ?operationMode rdfs:label ?modeLabelRaw . FILTER(LANG(?modeLabelRaw) = "zh" || LANG(?modeLabelRaw) = "") }}
    OPTIONAL {{ ?atm rdfs:label ?atmLabelRaw . FILTER(LANG(?atmLabelRaw) = "zh" || LANG(?atmLabelRaw) = "") }}
  }}

  FILTER(!BOUND(?minT) || ?tmax >= ?minT)
  FILTER(!BOUND(?maxT) || ?tmax <= ?maxT)

  FILTER(!BOUND(?minB) || ?basicity >= ?minB)
  FILTER(!BOUND(?maxB) || ?basicity <= ?maxB)
  FILTER(NOT EXISTS {{ GRAPH <http://example.com/graph/kb> {{ ?ms ex:notRecommendedInAtmosphere ?atm }} }})
  FILTER(NOT EXISTS {{ GRAPH <http://example.com/graph/kb> {{ ?ms ex:notRecommendedForSlagType ?slagType }} }})

  BIND(COALESCE(?msLabelRaw, REPLACE(STR(?ms), "^.*#", "")) AS ?msLabel)
  BIND(COALESCE(?workingLabelRaw, REPLACE(STR(?workingSpec), "^.*#", "")) AS ?workingLabel)
  BIND(COALESCE(?altLabelRaw, REPLACE(STR(?altSpec), "^.*#", "")) AS ?altLabel)
  BIND(COALESCE(?furnaceLabelRaw, REPLACE(STR(?furnaceType), "^.*#", "")) AS ?furnaceLabel)
  BIND(COALESCE(?zoneLabelRaw, REPLACE(STR(?zone), "^.*#", "")) AS ?zoneLabel)
  BIND(COALESCE(?slagLabelRaw, REPLACE(STR(?slagType), "^.*#", "")) AS ?slagLabel)
  BIND(COALESCE(?modeLabelRaw, REPLACE(STR(?operationMode), "^.*#", "")) AS ?modeLabel)
  BIND(COALESCE(?atmLabelRaw, REPLACE(STR(?atm), "^.*#", "")) AS ?atmLabel)

  BIND(IF(EXISTS {{ GRAPH <http://example.com/graph/kb> {{ ?ms ex:preferredForFurnace ?furnaceType }} }}, 0.20, 0.0) AS ?scoreFurnace)
  BIND(IF(EXISTS {{ GRAPH <http://example.com/graph/kb> {{ ?ms ex:preferredForZone ?zone }} }}, 0.15, 0.0) AS ?scoreZone)
  BIND(0.65 + ?scoreFurnace + ?scoreZone AS ?rawScore)
  BIND(IF(?rawScore > 0.95, 0.95, ?rawScore) AS ?confidence)

  BIND(
    CONCAT(
      "Tmax适用范围=", IF(BOUND(?minT), STR(?minT), "NA"), "-", IF(BOUND(?maxT), STR(?maxT), "NA"), "℃",
      "；允许气氛=", STR(?atmLabel),
      IF(BOUND(?minB) || BOUND(?maxB), CONCAT("；渣碱度范围=", IF(BOUND(?minB), STR(?minB), "-inf"), "-", IF(BOUND(?maxB), STR(?maxB), "+inf")), ""),
      "；目标炉龄=", STR(?campaign), "炉次",
      "；壳体温度上限=", STR(?shellTemp), "℃"
    ) AS ?thresholdText
  )

  BIND(
    CONCAT(
      "推荐工作层=", STR(?workingLabel),
      "；替代方案=", STR(?altLabel),
      "；炉型=", STR(?furnaceLabel),
      "；部位=", STR(?zoneLabel),
      "；渣型=", STR(?slagLabel),
      "；运行模式=", STR(?modeLabel),
      "；依据：Tmax=", STR(?tmax), "℃，气氛=", STR(?atmLabel), "，金属接触=", STR(?metalContact), "，渣碱度=", STR(?basicity),
      "；置信度=", STR(?confidence),
      "；阈值=", ?thresholdText
    ) AS ?desc
  )
}}
LIMIT 1
"""


def build_failure_query(wc: WorkConditionInput) -> str:
    metal = "true" if wc.metal_contact else "false"
    return f"""
PREFIX ex: <http://example.com/refractory#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
SELECT DISTINCT ?fm ?fmLabel
WHERE {{
  BIND(ex:{wc.atmosphere} AS ?atm)
  BIND({metal} AS ?metalContact)
  BIND({sparql_num(wc.slag_basicity)} AS ?basicity)
  BIND({sparql_num(wc.thermal_shock)} AS ?ts)
  BIND("{wc.abrasion_level}" AS ?abrasion)

  {{
    FILTER(LCASE(STR(?metalContact)) = "true")
    BIND(ex:MetalPenetration AS ?fm)
  }}
  UNION
  {{
    FILTER(LCASE(STR(?metalContact)) = "true")
    BIND(ex:SlagCorrosion AS ?fm)
  }}
  UNION
  {{
    FILTER(?basicity >= "1.2"^^xsd:double)
    BIND(ex:SlagCorrosion AS ?fm)
  }}
  UNION
  {{
    FILTER(?basicity >= "0.8"^^xsd:double && ?basicity <= "1.6"^^xsd:double)
    BIND(ex:AlkaliAttack AS ?fm)
  }}
  UNION
  {{
    FILTER(?ts >= "3.0"^^xsd:double)
    BIND(ex:ThermalShockSpalling AS ?fm)
  }}
  UNION
  {{
    FILTER(LCASE(STR(?abrasion)) = "high")
    BIND(ex:AbrasionErosion AS ?fm)
  }}
  UNION
  {{
    FILTER(LCASE(STR(?metalContact)) = "true" && ?atm = ex:Oxidizing)
    BIND(ex:OxidationBurnout AS ?fm)
  }}

  OPTIONAL {{
    GRAPH <http://example.com/graph/kb> {{ ?fm rdfs:label ?fmLabelRaw . }}
    FILTER(LANG(?fmLabelRaw) = "zh" || LANG(?fmLabelRaw) = "")
  }}
  BIND(COALESCE(?fmLabelRaw, REPLACE(STR(?fm), "^.*#", "")) AS ?fmLabel)
}}
ORDER BY ?fmLabel
"""


def build_spec_detail_query(working_spec_iri: str) -> str:
    return f"""
PREFIX ex: <http://example.com/refractory#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
SELECT ?procLabel ?riskLabel ?constraintLabel
WHERE {{
  BIND(<{working_spec_iri}> AS ?workingSpec)
  OPTIONAL {{
    GRAPH <http://example.com/graph/kb> {{
      ?workingSpec ex:requiresProcess ?proc .
      OPTIONAL {{ ?proc rdfs:label ?procLabelRaw . FILTER(LANG(?procLabelRaw) = "zh" || LANG(?procLabelRaw) = "") }}
    }}
    BIND(COALESCE(?procLabelRaw, REPLACE(STR(?proc), "^.*#", "")) AS ?procLabel)
  }}
  OPTIONAL {{
    GRAPH <http://example.com/graph/kb> {{
      ?workingSpec ex:hasRisk ?risk .
      OPTIONAL {{ ?risk rdfs:label ?riskLabelRaw . FILTER(LANG(?riskLabelRaw) = "zh" || LANG(?riskLabelRaw) = "") }}
    }}
    BIND(COALESCE(?riskLabelRaw, REPLACE(STR(?risk), "^.*#", "")) AS ?riskLabel)
  }}
  OPTIONAL {{
    GRAPH <http://example.com/graph/kb> {{
      ?workingSpec ex:requiresConstraint ?constraint .
      OPTIONAL {{ ?constraint rdfs:label ?constraintLabelRaw . FILTER(LANG(?constraintLabelRaw) = "zh" || LANG(?constraintLabelRaw) = "") }}
    }}
    BIND(COALESCE(?constraintLabelRaw, REPLACE(STR(?constraint), "^.*#", "")) AS ?constraintLabel)
  }}
}}
"""


def build_layer_query(working_spec_iri: str) -> str:
    return f"""
PREFIX ex: <http://example.com/refractory#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX schema: <http://schema.org/>
SELECT ?layer ?roleLabel ?layerLabel ?layerDesc
WHERE {{
  VALUES ?layer {{ <{working_spec_iri}> ex:Spec_Generic_Insulation ex:Spec_Generic_Backup }}
  OPTIONAL {{
    GRAPH <http://example.com/graph/kb> {{
      ?layer ex:layerRole ?role .
      OPTIONAL {{ ?role rdfs:label ?roleLabelRaw . FILTER(LANG(?roleLabelRaw) = "zh" || LANG(?roleLabelRaw) = "") }}
    }}
    BIND(COALESCE(?roleLabelRaw, REPLACE(STR(?role), "^.*#", "")) AS ?roleLabel)
  }}
  OPTIONAL {{
    GRAPH <http://example.com/graph/kb> {{ ?layer rdfs:label ?layerLabelRaw . }}
    FILTER(LANG(?layerLabelRaw) = "zh" || LANG(?layerLabelRaw) = "")
  }}
  OPTIONAL {{
    GRAPH <http://example.com/graph/kb> {{ ?layer schema:description ?layerDescRaw . }}
    FILTER(LANG(?layerDescRaw) = "zh" || LANG(?layerDescRaw) = "")
  }}
  BIND(COALESCE(?layerLabelRaw, REPLACE(STR(?layer), "^.*#", "")) AS ?layerLabel)
  BIND(COALESCE(?layerDescRaw, "") AS ?layerDesc)
}}
ORDER BY ?roleLabel ?layerLabel
"""


def build_component_encyclopedia_query(ms_iri: str) -> str:
    return f"""
PREFIX ex: <http://example.com/refractory#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX schema: <http://schema.org/>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
SELECT ?comp ?compLabel ?compDesc
       (SAMPLE(?formulaRaw) AS ?formula)
       (SAMPLE(?densityRaw) AS ?density)
       (SAMPLE(?meltRaw) AS ?melt)
       (SAMPLE(?wikiRaw) AS ?wiki)
WHERE {{
  BIND(<{ms_iri}> AS ?ms)

  GRAPH <http://example.com/graph/kb> {{
    ?ms ex:hasComponent ?comp .
  }}

  GRAPH <http://example.com/graph/wikidata> {{
    OPTIONAL {{ ?comp rdfs:label ?labelZh . FILTER(LANG(?labelZh) = "zh") }}
    OPTIONAL {{ ?comp rdfs:label ?labelEn . FILTER(LANG(?labelEn) = "en") }}
    OPTIONAL {{ ?comp schema:description ?descZh . FILTER(LANG(?descZh) = "zh") }}
    OPTIONAL {{ ?comp schema:description ?descEn . FILTER(LANG(?descEn) = "en") }}

    OPTIONAL {{ ?comp wdt:P274 ?formulaRaw }}
    OPTIONAL {{ ?comp wdt:P2054 ?densityRaw }}
    OPTIONAL {{ ?comp wdt:P2101 ?meltRaw }}
    OPTIONAL {{
      ?comp schema:sameAs ?wikiRaw .
      FILTER(CONTAINS(STR(?wikiRaw), "wikipedia.org/wiki/"))
    }}
  }}

  BIND(COALESCE(?labelZh, ?labelEn, REPLACE(STR(?comp), "^.*[/#]", "")) AS ?compLabel)
  BIND(COALESCE(?descZh, ?descEn, "") AS ?compDesc)
}}
GROUP BY ?comp ?compLabel ?compDesc
ORDER BY ?compLabel
"""


def build_component_encyclopedia_for_component_query(comp_iri: str) -> str:
    return f"""
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX schema: <http://schema.org/>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
SELECT ?comp ?compLabel ?compDesc
       (SAMPLE(?formulaRaw) AS ?formula)
       (SAMPLE(?densityRaw) AS ?density)
       (SAMPLE(?meltRaw) AS ?melt)
       (SAMPLE(?wikiRaw) AS ?wiki)
WHERE {{
  BIND(<{comp_iri}> AS ?comp)

  GRAPH <http://example.com/graph/wikidata> {{
    OPTIONAL {{ ?comp rdfs:label ?labelZh . FILTER(LANG(?labelZh) = "zh") }}
    OPTIONAL {{ ?comp rdfs:label ?labelEn . FILTER(LANG(?labelEn) = "en") }}
    OPTIONAL {{ ?comp schema:description ?descZh . FILTER(LANG(?descZh) = "zh") }}
    OPTIONAL {{ ?comp schema:description ?descEn . FILTER(LANG(?descEn) = "en") }}

    OPTIONAL {{ ?comp wdt:P274 ?formulaRaw }}
    OPTIONAL {{ ?comp wdt:P2054 ?densityRaw }}
    OPTIONAL {{ ?comp wdt:P2101 ?meltRaw }}
    OPTIONAL {{
      ?comp schema:sameAs ?wikiRaw .
      FILTER(CONTAINS(STR(?wikiRaw), "wikipedia.org/wiki/"))
    }}
  }}

  BIND(COALESCE(?labelZh, ?labelEn, REPLACE(STR(?comp), "^.*[/#]", "")) AS ?compLabel)
  BIND(COALESCE(?descZh, ?descEn, "") AS ?compDesc)
}}
GROUP BY ?comp ?compLabel ?compDesc
"""


def build_component_encyclopedia_by_keyword_query(keyword: str) -> str:
    kw = sparql_text(keyword)
    return f"""
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX schema: <http://schema.org/>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
SELECT ?comp ?compLabel ?compDesc
       (SAMPLE(?formulaRaw) AS ?formula)
       (SAMPLE(?densityRaw) AS ?density)
       (SAMPLE(?meltRaw) AS ?melt)
       (SAMPLE(?wikiRaw) AS ?wiki)
WHERE {{
  BIND({kw} AS ?kw)

  GRAPH <http://example.com/graph/wikidata> {{
    ?comp rdfs:label ?labelHit .
    FILTER(LANG(?labelHit) = "zh" || LANG(?labelHit) = "en")
    FILTER(CONTAINS(LCASE(STR(?labelHit)), LCASE(?kw)))

    OPTIONAL {{ ?comp rdfs:label ?labelZh . FILTER(LANG(?labelZh) = "zh") }}
    OPTIONAL {{ ?comp rdfs:label ?labelEn . FILTER(LANG(?labelEn) = "en") }}
    OPTIONAL {{ ?comp schema:description ?descZh . FILTER(LANG(?descZh) = "zh") }}
    OPTIONAL {{ ?comp schema:description ?descEn . FILTER(LANG(?descEn) = "en") }}

    OPTIONAL {{ ?comp wdt:P274 ?formulaRaw }}
    OPTIONAL {{ ?comp wdt:P2054 ?densityRaw }}
    OPTIONAL {{ ?comp wdt:P2101 ?meltRaw }}
    OPTIONAL {{
      ?comp schema:sameAs ?wikiRaw .
      FILTER(CONTAINS(STR(?wikiRaw), "wikipedia.org/wiki/"))
    }}
  }}

  BIND(COALESCE(?labelZh, ?labelEn, REPLACE(STR(?comp), "^.*[/#]", "")) AS ?compLabel)
  BIND(COALESCE(?descZh, ?descEn, "") AS ?compDesc)
}}
GROUP BY ?comp ?compLabel ?compDesc
ORDER BY ?compLabel
LIMIT 30
"""


def build_component_peer_query(ms_iri: str) -> str:
    return f"""
PREFIX ex: <http://example.com/refractory#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX schema: <http://schema.org/>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
SELECT ?comp ?compLabel ?parent ?parentLabel ?peer ?peerLabel ?peerDesc
       (SAMPLE(?peerFormulaRaw) AS ?peerFormula)
       (SAMPLE(?wikiRaw) AS ?wiki)
WHERE {{
  BIND(<{ms_iri}> AS ?ms)

  GRAPH <http://example.com/graph/kb> {{
    ?ms ex:hasComponent ?comp .
  }}

  GRAPH <http://example.com/graph/wikidata> {{
    ?comp wdt:P279 ?parent .
    ?peer wdt:P279 ?parent .
    FILTER(?peer != ?comp)

    OPTIONAL {{ ?comp rdfs:label ?compZh . FILTER(LANG(?compZh) = "zh") }}
    OPTIONAL {{ ?comp rdfs:label ?compEn . FILTER(LANG(?compEn) = "en") }}

    OPTIONAL {{ ?parent rdfs:label ?parentZh . FILTER(LANG(?parentZh) = "zh") }}
    OPTIONAL {{ ?parent rdfs:label ?parentEn . FILTER(LANG(?parentEn) = "en") }}

    OPTIONAL {{ ?peer rdfs:label ?peerZh . FILTER(LANG(?peerZh) = "zh") }}
    OPTIONAL {{ ?peer rdfs:label ?peerEn . FILTER(LANG(?peerEn) = "en") }}
    OPTIONAL {{ ?peer schema:description ?peerDescZh . FILTER(LANG(?peerDescZh) = "zh") }}
    OPTIONAL {{ ?peer schema:description ?peerDescEn . FILTER(LANG(?peerDescEn) = "en") }}

    OPTIONAL {{ ?peer wdt:P274 ?peerFormulaRaw }}
    OPTIONAL {{
      ?peer schema:sameAs ?wikiRaw .
      FILTER(CONTAINS(STR(?wikiRaw), "wikipedia.org/wiki/"))
    }}
  }}

  BIND(COALESCE(?compZh, ?compEn, REPLACE(STR(?comp), "^.*[/#]", "")) AS ?compLabel)
  BIND(COALESCE(?parentZh, ?parentEn, REPLACE(STR(?parent), "^.*[/#]", "")) AS ?parentLabel)
  BIND(COALESCE(?peerZh, ?peerEn, REPLACE(STR(?peer), "^.*[/#]", "")) AS ?peerLabel)
  BIND(COALESCE(?peerDescZh, ?peerDescEn, "") AS ?peerDesc)
}}
GROUP BY ?comp ?compLabel ?parent ?parentLabel ?peer ?peerLabel ?peerDesc
ORDER BY ?compLabel ?parentLabel ?peerLabel
LIMIT 80
"""


def build_component_peer_for_component_query(comp_iri: str) -> str:
    return f"""
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX schema: <http://schema.org/>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
SELECT ?comp ?compLabel ?parent ?parentLabel ?peer ?peerLabel ?peerDesc
       (SAMPLE(?peerFormulaRaw) AS ?peerFormula)
       (SAMPLE(?wikiRaw) AS ?wiki)
WHERE {{
  BIND(<{comp_iri}> AS ?comp)

  GRAPH <http://example.com/graph/wikidata> {{
    ?comp wdt:P279 ?parent .
    ?peer wdt:P279 ?parent .
    FILTER(?peer != ?comp)

    OPTIONAL {{ ?comp rdfs:label ?compZh . FILTER(LANG(?compZh) = "zh") }}
    OPTIONAL {{ ?comp rdfs:label ?compEn . FILTER(LANG(?compEn) = "en") }}

    OPTIONAL {{ ?parent rdfs:label ?parentZh . FILTER(LANG(?parentZh) = "zh") }}
    OPTIONAL {{ ?parent rdfs:label ?parentEn . FILTER(LANG(?parentEn) = "en") }}

    OPTIONAL {{ ?peer rdfs:label ?peerZh . FILTER(LANG(?peerZh) = "zh") }}
    OPTIONAL {{ ?peer rdfs:label ?peerEn . FILTER(LANG(?peerEn) = "en") }}
    OPTIONAL {{ ?peer schema:description ?peerDescZh . FILTER(LANG(?peerDescZh) = "zh") }}
    OPTIONAL {{ ?peer schema:description ?peerDescEn . FILTER(LANG(?peerDescEn) = "en") }}

    OPTIONAL {{ ?peer wdt:P274 ?peerFormulaRaw }}
    OPTIONAL {{
      ?peer schema:sameAs ?wikiRaw .
      FILTER(CONTAINS(STR(?wikiRaw), "wikipedia.org/wiki/"))
    }}
  }}

  BIND(COALESCE(?compZh, ?compEn, REPLACE(STR(?comp), "^.*[/#]", "")) AS ?compLabel)
  BIND(COALESCE(?parentZh, ?parentEn, REPLACE(STR(?parent), "^.*[/#]", "")) AS ?parentLabel)
  BIND(COALESCE(?peerZh, ?peerEn, REPLACE(STR(?peer), "^.*[/#]", "")) AS ?peerLabel)
  BIND(COALESCE(?peerDescZh, ?peerDescEn, "") AS ?peerDesc)
}}
GROUP BY ?comp ?compLabel ?parent ?parentLabel ?peer ?peerLabel ?peerDesc
ORDER BY ?parentLabel ?peerLabel
LIMIT 80
"""


def build_component_peer_by_keyword_query(keyword: str) -> str:
    kw = sparql_text(keyword)
    return f"""
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX schema: <http://schema.org/>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
SELECT ?comp ?compLabel ?parent ?parentLabel ?peer ?peerLabel ?peerDesc
       (SAMPLE(?peerFormulaRaw) AS ?peerFormula)
       (SAMPLE(?wikiRaw) AS ?wiki)
WHERE {{
  BIND({kw} AS ?kw)

  GRAPH <http://example.com/graph/wikidata> {{
    ?comp rdfs:label ?compHit .
    FILTER(LANG(?compHit) = "zh" || LANG(?compHit) = "en")
    FILTER(CONTAINS(LCASE(STR(?compHit)), LCASE(?kw)))

    ?comp wdt:P279 ?parent .
    ?peer wdt:P279 ?parent .
    FILTER(?peer != ?comp)

    OPTIONAL {{ ?comp rdfs:label ?compZh . FILTER(LANG(?compZh) = "zh") }}
    OPTIONAL {{ ?comp rdfs:label ?compEn . FILTER(LANG(?compEn) = "en") }}

    OPTIONAL {{ ?parent rdfs:label ?parentZh . FILTER(LANG(?parentZh) = "zh") }}
    OPTIONAL {{ ?parent rdfs:label ?parentEn . FILTER(LANG(?parentEn) = "en") }}

    OPTIONAL {{ ?peer rdfs:label ?peerZh . FILTER(LANG(?peerZh) = "zh") }}
    OPTIONAL {{ ?peer rdfs:label ?peerEn . FILTER(LANG(?peerEn) = "en") }}
    OPTIONAL {{ ?peer schema:description ?peerDescZh . FILTER(LANG(?peerDescZh) = "zh") }}
    OPTIONAL {{ ?peer schema:description ?peerDescEn . FILTER(LANG(?peerDescEn) = "en") }}

    OPTIONAL {{ ?peer wdt:P274 ?peerFormulaRaw }}
    OPTIONAL {{
      ?peer schema:sameAs ?wikiRaw .
      FILTER(CONTAINS(STR(?wikiRaw), "wikipedia.org/wiki/"))
    }}
  }}

  BIND(COALESCE(?compZh, ?compEn, REPLACE(STR(?comp), "^.*[/#]", "")) AS ?compLabel)
  BIND(COALESCE(?parentZh, ?parentEn, REPLACE(STR(?parent), "^.*[/#]", "")) AS ?parentLabel)
  BIND(COALESCE(?peerZh, ?peerEn, REPLACE(STR(?peer), "^.*[/#]", "")) AS ?peerLabel)
  BIND(COALESCE(?peerDescZh, ?peerDescEn, "") AS ?peerDesc)
}}
GROUP BY ?comp ?compLabel ?parent ?parentLabel ?peer ?peerLabel ?peerDesc
ORDER BY ?compLabel ?parentLabel ?peerLabel
LIMIT 80
"""


def build_diagnosis_solution_query(failure_iris: List[str]) -> str:
    if not failure_iris:
        failure_iris = ["ThermalShockSpalling"]

    values = " ".join(f"ex:{iri}" for iri in failure_iris)
    return f"""
PREFIX ex: <http://example.com/refractory#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX schema: <http://schema.org/>
SELECT ?fm ?fmLabel ?ms ?msLabel ?msFurnaceLabel ?msZoneLabel ?spec ?specLabel ?specDesc ?procLabel ?riskLabel ?constraintLabel ?rank
WHERE {{
  VALUES ?fm {{ {values} }}

  GRAPH <http://example.com/graph/kb> {{
    OPTIONAL {{ ?fm rdfs:label ?fmLabelRaw . FILTER(LANG(?fmLabelRaw) = "zh" || LANG(?fmLabelRaw) = "") }}
    ?fm ex:mitigatedBy ?ms .

    OPTIONAL {{ ?ms rdfs:label ?msLabelRaw . FILTER(LANG(?msLabelRaw) = "zh" || LANG(?msLabelRaw) = "") }}
    OPTIONAL {{
      ?ms ex:preferredForFurnace ?msFurnace .
      OPTIONAL {{ ?msFurnace rdfs:label ?msFurnaceLabelRaw . FILTER(LANG(?msFurnaceLabelRaw) = "zh" || LANG(?msFurnaceLabelRaw) = "") }}
    }}
    OPTIONAL {{
      ?ms ex:preferredForZone ?msZone .
      OPTIONAL {{ ?msZone rdfs:label ?msZoneLabelRaw . FILTER(LANG(?msZoneLabelRaw) = "zh" || LANG(?msZoneLabelRaw) = "") }}
    }}

    OPTIONAL {{
      ?spec schema:isPartOf ?ms ;
            ex:layerRole ex:WorkingLining .
      OPTIONAL {{ ?spec rdfs:label ?specLabelRaw . FILTER(LANG(?specLabelRaw) = "zh" || LANG(?specLabelRaw) = "") }}
      OPTIONAL {{ ?spec schema:description ?specDescRaw . FILTER(LANG(?specDescRaw) = "zh" || LANG(?specDescRaw) = "") }}
      OPTIONAL {{ ?spec ex:requiresProcess ?proc . OPTIONAL {{ ?proc rdfs:label ?procLabelRaw . FILTER(LANG(?procLabelRaw) = "zh" || LANG(?procLabelRaw) = "") }} }}
      OPTIONAL {{ ?spec ex:hasRisk ?risk . OPTIONAL {{ ?risk rdfs:label ?riskLabelRaw . FILTER(LANG(?riskLabelRaw) = "zh" || LANG(?riskLabelRaw) = "") }} }}
      OPTIONAL {{ ?spec ex:requiresConstraint ?constraint . OPTIONAL {{ ?constraint rdfs:label ?constraintLabelRaw . FILTER(LANG(?constraintLabelRaw) = "zh" || LANG(?constraintLabelRaw) = "") }} }}
      OPTIONAL {{ ?spec ex:alternativeRank ?rank . }}
    }}
  }}

  BIND(COALESCE(?fmLabelRaw, REPLACE(STR(?fm), "^.*#", "")) AS ?fmLabel)
  BIND(COALESCE(?msLabelRaw, REPLACE(STR(?ms), "^.*#", "")) AS ?msLabel)
  BIND(COALESCE(?msFurnaceLabelRaw, REPLACE(STR(?msFurnace), "^.*#", "")) AS ?msFurnaceLabel)
  BIND(COALESCE(?msZoneLabelRaw, REPLACE(STR(?msZone), "^.*#", "")) AS ?msZoneLabel)
  BIND(COALESCE(?specLabelRaw, REPLACE(STR(?spec), "^.*#", "")) AS ?specLabel)
  BIND(COALESCE(?specDescRaw, "") AS ?specDesc)
  BIND(COALESCE(?procLabelRaw, REPLACE(STR(?proc), "^.*#", "")) AS ?procLabel)
  BIND(COALESCE(?riskLabelRaw, REPLACE(STR(?risk), "^.*#", "")) AS ?riskLabel)
  BIND(COALESCE(?constraintLabelRaw, REPLACE(STR(?constraint), "^.*#", "")) AS ?constraintLabel)
}}
ORDER BY ?fmLabel ?rank ?msLabel ?specLabel ?procLabel ?riskLabel ?constraintLabel
"""


class RefractorySelectorApp(FluentWindow):
  def __init__(self) -> None:
    super().__init__()
    self.setWindowTitle("耐火材料KG选型助手（GraphDB）")
    self.resize(1220, 820)
    self.project_root = Path(__file__).resolve().parent

    self.base_url = "http://localhost:7200"
    self.repository_id = "RefMDB"

    self.last_ms_iri = ""
    self.ms_choice_values = ["最近推荐体系"] + list(MS_OPTIONS.keys())

    self.component_card_records: List[Dict[str, str]] = []
    self.peer_group_labels: List[str] = []
    self.peer_group_records: Dict[str, List[Dict[str, str]]] = {}
    self.page_actions: Dict[str, object] = {}
    self.page_widgets: Dict[str, QWidget] = {}
    self.chat_history: List[Dict[str, str]] = []
    self._response_timers: List[object] = []

    self._build_ui()

  def _build_ui(self) -> None:
    self._build_menu_bar()

    self.recommend_page = QWidget(self)
    self.recommend_page.setObjectName("recommendationPage")
    self.settings_page = QWidget(self)
    self.settings_page.setObjectName("settingsPage")
    self.diagnosis_page = QWidget(self)
    self.diagnosis_page.setObjectName("diagnosisPage")
    self.crawl_page = QWidget(self)
    self.crawl_page.setObjectName("crawlPage")
    self.encyclopedia_page = QWidget(self)
    self.encyclopedia_page.setObjectName("encyclopediaPage")
    self.peers_page = QWidget(self)
    self.peers_page.setObjectName("peersPage")
    self.chat_page = QWidget(self)
    self.chat_page.setObjectName("chatPage")

    self._build_recommendation_page(self.recommend_page)
    self._build_settings_page(self.settings_page)
    self._build_diagnosis_page(self.diagnosis_page)
    self._build_crawl_page(self.crawl_page)
    self._build_encyclopedia_page(self.encyclopedia_page)
    self._build_peers_page(self.peers_page)
    self._build_chat_page(self.chat_page)
    self._apply_round_styles()

    self.page_widgets = {
      "recommendation": self.recommend_page,
      "settings": self.settings_page,
      "diagnosis": self.diagnosis_page,
      "crawl": self.crawl_page,
      "encyclopedia": self.encyclopedia_page,
      "peers": self.peers_page,
      "chat": self.chat_page,
    }

    self.addSubInterface(self.recommend_page, FIF.HOME, "推荐选型")
    settings_icon = getattr(FIF, "SETTING", getattr(FIF, "SETTINGS", FIF.LIBRARY))
    self.addSubInterface(self.settings_page, settings_icon, "设置")
    diagnosis_icon = getattr(FIF, "SEARCH", getattr(FIF, "FIND", FIF.LIBRARY))
    self.addSubInterface(self.diagnosis_page, diagnosis_icon, "问题诊断")
    crawl_icon = getattr(FIF, "DOWNLOAD", getattr(FIF, "SYNC", FIF.LIBRARY))
    self.addSubInterface(self.crawl_page, crawl_icon, "数据抓取")
    self.addSubInterface(self.encyclopedia_page, FIF.BOOK_SHELF, "组件百科卡")
    self.addSubInterface(self.peers_page, FIF.LIBRARY, "同类候选")
    chat_icon = getattr(FIF, "CHAT", getattr(FIF, "MESSAGE", FIF.LIBRARY))
    self.addSubInterface(self.chat_page, chat_icon, "问答推荐")

    self._clear_encyclopedia_view()
    self._clear_peers_view()
    self._clear_settings_status()
    self._clear_diagnosis_view()
    self._clear_chat_session()
    self._clear_crawl_log()
    self._switch_main_page("recommendation")

  def _apply_round_styles(self) -> None:
    self.setStyleSheet(
      """
      QLineEdit,
      QComboBox {
        border: 1px solid #d8dbe2;
        border-radius: 10px;
        padding: 4px 8px;
        background: #ffffff;
      }

      QWidget#roundedArea {
        border: 1px solid #e2e5eb;
        border-radius: 12px;
        background: #ffffff;
      }

      QListWidget#roundedList,
      QTableWidget#roundedTable {
        border: 1px solid #d8dbe2;
        border-radius: 10px;
        background: #ffffff;
      }

      QWidget#chatPage {
        background: #343541;
      }

      QWidget#settingsPage {
        background: #f8fafc;
      }

      QFrame#settingsCard {
        background: #ffffff;
        border: 1px solid #d8dbe2;
        border-radius: 18px;
      }

      QLabel#settingsTitle {
        color: #0f172a;
        font-size: 20px;
        font-weight: 700;
      }

      QLabel#settingsSubtitle,
      QLabel#settingsStatus {
        color: #475569;
      }

      QWidget#crawlPage {
        background: #f4f7fb;
      }

      QWidget#diagnosisPage {
        background: #f8fafc;
      }

      QFrame#diagnosisCard,
      QPlainTextEdit#diagnosisInput,
      QPlainTextEdit#diagnosisOutput {
        background: #ffffff;
        border: 1px solid #d8dbe2;
        border-radius: 16px;
      }

      QPlainTextEdit#diagnosisInput,
      QPlainTextEdit#diagnosisOutput {
        color: #0f172a;
        font-size: 13px;
      }

      QLabel#crawlStatusLabel {
        color: #0f172a;
        font-size: 14px;
        font-weight: 600;
      }

      QPlainTextEdit#crawlLog {
        background: #0f172a;
        color: #e2e8f0;
        border: 1px solid #334155;
        border-radius: 16px;
        font-family: Consolas;
        font-size: 13px;
      }

      QFrame#chatSidebar {
        background: #202123;
        border: none;
      }

      QFrame#chatCard,
      QFrame#chatHeader,
      QFrame#chatComposerFrame {
        background: #40414f;
        border: 1px solid #565869;
        border-radius: 16px;
      }

      QFrame#assistantBubble {
        background-color: #1f4e8c;
        border: 1px solid #3b82f6;
        border-radius: 16px;
      }

      QFrame#userBubble {
        background-color: #2563eb;
        border: 1px solid #60a5fa;
        border-radius: 16px;
      }

      QLabel#chatTitle {
        color: #ececf1;
        font-size: 22px;
        font-weight: 700;
      }

      QLabel#chatSubtitle,
      QLabel#chatPanelText {
        color: #acacbe;
      }

      QLabel#chatPanelTitle {
        color: #ececf1;
        font-size: 15px;
        font-weight: 600;
      }

      QLabel#chatBubbleTitle {
        color: #aeb4c4;
        font-size: 12px;
        font-weight: 600;
      }

      QLabel#chatBubbleBody {
        color: #ececf1;
        font-size: 14px;
      }

      QScrollArea#chatTranscriptArea {
        background: #ffffff;
        border: 2px solid #000000;
        border-radius: 20px;
      }

      QWidget#chatTranscriptContainer {
        background: #ffffff;
      }

      QPlainTextEdit#chatComposer {
        background: transparent;
        border: none;
        color: #ececf1;
        selection-background-color: #10a37f;
      }

      QLineEdit#chatInputField,
      QComboBox#chatInputField {
        background: #2a2b32;
        border: 1px solid #565869;
        border-radius: 10px;
        color: #ececf1;
        padding: 6px 10px;
      }

      QPushButton#chatSendButton {
        background: #10a37f;
        border: none;
        color: white;
        border-radius: 12px;
        padding: 8px 16px;
        min-width: 84px;
      }

      QPushButton#chatSendButton:hover {
        background: #0f8f71;
      }

      QPushButton#chatSendButton:disabled {
        background: #3b3c46;
        color: #8b8ea3;
      }

      QScrollBar:vertical {
        background: transparent;
        width: 10px;
        margin: 0px;
      }

      QScrollBar::handle:vertical {
        background: #5a5d6d;
        border-radius: 4px;
        min-height: 24px;
      }

      QScrollBar::handle:vertical:hover {
        background: #707380;
      }
      """
    )

  def _build_menu_bar(self) -> None:
    menu_bar_fn = getattr(self, "menuBar", None)
    if not callable(menu_bar_fn):
      self.page_actions = {}
      return

    app_menu = menu_bar_fn().addMenu("应用")
    action_settings = app_menu.addAction("设置")
    action_settings.triggered.connect(self._open_settings_page)

    page_menu = menu_bar_fn().addMenu("功能选择")
    for page_name, label in [
      ("recommendation", "推荐选型"),
      ("settings", "设置"),
      ("diagnosis", "问题诊断"),
      ("crawl", "数据抓取"),
      ("encyclopedia", "组件百科卡"),
      ("peers", "同类候选"),
      ("chat", "问答推荐"),
    ]:
      action = page_menu.addAction(label)
      action.triggered.connect(partial(self._switch_main_page, page_name))
      self.page_actions[page_name] = action

  def _center_dialog(self, dialog: QDialog) -> None:
    dialog.adjustSize()
    frame = dialog.frameGeometry()
    frame.moveCenter(self.frameGeometry().center())
    dialog.move(frame.topLeft())

  def _open_connection_dialog(self) -> None:
    self._open_settings_page()

  def run_diagnosis(self) -> None:
    try:
      wc = self._collect_input()
      symptom_text = self.diagnosis_input.toPlainText().strip()
      self._switch_main_page("diagnosis")

      analysis = self._analyze_semantic_request(
        "diagnosis",
        symptom_text,
        {"work_condition_context": self._describe_work_condition(wc)},
      )
      if not analysis.get("is_refractory_related"):
        refusal_text = self._format_non_refractory_reply("diagnosis", analysis)
        self.diagnosis_output.setPlainText("正在生成回复……")
        self.diagnosis_status_label.setText("正在生成回复……")
        self._animate_plain_text_response(
          self.diagnosis_output,
          refusal_text,
          on_finished=lambda: self.diagnosis_status_label.setText("输入内容不属于耐火材料症状，已停止查询。"),
        )
        return

      client = self._client()
      payload = self._build_diagnosis_payload(symptom_text, wc, client, analysis=analysis)
      output_text = "\n".join(payload.get("out_lines", []))
      candidates = payload.get("candidates", [])

      self.diagnosis_output.setPlainText("正在生成回复……")
      self.diagnosis_status_label.setText("正在生成回复……")
      if candidates:
        top = candidates[0]
        final_status = f"已识别 {len(candidates)} 个问题，当前优先级最高: {top.get('label', '')}。"
      else:
        final_status = "知识图谱中暂未查到对应问题，请补充症状。"

      self._animate_plain_text_response(
        self.diagnosis_output,
        output_text,
        on_finished=lambda: self.diagnosis_status_label.setText(final_status),
      )
    except Exception as e:
      self.diagnosis_status_label.setText(f"诊断失败：{e}")
      QMessageBox.critical(self, "诊断失败", f"无法生成问题诊断:\n{e}")

  def _open_settings_page(self) -> None:
    self._switch_main_page("settings")

  def _clear_settings_status(self) -> None:
    if hasattr(self, "settings_status_label"):
      self.settings_status_label.setText("修改后立即生效，推荐页与问答页都会读取这里的配置。")

  def _model_api_key_value(self) -> str:
    return self.model_api_key_edit.text().strip() if hasattr(self, "model_api_key_edit") else ""

  def _model_base_url_value(self) -> str:
    return self.model_base_url_edit.text().strip() if hasattr(self, "model_base_url_edit") else DEFAULT_MODEL_BASE_URL

  def _model_name_value(self) -> str:
    return self.model_name_edit.text().strip() if hasattr(self, "model_name_edit") else DEFAULT_MODEL_NAME

  def _graphdb_base_url_value(self) -> str:
    return self.graphdb_url_edit.text().strip() if hasattr(self, "graphdb_url_edit") else "http://localhost:7200"

  def _graphdb_repository_id_value(self) -> str:
    return self.graphdb_repository_edit.text().strip() if hasattr(self, "graphdb_repository_edit") else "RefMDB"

  def _add_form_row(self, layout: QGridLayout, row: int, label: str, widget: QWidget) -> None:
    if label:
      layout.addWidget(QLabel(label), row, 0)
    layout.addWidget(widget, row, 1)

  def _add_path_row(self, layout: QGridLayout, row: int, label: str, widget: QLineEdit, browse_mode: str = "open") -> None:
    container = QWidget()
    container_layout = QHBoxLayout(container)
    container_layout.setContentsMargins(0, 0, 0, 0)
    container_layout.setSpacing(6)
    container_layout.addWidget(widget, 1)

    browse_btn = PushButton("浏览")

    def on_browse() -> None:
      current = widget.text().strip()
      initial_dir = current or str(self.project_root)
      if browse_mode == "save":
        selected, _ = QFileDialog.getSaveFileName(self, label, initial_dir, "TTL Files (*.ttl);;All Files (*)")
      else:
        selected, _ = QFileDialog.getOpenFileName(self, label, initial_dir, "Text Files (*.txt);;All Files (*)")
      if selected:
        widget.setText(selected)

    browse_btn.clicked.connect(on_browse)
    container_layout.addWidget(browse_btn)
    self._add_form_row(layout, row, label, container)

  def _build_settings_page(self, page: QWidget) -> None:
    page_layout = QVBoxLayout(page)
    page_layout.setContentsMargins(8, 8, 8, 8)
    page_layout.setSpacing(10)

    title = QLabel("集中设置", page)
    title.setObjectName("settingsTitle")
    subtitle = QLabel("GraphDB 和 LLM 的配置都集中在这里修改，推荐页与问答页会直接读取这些值。", page)
    subtitle.setObjectName("settingsSubtitle")
    subtitle.setWordWrap(True)
    page_layout.addWidget(title)
    page_layout.addWidget(subtitle)

    split = QSplitter(Qt.Horizontal, page)
    page_layout.addWidget(split, 1)

    graph_card = QFrame(split)
    graph_card.setObjectName("settingsCard")
    graph_layout = QVBoxLayout(graph_card)
    graph_layout.setContentsMargins(18, 18, 18, 18)
    graph_layout.setSpacing(10)

    graph_title = QLabel("GraphDB 配置", graph_card)
    graph_title.setObjectName("chatPanelTitle")
    graph_layout.addWidget(graph_title)

    graph_form = QFormLayout()
    graph_form.setSpacing(10)
    self.graphdb_url_edit = QLineEdit(os.getenv("GRAPHDB_BASE_URL", "http://localhost:7200"), graph_card)
    self.graphdb_url_edit.setPlaceholderText("GraphDB Base URL")
    self.graphdb_repository_edit = QLineEdit(os.getenv("GRAPHDB_REPOSITORY_ID", "RefMDB"), graph_card)
    self.graphdb_repository_edit.setPlaceholderText("Repository ID")
    graph_form.addRow("URL", self.graphdb_url_edit)
    graph_form.addRow("Repository", self.graphdb_repository_edit)
    graph_layout.addLayout(graph_form)

    graph_actions = QHBoxLayout()
    graph_actions.setContentsMargins(0, 0, 0, 0)
    graph_actions.setSpacing(6)
    graph_test_btn = PrimaryPushButton("测试 GraphDB")
    graph_test_btn.clicked.connect(self.test_connection)
    graph_actions.addWidget(graph_test_btn)
    graph_actions.addStretch(1)
    graph_layout.addLayout(graph_actions)

    graph_hint = QLabel("修改后立即生效，无需单独保存。", graph_card)
    graph_hint.setWordWrap(True)
    graph_layout.addWidget(graph_hint)

    llm_card = QFrame(split)
    llm_card.setObjectName("settingsCard")
    llm_layout = QVBoxLayout(llm_card)
    llm_layout.setContentsMargins(18, 18, 18, 18)
    llm_layout.setSpacing(10)

    llm_title = QLabel("LLM 配置", llm_card)
    llm_title.setObjectName("chatPanelTitle")
    llm_layout.addWidget(llm_title)

    llm_form = QFormLayout()
    llm_form.setSpacing(10)
    self.model_api_key_edit = QLineEdit(os.getenv("MODEL_API_KEY", ""), llm_card)
    self.model_api_key_edit.setEchoMode(QLineEdit.Password)
    self.model_api_key_edit.setPlaceholderText("模型 API Key")
    self.model_base_url_edit = QLineEdit(os.getenv("MODEL_BASE_URL", DEFAULT_MODEL_BASE_URL), llm_card)
    self.model_base_url_edit.setPlaceholderText("模型服务地址")
    self.model_name_edit = QLineEdit(os.getenv("MODEL_NAME", DEFAULT_MODEL_NAME), llm_card)
    self.model_name_edit.setPlaceholderText("模型名称")
    llm_form.addRow("API Key", self.model_api_key_edit)
    llm_form.addRow("Base URL", self.model_base_url_edit)
    llm_form.addRow("模型名", self.model_name_edit)
    llm_layout.addLayout(llm_form)

    llm_actions = QHBoxLayout()
    llm_actions.setContentsMargins(0, 0, 0, 0)
    llm_actions.setSpacing(6)
    llm_test_btn = PrimaryPushButton("测试 LLM")
    llm_test_btn.clicked.connect(self.test_model_connection)
    llm_actions.addWidget(llm_test_btn)
    llm_actions.addStretch(1)
    llm_layout.addLayout(llm_actions)

    llm_hint = QLabel("问答页会直接读取这里的配置。", llm_card)
    llm_hint.setWordWrap(True)
    llm_layout.addWidget(llm_hint)

    split.addWidget(graph_card)
    split.addWidget(llm_card)
    split.setStretchFactor(0, 1)
    split.setStretchFactor(1, 1)

    self.settings_status_label = QLabel("修改后立即生效，推荐页与问答页都会读取这里的配置。", page)
    self.settings_status_label.setObjectName("settingsStatus")
    self.settings_status_label.setWordWrap(True)
    page_layout.addWidget(self.settings_status_label)

  def _build_crawl_page(self, page: QWidget) -> None:
    page_layout = QVBoxLayout(page)
    page_layout.setContentsMargins(8, 8, 8, 8)
    page_layout.setSpacing(6)

    split = QSplitter(Qt.Horizontal, page)
    page_layout.addWidget(split)

    left = QWidget(split)
    right = QWidget(split)
    left.setMaximumWidth(650)
    split.addWidget(left)
    split.addWidget(right)
    split.setStretchFactor(0, 2)
    split.setStretchFactor(1, 5)
    split.setSizes([580, 640])

    left_layout = QVBoxLayout(left)
    left_layout.setContentsMargins(0, 0, 0, 14)
    left_layout.setSpacing(8)

    intro = QLabel("一键运行“扩词 -> 抽取 Wikidata -> 清洗 TTL”的抓取链路。", left)
    intro.setWordWrap(True)
    left_layout.addWidget(intro)

    input_panel = QWidget(left)
    input_panel.setObjectName("roundedArea")
    input_panel.setMaximumHeight(560)
    input_layout = QGridLayout(input_panel)
    input_layout.setContentsMargins(0, 0, 0, 0)
    input_layout.setHorizontalSpacing(8)
    input_layout.setVerticalSpacing(4)
    input_layout.setColumnStretch(1, 1)
    left_layout.addWidget(input_panel, 0, Qt.AlignTop)

    self.crawl_lang_combo = self._make_combo(["zh", "en"], "zh")
    self.crawl_seeds_file_edit = QLineEdit(str(self.project_root / "input_pages.txt"))
    self.crawl_expanded_out_edit = QLineEdit(str(self.project_root / "expanded_pages.txt"))
    self.crawl_ttl_out_edit = QLineEdit(str(self.project_root / "out.ttl"))
    self.crawl_sanitized_out_edit = QLineEdit(str(self.project_root / "out.sanitized.ttl"))
    self.crawl_depth_edit = QLineEdit("1")
    self.crawl_per_keyword_edit = QLineEdit("50")
    self.crawl_max_titles_edit = QLineEdit("0")
    self.crawl_max_links_edit = QLineEdit("200")
    self.crawl_sleep_edit = QLineEdit("0.2")
    self.crawl_user_agent_edit = QLineEdit(DEFAULT_CRAWL_USER_AGENT)
    self.crawl_include_qualifiers_checkbox = QCheckBox("导出修饰信息")
    self.crawl_skip_sanitize_checkbox = QCheckBox("跳过时间清洗")
    self.crawl_include_qualifiers_checkbox.setChecked(False)
    self.crawl_skip_sanitize_checkbox.setChecked(False)

    self._add_form_row(input_layout, 0, "语言", self.crawl_lang_combo)
    self._add_path_row(input_layout, 1, "种子文件", self.crawl_seeds_file_edit, browse_mode="open")
    self._add_path_row(input_layout, 2, "扩词输出", self.crawl_expanded_out_edit, browse_mode="save")
    self._add_path_row(input_layout, 3, "TTL 输出", self.crawl_ttl_out_edit, browse_mode="save")
    self._add_path_row(input_layout, 4, "清洗后 TTL", self.crawl_sanitized_out_edit, browse_mode="save")
    self._add_form_row(input_layout, 5, "扩词深度", self.crawl_depth_edit)
    self._add_form_row(input_layout, 6, "每个关键词上限", self.crawl_per_keyword_edit)
    self._add_form_row(input_layout, 7, "最大标题数", self.crawl_max_titles_edit)
    self._add_form_row(input_layout, 8, "最大链接数/种子", self.crawl_max_links_edit)
    self._add_form_row(input_layout, 9, "抓取睡眠(秒)", self.crawl_sleep_edit)
    self._add_form_row(input_layout, 10, "User-Agent", self.crawl_user_agent_edit)
    self._add_form_row(input_layout, 11, "", self.crawl_include_qualifiers_checkbox)
    self._add_form_row(input_layout, 12, "", self.crawl_skip_sanitize_checkbox)

    actions = QHBoxLayout()
    actions.setContentsMargins(0, 0, 0, 0)
    actions.setSpacing(6)
    self.crawl_start_button = PrimaryPushButton("开始抓取并导出 TTL")
    self.crawl_stop_button = PushButton("停止任务")
    self.crawl_clear_button = PushButton("清空日志")
    self.crawl_start_button.clicked.connect(self.start_crawl_pipeline)
    self.crawl_stop_button.clicked.connect(self.stop_crawl_pipeline)
    self.crawl_clear_button.clicked.connect(self._clear_crawl_log)
    actions.addWidget(self.crawl_start_button)
    actions.addWidget(self.crawl_stop_button)
    actions.addWidget(self.crawl_clear_button)
    actions.addStretch(1)
    left_layout.addLayout(actions)

    right_layout = QVBoxLayout(right)
    right_layout.setContentsMargins(0, 0, 0, 0)
    right_layout.setSpacing(8)
    right_layout.addWidget(QLabel("执行状态"))
    self.crawl_status_label = QLabel("等待启动。")
    self.crawl_status_label.setObjectName("crawlStatusLabel")
    self.crawl_status_label.setWordWrap(True)
    right_layout.addWidget(self.crawl_status_label)
    right_layout.addWidget(QLabel("运行日志"))
    self.crawl_log = QPlainTextEdit()
    self.crawl_log.setObjectName("crawlLog")
    self.crawl_log.setReadOnly(True)
    self.crawl_log.setLineWrapMode(QPlainTextEdit.NoWrap)
    self.crawl_log.document().setMaximumBlockCount(2000)
    right_layout.addWidget(self.crawl_log, 1)

    self.crawl_process = QProcess(self)
    self.crawl_process.setProcessChannelMode(QProcess.SeparateChannels)
    self.crawl_process.readyReadStandardOutput.connect(self._on_crawl_stdout)
    self.crawl_process.readyReadStandardError.connect(self._on_crawl_stderr)
    self.crawl_process.finished.connect(self._on_crawl_finished)
    error_signal = getattr(self.crawl_process, "errorOccurred", None)
    if error_signal is not None:
      error_signal.connect(self._on_crawl_error)

  def _build_diagnosis_page(self, page: QWidget) -> None:
    page_layout = QVBoxLayout(page)
    page_layout.setContentsMargins(8, 8, 8, 8)
    page_layout.setSpacing(6)

    split = QSplitter(Qt.Horizontal, page)
    page_layout.addWidget(split)

    left = QWidget(split)
    right = QWidget(split)
    left.setMaximumWidth(650)
    split.addWidget(left)
    split.addWidget(right)
    split.setStretchFactor(0, 2)
    split.setStretchFactor(1, 5)
    split.setSizes([580, 640])

    left_layout = QVBoxLayout(left)
    left_layout.setContentsMargins(0, 0, 0, 14)
    left_layout.setSpacing(8)

    intro = QLabel("输入症状或异常现象，系统会结合当前工况和图谱中的失效机理给出问题与方案。", left)
    intro.setWordWrap(True)
    left_layout.addWidget(intro)

    hint = QLabel("提示：诊断会优先读取“推荐选型”页当前工况，再结合症状文本做图谱检索。", left)
    hint.setWordWrap(True)
    left_layout.addWidget(hint)

    input_panel = QWidget(left)
    input_panel.setObjectName("diagnosisCard")
    input_layout = QVBoxLayout(input_panel)
    input_layout.setContentsMargins(16, 16, 16, 16)
    input_layout.setSpacing(8)

    input_label = QLabel("症状描述")
    input_layout.addWidget(input_label)
    self.diagnosis_input = QPlainTextEdit(input_panel)
    self.diagnosis_input.setObjectName("diagnosisInput")
    self.diagnosis_input.setPlaceholderText("例如：渣线掉块严重，裂纹很多，烘炉后继续开裂。")
    self.diagnosis_input.setPlainText("渣线出现裂纹、掉块，热震后加剧。")
    self.diagnosis_input.setMinimumHeight(120)
    input_layout.addWidget(self.diagnosis_input)

    example_label = QLabel("常见症状示例")
    input_layout.addWidget(example_label)
    example_grid = QGridLayout()
    example_grid.setHorizontalSpacing(8)
    example_grid.setVerticalSpacing(8)

    diagnosis_examples = [
      "渣线出现裂纹、掉块，热震后加剧。",
      "渣线被快速侵蚀，渣皮不稳，局部发白。",
      "金属液接触处出现渗透、疏松和渗漏。",
      "烘炉后粉化、受潮开裂，强度很差。",
      "氧化气氛下材料发红、掉碳、表层粉化。",
      "表面被磨损冲刷，出现明显坑槽和减薄。",
    ]
    for idx, example_text in enumerate(diagnosis_examples):
      button = PushButton(example_text)
      button.clicked.connect(partial(self._use_diagnosis_example, example_text))
      example_grid.addWidget(button, idx // 2, idx % 2)
    input_layout.addLayout(example_grid)

    actions = QHBoxLayout()
    actions.setContentsMargins(0, 0, 0, 0)
    actions.setSpacing(6)
    self.diagnosis_run_button = PrimaryPushButton("识别问题并给出方案")
    self.diagnosis_clear_button = PushButton("清空")
    self.diagnosis_run_button.clicked.connect(self.run_diagnosis)
    self.diagnosis_clear_button.clicked.connect(self._clear_diagnosis_view)
    actions.addWidget(self.diagnosis_run_button)
    actions.addWidget(self.diagnosis_clear_button)
    actions.addStretch(1)
    input_layout.addLayout(actions)

    left_layout.addWidget(input_panel, 0, Qt.AlignTop)
    left_layout.addStretch(1)

    right_layout = QVBoxLayout(right)
    right_layout.setContentsMargins(0, 0, 0, 0)
    right_layout.setSpacing(8)
    right_layout.addWidget(QLabel("诊断结果"))
    self.diagnosis_status_label = QLabel("等待输入症状。")
    self.diagnosis_status_label.setWordWrap(True)
    right_layout.addWidget(self.diagnosis_status_label)
    self.diagnosis_output = QPlainTextEdit()
    self.diagnosis_output.setObjectName("diagnosisOutput")
    self.diagnosis_output.setReadOnly(True)
    self.diagnosis_output.setLineWrapMode(QPlainTextEdit.NoWrap)
    right_layout.addWidget(self.diagnosis_output, 1)

  def _make_combo(self, items: List[str], default: str) -> QComboBox:
    combo = QComboBox()
    combo.addItems(items)
    index = combo.findText(default)
    if index >= 0:
      combo.setCurrentIndex(index)
    return combo

  def _build_recommendation_page(self, page: QWidget) -> None:
    page_layout = QVBoxLayout(page)
    page_layout.setContentsMargins(8, 8, 8, 8)
    page_layout.setSpacing(6)

    split = QSplitter(Qt.Horizontal, page)
    page_layout.addWidget(split)

    left = QWidget(split)
    right = QWidget(split)
    left.setMaximumWidth(600)
    split.addWidget(left)
    split.addWidget(right)
    split.setStretchFactor(0, 2)
    split.setStretchFactor(1, 5)
    split.setSizes([560, 660])

    left_layout = QVBoxLayout(left)
    left_layout.setContentsMargins(0, 0, 0, 14)
    left_layout.setSpacing(6)

    input_panel = QWidget(left)
    input_panel.setMaximumHeight(560)
    input_layout = QGridLayout(input_panel)
    input_layout.setContentsMargins(0, 0, 0, 0)
    input_layout.setHorizontalSpacing(8)
    input_layout.setVerticalSpacing(4)
    input_layout.setColumnStretch(1, 1)
    left_layout.addWidget(input_panel, 0, Qt.AlignTop)

    self.furnace_combo = self._make_combo(list(FURNACE_OPTIONS.keys()), "电弧炉")
    self.zone_combo = self._make_combo(list(ZONE_OPTIONS.keys()), "渣线")
    self.atm_combo = self._make_combo(list(ATMOSPHERE_OPTIONS.keys()), "还原")
    self.slag_type_combo = self._make_combo(list(SLAG_TYPE_OPTIONS.keys()), "碱性渣")
    self.op_mode_combo = self._make_combo(list(OPERATION_MODE_OPTIONS.keys()), "间歇式")
    self.abrasion_combo = self._make_combo(ABRASION_OPTIONS, "High")

    self.tmax_edit = QLineEdit("1650")
    self.ts_edit = QLineEdit("2")
    self.campaign_edit = QLineEdit("1800")
    self.shell_edit = QLineEdit("320")

    self.cao_edit = QLineEdit("48")
    self.sio2_edit = QLineEdit("24")
    self.al2o3_edit = QLineEdit("18")
    self.mgo_edit = QLineEdit("10")

    self._add_form_row(input_layout, 0, "炉型", self.furnace_combo)
    self._add_form_row(input_layout, 1, "炉衬部位", self.zone_combo)
    self._add_form_row(input_layout, 2, "最高温度(℃)", self.tmax_edit)
    self._add_form_row(input_layout, 3, "气氛", self.atm_combo)
    self._add_form_row(input_layout, 4, "渣型", self.slag_type_combo)
    self._add_form_row(input_layout, 5, "运行模式", self.op_mode_combo)

    slag_widget = QWidget(input_panel)
    slag_layout = QHBoxLayout(slag_widget)
    slag_layout.setContentsMargins(0, 0, 0, 0)
    slag_layout.addWidget(QLabel("CaO"))
    slag_layout.addWidget(self.cao_edit)
    slag_layout.addWidget(QLabel("SiO2"))
    slag_layout.addWidget(self.sio2_edit)
    slag_layout.addWidget(QLabel("Al2O3"))
    slag_layout.addWidget(self.al2o3_edit)
    slag_layout.addWidget(QLabel("MgO"))
    slag_layout.addWidget(self.mgo_edit)
    self._add_form_row(input_layout, 6, "渣系成分(%)", slag_widget)

    self._add_form_row(input_layout, 7, "热震频率(次/天)", self.ts_edit)
    self._add_form_row(input_layout, 8, "冲刷/磨损", self.abrasion_combo)
    self._add_form_row(input_layout, 9, "目标炉龄(炉次)", self.campaign_edit)
    self._add_form_row(input_layout, 10, "壳体温度上限(℃)", self.shell_edit)

    self.metal_contact_checkbox = QCheckBox("是否接触金属液")
    self.metal_contact_checkbox.setChecked(True)
    self._add_form_row(input_layout, 11, "", self.metal_contact_checkbox)

    input_layout.addWidget(QLabel("温度曲线（文本说明）"), 12, 0)
    self.temp_curve_text = QPlainTextEdit()
    self.temp_curve_text.setPlainText("室温升至1650℃，保温40分钟，间歇启停。")
    self.temp_curve_text.setFixedHeight(92)
    input_layout.addWidget(self.temp_curve_text, 12, 1)

    actions = QHBoxLayout()
    actions.setContentsMargins(0, 0, 0, 0)
    actions.setSpacing(6)
    settings_btn = PushButton("集中设置")
    run_btn = PrimaryPushButton("查询GraphDB并生成推荐")
    clear_btn = PushButton("清空输出")
    settings_btn.clicked.connect(self._open_settings_page)
    run_btn.clicked.connect(self.run_recommendation)
    clear_btn.clicked.connect(self.clear_output)
    actions.addWidget(settings_btn)
    actions.addWidget(run_btn)
    actions.addWidget(clear_btn)

    self.show_debug_checkbox = QCheckBox("显示开发调试JSON")
    actions.addWidget(self.show_debug_checkbox)
    actions.addStretch(1)

    left_layout.addStretch(1)
    left_layout.addLayout(actions)

    right_layout = QVBoxLayout(right)
    right_layout.addWidget(QLabel("推荐结果（可解释）"))
    self.output = QPlainTextEdit()
    self.output.setReadOnly(True)
    right_layout.addWidget(self.output)

  def _build_encyclopedia_page(self, page: QWidget) -> None:
    page_layout = QVBoxLayout(page)
    page_layout.setContentsMargins(8, 8, 8, 8)
    page_layout.setSpacing(6)

    input_panel = QWidget(page)
    input_panel.setMaximumHeight(110)
    input_layout = QGridLayout(input_panel)
    input_layout.setColumnStretch(1, 1)
    page_layout.addWidget(input_panel, 0)

    self.wiki_ms_combo = self._make_combo(self.ms_choice_values, "最近推荐体系")
    self.wiki_component_input = QLineEdit()

    self._add_form_row(input_layout, 0, "材质体系", self.wiki_ms_combo)
    self._add_form_row(input_layout, 1, "组件输入(Q号/IRI/关键词)", self.wiki_component_input)

    action_widget = QWidget(input_panel)
    action_layout = QHBoxLayout(action_widget)
    action_layout.setContentsMargins(0, 0, 0, 0)
    run_btn = PrimaryPushButton("查询组件百科卡")
    clear_btn = PushButton("清空本页输出")
    run_btn.clicked.connect(self.run_component_encyclopedia_query)
    clear_btn.clicked.connect(self._clear_encyclopedia_view)
    action_layout.addWidget(run_btn)
    action_layout.addWidget(clear_btn)
    input_layout.addWidget(action_widget, 1, 2, alignment=Qt.AlignRight | Qt.AlignVCenter)

    split = QSplitter(Qt.Horizontal, page)
    page_layout.addWidget(split, 1)

    list_panel = QWidget(split)
    list_panel.setObjectName("roundedArea")
    list_layout = QVBoxLayout(list_panel)
    list_layout.addWidget(QLabel("结果列表"))
    self.wiki_component_list = QListWidget()
    self.wiki_component_list.setObjectName("roundedList")
    self.wiki_component_list.setMinimumHeight(420)
    self.wiki_component_list.currentRowChanged.connect(self._on_wiki_component_select)
    list_layout.addWidget(self.wiki_component_list)
    split.addWidget(list_panel)

    table_panel = QWidget(split)
    table_panel.setObjectName("roundedArea")
    table_layout = QVBoxLayout(table_panel)
    table_layout.addWidget(QLabel("表格信息"))
    self.wiki_detail_table = QTableWidget(0, 2)
    self.wiki_detail_table.setObjectName("roundedTable")
    self.wiki_detail_table.setMinimumHeight(420)
    self.wiki_detail_table.setHorizontalHeaderLabels(["字段", "值"])
    self.wiki_detail_table.verticalHeader().setVisible(False)
    self.wiki_detail_table.setSelectionMode(QAbstractItemView.NoSelection)
    self.wiki_detail_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
    wiki_header = self.wiki_detail_table.horizontalHeader()
    wiki_header.setSectionResizeMode(0, QHeaderView.ResizeToContents)
    wiki_header.setSectionResizeMode(1, QHeaderView.Stretch)
    table_layout.addWidget(self.wiki_detail_table)
    split.addWidget(table_panel)

    split.setStretchFactor(0, 2)
    split.setStretchFactor(1, 5)

  def _build_peers_page(self, page: QWidget) -> None:
    page_layout = QVBoxLayout(page)
    page_layout.setContentsMargins(8, 8, 8, 8)
    page_layout.setSpacing(6)

    input_panel = QWidget(page)
    input_panel.setMaximumHeight(140)
    input_layout = QGridLayout(input_panel)
    input_layout.setColumnStretch(1, 1)
    page_layout.addWidget(input_panel, 0)

    self.peer_ms_combo = self._make_combo(self.ms_choice_values, "最近推荐体系")
    self.peer_component_combo = QComboBox()
    self.peer_component_input = QLineEdit()

    self._add_form_row(input_layout, 0, "材质体系", self.peer_ms_combo)
    self._add_form_row(input_layout, 1, "组件下拉选择", self.peer_component_combo)
    self._add_form_row(input_layout, 2, "组件输入(Q号/IRI/关键词)", self.peer_component_input)

    action_widget = QWidget(input_panel)
    action_layout = QHBoxLayout(action_widget)
    action_layout.setContentsMargins(0, 0, 0, 0)
    run_btn = PrimaryPushButton("查询同类候选")
    clear_btn = PushButton("清空本页输出")
    run_btn.clicked.connect(self.run_peer_candidate_query)
    clear_btn.clicked.connect(self._clear_peers_view)
    action_layout.addWidget(run_btn)
    action_layout.addWidget(clear_btn)
    input_layout.addWidget(action_widget, 2, 2, alignment=Qt.AlignRight | Qt.AlignVCenter)

    split = QSplitter(Qt.Horizontal, page)
    page_layout.addWidget(split, 1)

    list_panel = QWidget(split)
    list_panel.setObjectName("roundedArea")
    list_layout = QVBoxLayout(list_panel)
    list_layout.addWidget(QLabel("来源组件列表"))
    self.peer_group_list = QListWidget()
    self.peer_group_list.setObjectName("roundedList")
    self.peer_group_list.setMinimumHeight(420)
    self.peer_group_list.currentRowChanged.connect(self._on_peer_group_select)
    list_layout.addWidget(self.peer_group_list)
    split.addWidget(list_panel)

    table_panel = QWidget(split)
    table_panel.setObjectName("roundedArea")
    table_layout = QVBoxLayout(table_panel)
    table_layout.addWidget(QLabel("表格信息"))
    self.peer_detail_table = QTableWidget(0, 5)
    self.peer_detail_table.setObjectName("roundedTable")
    self.peer_detail_table.setMinimumHeight(420)
    self.peer_detail_table.setHorizontalHeaderLabels(["同类候选", "上位类", "化学式", "描述", "百科链接"])
    self.peer_detail_table.verticalHeader().setVisible(False)
    self.peer_detail_table.setSelectionMode(QAbstractItemView.NoSelection)
    self.peer_detail_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
    peer_header = self.peer_detail_table.horizontalHeader()
    peer_header.setSectionResizeMode(0, QHeaderView.ResizeToContents)
    peer_header.setSectionResizeMode(1, QHeaderView.ResizeToContents)
    peer_header.setSectionResizeMode(2, QHeaderView.ResizeToContents)
    peer_header.setSectionResizeMode(3, QHeaderView.Stretch)
    peer_header.setSectionResizeMode(4, QHeaderView.Stretch)
    table_layout.addWidget(self.peer_detail_table)
    split.addWidget(table_panel)

    split.setStretchFactor(0, 2)
    split.setStretchFactor(1, 6)

  def _build_chat_page(self, page: QWidget) -> None:
    page_layout = QHBoxLayout(page)
    page_layout.setContentsMargins(0, 0, 0, 0)
    page_layout.setSpacing(0)

    sidebar = QFrame(page)
    sidebar.setObjectName("chatSidebar")
    sidebar.setFixedWidth(320)
    sidebar_layout = QVBoxLayout(sidebar)
    sidebar_layout.setContentsMargins(18, 18, 18, 18)
    sidebar_layout.setSpacing(14)

    title = QLabel("智能问答推荐", sidebar)
    title.setObjectName("chatTitle")
    subtitle = QLabel("自然语言输入，自动映射到耐火材料选型。", sidebar)
    subtitle.setObjectName("chatSubtitle")
    subtitle.setWordWrap(True)
    sidebar_layout.addWidget(title)
    sidebar_layout.addWidget(subtitle)

    settings_hint_card = QFrame(sidebar)
    settings_hint_card.setObjectName("chatCard")
    settings_hint_layout = QVBoxLayout(settings_hint_card)
    settings_hint_layout.setContentsMargins(16, 16, 16, 16)
    settings_hint_layout.setSpacing(8)
    settings_hint_title = QLabel("统一配置", settings_hint_card)
    settings_hint_title.setObjectName("chatPanelTitle")
    settings_hint_text = QLabel("GraphDB 和 LLM 的配置都在“设置”页集中维护。", settings_hint_card)
    settings_hint_text.setWordWrap(True)
    settings_hint_layout.addWidget(settings_hint_title)
    settings_hint_layout.addWidget(settings_hint_text)
    sidebar_layout.addWidget(settings_hint_card)

    context_card = QFrame(sidebar)
    context_card.setObjectName("chatCard")
    context_layout = QVBoxLayout(context_card)
    context_layout.setContentsMargins(16, 16, 16, 16)
    context_layout.setSpacing(8)
    context_title = QLabel("当前默认工况", context_card)
    context_title.setObjectName("chatPanelTitle")
    self.chat_context_label = QLabel("尚未生成工况上下文。", context_card)
    self.chat_context_label.setObjectName("chatPanelText")
    self.chat_context_label.setWordWrap(True)
    context_layout.addWidget(context_title)
    context_layout.addWidget(self.chat_context_label)
    sidebar_layout.addWidget(context_card)

    action_card = QFrame(sidebar)
    action_card.setObjectName("chatCard")
    action_layout = QVBoxLayout(action_card)
    action_layout.setContentsMargins(16, 16, 16, 16)
    action_layout.setSpacing(8)
    action_title = QLabel("快捷操作", action_card)
    action_title.setObjectName("chatPanelTitle")
    action_layout.addWidget(action_title)

    settings_btn = PushButton("打开设置")
    settings_btn.clicked.connect(self._open_settings_page)
    clear_btn = PushButton("清空对话")
    clear_btn.clicked.connect(self._clear_chat_session)
    action_layout.addWidget(settings_btn)
    action_layout.addWidget(clear_btn)
    sidebar_layout.addWidget(action_card)

    prompt_card = QFrame(sidebar)
    prompt_card.setObjectName("chatCard")
    prompt_layout = QVBoxLayout(prompt_card)
    prompt_layout.setContentsMargins(16, 16, 16, 16)
    prompt_layout.setSpacing(8)
    prompt_title = QLabel("示例问题", prompt_card)
    prompt_title.setObjectName("chatPanelTitle")
    prompt_layout.addWidget(prompt_title)

    for prompt_text in [
      "电弧炉渣线在氧化气氛下怎么选？",
      "高炉渣线抗热震优先选什么体系？",
      "金属液接触且高碱度渣，推荐什么材料？",
    ]:
      button = PushButton(prompt_text)
      button.clicked.connect(partial(self._use_chat_prompt, prompt_text))
      prompt_layout.addWidget(button)
    sidebar_layout.addWidget(prompt_card)
    sidebar_layout.addStretch(1)

    main_area = QFrame(page)
    main_area.setObjectName("chatMainArea")
    main_layout = QVBoxLayout(main_area)
    main_layout.setContentsMargins(0, 0, 0, 0)
    main_layout.setSpacing(12)

    header = QFrame(main_area)
    header.setObjectName("chatHeader")
    header_layout = QVBoxLayout(header)
    header_layout.setContentsMargins(24, 18, 24, 18)
    header_layout.setSpacing(4)
    header_title = QLabel("GraphDB 耐火材料问答", header)
    header_title.setObjectName("chatPanelTitle")
    self.chat_status_label = QLabel("等待输入问题。", header)
    self.chat_status_label.setObjectName("chatPanelText")
    header_layout.addWidget(header_title)
    header_layout.addWidget(self.chat_status_label)
    main_layout.addWidget(header)

    self.chat_transcript_area = QScrollArea(main_area)
    self.chat_transcript_area.setObjectName("chatTranscriptArea")
    self.chat_transcript_area.setAttribute(Qt.WA_StyledBackground, True)
    self.chat_transcript_area.setWidgetResizable(True)
    self.chat_transcript_area.setFrameShape(QFrame.NoFrame)
    self.chat_transcript_area.setStyleSheet(
      """
      QScrollArea {
        background: #ffffff;
        
        border-radius: 20px;
      }
      """
    )

    self.chat_transcript_container = QWidget()
    self.chat_transcript_container.setObjectName("chatTranscriptContainer")
    self.chat_transcript_container.setAttribute(Qt.WA_StyledBackground, True)
    self.chat_transcript_container.setStyleSheet("background: #ffffff;")
    self.chat_transcript_layout = QVBoxLayout(self.chat_transcript_container)
    self.chat_transcript_layout.setContentsMargins(48, 24, 48, 24)
    self.chat_transcript_layout.setSpacing(18)
    self.chat_transcript_layout.addStretch(1)
    self.chat_transcript_area.setWidget(self.chat_transcript_container)
    viewport = self.chat_transcript_area.viewport()
    if viewport is not None:
      viewport.setStyleSheet("background: #ffffff;border-radius: 20px;")
    main_layout.addWidget(self.chat_transcript_area, 1)

    composer = QFrame(main_area)
    composer.setObjectName("chatComposerFrame")
    composer_layout = QHBoxLayout(composer)
    composer_layout.setContentsMargins(18, 18, 18, 18)
    composer_layout.setSpacing(12)

    self.chat_input = ChatComposerEdit(composer)
    self.chat_input.setObjectName("chatComposer")
    self.chat_input.setPlaceholderText("输入问题，Enter 发送，Shift+Enter 换行")
    self.chat_input.setMinimumHeight(92)
    self.chat_input.sendRequested.connect(self.send_chat_question)

    self.chat_send_button = PushButton("发送")
    self.chat_send_button.setObjectName("chatSendButton")
    self.chat_send_button.clicked.connect(self.send_chat_question)

    composer_layout.addWidget(self.chat_input, 1)
    composer_layout.addWidget(self.chat_send_button, 0, Qt.AlignBottom)
    main_layout.addWidget(composer)

    page_layout.addWidget(sidebar)
    page_layout.addWidget(main_area, 1)

    self._clear_chat_session()

  def _clear_crawl_log(self) -> None:
    if hasattr(self, "crawl_log"):
      self.crawl_log.clear()
    if hasattr(self, "crawl_status_label"):
      self.crawl_status_label.setText("等待启动。")
    if hasattr(self, "crawl_start_button"):
      self.crawl_start_button.setEnabled(True)
    if hasattr(self, "crawl_stop_button"):
      self.crawl_stop_button.setEnabled(False)

  def _set_crawl_running_state(self, running: bool) -> None:
    if hasattr(self, "crawl_start_button"):
      self.crawl_start_button.setEnabled(not running)
    if hasattr(self, "crawl_stop_button"):
      self.crawl_stop_button.setEnabled(running)

  def _append_crawl_log(self, text: str, prefix: str = "") -> None:
    if not hasattr(self, "crawl_log"):
      return
    content = text.strip("\n")
    if not content:
      return
    for line in content.splitlines():
      line = line.rstrip()
      if line:
        self.crawl_log.appendPlainText(f"{prefix}{line}" if prefix else line)
    scrollbar = self.crawl_log.verticalScrollBar()
    scrollbar.setValue(scrollbar.maximum())

  def _crawl_int(self, widget: QLineEdit, default_value: int) -> int:
    raw = widget.text().strip()
    if not raw:
      return int(default_value)
    try:
      return int(float(raw))
    except Exception:
      return int(default_value)

  def _crawl_float(self, widget: QLineEdit, default_value: float) -> float:
    raw = widget.text().strip()
    if not raw:
      return float(default_value)
    try:
      return float(raw)
    except Exception:
      return float(default_value)

  def _build_crawl_command(self) -> List[str]:
    script_path = self.project_root / "crawl_refractory_ttl.py"
    if not script_path.exists():
      raise FileNotFoundError(f"未找到抓取脚本: {script_path}")

    seeds_file = self.crawl_seeds_file_edit.text().strip() or "input_pages.txt"
    expanded_out = self.crawl_expanded_out_edit.text().strip() or "expanded_pages.txt"
    ttl_out = self.crawl_ttl_out_edit.text().strip() or "out.ttl"
    sanitized_out = self.crawl_sanitized_out_edit.text().strip() or "out.sanitized.ttl"
    user_agent = self.crawl_user_agent_edit.text().strip() or DEFAULT_CRAWL_USER_AGENT

    command = [
      sys.executable,
      "-u",
      str(script_path),
      "--lang",
      self.crawl_lang_combo.currentText().strip() or "zh",
      "--seeds-file",
      seeds_file,
      "--expanded-out",
      expanded_out,
      "--ttl-out",
      ttl_out,
      "--sanitized-out",
      sanitized_out,
      "--depth",
      str(self._crawl_int(self.crawl_depth_edit, 1)),
      "--per-keyword",
      str(self._crawl_int(self.crawl_per_keyword_edit, 50)),
      "--max-titles",
      str(self._crawl_int(self.crawl_max_titles_edit, 0)),
      "--max-links-per-seed",
      str(self._crawl_int(self.crawl_max_links_edit, 200)),
      "--sleep",
      str(self._crawl_float(self.crawl_sleep_edit, 0.2)),
      "--user-agent",
      user_agent,
    ]

    if self.crawl_include_qualifiers_checkbox.isChecked():
      command.append("--include-qualifiers")
    if self.crawl_skip_sanitize_checkbox.isChecked():
      command.append("--skip-sanitize")

    return command

  def start_crawl_pipeline(self) -> None:
    if hasattr(self, "crawl_process") and self.crawl_process.state() != QProcess.NotRunning:
      QMessageBox.information(self, "任务运行中", "抓取任务正在运行，请先停止或等待完成。")
      return

    try:
      command = self._build_crawl_command()
      self._clear_crawl_log()
      self._set_crawl_running_state(True)
      self.crawl_status_label.setText("正在启动抓取任务……")
      self._append_crawl_log("开始运行耐火材料抓取管线。")
      self._append_crawl_log(f"工作目录: {self.project_root}")
      self._append_crawl_log(f"命令: {json.dumps(command, ensure_ascii=False)}")

      self.crawl_process.setWorkingDirectory(str(self.project_root))
      self.crawl_process.start(command[0], command[1:])
      self.crawl_status_label.setText("抓取任务已启动，等待日志输出……")
    except Exception as e:
      self._set_crawl_running_state(False)
      self.crawl_status_label.setText(f"启动失败：{e}")
      QMessageBox.critical(self, "启动失败", f"无法启动抓取任务:\n{e}")

  def stop_crawl_pipeline(self) -> None:
    if not hasattr(self, "crawl_process") or self.crawl_process.state() == QProcess.NotRunning:
      self.crawl_status_label.setText("当前没有运行中的抓取任务。")
      return

    self._append_crawl_log("[INFO] 用户请求停止任务。")
    self.crawl_status_label.setText("正在停止任务……")
    self.crawl_process.terminate()
    if self.crawl_process.state() != QProcess.NotRunning:
      self.crawl_process.kill()

  def _on_crawl_stdout(self) -> None:
    if not hasattr(self, "crawl_process"):
      return
    data = bytes(self.crawl_process.readAllStandardOutput()).decode("utf-8", errors="replace")
    self._append_crawl_log(data)

  def _on_crawl_stderr(self) -> None:
    if not hasattr(self, "crawl_process"):
      return
    data = bytes(self.crawl_process.readAllStandardError()).decode("utf-8", errors="replace")
    self._append_crawl_log(data, prefix="[stderr] ")

  def _on_crawl_error(self, error: object) -> None:
    self._append_crawl_log(f"[ERROR] 进程异常: {error}")
    self.crawl_status_label.setText(f"抓取任务异常：{error}")
    self._set_crawl_running_state(False)

  def _on_crawl_finished(self, exit_code: int, exit_status: object) -> None:
    self._set_crawl_running_state(False)
    if exit_code == 0:
      self.crawl_status_label.setText("抓取完成，TTL 文件已生成。")
      self._append_crawl_log("[DONE] 抓取任务完成。")
    else:
      self.crawl_status_label.setText(f"抓取失败，退出码 {exit_code}。")
      self._append_crawl_log(f"[ERROR] 抓取任务结束，退出码 {exit_code}，状态 {exit_status}。")

  def _clear_diagnosis_view(self) -> None:
    self._cancel_response_animations()
    if hasattr(self, "diagnosis_input"):
      self.diagnosis_input.clear()
    if hasattr(self, "diagnosis_output"):
      self.diagnosis_output.clear()
    if hasattr(self, "diagnosis_status_label"):
      self.diagnosis_status_label.setText("等待输入症状。")

  def _use_diagnosis_example(self, example_text: str) -> None:
    self._switch_main_page("diagnosis")
    self.diagnosis_input.setPlainText(example_text)
    self.diagnosis_input.setFocus()

  def _normalize_diagnosis_text(self, text: str) -> str:
    return re.sub(r"\s+", "", (text or "")).lower()

  def _score_diagnosis_rule(self, rule: Dict[str, object], symptom_text: str, wc: WorkConditionInput) -> tuple[int, List[str]]:
    score = 0
    evidence: List[str] = []
    normalized_text = self._normalize_diagnosis_text(symptom_text)

    for keyword in rule.get("keywords", []):
      keyword_text = self._normalize_diagnosis_text(str(keyword))
      if keyword_text and keyword_text in normalized_text:
        score += 2
        evidence.append(f"症状关键词「{keyword}」")

    fm = str(rule.get("fm", ""))
    if fm == "ThermalShockSpalling":
      if wc.thermal_shock >= 2.5:
        score += 2
        evidence.append(f"热震频率 {pretty_num_str(str(wc.thermal_shock))} 次/天 偏高")
      if wc.tmax >= 1400:
        score += 1
        evidence.append(f"最高温度 {pretty_num_str(str(wc.tmax))} ℃")
    elif fm == "SlagCorrosion":
      if wc.zone in {"SlagLine", "ImpactZone"}:
        score += 2
        evidence.append(f"当前部位为 {self._label_from_code(ZONE_OPTIONS, wc.zone)}")
      if wc.slag_basicity >= 1.2:
        score += 2
        evidence.append(f"渣碱度 {pretty_num_str(str(wc.slag_basicity))} 偏高")
    elif fm == "AlkaliAttack":
      if wc.slag_basicity >= 0.8:
        score += 1
        evidence.append(f"渣碱度 {pretty_num_str(str(wc.slag_basicity))} 进入易结圈区间")
      if wc.slag_type == "BasicSlag":
        score += 2
        evidence.append("当前渣型为碱性渣")
    elif fm == "AbrasionErosion":
      if wc.abrasion_level == "High":
        score += 2
        evidence.append("磨损等级为 High")
      if wc.zone in {"SlagLine", "ImpactZone", "TapHole"}:
        score += 1
        evidence.append(f"当前部位为 {self._label_from_code(ZONE_OPTIONS, wc.zone)}")
    elif fm == "OxidationBurnout":
      if wc.atmosphere == "Oxidizing":
        score += 2
        evidence.append("当前气氛为氧化性")
      if wc.metal_contact:
        score += 1
        evidence.append("存在金属液接触")
    elif fm == "MetalPenetration":
      if wc.metal_contact:
        score += 3
        evidence.append("存在金属液接触")
      if wc.zone in {"SlagLine", "ImpactZone", "TapHole"}:
        score += 1
        evidence.append(f"当前部位为 {self._label_from_code(ZONE_OPTIONS, wc.zone)}")
    elif fm == "HydrationCracking":
      if wc.operation_mode == "BatchMode":
        score += 1
        evidence.append("间歇式运行更容易受潮/烘炉波动")

    return score, evidence

  def _build_diagnosis_payload(self, symptom_text: str, wc: WorkConditionInput, client: GraphDBClient, analysis: Optional[Dict[str, object]] = None) -> Dict[str, object]:
    analysis = analysis or {}
    candidate_map: Dict[str, Dict[str, object]] = {}
    rule_map = {str(rule.get("fm", "")): rule for rule in DIAGNOSIS_RULES}

    for order, rule in enumerate(DIAGNOSIS_RULES):
      fm = str(rule.get("fm", ""))
      if not fm:
        continue
      score, evidence = self._score_diagnosis_rule(rule, symptom_text, wc)
      if score <= 0:
        continue
      candidate_map[fm] = {
        "fm": fm,
        "label": str(rule.get("label", fm)),
        "score": score,
        "order": order,
        "evidence": evidence,
        "cause": str(rule.get("cause", "")),
        "solution_hint": str(rule.get("solution", "")),
        "from_graph": False,
      }

    analysis_reason = str(analysis.get("reason", "")).strip()
    analysis_summary = str(analysis.get("normalized_symptom", "")).strip()
    analysis_fms: List[str] = []
    raw_analysis_fms = analysis.get("suspected_failure_mechanisms", [])
    if isinstance(raw_analysis_fms, list):
      for item in raw_analysis_fms:
        fm_code = self._resolve_failure_mechanism_code(str(item))
        if fm_code and fm_code not in analysis_fms:
          analysis_fms.append(fm_code)

    for idx, fm_code in enumerate(analysis_fms):
      rule = rule_map.get(fm_code, {})
      candidate = candidate_map.setdefault(
        fm_code,
        {
          "fm": fm_code,
          "label": str(rule.get("label", fm_code)),
          "score": 0,
          "order": len(DIAGNOSIS_RULES) + idx,
          "evidence": [],
          "cause": str(rule.get("cause", "")),
          "solution_hint": str(rule.get("solution", "")),
          "from_graph": False,
        },
      )
      candidate["score"] = max(int(candidate.get("score", 0)), 4)
      evidence = candidate.setdefault("evidence", [])
      if isinstance(evidence, list):
        evidence.insert(0, "LLM 语义理解命中")
        if analysis_reason:
          evidence.append(f"语义原因：{analysis_reason}")

    failure_rows = client.select(build_failure_query(wc))
    for row in failure_rows:
      fm_iri = parse_sparql_binding(row, "fm")
      if not fm_iri:
        continue
      fm_tail = iri_tail(fm_iri)
      fm_label = parse_sparql_binding(row, "fmLabel", rule_map.get(fm_tail, {}).get("label", fm_tail))
      rule = rule_map.get(fm_tail)
      if fm_tail not in candidate_map:
        candidate_map[fm_tail] = {
          "fm": fm_tail,
          "label": fm_label,
          "score": 3,
          "order": len(DIAGNOSIS_RULES),
          "evidence": ["图谱工况规则命中"],
          "cause": str(rule.get("cause", "")) if rule else "",
          "solution_hint": str(rule.get("solution", "")) if rule else "",
          "from_graph": True,
        }
      else:
        candidate = candidate_map[fm_tail]
        candidate["score"] = int(candidate.get("score", 0)) + 2
        evidence = candidate.setdefault("evidence", [])
        if isinstance(evidence, list) and "图谱工况规则命中" not in evidence:
          evidence.append("图谱工况规则命中")
        candidate["from_graph"] = True
        if not candidate.get("label"):
          candidate["label"] = fm_label

    candidates = sorted(
      candidate_map.values(),
      key=lambda item: (-int(item.get("score", 0)), int(item.get("order", 0))),
    )

    failure_iris = [str(item.get("fm", "")) for item in candidates if str(item.get("fm", ""))]
    solution_rows: List[Dict[str, Dict[str, str]]] = []
    if failure_iris:
      solution_rows = client.select(build_diagnosis_solution_query(failure_iris))

    solution_map: Dict[str, Dict[str, object]] = {}
    for row in solution_rows:
      fm_iri = parse_sparql_binding(row, "fm")
      if not fm_iri:
        continue
      fm_tail = iri_tail(fm_iri)
      fm_entry = solution_map.setdefault(
        fm_tail,
        {
          "fm": fm_tail,
          "label": parse_sparql_binding(row, "fmLabel", fm_tail),
          "systems": {},
        },
      )

      ms_iri = parse_sparql_binding(row, "ms")
      if not ms_iri:
        continue
      systems: Dict[str, Dict[str, object]] = fm_entry["systems"]  # type: ignore[assignment]
      ms_entry = systems.setdefault(
        ms_iri,
        {
          "ms": ms_iri,
          "label": parse_sparql_binding(row, "msLabel", iri_tail(ms_iri)),
          "furnace_labels": [],
          "zone_labels": [],
          "specs": {},
        },
      )

      furnace_label = parse_sparql_binding(row, "msFurnaceLabel")
      zone_label = parse_sparql_binding(row, "msZoneLabel")
      if furnace_label and furnace_label not in ms_entry["furnace_labels"]:  # type: ignore[index]
        ms_entry["furnace_labels"].append(furnace_label)  # type: ignore[index]
      if zone_label and zone_label not in ms_entry["zone_labels"]:  # type: ignore[index]
        ms_entry["zone_labels"].append(zone_label)  # type: ignore[index]

      spec_iri = parse_sparql_binding(row, "spec")
      if not spec_iri:
        continue
      specs: Dict[str, Dict[str, object]] = ms_entry["specs"]  # type: ignore[assignment]
      spec_entry = specs.setdefault(
        spec_iri,
        {
          "spec": spec_iri,
          "label": parse_sparql_binding(row, "specLabel", iri_tail(spec_iri)),
          "desc": parse_sparql_binding(row, "specDesc"),
          "rank": parse_sparql_binding(row, "rank"),
          "processes": [],
          "risks": [],
          "constraints": [],
        },
      )

      spec_desc = parse_sparql_binding(row, "specDesc")
      if spec_desc and not spec_entry.get("desc"):
        spec_entry["desc"] = spec_desc
      rank_text = parse_sparql_binding(row, "rank")
      if rank_text and not spec_entry.get("rank"):
        spec_entry["rank"] = rank_text

      proc_label = parse_sparql_binding(row, "procLabel")
      risk_label = parse_sparql_binding(row, "riskLabel")
      constraint_label = parse_sparql_binding(row, "constraintLabel")
      if proc_label and proc_label not in spec_entry["processes"]:  # type: ignore[index]
        spec_entry["processes"].append(proc_label)  # type: ignore[index]
      if risk_label and risk_label not in spec_entry["risks"]:  # type: ignore[index]
        spec_entry["risks"].append(risk_label)  # type: ignore[index]
      if constraint_label and constraint_label not in spec_entry["constraints"]:  # type: ignore[index]
        spec_entry["constraints"].append(constraint_label)  # type: ignore[index]

    for fm_entry in solution_map.values():
      systems = fm_entry.get("systems", {})
      system_items = list(systems.values()) if isinstance(systems, dict) else []
      for system_item in system_items:
        specs = system_item.get("specs", {})
        spec_items = list(specs.values()) if isinstance(specs, dict) else []
        spec_items.sort(key=lambda item: (float(item.get("rank", 999) or 999), str(item.get("label", ""))))
        for spec_item in spec_items:
          spec_item["processes"] = sorted(spec_item.get("processes", []))
          spec_item["risks"] = sorted(spec_item.get("risks", []))
          spec_item["constraints"] = sorted(spec_item.get("constraints", []))
        system_item["specs"] = spec_items
        system_item["furnace_labels"] = sorted(system_item.get("furnace_labels", []))
        system_item["zone_labels"] = sorted(system_item.get("zone_labels", []))
      system_items.sort(key=lambda item: str(item.get("label", "")))
      fm_entry["systems"] = system_items

    if not candidates:
      return {
        "symptom_text": symptom_text,
        "work_condition": wc,
        "candidates": [],
        "failure_rows": failure_rows,
        "solution_rows": solution_rows,
        "out_lines": [
          "=== 问题诊断 ===",
          f"当前工况: {self._describe_work_condition(wc)}",
          f"症状输入: {symptom_text or '未填写'}",
          f"语义理解: {analysis_summary or '未提取到结构化症状'}",
          f"LLM判断: {analysis_reason or '知识图谱暂未查到对应问题。'}",
          "",
          "知识图谱中暂未查到对应失效机理或解决方案。",
        ],
      }

    issue_names = "、".join(str(item.get("label", "")) for item in candidates[:3] if str(item.get("label", "")))
    out_lines: List[str] = [
      "=== 问题诊断 ===",
      f"当前工况: {self._describe_work_condition(wc)}",
      f"症状输入: {symptom_text or '未填写'}",
      f"语义理解: {analysis_summary or '已完成语义判别'}",
      f"识别到的问题: {issue_names}",
      "",
    ]

    for idx, candidate in enumerate(candidates, start=1):
      fm = str(candidate.get("fm", ""))
      label = str(candidate.get("label", fm))
      score = int(candidate.get("score", 0))
      evidence = candidate.get("evidence", [])
      cause = str(candidate.get("cause", ""))
      solution_hint = str(candidate.get("solution_hint", ""))
      out_lines.append(f"[{idx}] {label}")
      out_lines.append(f"  诊断优先级: {score}/10")
      if evidence:
        out_lines.append(f"  触发依据: {'；'.join(str(item) for item in evidence if str(item).strip())}")

      fm_solution = solution_map.get(fm)
      if fm_solution:
        if cause:
          out_lines.append(f"  根因判断: {cause}")
        if solution_hint:
          out_lines.append(f"  快速建议: {solution_hint}")
        out_lines.append("  图谱方案:")
        systems = fm_solution.get("systems", [])
        if isinstance(systems, list) and systems:
          for system_idx, system_item in enumerate(systems[:3], start=1):
            system_label = str(system_item.get("label", ""))
            out_lines.append(f"    {system_idx}. 材质体系: {system_label}")
            furnace_labels = system_item.get("furnace_labels", [])
            zone_labels = system_item.get("zone_labels", [])
            if furnace_labels:
              out_lines.append(f"       适用炉型: {'、'.join(str(item) for item in furnace_labels)}")
            if zone_labels:
              out_lines.append(f"       适用部位: {'、'.join(str(item) for item in zone_labels)}")

            specs = system_item.get("specs", [])
            if isinstance(specs, list) and specs:
              for spec_item in specs[:2]:
                spec_label = str(spec_item.get("label", ""))
                out_lines.append(f"       方案: {spec_label}")
                spec_desc = str(spec_item.get("desc", ""))
                if spec_desc:
                  out_lines.append(f"         配方范围: {spec_desc}")
                processes = spec_item.get("processes", [])
                risks = spec_item.get("risks", [])
                constraints = spec_item.get("constraints", [])
                if processes:
                  out_lines.append(f"         施工工艺: {'、'.join(str(item) for item in processes)}")
                if risks:
                  out_lines.append(f"         风险点: {'、'.join(str(item) for item in risks)}")
                if constraints:
                  out_lines.append(f"         约束条件: {'、'.join(str(item) for item in constraints)}")
        else:
          out_lines.append("    图谱中暂无可用方案。")
      else:
        out_lines.append("  知识图谱中暂未查到对应方案。")

      out_lines.append("")

    return {
      "symptom_text": symptom_text,
      "work_condition": wc,
      "candidates": candidates,
      "failure_rows": failure_rows,
      "solution_rows": solution_rows,
      "solution_map": solution_map,
      "out_lines": out_lines,
    }

  def _switch_main_page(self, page_name: str) -> None:
    target = self.page_widgets.get(page_name, self.recommend_page)
    switcher = getattr(self, "switchTo", None)
    if callable(switcher):
      switcher(target)
    elif hasattr(self, "stackedWidget"):
      self.stackedWidget.setCurrentWidget(target)

    for name, action in self.page_actions.items():
      action.setEnabled(name != page_name)

  def _resolve_ms_iri(self, ms_choice: str) -> str:
    choice = ms_choice.strip()
    if not choice or choice == "最近推荐体系":
      return self.last_ms_iri
    ms_code = MS_OPTIONS.get(choice, "")
    return f"{EX_NS}{ms_code}" if ms_code else ""

  def _extract_iri_from_choice(self, text: str) -> str:
    raw = text.strip()
    if not raw:
      return ""
    if " | " in raw:
      return raw.rsplit(" | ", 1)[-1].strip()
    return normalize_wd_entity_iri(raw)

  def _update_peer_component_choices(self, component_rows: List[Dict[str, Dict[str, str]]]) -> None:
    values: List[str] = []
    seen = set()
    for row in component_rows:
      comp_iri = parse_sparql_binding(row, "comp")
      if not comp_iri or comp_iri in seen:
        continue
      seen.add(comp_iri)
      comp_label = parse_sparql_binding(row, "compLabel", iri_tail(comp_iri))
      values.append(f"{comp_label} | {comp_iri}")

    current = self.peer_component_combo.currentText().strip()
    self.peer_component_combo.clear()
    self.peer_component_combo.addItems(values)
    if not values:
      return
    if current in values:
      self.peer_component_combo.setCurrentText(current)
    else:
      self.peer_component_combo.setCurrentIndex(0)

  def _fill_wiki_detail_table(self, rows: List[tuple[str, str]]) -> None:
    self.wiki_detail_table.setRowCount(len(rows))
    for idx, (field, value) in enumerate(rows):
      self.wiki_detail_table.setItem(idx, 0, QTableWidgetItem(field))
      self.wiki_detail_table.setItem(idx, 1, QTableWidgetItem(value if value else "未提供"))

  def _clear_encyclopedia_view(self) -> None:
    self.component_card_records = []
    self.wiki_component_list.clear()
    self._fill_wiki_detail_table([("状态", "暂无查询结果")])

  def _show_component_card_by_index(self, index: int) -> None:
    if index < 0 or index >= len(self.component_card_records):
      return
    item = self.component_card_records[index]
    self._fill_wiki_detail_table(
      [
        ("组件", item.get("label", "")),
        ("实体IRI", item.get("iri", "")),
        ("描述", item.get("desc", "")),
        ("化学式", item.get("formula", "")),
        ("密度", item.get("density", "")),
        ("熔点", item.get("melt", "")),
        ("百科链接", item.get("wiki", "")),
      ]
    )

  def _on_wiki_component_select(self, index: int) -> None:
    self._show_component_card_by_index(index)

  def _fill_peer_detail_table(self, rows: List[Dict[str, str]]) -> None:
    if not rows:
      self.peer_detail_table.setRowCount(1)
      self.peer_detail_table.setItem(0, 0, QTableWidgetItem("未查询到同类候选"))
      self.peer_detail_table.setItem(0, 1, QTableWidgetItem(""))
      self.peer_detail_table.setItem(0, 2, QTableWidgetItem(""))
      self.peer_detail_table.setItem(0, 3, QTableWidgetItem(""))
      self.peer_detail_table.setItem(0, 4, QTableWidgetItem(""))
      return

    self.peer_detail_table.setRowCount(len(rows))
    for idx, row in enumerate(rows):
      self.peer_detail_table.setItem(idx, 0, QTableWidgetItem(row.get("peerLabel", "")))
      self.peer_detail_table.setItem(idx, 1, QTableWidgetItem(row.get("parentLabel", "")))
      self.peer_detail_table.setItem(idx, 2, QTableWidgetItem(row.get("peerFormula", "")))
      self.peer_detail_table.setItem(idx, 3, QTableWidgetItem(row.get("peerDesc", "")))
      self.peer_detail_table.setItem(idx, 4, QTableWidgetItem(row.get("wiki", "")))

  def _clear_peers_view(self) -> None:
    self.peer_group_labels = []
    self.peer_group_records = {}
    self.peer_group_list.clear()
    self._fill_peer_detail_table([])

  def _show_peer_group_by_index(self, index: int) -> None:
    if index < 0 or index >= len(self.peer_group_labels):
      return
    key = self.peer_group_labels[index]
    self._fill_peer_detail_table(self.peer_group_records.get(key, []))

  def _on_peer_group_select(self, index: int) -> None:
    self._show_peer_group_by_index(index)

  def _use_chat_prompt(self, prompt_text: str) -> None:
    self._switch_main_page("chat")
    self.chat_input.clear()
    self.chat_input.insertPlainText(prompt_text)
    self.chat_input.setFocus()

  def _label_from_code(self, options: Dict[str, str], code: str) -> str:
    for label, option_code in options.items():
      if option_code == code:
        return label
    return code

  def _resolve_option_code(self, value: object, options: Dict[str, str], default_label: str) -> str:
    default_code = options.get(default_label, next(iter(options.values())))
    if isinstance(value, str):
      raw = value.strip()
      if not raw:
        return default_code
      if raw in options.values():
        return raw
      if raw in options:
        return options[raw]
      lowered = raw.lower()
      for label, option_code in options.items():
        if lowered == label.lower() or lowered == option_code.lower():
          return option_code
    return default_code

  def _resolve_abrasion_level(self, value: object, default_value: str) -> str:
    if isinstance(value, str):
      raw = value.strip()
      if not raw:
        return default_value
      normalized = raw.lower()
      if normalized in {"low", "低"}:
        return "Low"
      if normalized in {"medium", "mid", "中"}:
        return "Medium"
      if normalized in {"high", "高"}:
        return "High"
      for option in ABRASION_OPTIONS:
        if normalized == option.lower():
          return option
    return default_value

  def _resolve_bool_value(self, value: object, default_value: bool) -> bool:
    if isinstance(value, bool):
      return value
    if isinstance(value, (int, float)):
      return bool(value)
    if isinstance(value, str):
      raw = value.strip().lower()
      if raw in {"true", "1", "yes", "y", "是", "接触", "需要", "有"}:
        return True
      if raw in {"false", "0", "no", "n", "否", "不", "无", "不接触"}:
        return False
    return default_value

  def _resolve_float_value(self, value: object, default_value: float) -> float:
    if isinstance(value, (int, float)):
      return float(value)
    if isinstance(value, str):
      raw = value.strip()
      if raw:
        try:
          return float(raw)
        except Exception:
          pass
    return float(default_value)

  def _snapshot_chat_defaults(self) -> Dict[str, object]:
    def safe_float(text: str, fallback: float) -> float:
      try:
        return float(text.strip())
      except Exception:
        return fallback

    furnace_label = self.furnace_combo.currentText().strip() or "电弧炉"
    zone_label = self.zone_combo.currentText().strip() or "渣线"
    atmosphere_label = self.atm_combo.currentText().strip() or "还原"
    slag_type_label = self.slag_type_combo.currentText().strip() or "碱性渣"
    operation_mode_label = self.op_mode_combo.currentText().strip() or "间歇式"
    abrasion_label = self.abrasion_combo.currentText().strip() or "High"
    metal_contact = self.metal_contact_checkbox.isChecked()

    cao = safe_float(self.cao_edit.text(), 48.0)
    sio2 = safe_float(self.sio2_edit.text(), 24.0)
    al2o3 = safe_float(self.al2o3_edit.text(), 18.0)
    mgo = safe_float(self.mgo_edit.text(), 10.0)
    tmax = safe_float(self.tmax_edit.text(), 1650.0)
    thermal_shock = safe_float(self.ts_edit.text(), 2.0)
    campaign = safe_float(self.campaign_edit.text(), 1800.0)
    shell_temp = safe_float(self.shell_edit.text(), 320.0)

    summary_text = (
      f"{furnace_label} / {zone_label} / Tmax={pretty_num_str(str(tmax))}℃ / "
      f"气氛={atmosphere_label} / 渣型={slag_type_label} / "
      f"热震={pretty_num_str(str(thermal_shock))}次/天 / 磨损={abrasion_label} / "
      f"金属接触={'是' if metal_contact else '否'}"
    )

    return {
      "furnace_type_label": furnace_label,
      "furnace_type_code": FURNACE_OPTIONS.get(furnace_label, "EAF"),
      "zone_label": zone_label,
      "zone_code": ZONE_OPTIONS.get(zone_label, "SlagLine"),
      "atmosphere_label": atmosphere_label,
      "atmosphere_code": ATMOSPHERE_OPTIONS.get(atmosphere_label, "Reducing"),
      "slag_type_label": slag_type_label,
      "slag_type_code": SLAG_TYPE_OPTIONS.get(slag_type_label, "BasicSlag"),
      "operation_mode_label": operation_mode_label,
      "operation_mode_code": OPERATION_MODE_OPTIONS.get(operation_mode_label, "BatchMode"),
      "abrasion_label": abrasion_label,
      "metal_contact": metal_contact,
      "tmax": tmax,
      "thermal_shock": thermal_shock,
      "target_campaign_heats": campaign,
      "max_shell_temp": shell_temp,
      "temperature_curve": self.temp_curve_text.toPlainText().strip() or "室温升至1650℃，保温40分钟，间歇启停。",
      "cao": cao,
      "sio2": sio2,
      "al2o3": al2o3,
      "mgo": mgo,
      "slag_basicity": round(cao / sio2, 4) if sio2 > 0 else 2.0,
      "summary_text": summary_text,
    }

  def _chat_history_excerpt(self) -> List[Dict[str, str]]:
    return self.chat_history[-CHAT_HISTORY_LIMIT:]

  def _build_semantic_gate_messages(self, mode: str, user_text: str, snapshot: Dict[str, object]) -> List[Dict[str, str]]:
    if mode == "diagnosis":
      allowed_failure_mechanisms = [
        {"code": rule.get("fm", ""), "label": rule.get("label", "")}
        for rule in DIAGNOSIS_RULES
        if rule.get("fm")
      ]
      system_prompt = (
        "你是耐火材料症状语义判别器。"
        "只输出严格 JSON，不要输出 Markdown、说明文字或代码块。"
        "任务是判断用户输入是否属于耐火材料相关症状/失效诊断。"
        "如果不是，请明确判定为 false，并给出简短原因。"
        "返回结构为："
        "{"
        '\"is_refractory_related\": boolean,'
        '\"confidence\": number,'
        '\"reason\": string,'
        '\"normalized_symptom\": string,'
        '\"suspected_failure_mechanisms\": [string],'
        '\"missing_context\": [string]'
        "}"
        "suspected_failure_mechanisms 只能从 allowed_failure_mechanisms 的 code 中选择。"
      )
      user_payload = {
        "mode": "diagnosis",
        "input_text": user_text,
        "work_condition_context": snapshot.get("work_condition_context", ""),
        "allowed_failure_mechanisms": allowed_failure_mechanisms,
      }
    else:
      system_prompt = (
        "你是耐火材料问题语义判别器。"
        "只输出严格 JSON，不要输出 Markdown、说明文字或代码块。"
        "任务是判断用户输入是否属于耐火材料选型、失效分析、施工工艺、材料体系或相关知识图谱查询。"
        "如果不是，请明确判定为 false，并给出简短原因。"
        "返回结构为："
        "{"
        '\"is_refractory_related\": boolean,'
        '\"confidence\": number,'
        '\"reason\": string,'
        '\"question_summary\": string,'
        '\"missing_fields\": [string]'
        "}"
      )
      user_payload = {
        "mode": "chat",
        "input_text": user_text,
        "current_context": snapshot,
      }

    return [
      {"role": "system", "content": system_prompt},
      {"role": "user", "content": json.dumps(user_payload, ensure_ascii=False, indent=2)},
    ]

  def _analyze_semantic_request(self, mode: str, user_text: str, snapshot: Dict[str, object]) -> Dict[str, object]:
    try:
      client = self._model_client()
      response = client.chat(self._build_semantic_gate_messages(mode, user_text, snapshot), temperature=0.0, max_tokens=700)
      payload = _json_extract_object(str(response.get("content", "")))
    except Exception as e:
      return {
        "mode": mode,
        "is_refractory_related": False,
        "confidence": 0.0,
        "reason": f"语义模型不可用或解析失败：{e}",
      }

    try:
      confidence = float(payload.get("confidence", 0.0))
    except Exception:
      confidence = 0.0

    is_related = bool(payload.get("is_refractory_related")) and confidence >= DOMAIN_INTENT_CONFIDENCE_THRESHOLD
    payload["mode"] = mode
    payload["confidence"] = confidence
    payload["is_refractory_related"] = is_related
    payload["reason"] = str(payload.get("reason", "")).strip()

    if mode == "diagnosis":
      mechanisms: List[str] = []
      raw_mechanisms = payload.get("suspected_failure_mechanisms", [])
      if isinstance(raw_mechanisms, list):
        for item in raw_mechanisms:
          code = self._resolve_failure_mechanism_code(str(item))
          if code and code not in mechanisms:
            mechanisms.append(code)
      payload["suspected_failure_mechanisms"] = mechanisms
      payload["normalized_symptom"] = str(payload.get("normalized_symptom", "")).strip()
      raw_missing = payload.get("missing_context", [])
      payload["missing_context"] = [str(item).strip() for item in raw_missing if str(item).strip()] if isinstance(raw_missing, list) else []
    else:
      payload["question_summary"] = str(payload.get("question_summary", "")).strip()
      raw_missing = payload.get("missing_fields", [])
      payload["missing_fields"] = [str(item).strip() for item in raw_missing if str(item).strip()] if isinstance(raw_missing, list) else []

    return payload

  def _format_non_refractory_reply(self, mode: str, analysis: Dict[str, object]) -> str:
    reason = str(analysis.get("reason", "")).strip() or "模型判断不属于耐火材料范围。"
    if mode == "diagnosis":
      return "\n".join([
        "这条症状不属于耐火材料相关问题，我不做硬答。",
        f"判断依据：{reason}",
        "请补充炉衬、渣线、热震、侵蚀、磨损、水化等耐火材料症状。",
      ])

    return "\n".join([
      "这个问题看起来不属于耐火材料/知识图谱范围，我不直接展开。",
      f"判断依据：{reason}",
      "如果你要问耐火材料选型或失效诊断，请补充炉型、炉衬部位、温度、气氛、渣系或损伤现象。",
    ])

  def _resolve_failure_mechanism_code(self, value: str) -> str:
    normalized = self._normalize_diagnosis_text(value)
    if not normalized:
      return ""
    for rule in DIAGNOSIS_RULES:
      fm = str(rule.get("fm", ""))
      label = str(rule.get("label", ""))
      if normalized == self._normalize_diagnosis_text(fm) or normalized == self._normalize_diagnosis_text(label):
        return fm
      if normalized in self._normalize_diagnosis_text(fm) or normalized in self._normalize_diagnosis_text(label):
        return fm
      if self._normalize_diagnosis_text(fm) in normalized or self._normalize_diagnosis_text(label) in normalized:
        return fm
    return ""

  def _build_model_messages(self, question_text: str, snapshot: Dict[str, object]) -> List[Dict[str, str]]:
    system_prompt = (
      "你是耐火材料选型助手，任务是把用户的自然语言问题归一化为结构化工况。"
      "只能输出严格 JSON，不要输出 Markdown、说明性文字或代码块。"
      "如果用户没有明确说明某个字段，请优先使用给定的 defaults。"
      "字段必须尽量使用下列内部编码。"
      "返回结构为："
      "{"
      "\"question_summary\": string,"
      "\"work_condition\": {"
      "\"furnace_type\": string, \"tmax\": number, \"atmosphere\": string,"
      "\"slag_basicity\": number, \"thermal_shock\": number, \"abrasion_level\": string,"
      "\"metal_contact\": boolean, \"zone\": string, \"slag_type\": string,"
      "\"operation_mode\": string, \"target_campaign_heats\": number, \"max_shell_temp\": number,"
      "\"temperature_curve\": string, \"cao\": number, \"sio2\": number,"
      "\"al2o3\": number, \"mgo\": number"
      "},"
      "\"assumptions\": [string],"
      "\"missing_fields\": [string],"
      "\"confidence\": number"
      "}"
      "其中 furnace_type/zone/atmosphere/slag_type/operation_mode 必须使用内部编码值。"
      "abrasion_level 只允许 Low、Medium、High。confidence 取 0 到 1 之间。"
    )

    user_payload = {
      "allowed_values": {
        "furnace_type": list(FURNACE_OPTIONS.values()),
        "zone": list(ZONE_OPTIONS.values()),
        "atmosphere": list(ATMOSPHERE_OPTIONS.values()),
        "slag_type": list(SLAG_TYPE_OPTIONS.values()),
        "operation_mode": list(OPERATION_MODE_OPTIONS.values()),
        "abrasion_level": ABRASION_OPTIONS,
      },
      "defaults": snapshot,
      "recent_dialogue": self._chat_history_excerpt(),
      "user_question": question_text,
    }

    return [
      {"role": "system", "content": system_prompt},
      {"role": "user", "content": json.dumps(user_payload, ensure_ascii=False, indent=2)},
    ]

  def _model_client(self) -> ModelClient:
    api_key = self._model_api_key_value()
    base_url = self._model_base_url_value()
    model = self._model_name_value()
    return ModelClient(api_key, base_url, model)

  def _build_work_condition_from_ai(self, ai_payload: Dict[str, object], snapshot: Dict[str, object]) -> WorkConditionInput:
    work_condition = ai_payload.get("work_condition")
    if not isinstance(work_condition, dict):
      work_condition = {}

    return WorkConditionInput(
      furnace_type=self._resolve_option_code(work_condition.get("furnace_type"), FURNACE_OPTIONS, snapshot["furnace_type_label"]),
      tmax=self._resolve_float_value(work_condition.get("tmax"), float(snapshot["tmax"])),
      atmosphere=self._resolve_option_code(work_condition.get("atmosphere"), ATMOSPHERE_OPTIONS, snapshot["atmosphere_label"]),
      slag_basicity=self._resolve_float_value(work_condition.get("slag_basicity"), float(snapshot["slag_basicity"])),
      thermal_shock=self._resolve_float_value(work_condition.get("thermal_shock"), float(snapshot["thermal_shock"])),
      abrasion_level=self._resolve_abrasion_level(work_condition.get("abrasion_level"), str(snapshot["abrasion_label"])),
      metal_contact=self._resolve_bool_value(work_condition.get("metal_contact"), bool(snapshot["metal_contact"])),
      zone=self._resolve_option_code(work_condition.get("zone"), ZONE_OPTIONS, snapshot["zone_label"]),
      slag_type=self._resolve_option_code(work_condition.get("slag_type"), SLAG_TYPE_OPTIONS, snapshot["slag_type_label"]),
      operation_mode=self._resolve_option_code(work_condition.get("operation_mode"), OPERATION_MODE_OPTIONS, snapshot["operation_mode_label"]),
      target_campaign_heats=self._resolve_float_value(work_condition.get("target_campaign_heats"), float(snapshot["target_campaign_heats"])),
      max_shell_temp=self._resolve_float_value(work_condition.get("max_shell_temp"), float(snapshot["max_shell_temp"])),
      temperature_curve=str(work_condition.get("temperature_curve") or snapshot["temperature_curve"]),
      cao=self._resolve_float_value(work_condition.get("cao"), float(snapshot["cao"])),
      sio2=self._resolve_float_value(work_condition.get("sio2"), float(snapshot["sio2"])),
      al2o3=self._resolve_float_value(work_condition.get("al2o3"), float(snapshot["al2o3"])),
      mgo=self._resolve_float_value(work_condition.get("mgo"), float(snapshot["mgo"])),
    )

  def _clear_chat_session(self) -> None:
    self._cancel_response_animations()
    self.chat_history = []
    while self.chat_transcript_layout.count():
      item = self.chat_transcript_layout.takeAt(0)
      widget = item.widget()
      if widget is not None:
        widget.deleteLater()
    self.chat_transcript_layout.addStretch(1)
    self.chat_context_label.setText("尚未生成工况上下文。")
    self.chat_status_label.setText("等待输入问题。")
    self.chat_input.clear()
    self._append_chat_message(
      "assistant",
      "助手",
      "你好，请描述炉型、部位、温度、气氛和渣系，我会给出选型建议。",
    )

  def _append_chat_message(self, role: str, title: str, content: str) -> ChatBubbleWidget:
    bubble = ChatBubbleWidget(role, title, content, self.chat_transcript_container)
    row = QWidget(self.chat_transcript_container)
    row_layout = QHBoxLayout(row)
    row_layout.setContentsMargins(0, 0, 0, 0)
    row_layout.setSpacing(0)

    if role == "user":
      row_layout.addStretch(1)
      row_layout.addWidget(bubble, 0, Qt.AlignRight)
    else:
      row_layout.addWidget(bubble, 0, Qt.AlignLeft)
      row_layout.addStretch(1)

    insert_at = max(0, self.chat_transcript_layout.count() - 1)
    self.chat_transcript_layout.insertWidget(insert_at, row)
    QtCore.QTimer.singleShot(0, self._scroll_chat_to_bottom)
    return bubble

  def _scroll_chat_to_bottom(self) -> None:
    bar = self.chat_transcript_area.verticalScrollBar()
    bar.setValue(bar.maximum())

  def _trim_chat_history(self) -> None:
    if len(self.chat_history) > CHAT_HISTORY_LIMIT:
      self.chat_history = self.chat_history[-CHAT_HISTORY_LIMIT:]

  def _cancel_response_animations(self) -> None:
    while self._response_timers:
      timer = self._response_timers.pop()
      try:
        timer.stop()
      except Exception:
        pass
      try:
        timer.deleteLater()
      except Exception:
        pass

  def _scroll_plain_text_to_bottom(self, widget: QPlainTextEdit) -> None:
    scrollbar = widget.verticalScrollBar()
    scrollbar.setValue(scrollbar.maximum())

  def _animate_text_stream(
    self,
    text: str,
    apply_fragment,
    *,
    on_finished=None,
    interval_ms: int = 14,
    chunk_size: int = 4,
  ) -> None:
    self._cancel_response_animations()
    final_text = text or ""
    if not final_text:
      apply_fragment("")
      if on_finished is not None:
        on_finished()
      return

    timer = QtCore.QTimer(self)
    timer.setInterval(interval_ms)
    state = {"index": 0, "finished": False}

    def finish() -> None:
      if state["finished"]:
        return
      state["finished"] = True
      try:
        timer.stop()
      except Exception:
        pass
      if timer in self._response_timers:
        self._response_timers.remove(timer)
      try:
        timer.deleteLater()
      except Exception:
        pass
      if on_finished is not None:
        on_finished()

    def tick() -> None:
      if state["finished"]:
        return
      step = max(2, min(6, len(final_text) // 180))
      step = max(step, chunk_size)
      state["index"] = min(len(final_text), state["index"] + step)
      apply_fragment(final_text[: state["index"]])
      if state["index"] >= len(final_text):
        finish()

    timer.timeout.connect(tick)
    self._response_timers.append(timer)
    timer.start()
    QtCore.QTimer.singleShot(0, tick)

  def _animate_plain_text_response(self, widget: QPlainTextEdit, text: str, on_finished=None) -> None:
    def apply_fragment(fragment: str) -> None:
      widget.setPlainText(fragment)
      self._scroll_plain_text_to_bottom(widget)

    self._animate_text_stream(text, apply_fragment, on_finished=on_finished)

  def _animate_chat_response(self, bubble: ChatBubbleWidget, text: str, on_finished=None) -> None:
    def apply_fragment(fragment: str) -> None:
      bubble.set_content(fragment)
      self._scroll_chat_to_bottom()

    self._animate_text_stream(text, apply_fragment, on_finished=on_finished)

  def _recommendation_output_text(self, payload: Dict[str, object], include_debug: bool = False) -> str:
    lines = list(payload.get("out_lines") or [])
    if include_debug:
      raw = payload.get("raw") or {}
      lines.extend([
        "",
        "=== 原始查询结果(JSON, 开发调试) ===",
        json.dumps(raw, ensure_ascii=False, indent=2),
      ])
    return "\n".join(lines)

  def _apply_recommendation_payload(self, payload: Dict[str, object], include_debug: bool = False) -> None:
    self.last_ms_iri = str(payload.get("ms_iri") or "")
    if not payload.get("matched"):
      self.clear_output()
      self.output.setPlainText(self._recommendation_output_text(payload, include_debug))
      self._fill_wiki_detail_table([("状态", "未生成推荐体系，无法联查组件百科卡")])
      self._fill_peer_detail_table([])
      self._update_peer_component_choices([])
      return

    component_rows = payload.get("component_rows") or []
    peer_rows = payload.get("peer_rows") or []
    self._render_component_cards(component_rows)
    self._render_peer_candidates(peer_rows)
    self._update_peer_component_choices(component_rows)
    self.output.setPlainText(self._recommendation_output_text(payload, include_debug))

  def _build_recommendation_payload(self, wc: WorkConditionInput, client: GraphDBClient) -> Dict[str, object]:
    main_rows = client.select(build_main_query(wc))
    if not main_rows:
      return {
        "matched": False,
        "main_rows": main_rows,
        "failure_rows": [],
        "detail_rows": [],
        "layer_rows": [],
        "component_rows": [],
        "peer_rows": [],
        "summary": {},
        "out_lines": ["知识图谱中暂未查到对应推荐结果。可补充或调整：气氛、渣型、炉衬部位、最高温度或金属液接触条件。"],
        "raw": {
          "main": main_rows,
          "failures": [],
          "details": [],
          "layers": [],
          "components": [],
          "peer_candidates": [],
        },
      }

    main = main_rows[0]
    working_spec_iri = parse_sparql_binding(main, "workingSpec")
    ms_iri = parse_sparql_binding(main, "ms")

    failure_rows = client.select(build_failure_query(wc))
    detail_rows = client.select(build_spec_detail_query(working_spec_iri))
    layer_rows = client.select(build_layer_query(working_spec_iri))

    component_rows: List[Dict[str, Dict[str, str]]] = []
    peer_rows: List[Dict[str, Dict[str, str]]] = []
    if ms_iri:
      component_rows = client.select(build_component_encyclopedia_query(ms_iri))
      peer_rows = client.select(build_component_peer_query(ms_iri))

    failures = sorted({parse_sparql_binding(r, "fmLabel") for r in failure_rows if parse_sparql_binding(r, "fmLabel")})
    processes = sorted({parse_sparql_binding(r, "procLabel") for r in detail_rows if parse_sparql_binding(r, "procLabel")})
    risks = sorted({parse_sparql_binding(r, "riskLabel") for r in detail_rows if parse_sparql_binding(r, "riskLabel")})
    constraints = sorted({parse_sparql_binding(r, "constraintLabel") for r in detail_rows if parse_sparql_binding(r, "constraintLabel")})

    layer_lines: List[str] = []
    for row in layer_rows:
      role_raw = parse_sparql_binding(row, "roleLabel", "未标注层位")
      role = ROLE_LABEL_MAP.get(role_raw, role_raw)
      label = parse_sparql_binding(row, "layerLabel", iri_tail(parse_sparql_binding(row, "layer")))
      desc = parse_sparql_binding(row, "layerDesc")
      if desc:
        layer_lines.append(f"- {role}: {label}\n  说明: {desc}")
      else:
        layer_lines.append(f"- {role}: {label}")

    summary = {
      "primary": parse_sparql_binding(main, "workingLabel"),
      "alternative": parse_sparql_binding(main, "altLabel"),
      "system": parse_sparql_binding(main, "msLabel"),
      "confidence": parse_sparql_binding(main, "confidence"),
      "threshold": parse_sparql_binding(main, "thresholdText"),
      "description": parse_sparql_binding(main, "desc"),
    }

    out_lines = [
      "=== 推荐结论 ===",
      f"主推荐: {summary['primary']}",
      f"替代方案: {summary['alternative']}",
      f"推荐体系: {summary['system']}",
      f"推荐置信度: {pretty_num_str(summary['confidence'])}",
      "",
      "=== 材料组合（工作层/隔热层/背衬） ===",
      "\n".join(layer_lines) if layer_lines else "未配置",
      "",
      "=== 工况摘要 ===",
      f"炉型: {self._label_from_code(FURNACE_OPTIONS, wc.furnace_type)}",
      f"部位: {self._label_from_code(ZONE_OPTIONS, wc.zone)}",
      f"运行模式: {self._label_from_code(OPERATION_MODE_OPTIONS, wc.operation_mode)}",
      f"温度曲线: {wc.temperature_curve}",
      f"最高温度: {pretty_num_str(str(wc.tmax))} ℃",
      f"气氛: {self._label_from_code(ATMOSPHERE_OPTIONS, wc.atmosphere)}",
      (
        f"渣系: CaO={pretty_num_str(str(wc.cao))}%, "
        f"SiO2={pretty_num_str(str(wc.sio2))}%, "
        f"Al2O3={pretty_num_str(str(wc.al2o3))}%, "
        f"MgO={pretty_num_str(str(wc.mgo))}% "
        f"(碱度={pretty_num_str(str(wc.slag_basicity))})"
      ),
      f"热震频率: {pretty_num_str(str(wc.thermal_shock))} 次/天",
      f"冲刷/磨损: {wc.abrasion_level}",
      f"接触金属液: {'是' if wc.metal_contact else '否'}",
      "",
      "=== KG 推理链 ===",
      f"工况 -> 失效机理: {'、'.join(failures) if failures else '未触发显式机理'}",
      f"失效机理 -> 候选材质体系: {summary['system']}",
      f"候选体系 -> 主推荐牌号/配方范围: {summary['primary']}",
      f"候选体系 -> 替代方案: {summary['alternative']}",
      "",
      "=== 施工工艺 ===",
      "、".join(processes) if processes else "未配置",
      "",
      "=== 风险点 ===",
      "、".join(risks) if risks else "未配置",
      "",
      "=== 约束条件 ===",
      "、".join(constraints) if constraints else "无",
      "",
      "=== 关键阈值与解释 ===",
      f"置信度: {pretty_num_str(summary['confidence'])}",
    ]

    threshold_items = [x for x in summary["threshold"].split("；") if x]
    if threshold_items:
      out_lines.extend([f"- {item}" for item in threshold_items])
    else:
      out_lines.append("- 无显式阈值描述")

    out_lines.extend([
      "",
      "说明: 推荐基于图谱规则匹配结果，建议结合现场历史寿命与检修窗口复核。",
    ])

    raw = {
      "main": main,
      "failures": failure_rows,
      "details": detail_rows,
      "layers": layer_rows,
      "components": component_rows,
      "peer_candidates": peer_rows,
    }

    return {
      "matched": True,
      "main_rows": main_rows,
      "main": main,
      "working_spec_iri": working_spec_iri,
      "ms_iri": ms_iri,
      "failure_rows": failure_rows,
      "detail_rows": detail_rows,
      "layer_rows": layer_rows,
      "component_rows": component_rows,
      "peer_rows": peer_rows,
      "summary": summary,
      "failures": failures,
      "processes": processes,
      "risks": risks,
      "constraints": constraints,
      "layer_lines": layer_lines,
      "out_lines": out_lines,
      "raw": raw,
    }

  def _format_chat_answer(self, question_text: str, ai_payload: Dict[str, object], wc: WorkConditionInput, payload: Dict[str, object]) -> str:
    question_summary = str(ai_payload.get("question_summary") or question_text)
    assumptions = ai_payload.get("assumptions") if isinstance(ai_payload.get("assumptions"), list) else []
    missing_fields = ai_payload.get("missing_fields") if isinstance(ai_payload.get("missing_fields"), list) else []
    confidence = ai_payload.get("confidence")

    lines = [
      f"我把你的问题理解为：{question_summary}",
      f"当前解析工况：{self._describe_work_condition(wc)}",
    ]

    if assumptions:
      lines.append(f"采用假设：{'；'.join(str(item) for item in assumptions if str(item).strip())}")

    if payload.get("matched"):
      summary = payload.get("summary", {})
      lines.extend([
        "",
        f"推荐体系：{summary.get('system', '未命中')}",
        f"主推荐：{summary.get('primary', '未命中')}",
        f"替代方案：{summary.get('alternative', '未命中')}",
        f"推荐置信度：{pretty_num_str(str(summary.get('confidence', '')))}",
      ])
      if payload.get("failures"):
        lines.append(f"失效机理：{'、'.join(payload.get('failures', []))}")
      if payload.get("processes"):
        lines.append(f"施工工艺：{'、'.join(payload.get('processes', []))}")
      if payload.get("risks"):
        lines.append(f"风险点：{'、'.join(payload.get('risks', []))}")
      if payload.get("constraints"):
        lines.append(f"约束条件：{'、'.join(payload.get('constraints', []))}")
      lines.append("如果你愿意，我可以按寿命优先、成本优先或施工优先继续细化。")
    else:
      lines.extend([
        "",
        str((payload.get("out_lines") or ["知识图谱中暂未查到对应推荐结果。"])[0]),
      ])
      lines.append("你可以补充炉型、炉衬部位、气氛或渣系成分，我会重新解析。")

    if missing_fields:
      lines.append(f"未明确字段：{'、'.join(str(item) for item in missing_fields if str(item).strip())}")
    if isinstance(confidence, (int, float)):
      lines.append(f"语义解析置信度：{pretty_num_str(str(confidence))}")

    return "\n".join(lines)

  def test_model_connection(self) -> None:
    try:
      client = self._model_client()
      response = client.chat([
        {"role": "system", "content": "只回复 OK。"},
        {"role": "user", "content": "OK"},
      ], max_tokens=8)
      if response.get("content"):
        self.chat_status_label.setText("模型连接正常。")
      if hasattr(self, "settings_status_label"):
        self.settings_status_label.setText(f"LLM 连接正常：{self._model_base_url_value()} / {self._model_name_value()}")
      QMessageBox.information(self, "连接成功", "模型接口连接正常。")
    except Exception as e:
      self.chat_status_label.setText(f"模型连接失败：{e}")
      if hasattr(self, "settings_status_label"):
        self.settings_status_label.setText(f"LLM 连接失败：{e}")
      QMessageBox.critical(self, "连接失败", f"模型接口不可用:\n{e}")

  def send_chat_question(self) -> None:
    question_text = self.chat_input.toPlainText().strip()
    if not question_text:
      return

    self._switch_main_page("chat")
    snapshot = self._snapshot_chat_defaults()
    self.chat_context_label.setText(snapshot["summary_text"])
    self.chat_input.clear()

    self._append_chat_message("user", "你", question_text)
    placeholder = self._append_chat_message("assistant", "助手", "正在解析问题并生成推荐……")
    self.chat_status_label.setText("模型正在解析问题……")
    QApplication.processEvents()

    try:
      analysis = self._analyze_semantic_request("chat", question_text, snapshot)
      if not analysis.get("is_refractory_related"):
        refusal_text = self._format_non_refractory_reply("chat", analysis)
        self.chat_status_label.setText("正在生成回复……")
        self._animate_chat_response(
          placeholder,
          refusal_text,
          on_finished=lambda: self.chat_status_label.setText("输入内容不属于耐火材料问题，已停止查询。"),
        )
        self.chat_history.extend([
          {"role": "user", "content": question_text},
          {"role": "assistant", "content": refusal_text},
        ])
        self._trim_chat_history()
        return

      model_client = self._model_client()
      ai_response = model_client.chat(self._build_model_messages(question_text, snapshot), temperature=0.1, max_tokens=1200)
      ai_payload = _json_extract_object(str(ai_response.get("content", "")))
      wc = self._build_work_condition_from_ai(ai_payload, snapshot)
      graphdb_client = self._client()
      payload = self._build_recommendation_payload(wc, graphdb_client)
      self._apply_recommendation_payload(payload)

      reply_text = self._format_chat_answer(question_text, ai_payload, wc, payload)
      self.chat_status_label.setText("正在生成回复……")
      self._animate_chat_response(
        placeholder,
        reply_text,
        on_finished=lambda: self.chat_status_label.setText("已生成推荐。"),
      )
      self.chat_history.extend([
        {"role": "user", "content": question_text},
        {"role": "assistant", "content": reply_text},
      ])
      self._trim_chat_history()
    except Exception as e:
      error_text = f"处理失败：{e}"
      self.chat_status_label.setText("正在生成回复……")
      self._animate_chat_response(
        placeholder,
        error_text,
        on_finished=lambda: self.chat_status_label.setText("问答推荐失败。"),
      )
      self.chat_history.extend([
        {"role": "user", "content": question_text},
        {"role": "assistant", "content": error_text},
      ])
      self._trim_chat_history()

  def _describe_work_condition(self, wc: WorkConditionInput) -> str:
    return (
      f"{self._label_from_code(FURNACE_OPTIONS, wc.furnace_type)} / "
      f"{self._label_from_code(ZONE_OPTIONS, wc.zone)} / "
      f"{pretty_num_str(str(wc.tmax))}℃ / "
      f"气氛={self._label_from_code(ATMOSPHERE_OPTIONS, wc.atmosphere)} / "
      f"渣型={self._label_from_code(SLAG_TYPE_OPTIONS, wc.slag_type)} / "
      f"热震={pretty_num_str(str(wc.thermal_shock))}次/天 / "
      f"磨损={wc.abrasion_level} / "
      f"金属接触={'是' if wc.metal_contact else '否'}"
    )

  def run_component_encyclopedia_query(self) -> None:
    try:
      client = self._client()
      query_input = self.wiki_component_input.text().strip()

      if query_input:
        comp_iri = normalize_wd_entity_iri(query_input)
        if comp_iri:
          rows = client.select(build_component_encyclopedia_for_component_query(comp_iri))
        else:
          rows = client.select(build_component_encyclopedia_by_keyword_query(query_input))
      else:
        ms_iri = self._resolve_ms_iri(self.wiki_ms_combo.currentText())
        if not ms_iri:
          raise ValueError("请先在推荐选型页生成推荐，或在本页手动选择材质体系。")
        rows = client.select(build_component_encyclopedia_query(ms_iri))

      self._render_component_cards(rows)
      self._update_peer_component_choices(rows)
      self._switch_main_page("encyclopedia")
    except Exception as e:
      QMessageBox.critical(self, "执行失败", f"查询组件百科卡失败:\n{e}")

  def run_peer_candidate_query(self) -> None:
    try:
      client = self._client()
      query_input = self.peer_component_input.text().strip()
      selected_component = self._extract_iri_from_choice(self.peer_component_combo.currentText())

      if query_input:
        comp_iri = normalize_wd_entity_iri(query_input)
        if comp_iri:
          rows = client.select(build_component_peer_for_component_query(comp_iri))
        else:
          rows = client.select(build_component_peer_by_keyword_query(query_input))
      elif selected_component:
        rows = client.select(build_component_peer_for_component_query(selected_component))
      else:
        ms_iri = self._resolve_ms_iri(self.peer_ms_combo.currentText())
        if not ms_iri:
          raise ValueError("请先在推荐选型页生成推荐，或在本页手动选择材质体系。")
        rows = client.select(build_component_peer_query(ms_iri))

      self._render_peer_candidates(rows)
      self._switch_main_page("peers")
    except Exception as e:
      QMessageBox.critical(self, "执行失败", f"查询同类候选失败:\n{e}")

  def _render_component_cards(self, component_rows: List[Dict[str, Dict[str, str]]]) -> None:
    self.component_card_records = []
    self.wiki_component_list.clear()

    for row in component_rows:
      comp_iri = parse_sparql_binding(row, "comp")
      comp_label = parse_sparql_binding(row, "compLabel", iri_tail(comp_iri))
      record = {
        "label": comp_label,
        "iri": comp_iri,
        "desc": parse_sparql_binding(row, "compDesc"),
        "formula": parse_sparql_binding(row, "formula"),
        "density": pretty_num_str(parse_sparql_binding(row, "density")),
        "melt": pretty_num_str(parse_sparql_binding(row, "melt")),
        "wiki": parse_sparql_binding(row, "wiki"),
      }
      self.component_card_records.append(record)
      self.wiki_component_list.addItem(f"{comp_label} ({iri_tail(comp_iri)})")

    if not self.component_card_records:
      self._fill_wiki_detail_table([("状态", "未查询到组件百科信息")])
      return

    self.wiki_component_list.setCurrentRow(0)
    self._show_component_card_by_index(0)

  def _render_peer_candidates(self, peer_rows: List[Dict[str, Dict[str, str]]]) -> None:
    grouped: Dict[str, List[Dict[str, str]]] = {}
    for row in peer_rows:
      comp_iri = parse_sparql_binding(row, "comp")
      comp_label = parse_sparql_binding(row, "compLabel", iri_tail(comp_iri))
      group_key = f"{comp_label} ({iri_tail(comp_iri)})"
      grouped.setdefault(group_key, []).append(
        {
          "peerLabel": parse_sparql_binding(row, "peerLabel", iri_tail(parse_sparql_binding(row, "peer"))),
          "parentLabel": parse_sparql_binding(row, "parentLabel"),
          "peerFormula": parse_sparql_binding(row, "peerFormula"),
          "peerDesc": parse_sparql_binding(row, "peerDesc"),
          "wiki": parse_sparql_binding(row, "wiki"),
        }
      )

    self.peer_group_labels = sorted(grouped.keys())
    self.peer_group_records = grouped

    self.peer_group_list.clear()
    self.peer_group_list.addItems(self.peer_group_labels)

    if not self.peer_group_labels:
      self._fill_peer_detail_table([])
      return

    self.peer_group_list.setCurrentRow(0)
    self._show_peer_group_by_index(0)

  def _client(self) -> GraphDBClient:
    base_url = self._graphdb_base_url_value()
    repo = self._graphdb_repository_id_value()
    if not base_url or not repo:
      raise ValueError("请先填写 GraphDB URL 和 Repository。")
    return GraphDBClient(base_url, repo)

  def test_connection(self) -> None:
    try:
      client = self._client()
      probe = "SELECT * WHERE { ?s ?p ?o } LIMIT 1"
      client.select(probe)
      if hasattr(self, "settings_status_label"):
        self.settings_status_label.setText(f"GraphDB 连接正常：{self._graphdb_base_url_value()} / {self._graphdb_repository_id_value()}")
      QMessageBox.information(self, "连接成功", "GraphDB连接正常，可执行查询。")
    except Exception as e:
      if hasattr(self, "settings_status_label"):
        self.settings_status_label.setText(f"GraphDB 连接失败：{e}")
      QMessageBox.critical(self, "连接失败", f"无法连接GraphDB:\n{e}")

  def clear_output(self) -> None:
    self.output.clear()
    self._clear_encyclopedia_view()
    self._clear_peers_view()

  def _collect_input(self) -> WorkConditionInput:
    cao = float(self.cao_edit.text().strip())
    sio2 = float(self.sio2_edit.text().strip())
    if sio2 <= 0:
      raise ValueError("SiO2 必须大于 0，才能计算渣碱度 CaO/SiO2。")

    return WorkConditionInput(
      furnace_type=FURNACE_OPTIONS[self.furnace_combo.currentText()],
      tmax=float(self.tmax_edit.text().strip()),
      atmosphere=ATMOSPHERE_OPTIONS[self.atm_combo.currentText()],
      slag_basicity=round(cao / sio2, 4),
      thermal_shock=float(self.ts_edit.text().strip()),
      abrasion_level=self.abrasion_combo.currentText().strip(),
      metal_contact=self.metal_contact_checkbox.isChecked(),
      zone=ZONE_OPTIONS[self.zone_combo.currentText()],
      slag_type=SLAG_TYPE_OPTIONS[self.slag_type_combo.currentText()],
      operation_mode=OPERATION_MODE_OPTIONS[self.op_mode_combo.currentText()],
      target_campaign_heats=float(self.campaign_edit.text().strip()),
      max_shell_temp=float(self.shell_edit.text().strip()),
      temperature_curve=self.temp_curve_text.toPlainText().strip(),
      cao=cao,
      sio2=sio2,
      al2o3=float(self.al2o3_edit.text().strip()),
      mgo=float(self.mgo_edit.text().strip()),
    )

  def run_recommendation(self) -> None:
    try:
      wc = self._collect_input()
      client = self._client()
      self._switch_main_page("recommendation")

      payload = self._build_recommendation_payload(wc, client)
      self._apply_recommendation_payload(payload, include_debug=self.show_debug_checkbox.isChecked())
    except Exception as e:
      QMessageBox.critical(self, "执行失败", f"生成推荐失败:\n{e}")


def main() -> None:
  qt_app = QApplication.instance() or QApplication([])
  setTheme(Theme.LIGHT)
  window = RefractorySelectorApp()
  window.show()
  qt_app.exec()


if __name__ == "__main__":
  main()
