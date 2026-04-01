from __future__ import annotations

import json
import importlib
import re
from dataclasses import dataclass
from functools import partial
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
QAction = getattr(QtGui, "QAction", QtWidgets.QAction)
QAbstractItemView = QtWidgets.QAbstractItemView
QApplication = QtWidgets.QApplication
QCheckBox = QtWidgets.QCheckBox
QComboBox = QtWidgets.QComboBox
QDialog = QtWidgets.QDialog
QFormLayout = QtWidgets.QFormLayout
QGridLayout = QtWidgets.QGridLayout
QHBoxLayout = QtWidgets.QHBoxLayout
QHeaderView = QtWidgets.QHeaderView
QLabel = QtWidgets.QLabel
QLineEdit = QtWidgets.QLineEdit
QListWidget = QtWidgets.QListWidget
QMessageBox = QtWidgets.QMessageBox
QPlainTextEdit = QtWidgets.QPlainTextEdit
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

WD_ENTITY_PREFIX = "http://www.wikidata.org/entity/"


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


class RefractorySelectorApp(FluentWindow):
  def __init__(self) -> None:
    super().__init__()
    self.setWindowTitle("耐火材料KG选型助手（GraphDB）")
    self.resize(1220, 820)

    self.base_url = "http://localhost:7200"
    self.repository_id = "RefMDB"

    self.last_ms_iri = ""
    self.ms_choice_values = ["最近推荐体系"] + list(MS_OPTIONS.keys())

    self.component_card_records: List[Dict[str, str]] = []
    self.peer_group_labels: List[str] = []
    self.peer_group_records: Dict[str, List[Dict[str, str]]] = {}
    self.page_actions: Dict[str, object] = {}
    self.page_widgets: Dict[str, QWidget] = {}

    self._build_ui()

  def _build_ui(self) -> None:
    self._build_menu_bar()

    self.recommend_page = QWidget(self)
    self.recommend_page.setObjectName("recommendationPage")
    self.encyclopedia_page = QWidget(self)
    self.encyclopedia_page.setObjectName("encyclopediaPage")
    self.peers_page = QWidget(self)
    self.peers_page.setObjectName("peersPage")

    self._build_recommendation_page(self.recommend_page)
    self._build_encyclopedia_page(self.encyclopedia_page)
    self._build_peers_page(self.peers_page)
    self._apply_round_styles()

    self.page_widgets = {
      "recommendation": self.recommend_page,
      "encyclopedia": self.encyclopedia_page,
      "peers": self.peers_page,
    }

    self.addSubInterface(self.recommend_page, FIF.HOME, "推荐选型")
    self.addSubInterface(self.encyclopedia_page, FIF.BOOK_SHELF, "组件百科卡")
    self.addSubInterface(self.peers_page, FIF.LIBRARY, "同类候选")

    self._clear_encyclopedia_view()
    self._clear_peers_view()
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
      """
    )

  def _build_menu_bar(self) -> None:
    menu_bar_fn = getattr(self, "menuBar", None)
    if not callable(menu_bar_fn):
      self.page_actions = {}
      return

    app_menu = menu_bar_fn().addMenu("应用")
    action_test = app_menu.addAction("测试连接")
    action_test.triggered.connect(self._open_connection_dialog)

    page_menu = menu_bar_fn().addMenu("功能选择")
    for page_name, label in [
      ("recommendation", "推荐选型"),
      ("encyclopedia", "组件百科卡"),
      ("peers", "同类候选"),
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
    dialog = QDialog(self)
    dialog.setWindowTitle("GraphDB连接测试")
    dialog.setModal(True)

    layout = QVBoxLayout(dialog)
    form = QFormLayout()
    url_edit = QLineEdit(self.base_url)
    repo_edit = QLineEdit(self.repository_id)
    form.addRow("URL", url_edit)
    form.addRow("Repository", repo_edit)
    layout.addLayout(form)

    button_row = QHBoxLayout()
    button_row.addStretch(1)
    test_btn = PrimaryPushButton("测试连接")
    close_btn = PushButton("关闭")
    button_row.addWidget(test_btn)
    button_row.addWidget(close_btn)
    layout.addLayout(button_row)

    def sync_connection_values() -> None:
      self.base_url = url_edit.text().strip()
      self.repository_id = repo_edit.text().strip()

    def on_test() -> None:
      sync_connection_values()
      self.test_connection()

    test_btn.clicked.connect(on_test)
    close_btn.clicked.connect(dialog.close)
    url_edit.returnPressed.connect(on_test)
    repo_edit.returnPressed.connect(on_test)

    dialog.resize(500, 180)
    self._center_dialog(dialog)
    url_edit.setFocus()
    dialog.exec()

    sync_connection_values()

  def _make_combo(self, values: List[str], default: str) -> QComboBox:
    combo = QComboBox()
    combo.addItems(values)
    if default in values:
      combo.setCurrentText(default)
    return combo

  def _add_form_row(self, layout: QGridLayout, row: int, label: str, widget: QWidget) -> None:
    layout.addWidget(QLabel(label), row, 0)
    layout.addWidget(widget, row, 1)

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
    test_btn = PushButton("测试连接")
    run_btn = PrimaryPushButton("查询GraphDB并生成推荐")
    clear_btn = PushButton("清空输出")
    test_btn.clicked.connect(self._open_connection_dialog)
    run_btn.clicked.connect(self.run_recommendation)
    clear_btn.clicked.connect(self.clear_output)
    actions.addWidget(test_btn)
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
    base_url = self.base_url.strip()
    repo = self.repository_id.strip()
    if not base_url or not repo:
      raise ValueError("请先填写 GraphDB URL 和 Repository。")
    return GraphDBClient(base_url, repo)

  def test_connection(self) -> None:
    try:
      client = self._client()
      probe = "SELECT * WHERE { ?s ?p ?o } LIMIT 1"
      client.select(probe)
      QMessageBox.information(self, "连接成功", "GraphDB连接正常，可执行查询。")
    except Exception as e:
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

      main_rows = client.select(build_main_query(wc))
      if not main_rows:
        self.clear_output()
        self.output.setPlainText("未命中推荐结果。可尝试调整：气氛、渣型、炉衬部位、最高温度或金属液接触条件。")
        self._fill_wiki_detail_table([("状态", "未生成推荐体系，无法联查组件百科卡")])
        self._fill_peer_detail_table([])
        return

      main = main_rows[0]
      working_spec_iri = parse_sparql_binding(main, "workingSpec")
      ms_iri = parse_sparql_binding(main, "ms")
      self.last_ms_iri = ms_iri

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
        f"炉型: {self.furnace_combo.currentText()}",
        f"部位: {self.zone_combo.currentText()}",
        f"运行模式: {self.op_mode_combo.currentText()}",
        f"温度曲线: {wc.temperature_curve}",
        f"最高温度: {pretty_num_str(str(wc.tmax))} ℃",
        f"气氛: {self.atm_combo.currentText()}",
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

      self._render_component_cards(component_rows)
      self._render_peer_candidates(peer_rows)
      self._update_peer_component_choices(component_rows)

      raw = {
        "main": main,
        "failures": failure_rows,
        "details": detail_rows,
        "layers": layer_rows,
        "components": component_rows,
        "peer_candidates": peer_rows,
      }
      if self.show_debug_checkbox.isChecked():
        out_lines.extend([
          "",
          "=== 原始查询结果(JSON, 开发调试) ===",
          json.dumps(raw, ensure_ascii=False, indent=2),
        ])

      self.output.setPlainText("\n".join(out_lines))
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
