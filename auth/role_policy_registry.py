from __future__ import annotations

import logging
from dataclasses import dataclass
from types import MappingProxyType
from typing import Iterable, Mapping

logger = logging.getLogger(__name__)

ROLE_GUEST = "guest"
ROLE_CONSUMER = "consumer"
ROLE_ANALYST = "analyst"
ROLE_ADMINISTRATOR = "administrator"

ACTION_READ = "read"
ACTION_WRITE = "write"
ACTION_EXECUTE = "execute"
ACTION_EXPORT = "export"
ACTION_ADMINISTER = "administer"


@dataclass(frozen=True)
class RoleCapability:
    role_identifier: str
    display_label: str
    permitted_actions: frozenset
    permitted_endpoints: frozenset
    priority_rank: int


@dataclass(frozen=True)
class PolicyDecision:
    permitted: bool
    matched_role: str
    matched_endpoint: str
    required_action: str
    denial_reason: str


_ENDPOINT_ACTION_MATRIX = MappingProxyType({
    "/api/job/start": ACTION_EXECUTE,
    "/api/job/read": ACTION_READ,
    "/api/job/cancel": ACTION_EXECUTE,
    "/api/ingest/rows/read": ACTION_READ,
    "/api/ingest/validation/read": ACTION_READ,
    "/api/data_ingestion_console/data": ACTION_WRITE,
    "/api/data_ingestion_console/action_2": ACTION_EXECUTE,
    "/api/data_ingestion_console/action_3": ACTION_EXECUTE,
    "/api/data_ingestion_console/rfm": ACTION_EXECUTE,
    "/api/cleaning_fusion_pipeline/run_task_cleaning_fusion_pipeline_5": ACTION_EXECUTE,
    "/api/cleaning_fusion_pipeline/run_task_cleaning_fusion_pipeline_9": ACTION_EXECUTE,
    "/api/cleaning_fusion_pipeline/run_task_cleaning_fusion_pipeline_15": ACTION_EXECUTE,
    "/api/cleaning_fusion_pipeline/run_task_cleaning_fusion_pipeline_18": ACTION_EXECUTE,
    "/api/sentiment_topic_mining/analyze": ACTION_EXECUTE,
    "/api/sentiment_topic_mining/export": ACTION_EXPORT,
    "/api/rfm_segmentation_studio/segment": ACTION_EXECUTE,
    "/api/rfm_segmentation_studio/export": ACTION_EXPORT,
    "/api/consumption_forecast_anomaly/forecast": ACTION_EXECUTE,
    "/api/consumption_forecast_anomaly/anomaly_scan": ACTION_EXECUTE,
    "/api/spatial_hotspot_map/aggregate": ACTION_EXECUTE,
    "/api/spatial_hotspot_map/export": ACTION_EXPORT,
    "/api/recommendation_workbench/rank": ACTION_EXPORT,
    "/api/realtime_api_monitor/sample": ACTION_WRITE,
    "/api/realtime_api_monitor/query": ACTION_READ,
    "/api/account/register": ACTION_WRITE,
    "/api/account/login": ACTION_WRITE,
    "/api/account/recover_password": ACTION_WRITE,
    "/api/account/profile": ACTION_READ,
    "/api/audit/login_records": ACTION_READ,
    "/api/audit/operation_records": ACTION_READ,
    "/api/admin/role_binding": ACTION_ADMINISTER,
    "/api/admin/policy_reload": ACTION_ADMINISTER,
})

_GUEST_ENDPOINTS = frozenset({
    "/api/account/register",
    "/api/account/login",
    "/api/account/recover_password",
})

_CONSUMER_ENDPOINTS = frozenset({
    "/api/account/profile",
    "/api/job/read",
    "/api/ingest/validation/read",
    "/api/data_ingestion_console/data",
    "/api/realtime_api_monitor/query",
    "/api/recommendation_workbench/rank",
})

_ANALYST_ENDPOINTS = frozenset({
    "/api/account/profile",
    "/api/job/start",
    "/api/job/read",
    "/api/job/cancel",
    "/api/ingest/rows/read",
    "/api/ingest/validation/read",
    "/api/data_ingestion_console/data",
    "/api/data_ingestion_console/action_2",
    "/api/data_ingestion_console/action_3",
    "/api/data_ingestion_console/rfm",
    "/api/cleaning_fusion_pipeline/run_task_cleaning_fusion_pipeline_5",
    "/api/cleaning_fusion_pipeline/run_task_cleaning_fusion_pipeline_9",
    "/api/cleaning_fusion_pipeline/run_task_cleaning_fusion_pipeline_15",
    "/api/cleaning_fusion_pipeline/run_task_cleaning_fusion_pipeline_18",
    "/api/sentiment_topic_mining/analyze",
    "/api/sentiment_topic_mining/export",
    "/api/rfm_segmentation_studio/segment",
    "/api/rfm_segmentation_studio/export",
    "/api/consumption_forecast_anomaly/forecast",
    "/api/consumption_forecast_anomaly/anomaly_scan",
    "/api/spatial_hotspot_map/aggregate",
    "/api/spatial_hotspot_map/export",
    "/api/recommendation_workbench/rank",
    "/api/realtime_api_monitor/query",
    "/api/realtime_api_monitor/sample",
})

_ADMINISTRATOR_ENDPOINTS = frozenset(_ENDPOINT_ACTION_MATRIX.keys())

_ROLE_CAPABILITY_TABLE = MappingProxyType({
    ROLE_GUEST: RoleCapability(
        role_identifier=ROLE_GUEST,
        display_label="访客",
        permitted_actions=frozenset({ACTION_WRITE, ACTION_READ}),
        permitted_endpoints=_GUEST_ENDPOINTS,
        priority_rank=0,
    ),
    ROLE_CONSUMER: RoleCapability(
        role_identifier=ROLE_CONSUMER,
        display_label="普通用户",
        permitted_actions=frozenset({ACTION_READ, ACTION_WRITE, ACTION_EXECUTE}),
        permitted_endpoints=_CONSUMER_ENDPOINTS,
        priority_rank=10,
    ),
    ROLE_ANALYST: RoleCapability(
        role_identifier=ROLE_ANALYST,
        display_label="分析师",
        permitted_actions=frozenset({ACTION_READ, ACTION_WRITE, ACTION_EXECUTE, ACTION_EXPORT}),
        permitted_endpoints=_ANALYST_ENDPOINTS,
        priority_rank=20,
    ),
    ROLE_ADMINISTRATOR: RoleCapability(
        role_identifier=ROLE_ADMINISTRATOR,
        display_label="管理员",
        permitted_actions=frozenset({
            ACTION_READ, ACTION_WRITE, ACTION_EXECUTE, ACTION_EXPORT, ACTION_ADMINISTER,
        }),
        permitted_endpoints=_ADMINISTRATOR_ENDPOINTS,
        priority_rank=30,
    ),
})

_ROLE_ALIAS_TABLE = MappingProxyType({
    "user": ROLE_CONSUMER,
    "normal_user": ROLE_CONSUMER,
    "member": ROLE_CONSUMER,
    "analytics": ROLE_ANALYST,
    "data_analyst": ROLE_ANALYST,
    "admin": ROLE_ADMINISTRATOR,
    "super_admin": ROLE_ADMINISTRATOR,
    "anonymous": ROLE_GUEST,
})


def normalize_role_identifier(raw_role_identifier: str) -> str:
    if not raw_role_identifier:
        return ROLE_GUEST
    canonical_role = raw_role_identifier.strip().lower()
    canonical_role = _ROLE_ALIAS_TABLE.get(canonical_role, canonical_role)
    if canonical_role not in _ROLE_CAPABILITY_TABLE:
        logger.warning("未知角色标识 %s，降级为访客", raw_role_identifier)
        return ROLE_GUEST
    return canonical_role


def resolve_endpoint_action(endpoint_path: str) -> str:
    if not endpoint_path:
        return ACTION_READ
    normalized_endpoint = endpoint_path.split("?", 1)[0].rstrip("/")
    action = _ENDPOINT_ACTION_MATRIX.get(normalized_endpoint)
    if action is not None:
        return action
    for registered_endpoint, registered_action in _ENDPOINT_ACTION_MATRIX.items():
        if normalized_endpoint.startswith(registered_endpoint):
            return registered_action
    return ACTION_READ


def fetch_role_capability(role_identifier: str) -> RoleCapability:
    canonical_role = normalize_role_identifier(role_identifier)
    return _ROLE_CAPABILITY_TABLE[canonical_role]


def iter_role_capabilities() -> Iterable[RoleCapability]:
    for capability in sorted(_ROLE_CAPABILITY_TABLE.values(), key=lambda item: item.priority_rank):
        yield capability


def evaluate_endpoint_access(role_identifier: str, endpoint_path: str) -> PolicyDecision:
    canonical_role = normalize_role_identifier(role_identifier)
    capability = _ROLE_CAPABILITY_TABLE[canonical_role]
    normalized_endpoint = (endpoint_path or "").split("?", 1)[0].rstrip("/")
    required_action = resolve_endpoint_action(normalized_endpoint)
    if required_action not in capability.permitted_actions:
        return PolicyDecision(
            permitted=False,
            matched_role=canonical_role,
            matched_endpoint=normalized_endpoint,
            required_action=required_action,
            denial_reason=f"角色 {capability.display_label} 不具备 {required_action} 操作能力",
        )
    endpoint_permitted = normalized_endpoint in capability.permitted_endpoints
    if not endpoint_permitted:
        endpoint_permitted = any(
            normalized_endpoint.startswith(registered_endpoint)
            for registered_endpoint in capability.permitted_endpoints
        )
    if not endpoint_permitted:
        return PolicyDecision(
            permitted=False,
            matched_role=canonical_role,
            matched_endpoint=normalized_endpoint,
            required_action=required_action,
            denial_reason=f"角色 {capability.display_label} 未授权访问 {normalized_endpoint}",
        )
    return PolicyDecision(
        permitted=True,
        matched_role=canonical_role,
        matched_endpoint=normalized_endpoint,
        required_action=required_action,
        denial_reason="",
    )


def evaluate_bridge_method_access(role_identifier: str, bridge_method_name: str) -> PolicyDecision:
    if not bridge_method_name:
        return PolicyDecision(
            permitted=False,
            matched_role=normalize_role_identifier(role_identifier),
            matched_endpoint="",
            required_action=ACTION_READ,
            denial_reason="桥接方法名为空，无法完成权限判定",
        )
    endpoint_path = _bridge_method_to_endpoint(bridge_method_name)
    return evaluate_endpoint_access(role_identifier, endpoint_path)


def _bridge_method_to_endpoint(bridge_method_name: str) -> str:
    method_name = bridge_method_name.strip()
    if method_name == "start_job":
        return "/api/job/start"
    if method_name == "cancel_job":
        return "/api/job/cancel"
    if method_name in ("get_job", "list_jobs"):
        return "/api/job/read"
    if method_name == "list_ingestion_rows":
        return "/api/ingest/rows/read"
    if method_name == "get_ingestion_validation":
        return "/api/ingest/validation/read"
    if method_name.startswith("run_task_cleaning_fusion_pipeline_"):
        return f"/api/cleaning_fusion_pipeline/{method_name}"
    if method_name.startswith("run_task_data_ingestion_console_"):
        # 接入台任务统一映射到已注册的写端点（/api/data_ingestion_console/data）
        return "/api/data_ingestion_console/data"
    if method_name.startswith("run_task_sentiment_topic_mining"):
        return "/api/sentiment_topic_mining/analyze"
    if method_name.startswith("run_task_rfm_segmentation_studio"):
        return "/api/rfm_segmentation_studio/segment"
    if method_name.startswith("run_task_consumption_forecast_anomaly"):
        return "/api/consumption_forecast_anomaly/forecast"
    if method_name.startswith("run_task_spatial_hotspot_map"):
        return "/api/spatial_hotspot_map/aggregate"
    if method_name.startswith("run_task_recommendation_workbench"):
        return "/api/recommendation_workbench/rank"
    return f"/api/{method_name}"
