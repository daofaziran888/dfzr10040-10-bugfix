"""熔炉工坊数据模型：业务的 dataclass 定义，承载跨模块数据流对象。"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass
class SourceDef:
    """数据源定义：维护多源异构数据的连接信息与认证配置。"""
    source_id: int
    source_code: str
    source_name: str
    source_type: str
    connection_string: str
    auth_method: str
    charset: str
    timezone: str
    access_status: str

    def to_record(self) -> dict[str, Any]:
        return {
            "源编号": self.source_code,
            "源名称": self.source_name,
            "源类型": self.source_type,
            "连接串": self.connection_string,
            "认证方式": self.auth_method,
            "字符编码": self.charset,
            "时区设置": self.timezone,
            "接入状态": self.access_status,
        }


@dataclass
class SchemaMapping:
    """Schema 映射：源表到目标宽表的字段映射、类型转换与缺失值策略。"""
    mapping_id: int
    plan_code: str
    source_table: str
    target_wide_table: str
    field_mapping_json: str
    type_conversion_rules: str
    missing_value_policy: str
    mapping_order: str
    plan_status: str

    def to_record(self) -> dict[str, Any]:
        return {
            "方案编号": self.plan_code,
            "源表名称": self.source_table,
            "目标宽表": self.target_wide_table,
            "字段映射JSON": self.field_mapping_json,
            "类型转换": self.type_conversion_rules,
            "缺失策略": self.missing_value_policy,
            "映射顺序": self.mapping_order,
            "方案状态": self.plan_status,
        }


@dataclass
class WideTableDef:
    """宽表定义：宽表元数据结构、字段清单与生命周期。"""
    table_id: int
    table_code: str
    table_name: str
    field_list: list[str]
    primary_key: str
    partition_key: str
    ttl_days: int
    source_count: int
    last_updated: str

    def to_record(self) -> dict[str, Any]:
        return {
            "表编号": self.table_code,
            "宽表名称": self.table_name,
            "字段列表": ",".join(self.field_list),
            "主键定义": self.primary_key,
            "分区键": self.partition_key,
            "TTL天数": str(self.ttl_days),
            "数据来源数": str(self.source_count),
            "最近更新": self.last_updated,
        }


@dataclass
class FusionJob:
    """融合作业：多源数据融合的编排、关联键与水位线管理。"""
    job_id: int
    job_code: str
    target_wide_table: str
    participant_sources: list[str]
    join_key: str
    fusion_window: str
    watermark_field: str
    output_rows: int = 0
    skipped_rows: int = 0
    merged_records: list[dict[str, Any]] = field(default_factory=list)

    def to_record(self) -> dict[str, Any]:
        return {
            "作业编号": self.job_code,
            "目标宽表": self.target_wide_table,
            "参与数据源": ",".join(self.participant_sources),
            "关联键": self.join_key,
            "融合窗口": self.fusion_window,
            "水位线字段": self.watermark_field,
            "输出行数": str(self.output_rows),
            "跳过行数": str(self.skipped_rows),
        }


@dataclass
class MergeConflictRule:
    """冲突仲裁规则：多源融合时的字段冲突仲裁与优先级排序。"""
    rule_id: int
    rule_code: str
    target_wide_table: str
    conflict_fields: list[str]
    arbitration_strategy: str
    priority_source: str
    time_baseline: str
    conflict_count: int = 0
    rule_status: str = "草稿"

    def resolve(self, records: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """执行冲突仲裁：按优先级来源与时间基准合并记录。"""
        merged: dict[str, dict[str, Any]] = {}
        conflicts: list[dict[str, Any]] = []

        for rec in records:
            key = str(rec.get("_fusion_key", ""))
            if not key:
                continue
            if key in merged:
                existing = merged[key]
                conflict_resolved = self._arbitrate(existing, rec)
                if conflict_resolved is None:
                    conflicts.append({"existing": dict(existing), "incoming": dict(rec), "reason": "仲裁策略无法裁决"})
                else:
                    merged[key] = conflict_resolved
            else:
                merged[key] = dict(rec)

        return list(merged.values()), conflicts

    def _arbitrate(self, a: dict[str, Any], b: dict[str, Any]) -> dict[str, Any] | None:
        strategy = self.arbitration_strategy
        if strategy == "source_priority":
            return a if a.get("_source") == self.priority_source else b
        elif strategy == "latest_timestamp":
            ta = a.get("_timestamp", "")
            tb = b.get("_timestamp", "")
            return a if ta >= tb else b
        elif strategy == "version_select":
            try:
                va = int(a.get("_version", 0))
            except (ValueError, TypeError):
                va = 0
            try:
                vb = int(b.get("_version", 0))
            except (ValueError, TypeError):
                vb = 0
            return a if va >= vb else b
        elif strategy == "merge_complement":
            merged = dict(a)
            for field in self.conflict_fields:
                if not merged.get(field) and b.get(field):
                    merged[field] = b[field]
            return merged
        return None


@dataclass
class QualityGateRule:
    """质量门禁规则：宽表数据的质量检查与阻断策略。"""
    rule_id: int
    rule_code: str
    target_wide_table: str
    check_type: str
    threshold_condition: str
    trigger_action: str
    block_level: str
    checked_rows: int = 0
    last_triggered: str = ""


@dataclass
class SnapshotPartition:
    """分区快照：按日分区快照管理与版本标记。

    归属信息与分区内行键保存在快照上；每次归属确认/移交都会追加一条
    OwnershipRecord，快照只暴露当前归属，历史范围由归属凭据回查。
    """
    partition_id: int
    partition_code: str
    target_wide_table: str
    partition_key_value: str
    record_count: int
    schema_digest: str
    status_mark: str
    dependency_partitions: list[str]
    created_time: str
    owner_party: str = ""
    row_keys: list[str] = field(default_factory=list)
    ownership_history: list["OwnershipRecord"] = field(default_factory=list)

    def to_record(self) -> dict[str, Any]:
        return {
            "分区编号": self.partition_code,
            "目标宽表": self.target_wide_table,
            "分区键值": self.partition_key_value,
            "记录行数": str(self.record_count),
            "Schema摘要": self.schema_digest,
            "状态标记": self.status_mark,
            "依赖分区": ",".join(self.dependency_partitions),
            "创建时间": self.created_time,
        }

    def effective_owner(self, at_time: str = "") -> str:
        """返回指定时刻的实际归属方；不传时刻则返回当前归属方。"""
        current = self.owner_party
        for record in self.ownership_history:
            if not at_time or record.confirmed_at <= at_time:
                current = record.owner_party
        return current


@dataclass
class OwnershipRecord:
    """分区归属确认记录：保留每次确认/移交的实际归属范围。"""
    record_id: int
    record_code: str
    partition_id: int
    partition_code: str
    target_wide_table: str
    owner_party: str
    previous_owner: str
    transfer_kind: str  # 初始确认 / 归属移交
    row_keys: list[str]
    confirmed_at: str

    def to_record(self) -> dict[str, Any]:
        return {
            "归属记录编号": self.record_code,
            "分区编号": self.partition_code,
            "目标宽表": self.target_wide_table,
            "归属方": self.owner_party,
            "原归属方": self.previous_owner,
            "确认类型": self.transfer_kind,
            "归属行范围": ",".join(self.row_keys),
            "确认时间": self.confirmed_at,
        }


@dataclass
class AccessCredential:
    """取数凭据：归属方对特定宽表+分区的行范围取数资格。"""
    credential_id: int
    credential_code: str
    owner_party: str
    target_wide_table: str
    partition_id: int
    partition_code: str
    row_keys: list[str]
    granted_at: str
    valid_until: str
    credential_status: str = "有效"  # 有效 / 已作废
    revoked_at: str = ""
    revoke_reason: str = ""

    def is_active(self, at_time: str = "") -> bool:
        if self.credential_status != "有效":
            return False
        return bool(not at_time or at_time <= self.valid_until)

    def covers(self, target_wide_table: str, partition_id: int,
                row_keys: list[str]) -> bool:
        """单次交付的行必须全部落在同一张凭据覆盖的宽表/分区/行范围内。"""
        if self.target_wide_table != target_wide_table:
            return False
        if self.partition_id != partition_id:
            return False
        scope = set(self.row_keys)
        return all(key in scope for key in row_keys)


@dataclass
class ExportApplication:
    """快照导出申请：按申请识别的取数申请、发放与下载重试。"""
    application_id: int
    application_code: str
    applicant_party: str
    target_wide_table: str
    plan_id: int
    plan_code: str
    partition_id: int
    partition_code: str
    credential_id: int
    credential_code: str
    prepared_row_keys: list[str]
    accepted_at: str
    application_status: str = "已受理"  # 已受理 / 已发放 / 已阻断
    delivered_row_keys: list[str] = field(default_factory=list)
    delivered_at: str = ""
    block_reason: str = ""
    download_token: str = ""
    download_attempts: int = 0
    last_download_at: str = ""

    def to_record(self) -> dict[str, Any]:
        return {
            "申请编号": self.application_code,
            "申请方": self.applicant_party,
            "目标宽表": self.target_wide_table,
            "计划编号": self.plan_code,
            "分区编号": self.partition_code,
            "取数凭据": self.credential_code,
            "申请状态": self.application_status,
            "备妥行数": str(len(self.prepared_row_keys)),
            "已发放行数": str(len(self.delivered_row_keys)),
            "阻断原因": self.block_reason,
        }


@dataclass
class BackfillTask:
    """数据回填任务：历史数据的批量回填与断点续传。"""
    task_id: int
    task_code: str
    target_wide_table: str
    backfill_range: str
    parallel_partitions: int
    dependency_check: str
    checkpoint_position: str = ""
    processed_rows: int = 0
    remaining_rows: int = 0


@dataclass
class ColdStoreStrategy:
    """冷热分层策略：宽表数据的冷热分层与自动迁移。"""
    strategy_id: int
    strategy_code: str
    target_wide_table: str
    hot_data_days: int
    cold_storage_type: str
    migration_window: str
    compression_method: str
    migrated_rows: int = 0
    last_migration: str = ""


@dataclass
class LineageEdge:
    """数据血缘边：宽表字段与源表字段之间的血缘追踪。"""
    edge_id: int
    edge_code: str
    upstream_node: str
    downstream_node: str
    related_fields: list[str]
    transform_function: str
    edge_type: str
    dependency_level: int
    last_traced: str


@dataclass
class MetricAggrRule:
    """指标聚合规则：宽表上的指标预聚合与物化视图。"""
    rule_id: int
    rule_code: str
    target_wide_table: str
    agg_function: str
    group_dimensions: list[str]
    time_granularity: str
    precompute_window: str
    storage_table: str
    last_computed: str = ""


@dataclass
class PartitionRule:
    """分区策略：宽表的物理分区策略与保留周期。"""
    rule_id: int
    strategy_code: str
    target_wide_table: str
    partition_type: str
    partition_field: str
    retention_days: int
    archive_policy: str
    partition_count: int
    last_adjusted: str


@dataclass
class WatermarkRecord:
    """水位追踪：各数据源的水位线追踪与消费延迟监控。"""
    record_id: int
    record_code: str
    data_source: str
    current_watermark: str
    previous_watermark: str
    advance_interval: str
    delay_minutes: int
    data_origin: str
    last_advance: str


@dataclass
class ExportPlan:
    """快照导出：宽表快照的按需导出与多格式转换。"""
    plan_id: int
    plan_code: str
    target_wide_table: str
    export_format: str
    partition_filter: str
    field_filter: list[str]
    export_path: str
    row_limit: int = 0
    export_status: str = "待导出"


@dataclass
class CrossValidatePlan:
    """交叉校验方案：多宽表间的交叉一致性校验。"""
    plan_id: int
    plan_code: str
    primary_wide_table: str
    reference_wide_table: str
    join_key: str
    validate_fields: list[str]
    deviation_threshold: str
    last_result: str = ""
    validate_status: str = "待校验"


@dataclass
class DataDriftRecord:
    """数据漂移记录：宽表数据的分布漂移检测。"""
    record_id: int
    record_code: str
    wide_table_name: str
    drift_type: str
    historical_baseline: str
    current_distribution: str
    kl_divergence: str
    drift_direction: str
    detected_at: str


@dataclass
class FusionMonitorMetric:
    """融合监控指标：数据融合管道的运行监控。"""
    metric_id: int
    metric_code: str
    fusion_job: str
    monitor_dimension: str
    collection_interval: str
    alert_threshold: str
    alert_channel: str
    silence_window: str
    last_sample: str = ""