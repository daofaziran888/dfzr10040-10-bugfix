"""熔炉引擎：跨模块数据融合流水线编排器。

负责执行数据源 -> Schema 映射 -> 宽表定义 -> 融合作业的完整链路，
并管理冲突仲裁、质量门禁、分区快照、水位追踪与增量归并。
"""
from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any

from app.models import (
    AccessCredential,
    BackfillTask,
    ColdStoreStrategy,
    CrossValidatePlan,
    DataDriftRecord,
    ExportApplication,
    ExportPlan,
    FusionJob,
    FusionMonitorMetric,
    LineageEdge,
    MergeConflictRule,
    MetricAggrRule,
    OwnershipRecord,
    PartitionRule,
    QualityGateRule,
    SchemaMapping,
    SnapshotPartition,
    SourceDef,
    WatermarkRecord,
    WideTableDef,
)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _generate_code(prefix: str, index: int) -> str:
    return f"{prefix}-{index:04d}"


class FusionEngine:
    """熔炉引擎：编排跨模块数据融合流水线。"""

    def __init__(self) -> None:
        self._sources: dict[int, SourceDef] = {}
        self._schemas: dict[int, SchemaMapping] = {}
        self._wide_tables: dict[int, WideTableDef] = {}
        self._fusion_jobs: dict[int, FusionJob] = {}
        self._conflict_rules: dict[int, MergeConflictRule] = {}
        self._quality_gates: dict[int, QualityGateRule] = {}
        self._snapshots: dict[int, SnapshotPartition] = {}
        self._backfills: dict[int, BackfillTask] = {}
        self._cold_stores: dict[int, ColdStoreStrategy] = {}
        self._lineages: dict[int, LineageEdge] = {}
        self._metrics: dict[int, MetricAggrRule] = {}
        self._partitions: dict[int, PartitionRule] = {}
        self._watermarks: dict[int, WatermarkRecord] = {}
        self._exports: dict[int, ExportPlan] = {}
        self._cross_validates: dict[int, CrossValidatePlan] = {}
        self._drifts: dict[int, DataDriftRecord] = {}
        self._monitors: dict[int, FusionMonitorMetric] = {}
        self._ownership_records: dict[int, OwnershipRecord] = {}
        self._credentials: dict[int, AccessCredential] = {}
        self._export_applications: dict[int, ExportApplication] = {}
        self._next_id: int = 1

    def _next_seq(self) -> int:
        sid = self._next_id
        self._next_id += 1
        return sid

    # ---- 1. source_def -> schema_map -> wide_table_def -> fusion_job ----

    def register_source(self, source_code: str, source_name: str, source_type: str,
                        connection_string: str, auth_method: str = "jdbc",
                        charset: str = "UTF-8", timezone: str = "Asia/Shanghai") -> SourceDef:
        sid = self._next_seq()
        source = SourceDef(
            source_id=sid, source_code=source_code, source_name=source_name,
            source_type=source_type, connection_string=connection_string,
            auth_method=auth_method, charset=charset, timezone=timezone,
            access_status="待验证",
        )
        self._sources[sid] = source
        return source

    def create_schema_mapping(self, source_table: str, target_wide_table: str,
                              field_mapping_json: str, type_conversion: str = "",
                              missing_policy: str = "skip") -> SchemaMapping:
        sid = self._next_seq()
        mapping = SchemaMapping(
            mapping_id=sid, plan_code=_generate_code("SCHE", sid),
            source_table=source_table, target_wide_table=target_wide_table,
            field_mapping_json=field_mapping_json,
            type_conversion_rules=type_conversion,
            missing_value_policy=missing_policy,
            mapping_order="ASC", plan_status="草稿",
        )
        self._schemas[sid] = mapping
        return mapping

    def define_wide_table(self, table_code: str, table_name: str,
                          field_list: list[str], primary_key: str = "id",
                          partition_key: str = "dt", ttl_days: int = 90) -> WideTableDef:
        sid = self._next_seq()
        wt = WideTableDef(
            table_id=sid, table_code=table_code, table_name=table_name,
            field_list=field_list, primary_key=primary_key,
            partition_key=partition_key, ttl_days=ttl_days,
            source_count=0, last_updated=_now_iso(),
        )
        self._wide_tables[sid] = wt
        return wt

    def create_fusion_job(self, target_wide_table: str, participant_sources: list[str],
                          join_key: str, fusion_window: str = "1d",
                          watermark_field: str = "update_time") -> FusionJob:
        sid = self._next_seq()
        job = FusionJob(
            job_id=sid, job_code=_generate_code("FUSI", sid),
            target_wide_table=target_wide_table,
            participant_sources=participant_sources,
            join_key=join_key, fusion_window=fusion_window,
            watermark_field=watermark_field,
            output_rows=0, skipped_rows=0,
        )
        self._fusion_jobs[sid] = job
        return job

    # ---- 2. fusion_job -> merge_conflict -> quality_gate -> snapshot_mgr ----

    def create_conflict_rule(self, target_wide_table: str, conflict_fields: list[str],
                             arbitration_strategy: str = "source_priority",
                             priority_source: str = "", time_baseline: str = "") -> MergeConflictRule:
        sid = self._next_seq()
        rule = MergeConflictRule(
            rule_id=sid, rule_code=_generate_code("MERG", sid),
            target_wide_table=target_wide_table,
            conflict_fields=conflict_fields,
            arbitration_strategy=arbitration_strategy,
            priority_source=priority_source,
            time_baseline=time_baseline or _now_iso(),
            conflict_count=0, rule_status="草稿",
        )
        self._conflict_rules[sid] = rule
        return rule

    def create_quality_gate(self, target_wide_table: str, check_type: str,
                            threshold_condition: str, trigger_action: str = "block",
                            block_level: str = "warning") -> QualityGateRule:
        sid = self._next_seq()
        gate = QualityGateRule(
            rule_id=sid, rule_code=_generate_code("QUAL", sid),
            target_wide_table=target_wide_table,
            check_type=check_type,
            threshold_condition=threshold_condition,
            trigger_action=trigger_action,
            block_level=block_level,
            checked_rows=0, last_triggered="",
        )
        self._quality_gates[sid] = gate
        return gate

    def create_snapshot(self, target_wide_table: str, partition_key_value: str,
                        dependency_partitions: list[str] | None = None,
                        schema_digest: str = "",
                        owner_party: str = "",
                        row_keys: list[str] | None = None) -> SnapshotPartition:
        sid = self._next_seq()
        snap = SnapshotPartition(
            partition_id=sid,
            partition_code=_generate_code("SNAP", sid),
            target_wide_table=target_wide_table,
            partition_key_value=partition_key_value,
            record_count=0,
            schema_digest=schema_digest or f"SHA256-{uuid.uuid4().hex[:8]}",
            status_mark="创建中",
            dependency_partitions=dependency_partitions or [],
            created_time=_now_iso(),
            owner_party=owner_party,
            row_keys=list(row_keys or []),
        )
        self._snapshots[sid] = snap
        return snap

    def stage_partition_rows(self, snapshot_id: int,
                             rows: list[dict[str, Any]],
                             key_field: str = "id") -> dict[str, Any]:
        """把明细行备入分区快照，登记行键与行数，作为后续归属与取数核对范围。"""
        snap = self._snapshots.get(snapshot_id)
        if snap is None:
            return {"ok": False, "message": f"分区快照 {snapshot_id} 不存在"}
        snap.row_keys = [str(row.get(key_field, "")) for row in rows]
        snap.record_count = len(rows)
        return {
            "ok": True,
            "partition_code": snap.partition_code,
            "row_keys": list(snap.row_keys),
            "record_count": snap.record_count,
        }

    # ---- 7b. 分区归属确认与移交 ----

    def confirm_partition_ownership(self, snapshot_id: int,
                                    owner_party: str) -> dict[str, Any]:
        """确认分区的初始归属，并保留该次确认的实际归属范围。"""
        snap = self._snapshots.get(snapshot_id)
        if snap is None:
            return {"ok": False, "message": f"分区快照 {snapshot_id} 不存在"}
        if not owner_party.strip():
            return {"ok": False, "message": "归属方不能为空"}
        if snap.owner_party:
            return {
                "ok": False,
                "message": (
                    f"分区 {snap.partition_code} 已归属 {snap.owner_party}，"
                    "初始确认只能执行一次，更换归属请走归属移交"
                ),
            }
        return self._append_ownership(
            snap, owner_party, previous_owner="", transfer_kind="初始确认",
        )

    def transfer_partition_ownership(self, snapshot_id: int,
                                     from_party: str,
                                     to_party: str) -> dict[str, Any]:
        """合法移交分区归属：校验原归属方后追加新的归属确认记录。"""
        snap = self._snapshots.get(snapshot_id)
        if snap is None:
            return {"ok": False, "message": f"分区快照 {snapshot_id} 不存在"}
        if not snap.owner_party:
            return {
                "ok": False,
                "message": f"分区 {snap.partition_code} 尚未确认初始归属，不能移交",
            }
        if snap.owner_party != from_party:
            return {
                "ok": False,
                "message": (
                    f"移交失败：分区 {snap.partition_code} 当前归属 {snap.owner_party}，"
                    f"与申请移交方 {from_party} 不一致"
                ),
            }
        if not to_party.strip():
            return {"ok": False, "message": "接手方不能为空"}
        if to_party == from_party:
            return {"ok": False, "message": "接手方与原归属方相同，无需移交"}
        return self._append_ownership(
            snap, to_party, previous_owner=from_party, transfer_kind="归属移交",
        )

    def _append_ownership(self, snap: SnapshotPartition, owner_party: str,
                          previous_owner: str, transfer_kind: str) -> dict[str, Any]:
        sid = self._next_seq()
        record = OwnershipRecord(
            record_id=sid,
            record_code=_generate_code("OWNR", sid),
            partition_id=snap.partition_id,
            partition_code=snap.partition_code,
            target_wide_table=snap.target_wide_table,
            owner_party=owner_party,
            previous_owner=previous_owner,
            transfer_kind=transfer_kind,
            row_keys=list(snap.row_keys),
            confirmed_at=_now_iso(),
        )
        snap.ownership_history.append(record)
        snap.owner_party = owner_party
        self._ownership_records[sid] = record
        return {
            "ok": True,
            "ownership_code": record.record_code,
            "partition_code": snap.partition_code,
            "owner_party": owner_party,
            "previous_owner": previous_owner,
            "transfer_kind": transfer_kind,
            "row_keys": list(record.row_keys),
            "confirmed_at": record.confirmed_at,
        }

    def get_ownership_history(self, snapshot_id: int) -> list[dict[str, Any]]:
        """回查分区每次确认的实际归属范围（含移交前后的行范围）。"""
        snap = self._snapshots.get(snapshot_id)
        if snap is None:
            return []
        return [
            {
                "ownership_code": record.record_code,
                "partition_code": record.partition_code,
                "target_wide_table": record.target_wide_table,
                "owner_party": record.owner_party,
                "previous_owner": record.previous_owner,
                "transfer_kind": record.transfer_kind,
                "row_keys": list(record.row_keys),
                "confirmed_at": record.confirmed_at,
            }
            for record in snap.ownership_history
        ]

    def owner_at_time(self, snapshot_id: int, at_time: str) -> str:
        """回查指定时刻分区的归属方，用于判断已受理申请当时的资格。"""
        snap = self._snapshots.get(snapshot_id)
        if snap is None:
            return ""
        owner = ""
        for record in snap.ownership_history:
            if record.confirmed_at <= at_time:
                owner = record.owner_party
        return owner

    # ---- 7c. 取数凭据（取数资格） ----

    def issue_access_credential(self, snapshot_id: int, owner_party: str,
                                valid_until: str = "2999-12-31T23:59:59+00:00"
                                ) -> dict[str, Any]:
        """按当前归属为归属方核发覆盖分区实际行范围的取数凭据。"""
        snap = self._snapshots.get(snapshot_id)
        if snap is None:
            return {"ok": False, "message": f"分区快照 {snapshot_id} 不存在"}
        if not snap.owner_party:
            return {
                "ok": False,
                "message": f"分区 {snap.partition_code} 尚未确认归属，无法核发取数凭据",
            }
        if snap.owner_party != owner_party:
            return {
                "ok": False,
                "message": (
                    f"核发被拒：{owner_party} 不是分区 {snap.partition_code} 的当前归属方"
                    f"（当前归属 {snap.owner_party}），旧归属方不能新申请"
                ),
            }
        sid = self._next_seq()
        credential = AccessCredential(
            credential_id=sid,
            credential_code=_generate_code("CRED", sid),
            owner_party=owner_party,
            target_wide_table=snap.target_wide_table,
            partition_id=snap.partition_id,
            partition_code=snap.partition_code,
            row_keys=list(snap.row_keys),
            granted_at=_now_iso(),
            valid_until=valid_until,
        )
        self._credentials[sid] = credential
        return {
            "ok": True,
            "credential_id": credential.credential_id,
            "credential_code": credential.credential_code,
            "owner_party": credential.owner_party,
            "target_wide_table": credential.target_wide_table,
            "partition_code": credential.partition_code,
            "row_keys": list(credential.row_keys),
            "valid_until": credential.valid_until,
            "credential_status": credential.credential_status,
        }

    def revoke_access_credential(self, credential_id: int,
                                 reason: str = "凭据作废") -> dict[str, Any]:
        """单独作废一张取数凭据；已发放下载的重试将因此被阻断。"""
        credential = self._credentials.get(credential_id)
        if credential is None:
            return {"ok": False, "message": f"取数凭据 {credential_id} 不存在"}
        if credential.credential_status != "有效":
            return {
                "ok": False,
                "message": f"取数凭据 {credential.credential_code} 已处于{credential.credential_status}状态",
            }
        credential.credential_status = "已作废"
        credential.revoked_at = _now_iso()
        credential.revoke_reason = reason
        return {
            "ok": True,
            "credential_code": credential.credential_code,
            "credential_status": credential.credential_status,
            "revoked_at": credential.revoked_at,
            "reason": reason,
        }

    # ---- 7d. 快照导出申请、发放与下载重试 ----

    def accept_export_application(self, export_plan_id: int, snapshot_id: int,
                                  applicant_party: str,
                                  credential_id: int) -> dict[str, Any]:
        """受理导出申请：校验分区归属、宽表一致性与取数凭据后备妥明细行。"""
        plan = self._exports.get(export_plan_id)
        snap = self._snapshots.get(snapshot_id)
        if plan is None:
            return {"ok": False, "message": f"导出计划 {export_plan_id} 不存在"}
        if snap is None:
            return {"ok": False, "message": f"分区快照 {snapshot_id} 不存在"}

        # 修复越权入口：分区必须属于申请注明的宽表，拒绝另一张宽表的分区编号。
        if snap.target_wide_table != plan.target_wide_table:
            return {
                "ok": False,
                "code": "PARTITION_TABLE_MISMATCH",
                "message": (
                    f"分区归属校验失败：分区 {snap.partition_code} 归属宽表 "
                    f"{snap.target_wide_table}，与导出计划 {plan.plan_code} 注明的 "
                    f"{plan.target_wide_table} 不一致，拒绝跨表取数"
                ),
            }

        if not snap.owner_party:
            return {
                "ok": False,
                "code": "OWNERSHIP_UNCONFIRMED",
                "message": f"分区 {snap.partition_code} 尚未确认归属，不能受理导出申请",
            }
        if snap.owner_party != applicant_party:
            return {
                "ok": False,
                "code": "OWNER_MISMATCH",
                "message": (
                    f"取数权限校验失败：{applicant_party} 不是分区 {snap.partition_code} "
                    f"的当前归属方（当前归属 {snap.owner_party}）"
                ),
            }

        credential = self._credentials.get(credential_id)
        if credential is None:
            return {"ok": False, "code": "CREDENTIAL_NOT_FOUND",
                    "message": f"取数凭据 {credential_id} 不存在"}
        if credential.credential_status != "有效":
            return {
                "ok": False, "code": "CREDENTIAL_REVOKED",
                "message": f"取数凭据 {credential.credential_code} 已作废，不能受理新申请",
            }
        if credential.owner_party != applicant_party:
            return {
                "ok": False, "code": "CREDENTIAL_PARTY_MISMATCH",
                "message": (
                    f"取数凭据 {credential.credential_code} 归属 {credential.owner_party}，"
                    f"与申请方 {applicant_party} 不一致"
                ),
            }
        if credential.target_wide_table != snap.target_wide_table \
                or credential.partition_id != snap.partition_id:
            return {
                "ok": False, "code": "CREDENTIAL_SCOPE_MISMATCH",
                "message": (
                    f"取数凭据 {credential.credential_code} 的覆盖范围 "
                    f"{credential.target_wide_table}/{credential.partition_code} "
                    f"与申请分区 {snap.target_wide_table}/{snap.partition_code} 不一致"
                ),
            }

        prepared_row_keys = self._partition_row_keys(snap)
        if not credential.covers(snap.target_wide_table, snap.partition_id,
                                 prepared_row_keys):
            return {
                "ok": False, "code": "CREDENTIAL_SCOPE_MISMATCH",
                "message": (
                    f"取数凭据 {credential.credential_code} 无法覆盖分区 "
                    f"{snap.partition_code} 的全部备妥行，受理被拒"
                ),
            }

        sid = self._next_seq()
        application = ExportApplication(
            application_id=sid,
            application_code=_generate_code("EXPA", sid),
            applicant_party=applicant_party,
            target_wide_table=plan.target_wide_table,
            plan_id=plan.plan_id,
            plan_code=plan.plan_code,
            partition_id=snap.partition_id,
            partition_code=snap.partition_code,
            credential_id=credential.credential_id,
            credential_code=credential.credential_code,
            prepared_row_keys=list(prepared_row_keys),
            accepted_at=_now_iso(),
        )
        self._export_applications[sid] = application
        plan.export_status = "导出中"
        return {
            "ok": True,
            "application_id": application.application_id,
            "application_code": application.application_code,
            "plan_code": plan.plan_code,
            "partition_code": snap.partition_code,
            "target_wide_table": plan.target_wide_table,
            "credential_code": credential.credential_code,
            "prepared_row_keys": list(prepared_row_keys),
            "prepared_rows": len(prepared_row_keys),
            "accepted_at": application.accepted_at,
        }

    def deliver_export_application(self, application_id: int) -> dict[str, Any]:
        """发放检查：用分区归属凭据核对可交付行，处理受理后的归属移交冲突。"""
        application = self._export_applications.get(application_id)
        if application is None:
            return {"ok": False, "message": f"导出申请 {application_id} 不存在"}
        if application.application_status == "已发放":
            return {
                "ok": False, "code": "ALREADY_DELIVERED",
                "message": f"申请 {application.application_code} 已发放，请走下载重试",
            }

        snap = self._snapshots.get(application.partition_id)
        plan = self._exports.get(application.plan_id)
        credential = self._credentials.get(application.credential_id)
        if snap is None or plan is None or credential is None:
            return self._block_application(
                application, "申请关联的分区/计划/凭据已缺失，发放被阻断")

        block = self._check_delivery_eligibility(application, snap, plan, credential)
        if block is not None:
            return self._block_application(application, block)

        records = self._collect_partition_records(snap, application.prepared_row_keys)
        application.delivered_row_keys = list(application.prepared_row_keys)
        application.application_status = "已发放"
        application.delivered_at = _now_iso()
        application.download_token = (
            f"DL-{application.application_code}-{uuid.uuid4().hex[:10]}"
        )
        application.download_attempts = 1
        application.last_download_at = application.delivered_at
        plan.export_status = "已完成"

        return {
            "ok": True,
            "application_code": application.application_code,
            "plan_code": plan.plan_code,
            "target_wide_table": plan.target_wide_table,
            "partition_code": snap.partition_code,
            "credential_code": credential.credential_code,
            "owner_at_acceptance": self.owner_at_time(snap.partition_id,
                                                      application.accepted_at),
            "current_owner": snap.owner_party,
            "ownership_transferred": (
                self.owner_at_time(snap.partition_id, application.accepted_at)
                != snap.owner_party
            ),
            "delivered_row_keys": list(application.delivered_row_keys),
            "exported_rows": len(records),
            "download_token": application.download_token,
            "records": records,
        }

    def retry_export_download(self, application_id: int) -> dict[str, Any]:
        """已交付下载按申请重试：只能用受理时核准的同一凭据，禁止用新资格重签。"""
        application = self._export_applications.get(application_id)
        if application is None:
            return {"ok": False, "message": f"导出申请 {application_id} 不存在"}
        if application.application_status != "已发放":
            return {
                "ok": False, "code": "NOT_DELIVERED",
                "message": (
                    f"申请 {application.application_code} 状态为{application.application_status}，"
                    "尚无已交付下载可重试"
                ),
            }

        snap = self._snapshots.get(application.partition_id)
        plan = self._exports.get(application.plan_id)
        credential = self._credentials.get(application.credential_id)
        if snap is None or plan is None or credential is None:
            return {
                "ok": False, "code": "DOWNLOAD_BLOCKED",
                "message": "申请关联的分区/计划/凭据已缺失，下载重试被阻断",
            }

        block = self._check_delivery_eligibility(application, snap, plan, credential)
        if block is not None:
            return {
                "ok": False,
                "code": "DOWNLOAD_BLOCKED",
                "message": block,
                "application_code": application.application_code,
                "credential_code": credential.credential_code,
                "credential_status": credential.credential_status,
            }

        application.download_attempts += 1
        application.last_download_at = _now_iso()
        records = self._collect_partition_records(snap, application.delivered_row_keys)
        return {
            "ok": True,
            "application_code": application.application_code,
            "plan_code": plan.plan_code,
            "download_token": application.download_token,
            "download_attempts": application.download_attempts,
            "credential_code": credential.credential_code,
            "delivered_row_keys": list(application.delivered_row_keys),
            "exported_rows": len(records),
            "records": records,
        }

    def get_export_application(self, application_id: int) -> dict[str, Any] | None:
        """回查申请当时的资格与移交是否影响本次发放。"""
        application = self._export_applications.get(application_id)
        if application is None:
            return None
        owner_at_acceptance = self.owner_at_time(
            application.partition_id, application.accepted_at)
        snap = self._snapshots.get(application.partition_id)
        current_owner = snap.owner_party if snap else ""
        credential = self._credentials.get(application.credential_id)
        return {
            "application_code": application.application_code,
            "applicant_party": application.applicant_party,
            "target_wide_table": application.target_wide_table,
            "plan_code": application.plan_code,
            "partition_code": application.partition_code,
            "credential_code": application.credential_code,
            "credential_status": credential.credential_status if credential else "缺失",
            "application_status": application.application_status,
            "prepared_row_keys": list(application.prepared_row_keys),
            "delivered_row_keys": list(application.delivered_row_keys),
            "block_reason": application.block_reason,
            "accepted_at": application.accepted_at,
            "delivered_at": application.delivered_at,
            "owner_at_acceptance": owner_at_acceptance,
            "current_owner": current_owner,
            "ownership_transferred_after_acceptance": (
                owner_at_acceptance != current_owner
            ),
            "qualified_at_acceptance": owner_at_acceptance == application.applicant_party,
            "download_token": application.download_token,
            "download_attempts": application.download_attempts,
        }

    def _check_delivery_eligibility(self, application: ExportApplication,
                                    snap: SnapshotPartition, plan: ExportPlan,
                                    credential: AccessCredential) -> str | None:
        """统一的发放/下载资格检查，返回 None 表示可交付，否则为阻断原因。"""
        if snap.target_wide_table != plan.target_wide_table \
                or snap.target_wide_table != application.target_wide_table:
            return (
                f"分区 {snap.partition_code} 归属宽表 {snap.target_wide_table} 与申请注明的 "
                f"{application.target_wide_table} 不一致，禁止跨表取数"
            )
        if credential.credential_status != "有效":
            return (
                f"取数凭据 {credential.credential_code} 已作废"
                f"（{credential.revoke_reason or '未注明原因'}），原方下载被阻断"
            )
        if not credential.is_active():
            return f"取数凭据 {credential.credential_code} 已过有效期"
        if credential.owner_party != application.applicant_party:
            return (
                f"取数凭据 {credential.credential_code} 归属 {credential.owner_party}，"
                f"不能用于申请方 {application.applicant_party} 的下载"
            )
        row_keys = (
            application.delivered_row_keys
            if application.application_status == "已发放"
            else application.prepared_row_keys
        )
        # 单次交付的行必须由同一次有效的取数资格完整覆盖。
        if not credential.covers(snap.target_wide_table, snap.partition_id, row_keys):
            return (
                f"取数凭据 {credential.credential_code} 无法覆盖本次交付的全部行，"
                "单次交付必须由同一次有效取数资格覆盖"
            )
        return None

    def _block_application(self, application: ExportApplication,
                           reason: str) -> dict[str, Any]:
        application.application_status = "已阻断"
        application.block_reason = reason
        plan = self._exports.get(application.plan_id)
        if plan is not None:
            plan.export_status = "导出失败"
        return {
            "ok": False,
            "code": "DELIVERY_BLOCKED",
            "message": reason,
            "application_code": application.application_code,
            "block_reason": reason,
            "pending_row_keys": list(application.prepared_row_keys),
            "pending_rows": len(application.prepared_row_keys),
        }

    def _partition_row_keys(self, snap: SnapshotPartition) -> list[str]:
        if snap.row_keys:
            return list(snap.row_keys)
        fusion_job = self._find_fusion_job_by_table(snap.target_wide_table)
        if fusion_job is None:
            return []
        return [str(rec.get("id", "")) for rec in fusion_job.merged_records]

    def _collect_partition_records(self, snap: SnapshotPartition,
                                   row_keys: list[str]) -> list[dict[str, Any]]:
        fusion_job = self._find_fusion_job_by_table(snap.target_wide_table)
        if fusion_job is None:
            return []
        wanted = set(row_keys)
        records = [
            dict(rec) for rec in fusion_job.merged_records
            if str(rec.get("id", "")) in wanted
        ]
        records.sort(key=lambda rec: str(rec.get("id", "")))
        return records

    # ---- 3. fusion_job -> watermark_track -> delta_merge ----

    def create_watermark(self, data_source: str, current_watermark: str,
                         data_origin: str = "fusion_job") -> WatermarkRecord:
        sid = self._next_seq()
        wm = WatermarkRecord(
            record_id=sid, record_code=_generate_code("WATE", sid),
            data_source=data_source,
            current_watermark=current_watermark,
            previous_watermark="",
            advance_interval="5m",
            delay_minutes=0,
            data_origin=data_origin,
            last_advance=_now_iso(),
        )
        self._watermarks[sid] = wm
        return wm

    def advance_watermark(self, record_id: int, new_watermark: str) -> WatermarkRecord | None:
        wm = self._watermarks.get(record_id)
        if wm is None:
            return None
        wm.previous_watermark = wm.current_watermark
        wm.current_watermark = new_watermark
        wm.last_advance = _now_iso()
        return wm

    # ---- 4. snapshot_mgr -> backfill_task -> cold_store ----

    def create_backfill_task(self, target_wide_table: str, backfill_range: str,
                             parallel_partitions: int = 4, dependency_check: str = "",
                             checkpoint_position: str = "") -> BackfillTask:
        sid = self._next_seq()
        task = BackfillTask(
            task_id=sid, task_code=_generate_code("BACK", sid),
            target_wide_table=target_wide_table,
            backfill_range=backfill_range,
            parallel_partitions=parallel_partitions,
            dependency_check=dependency_check,
            checkpoint_position=checkpoint_position,
            processed_rows=0, remaining_rows=0,
        )
        self._backfills[sid] = task
        return task

    def create_cold_store_strategy(self, target_wide_table: str, hot_data_days: int,
                                   cold_storage_type: str = "parquet",
                                   compression_method: str = "zstd",
                                   migration_window: str = "02:00-06:00") -> ColdStoreStrategy:
        sid = self._next_seq()
        cs = ColdStoreStrategy(
            strategy_id=sid, strategy_code=_generate_code("COLD", sid),
            target_wide_table=target_wide_table,
            hot_data_days=hot_data_days,
            cold_storage_type=cold_storage_type,
            migration_window=migration_window,
            compression_method=compression_method,
            migrated_rows=0, last_migration="",
        )
        self._cold_stores[sid] = cs
        return cs

    # ---- 5. wide_table_def -> lineage_edge -> metric_aggr ----

    def create_lineage_edge(self, upstream_node: str, downstream_node: str,
                            related_fields: list[str], transform_function: str = "",
                            edge_type: str = "direct", dependency_level: int = 1) -> LineageEdge:
        sid = self._next_seq()
        edge = LineageEdge(
            edge_id=sid, edge_code=_generate_code("LINE", sid),
            upstream_node=upstream_node, downstream_node=downstream_node,
            related_fields=related_fields,
            transform_function=transform_function,
            edge_type=edge_type,
            dependency_level=dependency_level,
            last_traced=_now_iso(),
        )
        self._lineages[sid] = edge
        return edge

    def create_metric_aggr_rule(self, target_wide_table: str, agg_function: str,
                                group_dimensions: list[str], time_granularity: str = "1d",
                                precompute_window: str = "7d",
                                storage_table: str = "") -> MetricAggrRule:
        sid = self._next_seq()
        rule = MetricAggrRule(
            rule_id=sid, rule_code=_generate_code("METR", sid),
            target_wide_table=target_wide_table,
            agg_function=agg_function,
            group_dimensions=group_dimensions,
            time_granularity=time_granularity,
            precompute_window=precompute_window,
            storage_table=storage_table or f"aggr_{target_wide_table}",
            last_computed="",
        )
        self._metrics[sid] = rule
        return rule

    # ---- 6. partition_rule -> cold_store -> archive ----

    def create_partition_rule(self, target_wide_table: str, partition_type: str,
                              partition_field: str, retention_days: int = 30,
                              archive_policy: str = "move_to_cold") -> PartitionRule:
        sid = self._next_seq()
        rule = PartitionRule(
            rule_id=sid, strategy_code=_generate_code("PART", sid),
            target_wide_table=target_wide_table,
            partition_type=partition_type,
            partition_field=partition_field,
            retention_days=retention_days,
            archive_policy=archive_policy,
            partition_count=0,
            last_adjusted=_now_iso(),
        )
        self._partitions[sid] = rule
        return rule

    # ---- 7. snapshot_mgr -> export_snapshot ----

    def create_export_plan(self, target_wide_table: str, export_format: str = "parquet",
                           partition_filter: str = "", field_filter: list[str] | None = None,
                           export_path: str = "/exports", row_limit: int = 0) -> ExportPlan:
        sid = self._next_seq()
        plan = ExportPlan(
            plan_id=sid, plan_code=_generate_code("EXPO", sid),
            target_wide_table=target_wide_table,
            export_format=export_format,
            partition_filter=partition_filter,
            field_filter=field_filter or [],
            export_path=export_path,
            row_limit=row_limit,
            export_status="待导出",
        )
        self._exports[sid] = plan
        return plan

    # ---- 8. cross_validate -> data_drift -> fusion_monitor ----

    def create_cross_validate_plan(self, primary_wide_table: str,
                                   reference_wide_table: str, join_key: str,
                                   validate_fields: list[str],
                                   deviation_threshold: str = "0.05") -> CrossValidatePlan:
        sid = self._next_seq()
        plan = CrossValidatePlan(
            plan_id=sid, plan_code=_generate_code("CROS", sid),
            primary_wide_table=primary_wide_table,
            reference_wide_table=reference_wide_table,
            join_key=join_key,
            validate_fields=validate_fields,
            deviation_threshold=deviation_threshold,
            last_result="",
            validate_status="待校验",
        )
        self._cross_validates[sid] = plan
        return plan

    def create_drift_record(self, wide_table_name: str, drift_type: str,
                            historical_baseline: str, current_distribution: str,
                            kl_divergence: str = "0.0",
                            drift_direction: str = "none") -> DataDriftRecord:
        sid = self._next_seq()
        record = DataDriftRecord(
            record_id=sid, record_code=_generate_code("DATA", sid),
            wide_table_name=wide_table_name,
            drift_type=drift_type,
            historical_baseline=historical_baseline,
            current_distribution=current_distribution,
            kl_divergence=kl_divergence,
            drift_direction=drift_direction,
            detected_at=_now_iso(),
        )
        self._drifts[sid] = record
        return record

    def create_fusion_monitor(self, fusion_job: str, monitor_dimension: str,
                              collection_interval: str = "60s",
                              alert_threshold: str = "",
                              alert_channel: str = "webhook",
                              silence_window: str = "") -> FusionMonitorMetric:
        sid = self._next_seq()
        metric = FusionMonitorMetric(
            metric_id=sid, metric_code=_generate_code("FUSI", sid),
            fusion_job=fusion_job,
            monitor_dimension=monitor_dimension,
            collection_interval=collection_interval,
            alert_threshold=alert_threshold,
            alert_channel=alert_channel,
            silence_window=silence_window,
            last_sample=_now_iso(),
        )
        self._monitors[sid] = metric
        return metric

    # ---- 复合融合操作 ----

    def execute_fusion_pipeline(self, job_id: int) -> dict[str, Any]:
        """执行完整融合流水线：数据源读取 -> 映射转换 -> 冲突仲裁 -> 质量检查 -> 产出快照。

        返回值包含融合记录、冲突日志、质量报告和快照信息。
        """
        job = self._fusion_jobs.get(job_id)
        if job is None:
            return {"ok": False, "message": f"融合作业 {job_id} 不存在"}

        raw_records: list[dict[str, Any]] = []
        for source_code in job.participant_sources:
            source = self._find_source_by_code(source_code)
            if source is None:
                continue
            source_records = self._simulate_source_read(source)
            for rec in source_records:
                rec["_source"] = source_code
                rec["_fusion_key"] = f"{source_code}#{rec.get(job.join_key, '')}"
                rec["_timestamp"] = _now_iso()
                rec["_version"] = 1
            raw_records.extend(source_records)

        mapped_records = self._apply_schema_mappings(raw_records, job.target_wide_table)

        job.output_rows = len(mapped_records)
        job.skipped_rows = len(raw_records) - len(mapped_records)
        job.merged_records = mapped_records

        result: dict[str, Any] = {
            "ok": True,
            "job_code": job.job_code,
            "total_sources": len(job.participant_sources),
            "raw_records": len(raw_records),
            "mapped_records": len(mapped_records),
            "skipped_records": job.skipped_rows,
            "records": mapped_records,
        }

        conflict_result = self._apply_conflict_resolution(mapped_records, job.target_wide_table)
        result["conflict_resolution"] = conflict_result

        quality_result = self._apply_quality_gates(
            conflict_result.get("resolved", mapped_records),
            job.target_wide_table,
        )
        result["quality_gate"] = quality_result

        if quality_result.get("passed", True):
            snap = self.create_snapshot(
                job.target_wide_table,
                partition_key_value=datetime.now(timezone.utc).strftime("%Y%m%d"),
            )
            snap.record_count = len(conflict_result.get("resolved", mapped_records))
            result["snapshot"] = {"partition_code": snap.partition_code, "record_count": snap.record_count}

        return result

    def execute_delta_merge(self, job_id: int, delta_records: list[dict[str, Any]],
                            watermark_record_id: int = 0) -> dict[str, Any]:
        """执行增量归并：将增量数据合并到融合作业的输出中。"""
        job = self._fusion_jobs.get(job_id)
        if job is None:
            return {"ok": False, "message": f"融合作业 {job_id} 不存在"}

        existing = {self._make_merge_key(r, job.join_key): r for r in job.merged_records}
        inserted = 0
        updated = 0
        deleted = 0

        for delta in delta_records:
            key = self._make_merge_key(delta, job.join_key)
            op = delta.get("_op", "upsert")
            if op == "delete":
                if key in existing:
                    del existing[key]
                    deleted += 1
            elif key in existing:
                existing[key].update(delta)
                updated += 1
            else:
                existing[key] = dict(delta)
                inserted += 1

        job.merged_records = list(existing.values())

        if watermark_record_id > 0:
            wm = self._watermarks.get(watermark_record_id)
            if wm is not None:
                wm.previous_watermark = wm.current_watermark
                wm.current_watermark = _now_iso()

        return {
            "ok": True,
            "inserted": inserted,
            "updated": updated,
            "deleted": deleted,
            "total_after_merge": len(job.merged_records),
        }

    def execute_backfill_with_checkpoint(self, backfill_id: int,
                                         source_records: list[dict[str, Any]]) -> dict[str, Any]:
        """执行带断点续传的数据回填。

        从 checkpoint_position 处继续处理，失败时保留断点可恢复。
        """
        task = self._backfills.get(backfill_id)
        if task is None:
            return {"ok": False, "message": f"回填任务 {backfill_id} 不存在"}

        start_idx = 0
        if task.checkpoint_position:
            try:
                start_idx = int(task.checkpoint_position)
            except (ValueError, TypeError):
                start_idx = 0

        batch_size = max(task.parallel_partitions * 100, 100)
        end_idx = min(start_idx + batch_size, len(source_records))
        batch = source_records[start_idx:end_idx]

        processed = len(batch)
        task.processed_rows += processed
        task.remaining_rows = max(0, len(source_records) - end_idx)

        if end_idx < len(source_records):
            task.checkpoint_position = str(end_idx)
            complete = False
        else:
            task.checkpoint_position = ""
            complete = True

        return {
            "ok": True,
            "batch_processed": processed,
            "total_processed": task.processed_rows,
            "remaining": task.remaining_rows,
            "checkpoint": task.checkpoint_position,
            "complete": complete,
        }

    def migrate_to_cold_store(self, strategy_id: int,
                              hot_partition_cutoff: str) -> dict[str, Any]:
        """执行冷热分层迁移：将超过热数据保留期的分区迁移到冷存储。"""
        strategy = self._cold_stores.get(strategy_id)
        if strategy is None:
            return {"ok": False, "message": f"冷热分层策略 {strategy_id} 不存在"}

        eligible_snaps = [
            snap for snap in self._snapshots.values()
            if snap.target_wide_table == strategy.target_wide_table
            and snap.partition_key_value < hot_partition_cutoff
        ]
        migrated = 0
        for snap in eligible_snaps:
            migrated += snap.record_count
            snap.status_mark = "已迁移至冷存储"

        strategy.migrated_rows += migrated
        strategy.last_migration = _now_iso()

        return {
            "ok": True,
            "migrated_partitions": len(eligible_snaps),
            "migrated_rows": migrated,
            "strategy_code": strategy.strategy_code,
        }

    def execute_export_from_snapshot(self, export_id: int,
                                     snapshot_id: int) -> dict[str, Any]:
        """从分区快照执行数据导出。

        修复越权取数：分区编号必须归属导出计划注明的同一张宽表，
        禁止拿另一张宽表的分区编号导出无关表的交易。
        """
        plan = self._exports.get(export_id)
        snap = self._snapshots.get(snapshot_id)
        if plan is None:
            return {"ok": False, "message": f"导出计划 {export_id} 不存在"}
        if snap is None:
            return {"ok": False, "message": f"分区快照 {snapshot_id} 不存在"}
        if snap.target_wide_table != plan.target_wide_table:
            return {
                "ok": False,
                "code": "PARTITION_TABLE_MISMATCH",
                "message": (
                    f"分区归属校验失败：分区 {snap.partition_code} 归属宽表 "
                    f"{snap.target_wide_table}，与导出计划 {plan.plan_code} 注明的 "
                    f"{plan.target_wide_table} 不一致，拒绝跨表取数"
                ),
            }

        fusion_job = self._find_fusion_job_by_table(snap.target_wide_table)
        records = fusion_job.merged_records if fusion_job else []

        if plan.field_filter:
            records = [
                {k: v for k, v in rec.items() if k in plan.field_filter}
                for rec in records
            ]

        if plan.row_limit > 0 and len(records) > plan.row_limit:
            records = records[:plan.row_limit]

        plan.export_status = "已完成"

        return {
            "ok": True,
            "plan_code": plan.plan_code,
            "snapshot_code": snap.partition_code,
            "target_wide_table": plan.target_wide_table,
            "exported_rows": len(records),
            "format": plan.export_format,
            "path": plan.export_path,
            "records": records,
        }

    def execute_cross_validation(self, plan_id: int) -> dict[str, Any]:
        """执行跨宽表交叉校验，检测数据一致性偏差。"""
        plan = self._cross_validates.get(plan_id)
        if plan is None:
            return {"ok": False, "message": f"交叉校验方案 {plan_id} 不存在"}

        primary_job = self._find_fusion_job_by_table(plan.primary_wide_table)
        ref_job = self._find_fusion_job_by_table(plan.reference_wide_table)

        primary_records = primary_job.merged_records if primary_job else []
        ref_records = ref_job.merged_records if ref_job else []

        primary_by_key = {str(r.get(plan.join_key, "")): r for r in primary_records}
        ref_by_key = {str(r.get(plan.join_key, "")): r for r in ref_records}

        mismatches: list[dict[str, Any]] = []
        drift_detected = False

        for key in set(primary_by_key.keys()) & set(ref_by_key.keys()):
            p = primary_by_key[key]
            r = ref_by_key[key]
            for field in plan.validate_fields:
                pv = str(p.get(field, ""))
                rv = str(r.get(field, ""))
                if pv != rv:
                    mismatches.append({
                        "key": key, "field": field,
                        "primary_value": pv, "reference_value": rv,
                    })
                    drift_detected = True

        plan.validate_status = "偏差告警" if mismatches else "一致"
        plan.last_result = json.dumps({
            "matched_keys": len(set(primary_by_key.keys()) & set(ref_by_key.keys())),
            "mismatch_count": len(mismatches),
            "drift_detected": drift_detected,
        }, ensure_ascii=False)

        result: dict[str, Any] = {
            "ok": True,
            "plan_code": plan.plan_code,
            "validate_status": plan.validate_status,
            "mismatch_count": len(mismatches),
            "drift_detected": drift_detected,
            "mismatches": mismatches[:20],
        }

        if drift_detected:
            drift_record = self.create_drift_record(
                wide_table_name=plan.primary_wide_table,
                drift_type="schema_mismatch",
                historical_baseline="baseline_" + _now_iso(),
                current_distribution="current_" + _now_iso(),
            )
            result["drift_record_code"] = drift_record.record_code

        return result

    def reconcile_partition_archive(self, partition_rule_id: int,
                                    cold_strategy_id: int) -> dict[str, Any]:
        """根据分区策略和冷热分层策略执行归档。

        将超出保留期的分区数据从冷存储转移到归档。
        """
        part_rule = self._partitions.get(partition_rule_id)
        cold_strat = self._cold_stores.get(cold_strategy_id)

        if part_rule is None or cold_strat is None:
            return {"ok": False, "message": "分区策略或冷热分层策略不存在"}

        eligible_snaps = [
            snap for snap in self._snapshots.values()
            if snap.target_wide_table == part_rule.target_wide_table
            and snap.status_mark == "已迁移至冷存储"
        ]

        archived = 0
        for snap in eligible_snaps:
            snap.status_mark = "已归档"
            archived += snap.record_count

        return {
            "ok": True,
            "archived_partitions": len(eligible_snaps),
            "archived_rows": archived,
            "archive_policy": part_rule.archive_policy,
        }

    def reconcile_watermark_with_fusion(self, job_id: int) -> dict[str, Any]:
        """协调水位线与融合作业的推进状态。

        水位延迟时推进受限，融合作业未完成时增量归并暂停。
        """
        job = self._fusion_jobs.get(job_id)
        if job is None:
            return {"ok": False, "message": f"融合作业 {job_id} 不存在"}

        related_watermarks = [
            wm for wm in self._watermarks.values()
            if wm.data_source in job.participant_sources
        ]

        stalled = [wm for wm in related_watermarks if wm.delay_minutes > 30]
        advancing = [wm for wm in related_watermarks if wm.delay_minutes <= 30]

        return {
            "ok": True,
            "job_code": job.job_code,
            "total_watermarks": len(related_watermarks),
            "advancing": len(advancing),
            "stalled": len(stalled),
            "can_proceed": len(stalled) == 0,
        }

    def build_wide_table_from_fusion(self, wide_table_id: int, fusion_job_id: int) -> dict[str, Any]:
        """从融合作业结果构建宽表定义，产生数据血缘边。"""
        wt = self._wide_tables.get(wide_table_id)
        job = self._fusion_jobs.get(fusion_job_id)

        if wt is None:
            return {"ok": False, "message": f"宽表定义 {wide_table_id} 不存在"}
        if job is None:
            return {"ok": False, "message": f"融合作业 {fusion_job_id} 不存在"}

        wt.source_count = len(job.participant_sources)
        wt.last_updated = _now_iso()

        lineages: list[str] = []
        for source_code in job.participant_sources:
            edge = self.create_lineage_edge(
                upstream_node=source_code,
                downstream_node=wt.table_name,
                related_fields=wt.field_list,
                edge_type="fusion_map",
                dependency_level=1,
            )
            lineages.append(edge.edge_code)

        return {
            "ok": True,
            "table_code": wt.table_code,
            "source_count": wt.source_count,
            "lineage_edges": lineages,
            "last_updated": wt.last_updated,
        }

    def compute_metric_from_lineage(self, lineage_edge_id: int,
                                    target_wide_table: str,
                                    agg_function: str,
                                    group_dimensions: list[str]) -> dict[str, Any]:
        """基于数据血缘边计算指标聚合规则。"""
        edge = self._lineages.get(lineage_edge_id)
        if edge is None:
            return {"ok": False, "message": f"数据血缘边 {lineage_edge_id} 不存在"}

        rule = self.create_metric_aggr_rule(
            target_wide_table=target_wide_table,
            agg_function=agg_function,
            group_dimensions=group_dimensions,
        )
        rule.last_computed = _now_iso()

        return {
            "ok": True,
            "metric_rule_code": rule.rule_code,
            "from_lineage": edge.edge_code,
            "agg_function": agg_function,
            "group_dimensions": group_dimensions,
        }

    # ---- 辅助方法 ----

    def _find_source_by_code(self, source_code: str) -> SourceDef | None:
        for source in self._sources.values():
            if source.source_code == source_code:
                return source
        return None

    def _find_fusion_job_by_table(self, wide_table_name: str) -> FusionJob | None:
        for job in self._fusion_jobs.values():
            if job.target_wide_table == wide_table_name:
                return job
        return None

    def _simulate_source_read(self, source: SourceDef) -> list[dict[str, Any]]:
        """模拟从数据源读取样本记录。"""
        return [
            {"id": f"{source.source_code}-rec-1", "name": f"{source.source_name}-rec1",
             "value": 100.0, "update_time": _now_iso()},
            {"id": f"{source.source_code}-rec-2", "name": f"{source.source_name}-rec2",
             "value": 200.0, "update_time": _now_iso()},
            {"id": f"{source.source_code}-rec-3", "name": f"{source.source_name}-rec3",
             "value": 300.0, "update_time": _now_iso()},
        ]

    def _apply_schema_mappings(self, records: list[dict[str, Any]],
                               target_wide_table: str) -> list[dict[str, Any]]:
        """应用 Schema 映射：对关联到目标宽表的映射方案进行字段转换。"""
        relevant = [
            m for m in self._schemas.values()
            if m.target_wide_table == target_wide_table
        ]
        if not relevant:
            return list(records)

        mapped: list[dict[str, Any]] = []
        for rec in records:
            source_table = rec.get("_source", "")
            applicable = [m for m in relevant if m.source_table == source_table]
            if applicable:
                new_rec = dict(rec)
                for mapping in applicable:
                    try:
                        field_map = json.loads(mapping.field_mapping_json)
                        for src_field, tgt_field in field_map.items():
                            if src_field in new_rec:
                                new_rec[tgt_field] = new_rec.pop(src_field)
                    except (json.JSONDecodeError, KeyError):
                        pass
                mapped.append(new_rec)
            else:
                mapped.append(rec)
        return mapped

    def _apply_conflict_resolution(self, records: list[dict[str, Any]],
                                   target_wide_table: str) -> dict[str, Any]:
        """应用冲突仲裁规则到融合记录。"""
        rules = [
            r for r in self._conflict_rules.values()
            if r.target_wide_table == target_wide_table and r.rule_status == "生效中"
        ]
        if not rules:
            return {"applied": False, "reason": "无生效的冲突规则", "resolved": records}

        resolved = list(records)
        all_conflicts: list[dict[str, Any]] = []
        for rule in rules:
            resolved, conflicts = rule.resolve(resolved)
            rule.conflict_count = len(conflicts)
            all_conflicts.extend(conflicts)

        return {
            "applied": True,
            "rules_used": len(rules),
            "conflicts_found": len(all_conflicts),
            "conflicts": all_conflicts[:50],
            "resolved": resolved,
        }

    def _apply_quality_gates(self, records: list[dict[str, Any]],
                             target_wide_table: str) -> dict[str, Any]:
        """应用质量门禁检查。"""
        gates = [
            g for g in self._quality_gates.values()
            if g.target_wide_table == target_wide_table
        ]
        if not gates:
            return {"passed": True, "gates_checked": 0, "violations": []}

        violations: list[dict[str, Any]] = []
        for gate in gates:
            gate.checked_rows = len(records)
            if gate.check_type == "row_count_min" and len(records) < int(gate.threshold_condition):
                violations.append({"rule_code": gate.rule_code, "check": "row_count_min",
                                   "actual": len(records), "threshold": gate.threshold_condition})
            elif gate.check_type == "null_check":
                null_count = sum(
                    1 for rec in records
                    if rec.get(gate.threshold_condition) is None
                )
                if null_count > 0:
                    violations.append({"rule_code": gate.rule_code, "check": "null_check",
                                       "null_count": null_count})

        passed = len(violations) == 0
        return {"passed": passed, "gates_checked": len(gates), "violations": violations}

    @staticmethod
    def _make_merge_key(record: dict[str, Any], join_key: str) -> str:
        return f"{record.get('_source', '_')}#{record.get(join_key, '_')}"

    def domain_snapshot(self) -> dict[str, Any]:
        """输出引擎全局数据质量快照。"""
        return {
            "sources": len(self._sources),
            "schemas": len(self._schemas),
            "wide_tables": len(self._wide_tables),
            "fusion_jobs": len(self._fusion_jobs),
            "conflict_rules": len(self._conflict_rules),
            "quality_gates": len(self._quality_gates),
            "snapshots": len(self._snapshots),
            "backfills": len(self._backfills),
            "cold_stores": len(self._cold_stores),
            "lineages": len(self._lineages),
            "metrics": len(self._metrics),
            "partitions": len(self._partitions),
            "watermarks": len(self._watermarks),
            "exports": len(self._exports),
            "cross_validates": len(self._cross_validates),
            "drifts": len(self._drifts),
            "monitors": len(self._monitors),
            "ownership_records": len(self._ownership_records),
            "access_credentials": len(self._credentials),
            "export_applications": len(self._export_applications),
        }


# 全局单例供测试和跨模块消费
fusion_engine = FusionEngine()