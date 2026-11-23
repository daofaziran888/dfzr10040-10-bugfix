"""分区归属校验与快照导出取数权限测试。

覆盖题设完整场景：
1. 订单计划请求退款分区（另一张宽表的分区编号）必须被阻断，拿不到退款明细且计划不完成；
2. 订单分区两行随合法移交更换归属：受理原方申请 -> 发放前完成移交 ->
   原凭据有效期内旧申请仍可拿到受理时核准的行，并能回查当时资格与移交影响；
3. 接手方按新归属独立申请，确认新归属对应的明细范围；
4. 原凭据单独作废后重试原下载被阻断，接手方下载仍可用；
5. 单次交付的行必须由同一次有效取数资格完整覆盖；
6. 分区快照保留每次确认的实际归属范围。
"""
from __future__ import annotations

import pytest

from app.fusion_engine import FusionEngine

OLD_PARTY = "TEAM_ALPHA"
NEW_PARTY = "TEAM_BETA"
ORDER_TABLE = "WT_ORDER"
REFUND_TABLE = "WT_REFUND"


def _seed_partition(engine: FusionEngine, table: str,
                    rows: list[dict]) -> tuple[object, object]:
    """建融合作业并把确定性明细行备入分区快照。"""
    job = engine.create_fusion_job(
        target_wide_table=table,
        participant_sources=["SRC-SEED"],
        join_key="id",
    )
    job.merged_records = [dict(row) for row in rows]
    snap = engine.create_snapshot(table, partition_key_value="20261123")
    staged = engine.stage_partition_rows(snap.partition_id, rows)
    assert staged["ok"] is True
    return job, snap


@pytest.fixture
def engine() -> FusionEngine:
    return FusionEngine()


class TestCrossTablePartitionRejection:
    """越权入口：另一张宽表的分区编号不能被订单计划接受。"""

    def test_order_plan_cannot_consume_refund_partition(self, engine: FusionEngine) -> None:
        """订单计划请求退款分区：阻断、无退款明细、计划未标完成。"""
        refund_rows = [
            {"id": "RF-1", "name": "退款明细1", "value": 10},
            {"id": "RF-2", "name": "退款明细2", "value": 20},
        ]
        _, refund_snap = _seed_partition(engine, REFUND_TABLE, refund_rows)
        engine.confirm_partition_ownership(refund_snap.partition_id, "TEAM_REFUND")

        order_plan = engine.create_export_plan(
            target_wide_table=ORDER_TABLE, export_format="csv",
        )

        # 旧导出入口：直接拿退款分区编号喂给订单计划。
        result = engine.execute_export_from_snapshot(
            order_plan.plan_id, refund_snap.partition_id)
        assert result["ok"] is False
        assert result["code"] == "PARTITION_TABLE_MISMATCH"
        assert REFUND_TABLE in result["message"]
        assert ORDER_TABLE in result["message"]
        # 没有任何退款明细泄露，计划不能被标成完成。
        assert "records" not in result
        assert order_plan.export_status == "待导出"

    def test_export_application_rejects_foreign_partition(self, engine: FusionEngine) -> None:
        """新申请入口同样拒绝跨表分区，并校验归属/凭据链路。"""
        _, refund_snap = _seed_partition(
            engine, REFUND_TABLE, [{"id": "RF-9", "name": "退款", "value": 1}])
        engine.confirm_partition_ownership(refund_snap.partition_id, "TEAM_REFUND")
        cred = engine.issue_access_credential(refund_snap.partition_id, "TEAM_REFUND")
        assert cred["ok"] is True

        order_plan = engine.create_export_plan(
            target_wide_table=ORDER_TABLE, export_format="parquet")
        accept = engine.accept_export_application(
            order_plan.plan_id, refund_snap.partition_id,
            applicant_party="TEAM_REFUND", credential_id=cred["credential_id"])
        assert accept["ok"] is False
        assert accept["code"] == "PARTITION_TABLE_MISMATCH"
        assert order_plan.export_status == "待导出"

    def test_legacy_matching_table_export_still_works(self, engine: FusionEngine) -> None:
        """同表分区的既有导出路径保持兼容，且输出带目标宽表便于核对。"""
        rows = [{"id": f"O-{i}", "name": f"订单{i}", "value": i} for i in range(3)]
        _, snap = _seed_partition(engine, "WT_LEGACY", rows)
        plan = engine.create_export_plan(target_wide_table="WT_LEGACY",
                                         export_format="csv")
        result = engine.execute_export_from_snapshot(plan.plan_id, snap.partition_id)
        assert result["ok"] is True
        assert result["target_wide_table"] == "WT_LEGACY"
        assert result["exported_rows"] == 3
        assert plan.export_status == "已完成"


class TestOwnershipTransferAndDelivery:
    """受理后移交、发放前冲突的完整场景。"""

    def _build_order_partition(self, engine: FusionEngine):
        order_rows = [
            {"id": "ORD-1001", "name": "订单交易1001", "amount": 100.0},
            {"id": "ORD-1002", "name": "订单交易1002", "amount": 200.0},
        ]
        _, snap = _seed_partition(engine, ORDER_TABLE, order_rows)
        # 分区初始归属原方，快照保留第一次确认的实际归属范围。
        confirm = engine.confirm_partition_ownership(snap.partition_id, OLD_PARTY)
        assert confirm["ok"] is True
        assert confirm["row_keys"] == ["ORD-1001", "ORD-1002"]
        return snap, order_rows

    def test_full_handover_scenario(self, engine: FusionEngine) -> None:
        snap, order_rows = self._build_order_partition(engine)

        # 原方在移交前取得取数凭据并被受理申请（明细行已备好，结果未发放）。
        old_cred = engine.issue_access_credential(snap.partition_id, OLD_PARTY)
        assert old_cred["ok"] is True
        old_plan = engine.create_export_plan(target_wide_table=ORDER_TABLE,
                                             export_format="csv")
        accept = engine.accept_export_application(
            old_plan.plan_id, snap.partition_id,
            applicant_party=OLD_PARTY,
            credential_id=old_cred["credential_id"])
        assert accept["ok"] is True
        assert accept["prepared_row_keys"] == ["ORD-1001", "ORD-1002"]
        assert accept["target_wide_table"] == ORDER_TABLE
        assert old_plan.export_status == "导出中"
        old_app_id = accept["application_id"]

        # 发放前完成合法归属移交；快照追加第二次确认，归属范围仍可逐次回查。
        transfer = engine.transfer_partition_ownership(
            snap.partition_id, from_party=OLD_PARTY, to_party=NEW_PARTY)
        assert transfer["ok"] is True
        assert transfer["transfer_kind"] == "归属移交"
        history = engine.get_ownership_history(snap.partition_id)
        assert [h["owner_party"] for h in history] == [OLD_PARTY, NEW_PARTY]
        assert all(h["row_keys"] == ["ORD-1001", "ORD-1002"] for h in history)

        # 移交确认后旧方不能新申请、不能再拿新凭据；接手方按新归属申请。
        new_cred = engine.issue_access_credential(snap.partition_id, NEW_PARTY)
        assert new_cred["ok"] is True
        stale_cred = engine.issue_access_credential(snap.partition_id, OLD_PARTY)
        assert stale_cred["ok"] is False
        new_plan = engine.create_export_plan(target_wide_table=ORDER_TABLE,
                                             export_format="parquet")
        old_reapply = engine.accept_export_application(
            new_plan.plan_id, snap.partition_id,
            applicant_party=OLD_PARTY,
            credential_id=old_cred["credential_id"])
        assert old_reapply["ok"] is False
        assert old_reapply["code"] == "OWNER_MISMATCH"
        assert new_plan.export_status == "待导出"

        # 发放检查与移交冲突：原凭据仍在有效期且受理时已核准这批行，旧申请放行。
        delivery = engine.deliver_export_application(old_app_id)
        assert delivery["ok"] is True
        assert delivery["ownership_transferred"] is True
        assert delivery["owner_at_acceptance"] == OLD_PARTY
        assert delivery["current_owner"] == NEW_PARTY
        assert delivery["credential_code"] == old_cred["credential_code"]
        assert delivery["delivered_row_keys"] == ["ORD-1001", "ORD-1002"]
        assert delivery["exported_rows"] == 2
        # 导出详情必须是申请注明宽表的订单交易，不能出现无关表数据。
        assert {r["id"] for r in delivery["records"]} == {"ORD-1001", "ORD-1002"}
        assert all(r["amount"] in (100.0, 200.0) for r in delivery["records"])
        assert old_plan.export_status == "已完成"

        # 已受理旧申请可回查当时资格以及移交是否影响这次发放。
        review = engine.get_export_application(old_app_id)
        assert review["qualified_at_acceptance"] is True
        assert review["owner_at_acceptance"] == OLD_PARTY
        assert review["current_owner"] == NEW_PARTY
        assert review["ownership_transferred_after_acceptance"] is True
        assert review["application_status"] == "已发放"

        # 接手方独立申请：新归属对应的明细范围与原方一致，由新凭据覆盖。
        new_accept = engine.accept_export_application(
            new_plan.plan_id, snap.partition_id,
            applicant_party=NEW_PARTY,
            credential_id=new_cred["credential_id"])
        assert new_accept["ok"] is True
        assert new_accept["prepared_row_keys"] == ["ORD-1001", "ORD-1002"]
        new_delivery = engine.deliver_export_application(
            new_accept["application_id"])
        assert new_delivery["ok"] is True
        assert new_delivery["credential_code"] == new_cred["credential_code"]
        assert new_delivery["exported_rows"] == 2

        # 原凭据单独作废后，原方下载重试按申请识别被阻断，不能用接手方资格重签。
        revoke = engine.revoke_access_credential(
            old_cred["credential_id"], reason="原方取数资格单独作废")
        assert revoke["ok"] is True
        retry_old = engine.retry_export_download(old_app_id)
        assert retry_old["ok"] is False
        assert retry_old["code"] == "DOWNLOAD_BLOCKED"
        assert "已作废" in retry_old["message"]
        assert retry_old["credential_code"] == old_cred["credential_code"]

        # 接手方凭据不受影响，其下载重试仍可用，且仍是本申请自己的凭据。
        retry_new = engine.retry_export_download(new_accept["application_id"])
        assert retry_new["ok"] is True
        assert retry_new["credential_code"] == new_cred["credential_code"]
        assert retry_new["download_attempts"] == 2
        assert {r["id"] for r in retry_new["records"]} == {"ORD-1001", "ORD-1002"}

    def test_blocked_delivery_records_pending_reason(self, engine: FusionEngine) -> None:
        """受理后、发放前凭据被作废：发放阻断并留待核原因，计划标记失败。"""
        snap, _ = self._build_order_partition(engine)
        cred = engine.issue_access_credential(snap.partition_id, OLD_PARTY)
        plan = engine.create_export_plan(target_wide_table=ORDER_TABLE,
                                         export_format="csv")
        accept = engine.accept_export_application(
            plan.plan_id, snap.partition_id,
            applicant_party=OLD_PARTY, credential_id=cred["credential_id"])
        engine.revoke_access_credential(cred["credential_id"])

        result = engine.deliver_export_application(accept["application_id"])
        assert result["ok"] is False
        assert result["code"] == "DELIVERY_BLOCKED"
        assert "已作废" in result["message"]
        assert result["pending_rows"] == 2
        assert plan.export_status == "导出失败"

        review = engine.get_export_application(accept["application_id"])
        assert review["application_status"] == "已阻断"
        assert review["block_reason"] == result["block_reason"]

    def test_single_credential_must_cover_all_rows(self, engine: FusionEngine) -> None:
        """单次交付的行须由同一次有效取数资格覆盖：缩窄凭据范围即拒受理。"""
        snap, _ = self._build_order_partition(engine)
        issued = engine.issue_access_credential(snap.partition_id, OLD_PARTY)
        credential = engine._credentials[issued["credential_id"]]
        # 凭据只覆盖两行中的一行，无法覆盖整次交付。
        credential.row_keys = ["ORD-1001"]

        plan = engine.create_export_plan(target_wide_table=ORDER_TABLE,
                                         export_format="csv")
        accept = engine.accept_export_application(
            plan.plan_id, snap.partition_id,
            applicant_party=OLD_PARTY,
            credential_id=issued["credential_id"])
        assert accept["ok"] is False
        assert accept["code"] == "CREDENTIAL_SCOPE_MISMATCH"
        assert plan.export_status == "待导出"

    def test_illegal_transfer_from_non_owner_rejected(self, engine: FusionEngine) -> None:
        """非当前归属方发起的移交不合法，归属保持不变。"""
        snap, _ = self._build_order_partition(engine)
        bad = engine.transfer_partition_ownership(
            snap.partition_id, from_party=NEW_PARTY, to_party="TEAM_GAMMA")
        assert bad["ok"] is False
        assert snap.owner_party == OLD_PARTY
        assert len(engine.get_ownership_history(snap.partition_id)) == 1

    def test_credential_issued_before_transfer_retains_scope(self, engine: FusionEngine) -> None:
        """移交不回改旧凭据覆盖的行范围，旧凭据仍能核对受理时核准的行。"""
        snap, _ = self._build_order_partition(engine)
        issued = engine.issue_access_credential(snap.partition_id, OLD_PARTY)
        old_credential = engine._credentials[issued["credential_id"]]
        engine.transfer_partition_ownership(
            snap.partition_id, from_party=OLD_PARTY, to_party=NEW_PARTY)
        assert old_credential.credential_status == "有效"
        assert old_credential.row_keys == ["ORD-1001", "ORD-1002"]
        assert old_credential.covers(ORDER_TABLE, snap.partition_id,
                                     ["ORD-1001", "ORD-1002"]) is True
        # 同一次资格不能覆盖别的宽表或别的分区。
        assert old_credential.covers(REFUND_TABLE, snap.partition_id,
                                     ["ORD-1001"]) is False
        assert old_credential.covers(ORDER_TABLE, snap.partition_id + 999,
                                     ["ORD-1001"]) is False

    def test_retry_unknown_application_and_undelivered(self, engine: FusionEngine) -> None:
        """下载重试按申请识别：不存在或未发放的申请不能重试。"""
        missing = engine.retry_export_download(99999)
        assert missing["ok"] is False

        snap, _ = self._build_order_partition(engine)
        cred = engine.issue_access_credential(snap.partition_id, OLD_PARTY)
        plan = engine.create_export_plan(target_wide_table=ORDER_TABLE,
                                         export_format="csv")
        accept = engine.accept_export_application(
            plan.plan_id, snap.partition_id,
            applicant_party=OLD_PARTY, credential_id=cred["credential_id"])
        early = engine.retry_export_download(accept["application_id"])
        assert early["ok"] is False
        assert early["code"] == "NOT_DELIVERED"
