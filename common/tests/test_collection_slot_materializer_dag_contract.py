from pathlib import Path


DAG = Path(__file__).resolve().parents[2] / "collection_slot_materializer.py"


def test_materializer_dag_is_separate_batch_with_single_active_run():
    source = DAG.read_text(encoding="utf-8")
    assert 'DAG_ID = "collection_slot_materializer"' in source
    assert 'TASK_ID = "materialize_collection_slot_receipts"' in source
    assert 'schedule="*/5 * * * *"' in source
    assert "max_active_runs=1" in source
    assert "validate_dev_runtime(\"traffic\")" in source
    assert "CollectionSlotReceiptReconciler" in source
    assert "_resolve_legacy_expected_slot" in source
    assert "CollectionSlotMaterializer(storage, sink).run()" in source
