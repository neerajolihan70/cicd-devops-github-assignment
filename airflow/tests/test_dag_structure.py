from airflow.models import DagBag


def test_dag_loaded():
    """DAG should load without import errors"""
    dagbag = DagBag()
    dag = dagbag.get_dag("s3_to_redshift_pipeline")
    assert dag is not None, "DAG not found"
    assert len(dag.tasks) == 4


def test_dag_has_correct_tasks():
    """Check all expected tasks exist"""
    dagbag = DagBag()
    dag = dagbag.get_dag("s3_to_redshift_pipeline")
    
    expected = {'load_staging', 'validate_staging', 'promote_curated', 'final_quality_check'}
    actual = {task.task_id for task in dag.tasks}
    
    assert expected == actual


def test_dag_dependencies():
    """Verify task order: load → validate → curate → quality"""
    dagbag = DagBag()
    dag = dagbag.get_dag("s3_to_redshift_pipeline")
    
    load = dag.get_task('load_staging')
    validate = dag.get_task('validate_staging')
    curate = dag.get_task('promote_curated')
    quality = dag.get_task('final_quality_check')
    
    assert validate in load.downstream_list
    assert curate in validate.downstream_list
    assert quality in curate.downstream_list
