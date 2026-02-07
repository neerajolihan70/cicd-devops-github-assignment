"""
Test DAG structure and validation
"""
from airflow.models import DagBag

def test_dag_loaded():
    """Test that the DAG loads without errors"""
    dagbag = DagBag()
    dag = dagbag.get_dag("s3_to_redshift_pipeline")
    assert dag is not None, "DAG 's3_to_redshift_pipeline' not found"
    assert len(dag.tasks) == 4, f"Expected 4 tasks, found {len(dag.tasks)}"

def test_dag_has_correct_tasks():
    """Test that the DAG has all expected tasks"""
    dagbag = DagBag()
    dag = dagbag.get_dag("s3_to_redshift_pipeline")
    
    expected_tasks = {
        'load_staging',
        'validate_staging', 
        'promote_curated',
        'final_quality_check'
    }
    actual_tasks = {task.task_id for task in dag.tasks}
    
    assert expected_tasks == actual_tasks, f"Expected {expected_tasks}, got {actual_tasks}"

def test_dag_dependencies():
    """Test task dependencies are correct"""
    dagbag = DagBag()
    dag = dagbag.get_dag("s3_to_redshift_pipeline")
    
    # Get tasks
    load = dag.get_task('load_staging')
    validate = dag.get_task('validate_staging')
    curate = dag.get_task('promote_curated')
    quality = dag.get_task('final_quality_check')
    
    # Check dependencies
    assert validate in load.downstream_list
    assert curate in validate.downstream_list
    assert quality in curate.downstream_list
