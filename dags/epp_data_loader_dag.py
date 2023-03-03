from airflow import DAG
from datetime import datetime, timedelta


log = LoggingMixin().log

try:
    # Kubernetes is optional, so not available in vanilla Airflow
    # pip install apache-airflow[kubernetes]
    from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator

    default_args = {
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': datetime.utcnow(),
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    }


    dag = DAG(
        'epp_data_loader', default_args=default_args, schedule_interval=timedelta(minutes=10))


    pgp_decrypting = KubernetesPodOperator(
        namespace='default',
        image="fsr-artifactory.aws.foreseeresults.com:9001/workflow_tasks/xm_pgp_decryption:0.1",
        env_vars={"ARGS": "--context_param csvfolder=/opt/input_files/ --context_param "},
        name="pgp-decryption-pod",
        in_cluster=True,
        task_id="pgp-decryption",
        get_logs=True,
        dag=dag,
        is_delete_operator_pod=False,
        tolerations=tolerations
    )


    epp_preprocessing = KubernetesPodOperator(
        namespace='default',
        image="fsr-artifactory.aws.foreseeresults.com:9001/workflow_tasks/xm_epp_data_preprocesser:0.1",
        env_vars={"ARGS": "--context_param csvfolder=/opt/input_files/ --context_param "},
        name="epp-preprocesser-pod",
        in_cluster=True,
        task_id="epp-preprocesser",
        get_logs=True,
        dag=dag,
        is_delete_operator_pod=False,
        tolerations=tolerations
    )

    epp_dataloading = KubernetesPodOperator(
        namespace='default',
        image="fsr-artifactory.aws.foreseeresults.com:9001/workflow_tasks/xm_epp_data_loader:0.1",
        env_vars={"ARGS": "--context_param csvfolder=/opt/input_files/ --context_param "},
        name="epp-dataloader-pod",
        in_cluster=True,
        task_id="epp-dataloader",
        get_logs=True,
        dag=dag,
        is_delete_operator_pod=False,
        tolerations=tolerations
    )

    epp_postprocessing = KubernetesPodOperator(
        namespace='default',
        image="fsr-artifactory.aws.foreseeresults.com:9001/workflow_tasks/xm_epp_postprocesser:0.1",
        env_vars={"ARGS": "--context_param csvfolder=/opt/input_files/ --context_param "},
        name="epp-postprocesser-pod",
        in_cluster=True,
        task_id="epp-postprocesser",
        get_logs=True,
        dag=dag,
        is_delete_operator_pod=False,
        tolerations=tolerations
    )

    pgp_decrypting >> epp_preprocessing >> epp_dataloading >> epp_postprocessing


except ImportError as e:
    log.warn("Could not import KubernetesPodOperator: " + str(e))
    log.warn("Install kubernetes dependencies with: "
             "    pip install apache-airflow[kubernetes]")
