Airflow task context dict.
{
    'dag': <DAG: ff-transactions-data-transport>,
    'inlets': [],
    'map_index_template': None,
    'outlets': [],
    'run_id': 'manual__2026-04-11T18:54:34.282443+00:00',

    'task': <Task(PythonOperator): py-transfer-data>,

    'task_instance': RuntimeTaskInstance(
        id=UUID('019d7de5-5343-7b16-aff5-bb57575184b0'),
        task_id='py-transfer-data',
        dag_id='ff-transactions-data-transport',
        run_id='manual__2026-04-11T18:54:34.282443+00:00',
        try_number=1,
        dag_version_id=UUID('019d7dad-9c0c-742d-9e26-f1ee1fe1b0c9'),
        map_index=-1,
        hostname='54bfdefc8bf1',
        context_carrier={},
        task=<Task(PythonOperator): py-transfer-data>,
        bundle_instance=LocalDagBundle(name=dags-folder),
        max_tries=0,
        start_date=datetime.datetime(2026, 4, 11, 18, 54, 34, 711464, tzinfo=datetime.timezone.utc),
        end_date=None,
        state=<TaskInstanceState.RUNNING: 'running'>,
        is_mapped=False,
        rendered_map_index=None
    ),

    'ti': RuntimeTaskInstance(
        id=UUID('019d7de5-5343-7b16-aff5-bb57575184b0'),
        task_id='py-transfer-data',
        dag_id='ff-transactions-data-transport',
        run_id='manual__2026-04-11T18:54:34.282443+00:00',
        try_number=1,
        dag_version_id=UUID('019d7dad-9c0c-742d-9e26-f1ee1fe1b0c9'),
        map_index=-1,
        hostname='54bfdefc8bf1',
        context_carrier={},
        task=<Task(PythonOperator): py-transfer-data>,
        bundle_instance=LocalDagBundle(name=dags-folder),
        max_tries=0,
        start_date=datetime.datetime(2026, 4, 11, 18, 54, 34, 711464, tzinfo=datetime.timezone.utc),
        end_date=None,
        state=<TaskInstanceState.RUNNING: 'running'>,
        is_mapped=False,
        rendered_map_index=None
    ),

    'outlet_events': <OutletEventAccessors>,
    'inlet_events': InletEventsAccessors(
        _inlets=[],
        _assets={},
        _asset_aliases={}
    ),

    'macros': <MacrosAccessor>,
    'params': {},

    'var': {
        'json': <VariableAccessor>,
        'value': <VariableAccessor>
    },

    'conn': <ConnectionAccessor>,

    'dag_run': DagRun(
        dag_id='ff-transactions-data-transport',
        run_id='manual__2026-04-11T18:54:34.282443+00:00',
        logical_date=datetime.datetime(2026, 4, 11, 18, 54, 31, tzinfo=datetime.timezone.utc),
        data_interval_start=datetime.datetime(2026, 4, 11, 18, 54, 31, tzinfo=datetime.timezone.utc),
        data_interval_end=datetime.datetime(2026, 4, 11, 18, 54, 31, tzinfo=datetime.timezone.utc),
        run_after=datetime.datetime(2026, 4, 11, 18, 54, 34, 282443, tzinfo=datetime.timezone.utc),
        start_date=datetime.datetime(2026, 4, 11, 18, 54, 34, 584254, tzinfo=datetime.timezone.utc),
        end_date=None,
        clear_number=0,
        run_type=<DagRunType.MANUAL: 'manual'>,
        state=<DagRunState.RUNNING: 'running'>,
        conf={},
        triggering_user_name='af_majunior',
        consumed_asset_events=[]
    ),

    'triggering_asset_events': TriggeringAssetEventsAccessor(
        _events=defaultdict(list, {})
    ),

    'task_instance_key_str': 'ff-transactions-data-transport__py-transfer-data__20260411',
    'task_reschedule_count': 0,

    'prev_start_date_success': <Proxy>,
    'prev_end_date_success': <Proxy>,

    'logical_date': datetime.datetime(2026, 4, 11, 18, 54, 31, tzinfo=UTC),

    'ds': '2026-04-11',
    'ds_nodash': '20260411',

    'ts': '2026-04-11T18:54:31+00:00',
    'ts_nodash': '20260411T185431',
    'ts_nodash_with_tz': '20260411T185431+0000',

    'data_interval_end': datetime.datetime(2026, 4, 11, 18, 54, 31, tzinfo=UTC),
    'data_interval_start': datetime.datetime(2026, 4, 11, 18, 54, 31, tzinfo=UTC),

    'prev_data_interval_start_success': <Proxy>,
    'prev_data_interval_end_success': <Proxy>,

    'templates_dict': None
}



Sample cursor rows:
((1, datetime.datetime(2025, 12, 30, 19, 5, 56), datetime.datetime(2026, 1, 5, 14, 54, 12), None, 1, 1, 4, 1, None, 12, 'Initial balance for "Mortgage Loan"', datetime.datetime(2026, 1, 1, 0, 0), 'America/New_York', None, None, None, 0, 0, 1, 1), (1840, datetime.datetime(2026, 1, 4, 19, 31, 3), datetime.datetime(2026, 1, 4, 19, 31, 3), None, 1, 1, 4, 1840, None, 12, 'Initial balance for "Cash"', datetime.datetime(2026, 1, 4, 0, 0), 'America/New_York', None, None, None, 0, 0, 1, 1))