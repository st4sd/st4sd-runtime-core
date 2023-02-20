import experiment.settings


def test_parse_options_from_environment_variables():
    orig = experiment.settings.load_settings_orchestrator(reuse_if_existing=False)

    try:
        environ = dict()

        environ['ST4SD_ORCHESTRATOR_WORKERS_CONTROLLER'] = '1'
        environ['ST4SD_ORCHESTRATOR_WORKERS_COMPONENT_STATE'] = '2'
        environ['ST4SD_ORCHESTRATOR_WORKERS_ENGINE'] = '3'
        environ['ST4SD_ORCHESTRATOR_WORKERS_ENGINE_TRIGGER'] = '4'
        environ['ST4SD_ORCHESTRATOR_WORKERS_ENGINE_TASK'] = '5'
        environ['ST4SD_ORCHESTRATOR_WORKERS_BACKEND_K8S'] = '6'

        custom = experiment.settings.load_settings_orchestrator(environ=environ, reuse_if_existing=False)

        assert custom.workers_default_all is None
        assert custom.workers_controller == 1
        assert custom.workers_component_state == 2
        assert custom.workers_engine == 3
        assert custom.workers_engine_trigger == 4
        assert custom.workers_engine_task == 5
        assert custom.workers_backend_k8s == 6

        assert orig.dict() != custom.dict()
    finally:
        restored = experiment.settings.load_settings_orchestrator(reuse_if_existing=False)
        assert orig.dict() == restored.dict()


def test_override_defaults():
    orig = experiment.settings.load_settings_orchestrator(reuse_if_existing=False)

    try:
        environ = {'ST4SD_ORCHESTRATOR_WORKERS_DEFAULT_ALL': '424242'}
        custom = experiment.settings.load_settings_orchestrator(environ=environ, reuse_if_existing=False)

        assert custom.workers_default_all == 424242
        assert custom.workers_controller == 424242
        assert custom.workers_component_state == 424242
        assert custom.workers_engine == 424242
        assert custom.workers_engine_trigger == 424242
        assert custom.workers_engine_task == 424242
        assert custom.workers_backend_k8s == 424242

        assert orig.dict() != custom.dict()
    finally:
        restored = experiment.settings.load_settings_orchestrator(reuse_if_existing=False)
        assert orig.dict() == restored.dict()
