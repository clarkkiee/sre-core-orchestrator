from app.infrastructure.chaos.manager import LitmusChaosManager
from app.infrastructure.config_values import load_values
from app.infrastructure.metrics.deployer import MonitoringStackDeployer
from app.utils.config import settings


def build_litmus_manager() -> LitmusChaosManager:
    litmus = load_values()["litmus"]
    return LitmusChaosManager(
        kubectl_binary=settings.KUBECTL_BINARY,
        litmus_runner_image=litmus["runner_image"],
        litmus_version=litmus["version"]
    )

def build_monitoring_deployer() -> MonitoringStackDeployer:
    monitoring = load_values()["monitoring"]
    return MonitoringStackDeployer(
        bbe_image=monitoring["bbe_image"],
        ksm_image=monitoring["ksm_image"],
        vm_image=monitoring["vm_image"],
        vm_nodeport=monitoring["vm_nodeport"]
    )
