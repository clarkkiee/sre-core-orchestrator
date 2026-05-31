from app.models.chaos import ExperimentType

TYPE_TO_LITMUS_NAME: dict[ExperimentType, str] = {
    ExperimentType.POD_DELETE: "pod-delete",
    ExperimentType.POD_CPU_HOG: "pod-cpu-hog",
    ExperimentType.POD_MEMORY_HOG: "pod-memory-hog",
    ExperimentType.POD_NETWORK_LATENCY: "pod-network-latency",
    ExperimentType.POD_NETWORK_LOSS: "pod-network-loss",
}

LITMUS_NAME_TO_TYPE: dict[str, ExperimentType] = {v: k for k, v in TYPE_TO_LITMUS_NAME.items()}
