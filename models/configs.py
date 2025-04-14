import yaml
from dataclasses import dataclass, field

BASE_DIR = ""

tree_yaml_file = 'tree_config.yaml'
encoder_yaml_file = 'encoder_config.yaml'

# Reading YAML data from file
with open(tree_yaml_file, 'r') as f:
    yaml_data = yaml.load(f, Loader=yaml.FullLoader)

@dataclass
class TreeConfig:
    objective: str = yaml_data.get("objective", "binary")
    learning_rate: float = yaml_data.get("learning_rate", 0.1)
    num_leaves: int = yaml_data.get("num_leaves", 31)
    max_depth: int = yaml_data.get("max_depth", -1)
    n_estimators: int = yaml_data.get("n_estimators", 100)
    metric: str = yaml_data.get("metric", "auc")
    random_state: int = yaml_data.get("random_state", 42)
    extra_params: dict = field(default_factory=lambda: yaml_data.get("extra_params", {}))

@dataclass
class BERTEncoderCOnfig:
    pass


tree_config = TreeConfig()
encoder_config = BERTEncoderCOnfig()