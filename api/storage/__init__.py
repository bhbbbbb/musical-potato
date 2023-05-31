import sys
from .validator import Validator

storage = Validator(is_test="pytest" in sys.modules)
