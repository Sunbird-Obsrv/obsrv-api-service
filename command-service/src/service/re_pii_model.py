import re
from typing import List

import yaml

from model.data_models import PIIModel, PIIReason, PIIResult


class REPIIModel(PIIModel):
    def __init__(self):
        def join(loader, node):
            seq = loader.construct_sequence(node)
            return "".join([str(i) for i in seq])

        yaml.add_constructor("!join", join)
        with open("config/pii_rules.yml", "r") as f:
            self.pii_rules = yaml.load(f, Loader=yaml.FullLoader)

    def detect_pii(self, entity, field, value) -> List[PIIResult]:
        results = []
        reasons = []
        reasons += self.detect_entity(entity, value)
        reasons += self.detect_entity_in_fieldname(entity, field)
        if len(reasons) != 0:
            score = 1 / len(reasons)
            result: PIIResult = {
                "field": field,
                "type": entity,
                "score": score,
                "reason": reasons,
            }
            results.append(result)
        return results

    def detect_entity(self, entity, value) -> List[PIIReason]:
        matches = []
        for rule in list(self.pii_rules["values"][entity].keys()):
            rule_matches = list(
                re.findall(self.pii_rules["values"][entity][rule]["rule"], value)
            )
            if len(rule_matches) != 0:
                reason: PIIReason = {
                    "code": self.pii_rules["values"][entity][rule]["code"],
                    "resourceKey": self.pii_rules["values"][entity][rule][
                        "resourceKey"
                    ],
                    "region": self.pii_rules["values"][entity][rule]["locale"],
                    "score": 1 / len(rule_matches),
                }
                matches.append(reason)
        return matches

    def detect_entity_in_fieldname(self, entity, value) -> List[PIIReason]:
        matches = []
        rule_matches = list(re.findall(self.pii_rules["keys"][entity]["rule"], value))
        if len(rule_matches) != 0:
            reason: PIIReason = {
                "code": self.pii_rules["keys"][entity]["code"],
                "resourceKey": self.pii_rules["keys"][entity]["resourceKey"],
                "region": self.pii_rules["keys"][entity]["locale"],
                "score": 1 / len(rule_matches),
            }
            matches.append(reason)
        return matches
