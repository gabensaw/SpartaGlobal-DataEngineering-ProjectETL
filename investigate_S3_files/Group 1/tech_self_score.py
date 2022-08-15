import csv

test_data = {"tech_self_score": {"C#": 6, "Java": 5, "R": 2, "JavaScript": 2}}


def create_tech_self_score_list(cand_id, scores):
    row = [cand_id]
    for skill in scores["tech_self_score"].keys():
        row.append(skill)
        row.append(scores["tech_self_score"][skill])
    return row


create_tech_self_score_list(1, test_data)
